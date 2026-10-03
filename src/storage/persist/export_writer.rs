//! Writing a new persist directory in one pass, for a checkpoint export.
//!
//! Each shard becomes one batch file with all its facts at one revision, and
//! its metadata file; there is no WAL, so the engine starts with an empty one.
//! Unlike [`FilePersist`](super::FilePersist), nothing is made durable as it
//! is written: the export syncs every file once before it writes the manifest
//! that makes it valid, so per-file syncs would only multiply the work.

use super::batch::{Batch, BatchRef, ShardMeta, Update};
use super::{encode_updates_parquet, shard_meta_json, shard_meta_path};
use crate::storage::StorageResult;
use crate::value::Tuple;
use std::fs;
use std::path::{Path, PathBuf};

/// Writes shards into a new persist directory.
#[derive(Debug)]
pub struct ExportWriter {
    shards_dir: PathBuf,
    batches_dir: PathBuf,
    next_batch_id: u64,
}

impl ExportWriter {
    /// Start a persist directory at `path`, which must not hold one.
    ///
    /// # Errors
    /// Creating the directories failed.
    pub fn create(path: &Path) -> StorageResult<Self> {
        let writer = Self {
            shards_dir: path.join("shards"),
            batches_dir: path.join("batches"),
            next_batch_id: 1,
        };
        fs::create_dir_all(&writer.shards_dir)?;
        fs::create_dir_all(&writer.batches_dir)?;
        Ok(writer)
    }

    /// Write `shard` holding exactly `tuples`, each inserted at `revision`.
    /// A shard without tuples is not written.
    ///
    /// # Errors
    /// Writing the batch or metadata file failed.
    pub fn write_shard<'a>(
        &mut self,
        shard: &str,
        revision: u64,
        tuples: impl IntoIterator<Item = &'a Tuple>,
    ) -> StorageResult<()> {
        let updates: Vec<Update> = tuples
            .into_iter()
            .map(|tuple| Update {
                data: tuple.clone(),
                time: revision,
                diff: 1,
            })
            .collect();
        if updates.is_empty() {
            return Ok(());
        }
        let id = self.next_batch_id.to_string();
        self.next_batch_id += 1;
        let path = self.batches_dir.join(format!("{id}.parquet"));
        encode_updates_parquet(fs::File::create(&path)?, &updates)?;

        let batch = Batch::new(updates);
        let mut meta = ShardMeta::new(shard.to_string());
        meta.add_batch(BatchRef {
            id,
            path,
            lower: batch.lower,
            upper: batch.upper,
            len: batch.len(),
        });
        fs::write(
            shard_meta_path(&self.shards_dir, shard),
            shard_meta_json(&meta)?,
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::DurabilityMode;
    use crate::storage::persist::{FilePersist, PersistBackend, PersistConfig};
    use crate::value::Value;

    #[test]
    fn written_shards_load_as_their_tuples_at_the_revision() {
        let tmp = tempfile::tempdir().unwrap();
        let tuples: Vec<Tuple> = (0..5).map(|i| Tuple::new(vec![Value::Int64(i)])).collect();
        let mut writer = ExportWriter::create(tmp.path()).unwrap();
        writer.write_shard("kg:r", 41, &tuples).unwrap();
        writer.write_shard("kg:empty", 41, &[]).unwrap();

        let persist = FilePersist::new(PersistConfig {
            path: tmp.path().to_path_buf(),
            durability_mode: DurabilityMode::Immediate,
            ..PersistConfig::default()
        })
        .unwrap();

        assert_eq!(persist.list_shards().unwrap(), ["kg:r"]);
        let info = persist.shard_info("kg:r").unwrap();
        assert_eq!(info.upper, 42);
        let read = persist.read("kg:r", 0).unwrap();
        assert_eq!(
            read.iter().map(|u| u.data.clone()).collect::<Vec<_>>(),
            tuples
        );
        assert!(read.iter().all(|u| u.time == 41 && u.diff == 1));
    }
}
