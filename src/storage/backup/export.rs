//! Writing a [`Checkpoint`] out as a backup.
//!
//! The export is a complete, compacted data directory holding exactly the
//! checkpoint's revision: one batch file per relation with every fact at that
//! revision, no WAL, each knowledge graph's rule, schema and index
//! catalogs, and the knowledge graph listing. The manifest is written last and
//! records the revision, so [`verify`](super::verify) and
//! [`restore`](super::restore) handle an export like any other backup, and
//! the engine loads it as it loads any data directory.
//!
//! The destination is claimed ([`Export::claim`]) before the checkpoint is
//! captured, so a refused destination is reported at once.
//!
//! The export shares the machine with the serving engine, so it never uses
//! the query thread pool: files are written sequentially on the calling
//! thread, and synced at the end by a few dedicated threads
//! ([`SEAL_THREADS`]). It checks `cancel` between files; a cancelled or
//! failed export removes what it wrote.

use super::checkpoint::Checkpoint;
use super::destination::{canonical, Destination};
use super::manifest::{FileEntry, Manifest};
use super::offline::{report, Report};
use super::tree::{self, seal_file, sync_dir};
use super::{BackupError, BackupResult};
use crate::index_manager::{IndexManager, RegisteredIndex, INDEX_DEFINITIONS_FILE};
use crate::schema::catalog::SCHEMA_CATALOG_FILE;
use crate::storage::metadata::{KnowledgeGraphInfo, KnowledgeGraphsMetadata};
use crate::storage::persist::ExportWriter;
use crate::storage::StorageError;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Longest accepted checkpoint name.
pub const MAX_NAME_LEN: usize = 128;

/// Threads that sync files at the end of an export. Syncing many small files
/// is bound by fsync latency, and concurrent fsyncs share journal commits;
/// the count stays small and fixed so the export never competes with
/// queries for more than a few cores.
const SEAL_THREADS: usize = 4;

/// A destination claimed for an export: a new or empty directory outside
/// the data directory being exported.
#[derive(Debug)]
pub struct Export {
    destination: Destination,
    source: PathBuf,
}

impl Export {
    /// Claim `dest` for an export of the engine at `data_dir`, before
    /// anything is captured, so a refused destination costs nothing.
    ///
    /// # Errors
    /// `dest` is inside `data_dir`, or exists and is not an empty directory.
    pub fn claim(dest: &Path, data_dir: &Path) -> BackupResult<Self> {
        let source = canonical(data_dir)?;
        let destination = Destination::claim(dest, &source)?;
        Ok(Self {
            destination,
            source,
        })
    }

    /// The directory being written.
    pub fn dir(&self) -> &Path {
        self.destination.path()
    }

    /// Write `checkpoint` (taken from the claimed data directory) and
    /// return what was written.
    ///
    /// # Errors
    /// A write fails, or `cancel` is set before the export finishes
    /// ([`BackupError::Cancelled`]). Either way the destination is returned
    /// to how it was found.
    pub fn write(self, checkpoint: &Checkpoint, cancel: &AtomicBool) -> BackupResult<Report> {
        let start = Instant::now();
        let source = self.source;
        let (dir, manifest) = self.destination.fill(|dir| {
            write_data_dir(checkpoint, dir, cancel)?;
            seal(checkpoint, &source, dir, cancel)
        })?;
        Ok(report(dir, &manifest, start))
    }

    /// Give the destination back unwritten.
    ///
    /// # Errors
    /// Removing what the claim created failed.
    pub fn abandon(self) -> BackupResult<()> {
        let dir = self.dir().to_path_buf();
        self.destination
            .release()
            .map_err(|e| BackupError::io(&dir, e))
    }
}

/// The directory name for an export: `name` when it is a plain name, or one
/// derived from the current UTC time.
///
/// # Errors
/// [`BackupError::InvalidName`] unless `name` is 1 to `MAX_NAME_LEN` (128) ASCII
/// letters, digits, `-`, `_` or `.`, not starting with `.`; so it can never
/// name a parent, a hidden file or a path.
pub fn checkpoint_name(name: Option<&str>) -> BackupResult<String> {
    let Some(name) = name else {
        return Ok(chrono::Utc::now()
            .format("checkpoint-%Y%m%dT%H%M%S%.3fZ")
            .to_string());
    };
    let plain = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !plain || name.is_empty() || name.starts_with('.') || name.len() > MAX_NAME_LEN {
        return Err(BackupError::InvalidName(name.to_string()));
    }
    Ok(name.to_string())
}

/// Write the data directory layout the engine loads at startup.
fn write_data_dir(checkpoint: &Checkpoint, dir: &Path, cancel: &AtomicBool) -> BackupResult<()> {
    let exported_at = chrono::Utc::now().to_rfc3339();
    KnowledgeGraphsMetadata {
        version: "1.0".to_string(),
        knowledge_graphs: checkpoint
            .knowledge_graphs
            .iter()
            .map(|kg| KnowledgeGraphInfo {
                name: kg.name.clone(),
                created_at: kg.created_at.clone(),
                last_accessed: exported_at.clone(),
                relations_count: kg.relations.len(),
                total_tuples: kg.tuple_count(),
            })
            .collect(),
    }
    .save(&dir.join("metadata").join("knowledge_graphs.json"))?;

    let mut persist = ExportWriter::create(&dir.join("persist"))?;
    for kg in &checkpoint.knowledge_graphs {
        let kg_dir = dir.join(&kg.name);
        let relations_dir = kg_dir.join("relations");
        fs::create_dir_all(&relations_dir).map_err(|e| BackupError::io(&relations_dir, e))?;
        for (relation, tuples) in &kg.relations {
            check(cancel)?;
            let shard = format!("{}:{relation}", kg.name);
            persist.write_shard(&shard, checkpoint.revision, tuples.iter())?;
        }
        if !kg.rules.is_empty() {
            kg.rules.save_copy_to(&kg_dir).map_err(engine_file)?;
        }
        if kg.schemas.persistent_len() > 0 {
            kg.schemas
                .save(&kg_dir.join(SCHEMA_CATALOG_FILE))
                .map_err(|e| engine_file(e.to_string()))?;
        }
        if !kg.indexes.is_empty() {
            let definitions: Vec<&RegisteredIndex> = kg.indexes.iter().collect();
            IndexManager::write_definitions(&kg_dir.join(INDEX_DEFINITIONS_FILE), &definitions)
                .map_err(engine_file)?;
        }
    }
    Ok(())
}

/// Make every file and directory durable, hash every file, and write the
/// manifest last: its appearance marks the export complete.
fn seal(
    checkpoint: &Checkpoint,
    source: &Path,
    dir: &Path,
    cancel: &AtomicBool,
) -> BackupResult<Manifest> {
    let tree = tree::walk(dir, &[])?;
    let paths = tree::manifest_paths(&tree.files)?;
    let per_thread = tree.files.len().div_ceil(SEAL_THREADS).max(1);
    let sealed: Vec<BackupResult<Vec<(u64, String)>>> = std::thread::scope(|scope| {
        let workers: Vec<_> = tree
            .files
            .chunks(per_thread)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(|file| {
                            check(cancel)?;
                            seal_file(&dir.join(file))
                        })
                        .collect()
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|w| {
                w.join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
            .collect()
    });
    let mut digests = Vec::with_capacity(paths.len());
    for chunk in sealed {
        digests.extend(chunk?);
    }
    let files = paths
        .into_iter()
        .zip(digests)
        .map(|(path, (size, sha256))| FileEntry { path, size, sha256 })
        .collect();
    for directory in &tree.directories {
        sync_dir(&dir.join(directory))?;
    }
    sync_dir(dir)?;

    let manifest = Manifest::new(
        source.to_path_buf(),
        tree::manifest_paths(&tree.directories)?,
        files,
    )
    .at_revision(checkpoint.revision);
    manifest.store(dir)?;
    sync_dir(dir)?;
    Ok(manifest)
}

fn check(cancel: &AtomicBool) -> BackupResult<()> {
    if cancel.load(Ordering::Relaxed) {
        return Err(BackupError::Cancelled);
    }
    Ok(())
}

fn engine_file(message: String) -> BackupError {
    BackupError::Storage(StorageError::Other(message))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn plain_names_are_kept_and_a_default_is_derived_from_the_time() {
        assert_eq!(
            checkpoint_name(Some("nightly-2026.10.03_a")).unwrap(),
            "nightly-2026.10.03_a"
        );
        let default = checkpoint_name(None).unwrap();
        assert!(
            default.starts_with("checkpoint-") && default.ends_with('Z'),
            "{default}"
        );
        assert_eq!(checkpoint_name(Some(&default)).unwrap(), default);
    }

    #[test]
    fn names_that_could_leave_the_backup_directory_are_rejected() {
        let too_long = "a".repeat(MAX_NAME_LEN + 1);
        for bad in [
            "", ".", "..", ".hidden", "a/b", "../x", "/abs", "a b", "a\\b", &too_long,
        ] {
            assert!(
                matches!(checkpoint_name(Some(bad)), Err(BackupError::InvalidName(_))),
                "{bad:?}"
            );
        }
    }
}
