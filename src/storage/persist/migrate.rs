//! One-shot migration of v1 persist data to v2.
//!
//! v1 named shard metadata after a lossy sanitized name, so `a:b_c` and `a_b:c` shared one
//! file, and stored batches as typed columns inferred from the first row. Migration rewrites
//! each v1 batch in place as v2, saves the metadata under its v2 name, then removes the v1
//! file. Each step is atomic and re-runnable, so a crash mid-migration resumes on restart.
//! Batch files no shard references are moved to `batches/quarantine/`, not deleted: a v1
//! name collision may have orphaned another shard's data.

use super::batch::SHARD_META_VERSION;
use super::{
    read_shard_meta, read_updates_parquet, rebase_batch_paths, shard_meta_path, sync_directory,
    write_shard_meta, write_updates_parquet, ShardMeta,
};
use crate::storage::{StorageError, StorageResult};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Directory under `batches/` holding unreferenced batch files found during migration.
const QUARANTINE_DIR: &str = "quarantine";

fn meta_files(shards_dir: &Path) -> StorageResult<Vec<(PathBuf, ShardMeta)>> {
    let mut metas = Vec::new();
    for entry in fs::read_dir(shards_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let meta = read_shard_meta(&path)?;
            metas.push((path, meta));
        }
    }
    Ok(metas)
}

/// Migrate every v1 shard under `root` to v2. A no-op when none remain.
pub(super) fn migrate_v1(root: &Path) -> StorageResult<()> {
    let shards_dir = root.join("shards");
    let batches_dir = root.join("batches");

    let mut migrated: HashSet<String> = HashSet::new();
    let mut v1 = Vec::new();
    for (path, meta) in meta_files(&shards_dir)? {
        if meta.version < SHARD_META_VERSION {
            v1.push((path, meta));
        } else if path == shard_meta_path(&shards_dir, &meta.name) {
            migrated.insert(meta.name);
        }
    }
    if v1.is_empty() {
        return Ok(());
    }

    let mut seen = HashSet::new();
    for (path, meta) in &v1 {
        if !seen.insert(meta.name.as_str()) {
            return Err(StorageError::Other(format!(
                "v1 shard '{}' appears twice (second copy '{}'); refusing to migrate",
                meta.name,
                path.display()
            )));
        }
    }

    tracing::info!(shards = v1.len(), "persist_migrate_v1_start");
    for (path, mut meta) in v1 {
        let target = shard_meta_path(&shards_dir, &meta.name);
        if !migrated.contains(&meta.name) {
            rebase_batch_paths(&mut meta, &batches_dir);
            meta.batches.retain(|b| {
                let exists = b.path.exists();
                if !exists {
                    tracing::warn!(
                        shard = %meta.name,
                        path = %b.path.display(),
                        "v1 batch file missing - dropping reference"
                    );
                }
                exists
            });
            for batch_ref in &meta.batches {
                let updates = read_updates_parquet(&batch_ref.path)?;
                write_updates_parquet(&batch_ref.path, &updates)?;
            }
            meta.total_updates = meta.batches.iter().map(|b| b.len).sum();
            meta.version = SHARD_META_VERSION;
            write_shard_meta(&shards_dir, &meta)?;
        }
        if path != target {
            fs::remove_file(&path)?;
        }
    }
    sync_directory(&batches_dir);
    sync_directory(&shards_dir);

    quarantine_orphans(&shards_dir, &batches_dir)?;
    tracing::info!("persist_migrate_v1_done");
    Ok(())
}

fn quarantine_orphans(shards_dir: &Path, batches_dir: &Path) -> StorageResult<()> {
    let mut referenced = HashSet::new();
    for (_, mut meta) in meta_files(shards_dir)? {
        rebase_batch_paths(&mut meta, batches_dir);
        referenced.extend(meta.batches.into_iter().map(|b| b.path));
    }

    let quarantine = batches_dir.join(QUARANTINE_DIR);
    let mut moved = 0usize;
    for entry in fs::read_dir(batches_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("parquet")
            || referenced.contains(&path)
        {
            continue;
        }
        let Some(file) = path.file_name() else {
            continue;
        };
        fs::create_dir_all(&quarantine)?;
        fs::rename(&path, quarantine.join(file))?;
        moved += 1;
    }
    if moved > 0 {
        tracing::warn!(
            files = moved,
            dir = %quarantine.display(),
            "v1 migration quarantined unreferenced batch files"
        );
        sync_directory(&quarantine);
        sync_directory(batches_dir);
    }
    Ok(())
}
