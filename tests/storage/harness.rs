//! Helpers the storage modules share.

use inputlayer::storage::persist::batch::Update;
use inputlayer::storage::persist::{FilePersist, PersistBackend, Transaction};
use inputlayer::storage::StorageResult;
use inputlayer::StorageEngine;
use std::path::{Path, PathBuf};

/// Size the process's thread pool to the 2 threads the storage modules run
/// their engines on, before building an engine: the first engine would
/// otherwise size the pool for every module, from its own `num_threads`.
pub fn pool() {
    static POOL: std::sync::Once = std::sync::Once::new();
    POOL.call_once(|| StorageEngine::set_num_threads(2).expect("size the thread pool"));
}

/// Commit `updates` to `shard`, one transaction per run of equal times.
pub fn commit(persist: &FilePersist, shard: &str, updates: &[Update]) -> StorageResult<()> {
    for run in updates.chunk_by(|a, b| a.time == b.time) {
        let mut txn = Transaction::new(run[0].time);
        txn.facts(
            shard,
            run.iter().map(|u| (u.data.clone(), u.diff)).collect(),
        );
        persist.commit(txn)?;
    }
    Ok(())
}

/// The live WAL file of the persist directory `dir`.
pub fn wal_file(dir: &Path) -> PathBuf {
    dir.join("wal").join("current.wal")
}
