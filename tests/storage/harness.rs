//! Helpers the storage modules share.

use inputlayer::storage::persist::batch::Update;
use inputlayer::storage::persist::{FilePersist, PersistBackend, Transaction};
use inputlayer::storage::StorageResult;
use std::path::{Path, PathBuf};

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
