//! A failed commit returns an error and is invisible: to reads now, and to replay
//! after a crash. A successful commit after it is unaffected.
#![allow(clippy::unwrap_used)]

use super::wal::WalFault;
use super::*;
use crate::value::Tuple;
use tempfile::TempDir;

fn open(dir: &Path) -> FilePersist {
    FilePersist::new(PersistConfig {
        path: dir.to_path_buf(),
        buffer_size: 1000,
        durability_mode: DurabilityMode::Immediate,
        ..PersistConfig::default()
    })
    .unwrap()
}

fn txn(rev: u64, shards: &[&str]) -> Transaction {
    let mut txn = Transaction::new(rev);
    for shard in shards {
        txn.insert(*shard, [Tuple::from_pair(rev as i32, rev as i32)]);
    }
    txn
}

fn revisions(persist: &FilePersist, shard: &str) -> Vec<u64> {
    let mut times: Vec<u64> = persist
        .read(shard, 0)
        .map(|updates| updates.iter().map(|u| u.time).collect())
        .unwrap_or_default();
    times.sort_unstable();
    times
}

/// Commit 1, fail commit 2 at `faults`, commit 3; check live state and replay agree.
fn failed_commit_is_invisible(faults: &[WalFault]) {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path());
    persist.commit(txn(1, &["kg:a", "kg:b"])).unwrap();

    for fault in faults {
        persist.inject_wal_fault(*fault);
    }
    assert!(persist.commit(txn(2, &["kg:a", "kg:b"])).is_err());
    assert_eq!(revisions(&persist, "kg:a"), [1]);
    assert_eq!(revisions(&persist, "kg:b"), [1]);

    persist.commit(txn(3, &["kg:b"])).unwrap();
    assert_eq!(revisions(&persist, "kg:a"), [1]);
    assert_eq!(revisions(&persist, "kg:b"), [1, 3]);
    std::mem::forget(persist);

    let reopened = open(temp.path());
    assert_eq!(revisions(&reopened, "kg:a"), [1]);
    assert_eq!(revisions(&reopened, "kg:b"), [1, 3]);
}

#[test]
fn failed_write_is_invisible() {
    failed_commit_is_invisible(&[WalFault::Write]);
}

#[test]
fn failed_sync_is_invisible() {
    failed_commit_is_invisible(&[WalFault::Sync]);
}

#[test]
fn failed_sync_with_failed_cut_back_is_invisible() {
    failed_commit_is_invisible(&[WalFault::Sync, WalFault::Restore]);
}

#[test]
fn failed_cut_back_is_repaired_before_a_flush_rewrites_the_wal() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path());
    persist.commit(txn(1, &["kg:a", "kg:b"])).unwrap();
    persist.inject_wal_fault(WalFault::Sync);
    persist.inject_wal_fault(WalFault::Restore);
    assert!(persist.commit(txn(2, &["kg:a", "kg:b"])).is_err());

    persist.flush("kg:a").unwrap();
    std::mem::forget(persist);

    let reopened = open(temp.path());
    assert_eq!(revisions(&reopened, "kg:a"), [1]);
    assert_eq!(revisions(&reopened, "kg:b"), [1]);
}

#[test]
fn transaction_is_one_wal_record() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path());
    persist.commit(txn(1, &["kg:a", "kg:b", "kg:c"])).unwrap();
    let wal = fs::read_to_string(temp.path().join("wal/current.wal")).unwrap();
    assert_eq!(wal.lines().count(), 1);
}

#[test]
fn full_buffers_flush_every_shard_of_the_transaction() {
    let temp = TempDir::new().unwrap();
    let persist = FilePersist::new(PersistConfig {
        path: temp.path().to_path_buf(),
        buffer_size: 1,
        durability_mode: DurabilityMode::Immediate,
        ..PersistConfig::default()
    })
    .unwrap();
    persist.commit(txn(1, &["kg:a", "kg:b"])).unwrap();
    assert_eq!(persist.wal.lock().file_size(), 0);
    for shard in ["kg:a", "kg:b"] {
        assert_eq!(persist.shard_info(shard).unwrap().batch_count, 1);
    }
}
