//! The commit contract across crashes: a commit reported failed never reappears
//! after a restart, one reported committed always does, and a deleted fact never
//! comes back. Each test fails one step of a commit or flush, then crashes
//! (`mem::forget` skips every shutdown repair) and reopens.
#![allow(clippy::unwrap_used)]

use super::wal::WalFault;
use super::*;
use crate::value::Tuple;
use tempfile::TempDir;

const SHARD: &str = "kg:a";

fn open(dir: &Path, buffer_size: usize) -> FilePersist {
    FilePersist::new(PersistConfig {
        path: dir.to_path_buf(),
        buffer_size,
        durability_mode: DurabilityMode::Immediate,
        ..PersistConfig::default()
    })
    .unwrap()
}

fn fact(n: i32) -> Tuple {
    Tuple::from_pair(n, n)
}

fn insert(rev: u64, n: i32) -> Transaction {
    let mut txn = Transaction::new(rev);
    txn.insert(SHARD, [fact(n)]);
    txn
}

fn delete(rev: u64, n: i32) -> Transaction {
    let mut txn = Transaction::new(rev);
    txn.delete(SHARD, [fact(n)]);
    txn
}

/// The shard's current facts with their multiplicities.
fn facts(persist: &FilePersist) -> Vec<(Tuple, i64)> {
    let mut updates = persist.read(SHARD, 0).unwrap();
    consolidate_to_current(&mut updates);
    to_tuples_with_multiplicity(&updates)
}

fn crash_and_reopen(persist: FilePersist, dir: &Path) -> FilePersist {
    std::mem::forget(persist);
    open(dir, 1000)
}

fn wal_records(dir: &Path) -> usize {
    fs::read_to_string(dir.join("wal/current.wal")).map_or(0, |wal| wal.lines().count())
}

/// A full buffer flushes inside the commit; ENOSPC there must not report the
/// commit failed, since its WAL record already makes it durable.
#[test]
fn enospc_on_flush_after_commit_keeps_the_commit() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1);
    persist.inject_flush_fault(FlushFault::BatchWrite);

    persist.commit(insert(1, 1)).unwrap();
    assert_eq!(facts(&persist), [(fact(1), 1)]);

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1)]);
}

#[test]
fn flush_retried_after_enospc_keeps_every_commit_once() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1);
    persist.inject_flush_fault(FlushFault::BatchWrite);
    persist.commit(insert(1, 1)).unwrap();

    persist.commit(insert(2, 2)).unwrap();
    assert_eq!(persist.shard_info(SHARD).unwrap().batch_count, 1);
    assert_eq!(wal_records(temp.path()), 0);

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1), (fact(2), 1)]);
}

/// The batch file is written but the metadata pointing at it is not: the
/// updates must stay readable from the buffer, not from the removed file.
#[test]
fn failed_metadata_save_after_commit_keeps_the_commit_readable() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1);
    persist.inject_flush_fault(FlushFault::MetaSave);

    persist.commit(insert(1, 1)).unwrap();
    assert_eq!(facts(&persist), [(fact(1), 1)]);
    assert_eq!(persist.shard_info(SHARD).unwrap().batch_count, 0);

    persist.commit(insert(2, 2)).unwrap();
    assert_eq!(facts(&persist), [(fact(1), 1), (fact(2), 1)]);

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1), (fact(2), 1)]);
}

/// The flush reaches the batch files but its WAL records stay: replay must
/// not apply them a second time, or a later delete leaves the fact behind.
#[test]
fn failed_wal_retirement_never_resurrects_a_deleted_fact() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1000);
    persist.commit(insert(1, 1)).unwrap();
    persist.inject_wal_fault(WalFault::Rewrite);
    assert!(persist.flush(SHARD).is_err());
    assert_eq!(persist.shard_info(SHARD).unwrap().batch_count, 1);

    persist.commit(delete(2, 1)).unwrap();
    assert_eq!(facts(&persist), []);

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), []);
    let reopened = crash_and_reopen(reopened, temp.path());
    assert_eq!(facts(&reopened), []);
}

#[test]
fn replay_retires_records_already_in_batches() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1000);
    persist.commit(insert(1, 1)).unwrap();
    persist.inject_wal_fault(WalFault::Rewrite);
    assert!(persist.flush(SHARD).is_err());
    assert_eq!(wal_records(temp.path()), 1);

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(wal_records(temp.path()), 0);
    assert_eq!(facts(&reopened), [(fact(1), 1)]);
}

/// Replay skips records by revision, so a shard must never take a revision
/// at or below what its batches already hold.
#[test]
fn revision_already_flushed_is_rejected() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1000);
    persist.commit(insert(5, 1)).unwrap();
    persist.flush(SHARD).unwrap();

    assert!(persist.commit(insert(5, 2)).is_err());
    assert!(persist.commit(insert(4, 2)).is_err());
    assert_eq!(wal_records(temp.path()), 0);
    persist.commit(insert(6, 2)).unwrap();

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1), (fact(2), 1)]);
}

/// fsync fails and so does cutting the record back off: a crash before the
/// next write must still not replay it.
#[test]
fn failed_cut_back_then_crash_never_replays_the_failed_commit() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1000);
    persist.commit(insert(1, 1)).unwrap();
    persist.inject_wal_fault(WalFault::Sync);
    persist.inject_wal_fault(WalFault::Restore);

    assert!(persist.commit(insert(2, 2)).is_err());

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1)]);
    reopened.commit(insert(3, 3)).unwrap();
    let reopened = crash_and_reopen(reopened, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1), (fact(3), 1)]);
}

/// The cut can be neither made nor recorded: the commit cannot be reported
/// failed, because a restart may recover it.
#[test]
fn unrecordable_cut_back_reports_an_unknown_outcome() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1000);
    persist.commit(insert(1, 1)).unwrap();
    persist.inject_wal_fault(WalFault::Sync);
    persist.inject_wal_fault(WalFault::Restore);
    persist.inject_wal_fault(WalFault::SaveCut);

    let err = persist.commit(insert(2, 2)).unwrap_err();
    assert!(matches!(err, StorageError::OutcomeUnknown { .. }), "{err}");

    assert!(matches!(
        persist.commit(insert(3, 3)),
        Err(StorageError::StoreReadOnly)
    ));
    assert!(matches!(
        persist.flush(SHARD),
        Err(StorageError::StoreReadOnly)
    ));
    assert!(matches!(
        persist.commit(Transaction::new(4)),
        Err(StorageError::StoreReadOnly)
    ));
    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(facts(&reopened), [(fact(1), 1), (fact(2), 1)]);
    reopened.commit(insert(3, 3)).unwrap();
}

#[test]
fn durability_directory_failure_preserves_flush_memory_and_wal() {
    for directory in ["batches", "shards"] {
        for retry in [false, true] {
            let temp = TempDir::new().unwrap();
            let persist = open(temp.path(), 1000);
            persist.commit(insert(1, 1)).unwrap();
            persist.flush(SHARD).unwrap();
            persist.commit(delete(2, 1)).unwrap();
            let frontier = persist.shards.read()[SHARD].meta.flushed_upper;
            inject_sync_fault(temp.path().join(directory));
            assert!(persist.flush(SHARD).is_err());
            assert_eq!(persist.shards.read()[SHARD].meta.flushed_upper, frontier);
            assert_eq!(persist.shards.read()[SHARD].buffer.len(), 1);
            assert_eq!(wal_records(temp.path()), 1);
            assert!(facts(&persist).is_empty());
            if retry {
                persist.flush(SHARD).unwrap();
            }
            let reopened = crash_and_reopen(persist, temp.path());
            assert!(facts(&reopened).is_empty());
            let reopened = crash_and_reopen(reopened, temp.path());
            assert!(facts(&reopened).is_empty());
        }
    }
}

#[test]
fn durability_startup_barriers_precede_wal_retirement_and_orphan_cleanup() {
    for directory in ["batches", "shards"] {
        let temp = TempDir::new().unwrap();
        let persist = open(temp.path(), 1000);
        persist.commit(insert(1, 1)).unwrap();
        persist.inject_wal_fault(WalFault::Rewrite);
        assert!(persist.flush(SHARD).is_err());
        std::mem::forget(persist);
        let orphan = temp.path().join("batches/orphan.parquet");
        fs::write(&orphan, b"orphan").unwrap();
        let config = PersistConfig {
            path: temp.path().to_path_buf(),
            ..PersistConfig::default()
        };
        for _ in 0..2 {
            inject_sync_fault(temp.path().join(directory));
            assert!(FilePersist::new(config.clone()).is_err());
            assert_eq!(wal_records(temp.path()), 1);
            assert!(orphan.exists());
        }
        let reopened = FilePersist::new(config).unwrap();
        assert_eq!(facts(&reopened), [(fact(1), 1)]);
        assert!(!orphan.exists());
    }
}

#[test]
fn durability_compaction_failure_leaves_memory_readable() {
    for directory in ["batches", "shards"] {
        let temp = TempDir::new().unwrap();
        let persist = open(temp.path(), 1000);
        persist.commit(insert(1, 1)).unwrap();
        persist.flush(SHARD).unwrap();
        inject_sync_fault(temp.path().join(directory));
        assert!(persist.compact(SHARD, 0).is_err());
        assert_eq!(facts(&persist), [(fact(1), 1)]);
        let reopened = crash_and_reopen(persist, temp.path());
        assert_eq!(facts(&reopened), [(fact(1), 1)]);
    }
}

#[test]
fn durability_directory_creation_retries_parent_barriers() {
    for target in ["ancestor", "root", "shards", "batches"] {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("nested/persist");
        let barrier = match target {
            "ancestor" => temp.path().to_path_buf(),
            "root" => root.clone(),
            child => root.join(child),
        };
        let config = PersistConfig {
            path: root.clone(),
            ..PersistConfig::default()
        };
        for _ in 0..2 {
            inject_sync_fault(barrier.clone());
            assert!(FilePersist::new(config.clone()).is_err(), "{target}");
            assert!(root.is_dir());
        }
        let persist = FilePersist::new(config).unwrap();
        persist.commit(insert(1, 1)).unwrap();
        let recovered = crash_and_reopen(persist, &root);
        assert_eq!(facts(&recovered), [(fact(1), 1)]);
    }
    let temp = TempDir::new().unwrap();
    let parent = temp.path().join("persist");
    let wal_dir = parent.join("wal");
    for _ in 0..2 {
        inject_sync_fault(parent.clone());
        assert!(PersistWal::open(wal_dir.clone()).is_err());
        assert!(wal_dir.is_dir());
    }
    let (mut wal, _) = PersistWal::open(wal_dir.clone()).unwrap();
    wal.append(&insert(1, 1), true).unwrap();
    std::mem::forget(wal);
    let (_, recovered) = PersistWal::open(wal_dir).unwrap();
    assert_eq!(recovered, [insert(1, 1)]);
}
