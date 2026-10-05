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
    for target in ["parent", "root", "shards", "batches"] {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("nested/persist");
        let barrier = match target {
            "parent" => temp.path().join("nested"),
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

/// A start that failed at the root parent's fsync left the root behind; the
/// restart must still make that entry durable before accepting a commit.
#[test]
fn durability_restart_after_failed_parent_sync_syncs_it_again() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("persist");
    let config = PersistConfig {
        path: root.clone(),
        ..PersistConfig::default()
    };
    inject_sync_fault(temp.path().to_path_buf());
    assert!(FilePersist::new(config.clone()).is_err());
    assert!(root.is_dir());

    inject_sync_fault(temp.path().to_path_buf());
    assert!(FilePersist::new(config.clone()).is_err());

    let persist = FilePersist::new(config).unwrap();
    persist.commit(insert(1, 1)).unwrap();
    let recovered = crash_and_reopen(persist, &root);
    assert_eq!(facts(&recovered), [(fact(1), 1)]);
}

/// Only the root, its subdirectories and its parent are synced, so a root
/// opens under a higher ancestor this process cannot read.
#[cfg(unix)]
#[test]
fn durability_existing_root_opens_under_unreadable_ancestor() {
    use std::os::unix::fs::PermissionsExt;
    let temp = TempDir::new().unwrap();
    let locked = temp.path().join("locked");
    let root = locked.join("data/persist");
    fs::create_dir_all(&root).unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o111)).unwrap();
    let opened = FilePersist::new(PersistConfig {
        path: root.clone(),
        ..PersistConfig::default()
    });
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
    let persist = opened.unwrap();
    persist.commit(insert(1, 1)).unwrap();
    let recovered = crash_and_reopen(persist, &root);
    assert_eq!(facts(&recovered), [(fact(1), 1)]);
}

/// Facts of `shard`, consolidated, without multiplicities.
fn shard_facts(persist: &FilePersist, shard: &str) -> Vec<Tuple> {
    let mut updates = persist.read(shard, 0).unwrap();
    consolidate_to_current(&mut updates);
    let mut tuples = to_tuples(&updates);
    tuples.sort();
    tuples
}

/// After a crash with many shards in the WAL, startup flushes them all and
/// rewrites the WAL once, not once per shard, and a second crash replays
/// nothing twice.
#[test]
fn startup_drain_of_many_shards_rewrites_the_wal_once() {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path(), 1000);
    let shards: Vec<String> = (0..40).map(|i| format!("kg:r{i}")).collect();
    for (i, shard) in shards.iter().enumerate() {
        let mut txn = Transaction::new(i as u64 + 1);
        txn.insert(shard.clone(), [fact(i as i32), fact(-1)]);
        persist.commit(txn).unwrap();
    }
    let mut txn = Transaction::new(100);
    txn.delete(shards[0].clone(), [fact(-1)]);
    persist.commit(txn).unwrap();
    assert_eq!(wal_records(temp.path()), 41);

    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(reopened.wal.lock().rewrites, 1);
    assert_eq!(wal_records(temp.path()), 0);
    let expected = |i: usize| {
        let mut facts = vec![fact(i as i32)];
        if i > 0 {
            facts.push(fact(-1));
        }
        facts.sort();
        facts
    };
    for (i, shard) in shards.iter().enumerate() {
        assert_eq!(shard_facts(&reopened, shard), expected(i), "{shard}");
    }

    let reopened = crash_and_reopen(reopened, temp.path());
    for (i, shard) in shards.iter().enumerate() {
        assert_eq!(shard_facts(&reopened, shard), expected(i), "{shard}");
    }
}

/// The WAL size limit flushes every dirty shard with one WAL rewrite.
#[test]
fn wal_size_limit_flushes_every_dirty_shard_with_one_rewrite() {
    let temp = TempDir::new().unwrap();
    let persist = FilePersist::new(PersistConfig {
        path: temp.path().to_path_buf(),
        buffer_size: 1000,
        durability_mode: DurabilityMode::Immediate,
        max_wal_size_bytes: 1,
    })
    .unwrap();
    for i in 0..8 {
        let mut txn = Transaction::new(i + 1);
        // The last commit carries every shard over the limit at once.
        for shard in 0..=(i / 7) * 7 {
            txn.insert(format!("kg:s{shard}"), [fact(i as i32)]);
        }
        persist.commit(txn).unwrap();
    }
    assert_eq!(wal_records(temp.path()), 0);
    let rewrites = persist.wal.lock().rewrites;
    assert_eq!(rewrites, 8, "one rewrite per commit over the limit");
    let reopened = crash_and_reopen(persist, temp.path());
    assert_eq!(
        shard_facts(&reopened, "kg:s0"),
        (0..8).map(fact).collect::<Vec<_>>()
    );
    for shard in 1..=7 {
        assert_eq!(shard_facts(&reopened, &format!("kg:s{shard}")), [fact(7)]);
    }
}

/// A read takes no lock while it reads batch files: a compaction that removes
/// them meanwhile makes it start over, and it still sees every fact.
#[test]
fn read_racing_compaction_sees_every_fact() {
    let temp = TempDir::new().unwrap();
    let persist = Arc::new(open(temp.path(), 1000));
    let expected: Vec<Tuple> = (0..200).map(fact).collect();
    for (i, tuple) in expected.iter().enumerate() {
        let mut txn = Transaction::new(i as u64 + 1);
        txn.insert(SHARD, [tuple.clone()]);
        persist.commit(txn).unwrap();
        persist.flush(SHARD).unwrap();
    }
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let compactor = {
        let persist = Arc::clone(&persist);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut rounds = 0;
            while !done.load(Ordering::Relaxed) {
                persist.compact(SHARD, 0).unwrap();
                rounds += 1;
            }
            rounds
        })
    };
    for _ in 0..200 {
        assert_eq!(shard_facts(&persist, SHARD), expected);
    }
    done.store(true, Ordering::Relaxed);
    assert!(compactor.join().unwrap() > 0);
}
