//! WAL crash consistency: acknowledged writes survive a crash (`mem::forget`, no drop)
//! under concurrent flushes, torn tails, and invalid UTF-8.

use inputlayer::config::DurabilityMode;
use inputlayer::storage::persist::batch::Update;
use inputlayer::storage::persist::{FilePersist, PersistBackend, PersistConfig, Transaction};
use inputlayer::storage::StorageResult;
use inputlayer::value::Tuple;
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

/// Commit `updates` to `shard`, one transaction per run of equal times.
fn commit(persist: &FilePersist, shard: &str, updates: &[Update]) -> StorageResult<()> {
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

fn open(path: PathBuf, buffer_size: usize, durability_mode: DurabilityMode) -> FilePersist {
    FilePersist::new(PersistConfig {
        path,
        buffer_size,
        durability_mode,
        ..Default::default()
    })
    .expect("open persist")
}

fn keys(persist: &FilePersist, shard: &str) -> BTreeSet<(i64, i64)> {
    let updates = match persist.read(shard, 0) {
        Ok(updates) => updates,
        // A delete that lands after the writer's last append leaves no shard.
        Err(e) if e.to_string().contains("Shard not found") => Vec::new(),
        Err(e) => panic!("read shard: {e:?}"),
    };
    updates
        .into_iter()
        .map(|u| {
            let v = u.data.values();
            (v[0].as_i64().expect("int"), v[1].as_i64().expect("int"))
        })
        .collect()
}

fn wal_file(dir: &Path) -> PathBuf {
    dir.join("wal").join("current.wal")
}

fn append_raw(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().expect("parent")).expect("create wal dir");
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open wal")
        .write_all(bytes)
        .expect("write wal");
}

#[test]
fn concurrent_appends_and_flushes_lose_no_acked_write() {
    const THREADS: i32 = 8;
    const PER_THREAD: i32 = 200;

    let temp = TempDir::new().expect("tempdir");
    let persist = Arc::new(open(
        temp.path().to_path_buf(),
        7,
        DurabilityMode::Immediate,
    ));
    persist.ensure_shard("db:r").expect("ensure shard");

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let persist = Arc::clone(&persist);
            std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let update = Update::insert(Tuple::from_pair(t, i), 1);
                    commit(&persist, "db:r", &[update]).expect("append");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("writer thread");
    }

    let persist = Arc::try_unwrap(persist).ok().expect("sole owner");
    std::mem::forget(persist);

    let reopened = open(temp.path().to_path_buf(), 7, DurabilityMode::Immediate);
    assert_eq!(
        keys(&reopened, "db:r").len(),
        (THREADS * PER_THREAD) as usize
    );
}

#[test]
fn concurrent_append_and_delete_recover_what_was_acked() {
    const ROUNDS: i32 = 10;
    const BATCHES: i32 = 40;
    const WRITES: i32 = 40;

    for round in 0..ROUNDS {
        let temp = TempDir::new().expect("tempdir");
        let persist = Arc::new(open(
            temp.path().to_path_buf(),
            1000,
            DurabilityMode::Immediate,
        ));
        // Batch files give delete_shard work to do while a writer races it.
        for i in 0..BATCHES {
            let update = Update::insert(Tuple::from_pair(-1, i), 1);
            commit(&persist, "db:r", &[update]).expect("append");
            persist.flush("db:r").expect("flush");
        }
        let writer = {
            let persist = Arc::clone(&persist);
            std::thread::spawn(move || {
                for i in 0..WRITES {
                    let update = Update::insert(Tuple::from_pair(round, i), 1);
                    commit(&persist, "db:r", &[update]).expect("append");
                }
            })
        };
        while persist.read("db:r", 0).expect("read").len() <= (BATCHES + round) as usize {
            std::thread::yield_now();
        }
        persist.delete_shard("db:r").expect("delete shard");
        writer.join().expect("writer thread");

        let persist = Arc::try_unwrap(persist).ok().expect("sole owner");
        let acked = keys(&persist, "db:r");
        std::mem::forget(persist);

        let reopened = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
        assert_eq!(keys(&reopened, "db:r"), acked, "round {round}");
    }
}

#[test]
fn append_after_torn_tail_survives_restart() {
    let temp = TempDir::new().expect("tempdir");
    append_raw(
        &wal_file(temp.path()),
        b"0badc0de:{\"shard\":\"db:r\",\"upd",
    );

    let persist = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    commit(
        &persist,
        "db:r",
        &[Update::insert(Tuple::from_pair(1, 2), 1)],
    )
    .expect("append");
    std::mem::forget(persist);

    let reopened = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    assert_eq!(keys(&reopened, "db:r"), BTreeSet::from([(1, 2)]));
}

#[test]
fn torn_tail_after_valid_records_is_truncated() {
    let temp = TempDir::new().expect("tempdir");
    let persist = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    commit(
        &persist,
        "db:r",
        &[Update::insert(Tuple::from_pair(1, 1), 1)],
    )
    .expect("append");
    std::mem::forget(persist);

    append_raw(&wal_file(temp.path()), b"deadbeef:{\"sha");

    let persist = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    commit(
        &persist,
        "db:r",
        &[Update::insert(Tuple::from_pair(2, 2), 1)],
    )
    .expect("append");
    std::mem::forget(persist);

    let reopened = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    assert_eq!(keys(&reopened, "db:r"), BTreeSet::from([(1, 1), (2, 2)]));
}

#[test]
fn invalid_utf8_tail_does_not_block_startup() {
    let temp = TempDir::new().expect("tempdir");
    let persist = open(temp.path().to_path_buf(), 1000, DurabilityMode::Batched);
    commit(
        &persist,
        "db:r",
        &[Update::insert(Tuple::from_pair(1, 1), 1)],
    )
    .expect("append");
    persist.sync().expect("sync");
    std::mem::forget(persist);

    // A torn multi-byte sequence: the first two bytes of a three-byte '€'.
    append_raw(&wal_file(temp.path()), b"00000000:{\"shard\":\"\xE2\x82");

    let reopened = open(temp.path().to_path_buf(), 1000, DurabilityMode::Batched);
    assert_eq!(keys(&reopened, "db:r"), BTreeSet::from([(1, 1)]));
}

#[test]
fn invalid_utf8_middle_record_is_skipped() {
    let temp = TempDir::new().expect("tempdir");
    let wal = wal_file(temp.path());
    append_raw(&wal, b"\xFF\xFE garbage\n");
    let persist = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    commit(
        &persist,
        "db:r",
        &[Update::insert(Tuple::from_pair(3, 3), 1)],
    )
    .expect("append");
    std::mem::forget(persist);

    let reopened = open(temp.path().to_path_buf(), 1000, DurabilityMode::Immediate);
    assert_eq!(keys(&reopened, "db:r"), BTreeSet::from([(3, 3)]));
}

#[test]
fn batched_flush_keeps_other_shards_buffered_wal_entries() {
    let temp = TempDir::new().expect("tempdir");
    let persist = open(temp.path().to_path_buf(), 1000, DurabilityMode::Batched);
    commit(
        &persist,
        "db:a",
        &[Update::insert(Tuple::from_pair(1, 1), 1)],
    )
    .expect("append a");
    commit(
        &persist,
        "db:b",
        &[Update::insert(Tuple::from_pair(2, 2), 1)],
    )
    .expect("append b");
    persist.flush("db:b").expect("flush b");
    std::mem::forget(persist);

    let reopened = open(temp.path().to_path_buf(), 1000, DurabilityMode::Batched);
    assert_eq!(keys(&reopened, "db:a"), BTreeSet::from([(1, 1)]));
    assert_eq!(keys(&reopened, "db:b"), BTreeSet::from([(2, 2)]));
}
