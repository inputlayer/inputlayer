//! Transactions recover atomically: after the WAL is cut or damaged anywhere, reopening
//! yields the state after an exact number of committed transactions, never a subset of
//! one.
#![allow(clippy::unwrap_used)]

use inputlayer::config::DurabilityMode;
use inputlayer::storage::persist::{
    consolidate_to_current, to_tuples, FilePersist, PersistBackend, PersistConfig, Transaction,
};
use inputlayer::value::Tuple;
use inputlayer::{Config, StorageEngine};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

type State = BTreeMap<String, BTreeSet<Tuple>>;

const SHARDS: [&str; 3] = ["kg:a", "kg:b", "kg:c"];

fn open(path: &Path) -> FilePersist {
    FilePersist::new(PersistConfig {
        path: path.to_path_buf(),
        buffer_size: 10_000,
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
    })
    .unwrap()
}

fn wal_file(dir: &Path) -> PathBuf {
    dir.join("wal").join("current.wal")
}

fn state(persist: &FilePersist) -> State {
    SHARDS
        .iter()
        .filter_map(|shard| {
            let mut updates = persist.read(shard, 0).ok()?;
            consolidate_to_current(&mut updates);
            let tuples: BTreeSet<Tuple> = to_tuples(&updates).into_iter().collect();
            (!tuples.is_empty()).then(|| ((*shard).to_string(), tuples))
        })
        .collect()
}

/// Transactions that each insert into several shards and delete earlier facts, so
/// replaying part of one is visible as a state no commit sequence reached.
fn transactions() -> Vec<Transaction> {
    (1..=5u64)
        .map(|rev| {
            let n = rev as i32;
            let mut txn = Transaction::new(rev);
            for (i, shard) in SHARDS.iter().enumerate().take(1 + (n as usize % 3)) {
                txn.insert(*shard, (0..n).map(|k| Tuple::from_pair(n, k + i as i32)));
            }
            if n > 1 {
                txn.delete("kg:a", [Tuple::from_pair(n - 1, 0)]);
            }
            txn
        })
        .collect()
}

/// The state after each prefix of `txns`: `states[k]` follows the first `k`.
fn states_after_each_commit(txns: &[Transaction]) -> Vec<State> {
    let mut states = vec![State::new()];
    for txn in txns {
        let mut next = states.last().unwrap().clone();
        for (shard, updates) in txn.clone().split().0 {
            let set = next.entry(shard).or_default();
            for u in updates {
                if u.diff > 0 {
                    set.insert(u.data);
                } else {
                    set.remove(&u.data);
                }
            }
        }
        next.retain(|_, set| !set.is_empty());
        states.push(next);
    }
    states
}

/// Commit `txns` and return the WAL bytes plus the end offset of each record.
fn committed_wal(txns: &[Transaction]) -> (Vec<u8>, Vec<usize>) {
    let temp = TempDir::new().unwrap();
    let persist = open(temp.path());
    let mut ends = Vec::new();
    for txn in txns {
        persist.commit(txn.clone()).unwrap();
        ends.push(fs::metadata(wal_file(temp.path())).unwrap().len() as usize);
    }
    std::mem::forget(persist);
    (fs::read(wal_file(temp.path())).unwrap(), ends)
}

fn reopen_with_wal(bytes: &[u8]) -> (TempDir, State) {
    let temp = TempDir::new().unwrap();
    fs::create_dir_all(temp.path().join("wal")).unwrap();
    fs::write(wal_file(temp.path()), bytes).unwrap();
    let state = state(&open(temp.path()));
    (temp, state)
}

fn corrupt_files(dir: &Path) -> Vec<PathBuf> {
    fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "corrupt"))
        .collect()
}

#[test]
fn every_truncation_recovers_a_whole_number_of_transactions() {
    // A truncation is a crash-torn tail: dropped without a `.corrupt` file.
    let txns = transactions();
    let states = states_after_each_commit(&txns);
    let (bytes, ends) = committed_wal(&txns);
    assert_eq!(ends.len(), txns.len());
    assert_eq!(*ends.last().unwrap(), bytes.len());

    for cut in 0..=bytes.len() {
        let committed = ends.iter().filter(|&&end| end <= cut).count();
        let (dir, recovered) = reopen_with_wal(&bytes[..cut]);
        assert_eq!(
            recovered, states[committed],
            "cut at byte {cut} must recover exactly {committed} transactions"
        );
        assert!(corrupt_files(dir.path()).is_empty(), "cut at byte {cut}");
    }
}

#[test]
fn damage_in_any_record_recovers_the_transactions_before_it() {
    let txns = transactions();
    let states = states_after_each_commit(&txns);
    let (bytes, ends) = committed_wal(&txns);

    let mut start: usize = 0;
    for (k, &end) in ends.iter().enumerate() {
        for offset in [start, start.midpoint(end), end - 2, end - 1] {
            let mut damaged = bytes.clone();
            damaged[offset] ^= if damaged[offset] == b'\n' { 0x20 } else { 0x01 };
            let (dir, recovered) = reopen_with_wal(&damaged);
            assert_eq!(
                recovered, states[k],
                "damage at byte {offset} (record {k}) must recover exactly {k} transactions"
            );
            let saved = corrupt_files(dir.path());
            if offset == bytes.len() - 1 {
                // Without its newline the last record is indistinguishable from a tear.
                assert!(saved.is_empty(), "damage at byte {offset}");
            } else {
                assert_eq!(saved.len(), 1, "damage at byte {offset}: bytes are kept");
                assert_eq!(fs::read(&saved[0]).unwrap(), &damaged[start..]);
            }
        }
        start = end;
    }
}

#[test]
fn recovery_is_idempotent_across_repeated_crashes() {
    let txns = transactions();
    let states = states_after_each_commit(&txns);
    let (bytes, ends) = committed_wal(&txns);
    let cut = ends[2] + 7;

    let temp = TempDir::new().unwrap();
    fs::create_dir_all(temp.path().join("wal")).unwrap();
    fs::write(wal_file(temp.path()), &bytes[..cut]).unwrap();
    for _ in 0..3 {
        let persist = open(temp.path());
        assert_eq!(state(&persist), states[3]);
        std::mem::forget(persist);
    }
}

fn engine_config(dir: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    config
}

fn relation(storage: &StorageEngine, name: &str) -> BTreeSet<Tuple> {
    let snapshot = storage.get_snapshot_for("default").unwrap();
    snapshot
        .input_tuples
        .get(name)
        .map(|ts| ts.iter().cloned().collect())
        .unwrap_or_default()
}

#[test]
fn prefix_clear_is_one_transaction_across_relations() {
    let temp = TempDir::new().unwrap();
    let engine_wal = temp.path().join("persist").join("wal").join("current.wal");
    let facts = |n: i32| (0..n).map(|i| Tuple::from_pair(n, i)).collect::<Vec<_>>();
    {
        let storage = StorageEngine::new(engine_config(temp.path())).unwrap();
        storage
            .insert_tuples_into("default", "p_a", facts(3))
            .unwrap();
        storage
            .insert_tuples_into("default", "p_b", facts(4))
            .unwrap();
    }
    // Reopening drains the WAL into batch files, so the clear is its only record.
    let storage = StorageEngine::new(engine_config(temp.path())).unwrap();
    assert_eq!(fs::metadata(&engine_wal).map_or(0, |m| m.len()), 0);
    let cleared = storage
        .clear_relations_by_prefix_in("default", "p_")
        .unwrap();
    assert_eq!(cleared, [("p_a".into(), 3), ("p_b".into(), 4)]);
    drop(storage);
    let clear_record = fs::read(&engine_wal).unwrap();
    assert_eq!(
        String::from_utf8_lossy(&clear_record).lines().count(),
        1,
        "one record clears both relations"
    );

    let snapshot = temp.path().join("snapshot");
    copy_dir(temp.path(), &snapshot);
    let len = clear_record.len();
    for cut in [0, 1, 9, len / 2, len - 2, len - 1, len] {
        let dir = TempDir::new().unwrap();
        copy_dir(&snapshot, dir.path());
        fs::write(
            dir.path().join("persist/wal/current.wal"),
            &clear_record[..cut],
        )
        .unwrap();
        let storage = StorageEngine::new(engine_config(dir.path())).unwrap();
        let (a, b) = (relation(&storage, "p_a"), relation(&storage, "p_b"));
        if cut == clear_record.len() {
            assert!(a.is_empty() && b.is_empty(), "complete record clears both");
        } else {
            assert_eq!(a, facts(3).into_iter().collect(), "cut at {cut}");
            assert_eq!(b, facts(4).into_iter().collect(), "cut at {cut}");
        }
    }
}

/// Copy a data directory, skipping its lock file and any nested copy.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let (src, name) = (entry.path(), entry.file_name());
        if name == "LOCK" || src == to || name == "snapshot" {
            continue;
        }
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&src, &to.join(&name));
        } else {
            fs::copy(&src, to.join(&name)).unwrap();
        }
    }
}
