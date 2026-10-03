//! Live state must equal the state recovered after restart.
#![allow(clippy::unwrap_used)]

use inputlayer::config::DurabilityMode;
use inputlayer::storage::persist::{
    FilePersist, PersistBackend, PersistConfig, Transaction, Update,
};
use inputlayer::storage::StorageResult;
use inputlayer::value::Tuple;
use inputlayer::{Config, StorageEngine};
use proptest::prelude::*;
use std::collections::BTreeSet;
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

const KG: &str = "default";

fn config(dir: &Path, buffer_size: usize) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    config.storage.persist.buffer_size = buffer_size;
    config
}

fn open(dir: &Path) -> StorageEngine {
    StorageEngine::new(config(dir, 1000)).unwrap()
}

fn t(a: i32) -> Tuple {
    Tuple::from_pair(a, a)
}

fn rows(storage: &StorageEngine, relation: &str) -> BTreeSet<Tuple> {
    let snapshot = storage.get_snapshot_for(KG).unwrap();
    snapshot
        .input_tuples
        .get(relation)
        .map(|ts| ts.iter().cloned().collect())
        .unwrap_or_default()
}

fn wal_size(dir: &Path) -> u64 {
    let wal: PathBuf = dir.join("persist").join("wal").join("current.wal");
    std::fs::metadata(wal).map_or(0, |m| m.len())
}

#[test]
fn duplicate_insert_then_delete_stays_deleted() {
    let temp = TempDir::new().unwrap();
    {
        let s = open(temp.path());
        assert_eq!(s.insert_tuples_into(KG, "r", vec![t(1)]).unwrap(), (1, 0));
        assert_eq!(s.insert_tuples_into(KG, "r", vec![t(1)]).unwrap(), (0, 1));
        assert_eq!(s.delete_tuples_from(KG, "r", vec![t(1)]).unwrap(), 1);
        assert!(rows(&s, "r").is_empty());
    }
    assert!(rows(&open(temp.path()), "r").is_empty());
}

#[test]
fn delete_absent_then_insert_stays_present() {
    let temp = TempDir::new().unwrap();
    {
        let s = open(temp.path());
        assert_eq!(s.delete_tuples_from(KG, "r", vec![t(1)]).unwrap(), 0);
        assert_eq!(s.insert_tuples_into(KG, "r", vec![t(1)]).unwrap(), (1, 0));
        assert_eq!(rows(&s, "r").len(), 1);
    }
    assert_eq!(rows(&open(temp.path()), "r").len(), 1);
}

#[test]
fn duplicates_within_one_batch_persist_once() {
    let temp = TempDir::new().unwrap();
    {
        let s = open(temp.path());
        assert_eq!(
            s.insert_tuples_into(KG, "r", vec![t(1), t(1), t(2)])
                .unwrap(),
            (2, 1)
        );
        assert_eq!(s.delete_tuples_from(KG, "r", vec![t(1), t(1)]).unwrap(), 1);
    }
    assert_eq!(rows(&open(temp.path()), "r"), BTreeSet::from([t(2)]));
}

#[test]
fn reinserting_known_facts_does_not_grow_wal() {
    let temp = TempDir::new().unwrap();
    let s = open(temp.path());
    let facts: Vec<Tuple> = (0..20).map(t).collect();
    assert_eq!(
        s.insert_tuples_into(KG, "r", facts.clone()).unwrap(),
        (20, 0)
    );
    let size = wal_size(temp.path());
    assert!(size > 0);
    for _ in 0..5 {
        assert_eq!(
            s.insert_tuples_into(KG, "r", facts.clone()).unwrap(),
            (0, 20)
        );
    }
    assert_eq!(wal_size(temp.path()), size);
}

#[test]
fn clear_after_duplicate_inserts_survives_restart() {
    let temp = TempDir::new().unwrap();
    {
        let s = open(temp.path());
        s.insert_tuples_into(KG, "p_a", vec![t(1), t(2)]).unwrap();
        s.insert_tuples_into(KG, "p_a", vec![t(1)]).unwrap();
        let cleared = s.clear_relations_by_prefix_in(KG, "p_").unwrap();
        assert_eq!(cleared, vec![("p_a".to_string(), 2)]);
    }
    assert!(rows(&open(temp.path()), "p_a").is_empty());
}

#[test]
fn recovery_clamps_drifted_multiplicities() {
    let temp = TempDir::new().unwrap();
    {
        let s = open(temp.path());
        s.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
    }
    // Disk state written with multiset semantics: X at +2, Y at -1.
    {
        let persist = FilePersist::new(PersistConfig {
            path: temp.path().join("persist"),
            durability_mode: DurabilityMode::Immediate,
            ..Default::default()
        })
        .unwrap();
        commit(
            &persist,
            "default:r",
            &[Update::insert(t(1), 1_000), Update::delete(t(2), 1_000)],
        )
        .unwrap();
    }
    {
        let s = open(temp.path());
        assert_eq!(rows(&s, "r"), BTreeSet::from([t(1)]));
        assert_eq!(s.delete_tuples_from(KG, "r", vec![t(1)]).unwrap(), 1);
        assert_eq!(s.insert_tuples_into(KG, "r", vec![t(2)]).unwrap(), (1, 0));
    }
    assert_eq!(rows(&open(temp.path()), "r"), BTreeSet::from([t(2)]));
}

#[test]
fn concurrent_assert_retract_memory_matches_disk() {
    let temp = TempDir::new().unwrap();
    let live = {
        let s = Arc::new(open(temp.path()));
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    for j in 0..200 {
                        let tuple = vec![t((i + j) % 3)];
                        if (i + j) % 2 == 0 {
                            s.insert_tuples_into(KG, "r", tuple).unwrap();
                        } else {
                            s.delete_tuples_from(KG, "r", tuple).unwrap();
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        rows(&s, "r")
    };
    assert_eq!(rows(&open(temp.path()), "r"), live);
}

#[derive(Debug, Clone)]
enum Op {
    Insert(Vec<i32>),
    Delete(Vec<i32>),
    Clear,
    Restart,
}

fn op() -> impl Strategy<Value = Op> {
    let batch = || prop::collection::vec(0..6i32, 1..5);
    prop_oneof![
        4 => batch().prop_map(Op::Insert),
        3 => batch().prop_map(Op::Delete),
        1 => Just(Op::Clear),
        1 => Just(Op::Restart),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn live_state_equals_restarted_state(ops in prop::collection::vec(op(), 1..40)) {
        let temp = TempDir::new().unwrap();
        let mut model: BTreeSet<Tuple> = BTreeSet::new();
        let mut s = StorageEngine::new(config(temp.path(), 4)).unwrap();
        for op in ops {
            match op {
                Op::Insert(vals) => {
                    let batch: Vec<Tuple> = vals.into_iter().map(t).collect();
                    let (new, dup) = s.insert_tuples_into(KG, "p_r", batch.clone()).unwrap();
                    prop_assert_eq!(new + dup, batch.len());
                    model.extend(batch);
                }
                Op::Delete(vals) => {
                    let batch: Vec<Tuple> = vals.into_iter().map(t).collect();
                    let expected = batch
                        .iter()
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .filter(|x| model.contains(x))
                        .count();
                    prop_assert_eq!(s.delete_tuples_from(KG, "p_r", batch.clone()).unwrap(), expected);
                    for x in &batch {
                        model.remove(x);
                    }
                }
                Op::Clear => {
                    s.clear_relations_by_prefix_in(KG, "p_").unwrap();
                    model.clear();
                }
                Op::Restart => {
                    drop(s);
                    s = StorageEngine::new(config(temp.path(), 4)).unwrap();
                }
            }
            prop_assert_eq!(&rows(&s, "p_r"), &model);
        }
        drop(s);
        let s = StorageEngine::new(config(temp.path(), 4)).unwrap();
        prop_assert_eq!(&rows(&s, "p_r"), &model);
    }
}
