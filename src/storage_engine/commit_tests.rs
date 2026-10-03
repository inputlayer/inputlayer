//! A write whose commit fails returns an error and changes nothing: not memory, not
//! the snapshot, and not the state recovered after a restart.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
use crate::storage::persist::wal::WalFault;
use tempfile::TempDir;

const KG: &str = "default";

fn open(temp: &TempDir) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    config.storage.persist.durability_mode = crate::config::DurabilityMode::Immediate;
    StorageEngine::new(config).unwrap()
}

fn rows(storage: &StorageEngine, relation: &str) -> Vec<Tuple> {
    let snapshot = storage.get_snapshot_for(KG).unwrap();
    let mut rows: Vec<Tuple> = snapshot
        .input_tuples
        .get(relation)
        .map(|ts| ts.iter().cloned().collect())
        .unwrap_or_default();
    rows.sort();
    rows
}

fn t(a: i32) -> Tuple {
    Tuple::from_pair(a, a)
}

#[test]
fn failed_insert_changes_nothing_now_or_after_restart() {
    for fault in [WalFault::Write, WalFault::Sync] {
        let temp = TempDir::new().unwrap();
        {
            let storage = open(&temp);
            storage.insert_tuples_into(KG, "r", vec![t(1)]).unwrap();
            storage.persist.inject_wal_fault(fault);
            assert!(storage.insert_tuples_into(KG, "r", vec![t(2)]).is_err());
            assert_eq!(rows(&storage, "r"), [t(1)], "{fault:?}");
            // The same facts commit once the fault is gone.
            assert_eq!(
                storage.insert_tuples_into(KG, "r", vec![t(2)]).unwrap(),
                (1, 0)
            );
        }
        assert_eq!(rows(&open(&temp), "r"), [t(1), t(2)], "{fault:?}");
    }
}

#[test]
fn failed_delete_changes_nothing_now_or_after_restart() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        storage
            .insert_tuples_into(KG, "r", vec![t(1), t(2)])
            .unwrap();
        storage.persist.inject_wal_fault(WalFault::Sync);
        storage.persist.inject_wal_fault(WalFault::Restore);
        assert!(storage.delete_tuples_from(KG, "r", vec![t(1)]).is_err());
        assert_eq!(rows(&storage, "r"), [t(1), t(2)]);
    }
    assert_eq!(rows(&open(&temp), "r"), [t(1), t(2)]);
}

#[test]
fn failed_prefix_clear_keeps_every_relation() {
    let temp = TempDir::new().unwrap();
    {
        let storage = open(&temp);
        for rel in ["p_a", "p_b"] {
            storage.insert_tuples_into(KG, rel, vec![t(1)]).unwrap();
        }
        storage.persist.inject_wal_fault(WalFault::Write);
        assert!(storage.clear_relations_by_prefix_in(KG, "p_").is_err());
        for rel in ["p_a", "p_b"] {
            assert_eq!(rows(&storage, rel), [t(1)]);
        }
    }
    let storage = open(&temp);
    for rel in ["p_a", "p_b"] {
        assert_eq!(rows(&storage, rel), [t(1)]);
    }
}
