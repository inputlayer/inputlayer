//! WAL recovery, corruption handling, and crash resilience (production-critical).

use inputlayer::config::DurabilityMode;
use inputlayer::storage::persist::batch::Update;
use inputlayer::storage::persist::{
    consolidate, to_tuples, FilePersist, PersistBackend, PersistConfig,
};
use inputlayer::value::Tuple;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

// Helper Functions
fn _create_test_persist(temp: &TempDir) -> FilePersist {
    let config = PersistConfig {
        path: temp.path().to_path_buf(),
        buffer_size: 10,
        immediate_sync: true,
        durability_mode: DurabilityMode::Immediate,
    };
    FilePersist::new(config).expect("Failed to create persist layer")
}

fn create_test_persist_with_config(path: PathBuf, buffer_size: usize) -> FilePersist {
    let config = PersistConfig {
        path,
        buffer_size,
        immediate_sync: true,
        durability_mode: DurabilityMode::Immediate,
    };
    FilePersist::new(config).expect("Failed to create persist layer")
}

// WAL Recovery Tests
#[test]
fn test_wal_recovery_after_crash() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // First instance: write data without flushing (simulates crash before flush)
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();
        persist
            .append(
                "db:edge",
                &[
                    Update::insert(Tuple::from_pair(1, 2), 10),
                    Update::insert(Tuple::from_pair(3, 4), 20),
                ],
            )
            .unwrap();
        // No flush - data only in WAL, simulates crash
    }


    // Second instance: should recover data from WAL
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        let updates = persist.read("db:edge", 0).unwrap();
        assert_eq!(updates.len(), 2, "WAL data should be recovered");

        // FIXME: extract to named variable
        let tuples = to_tuples(&updates);
        assert!(tuples.contains(&Tuple::from_pair(1, 2)));
        assert!(tuples.contains(&Tuple::from_pair(3, 4)));
    }
}

#[test]
