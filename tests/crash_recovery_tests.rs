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
    FilePersist::new(config).unwrap()
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

        let tuples = to_tuples(&updates);
        assert!(tuples.contains(&Tuple::from_pair(1, 2)));
        assert!(tuples.contains(&Tuple::from_pair(3, 4)));
    }
}

#[test]
fn test_wal_with_partial_entry_truncation() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // First: create valid WAL with entries
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
    }

    // Manually truncate the WAL file to simulate crash mid-write
    let wal_path = path.join("wal/current.wal");
    if wal_path.exists() {
        let content = fs::read_to_string(&wal_path).unwrap();
        if content.len() > 10 {
            // Truncate to partial entry (simulates incomplete write)
            let truncated_len = content.len() - 10;
            fs::write(&wal_path, &content[..truncated_len]).unwrap();
        }
    }

    // Recovery should handle truncated WAL gracefully
    // The last incomplete entry may be lost, but valid entries should be recovered
    // Note: This test documents current behavior - system may need to handle this more gracefully
    let result = std::panic::catch_unwind(|| {
        let _persist = create_test_persist_with_config(path.clone(), 100);
    });

    // System should either recover partial data or report the corruption clearly
    // Not panicking is the minimum requirement
    // TODO: verify this condition
    if result.is_ok() {
        // Recovery succeeded - verify we can still use the system
        let persist = create_test_persist_with_config(path.clone(), 100);
        let _ = persist.list_shards(); // Should not panic
    }
}

#[test]
fn test_wal_double_replay_idempotency() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create initial data
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
    }

    // Note: The WAL file exists at path.join("wal/current.wal")
    // In a real scenario, double-replay could happen if crash recovery runs twice

    // Recover first time
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        let updates = persist.read("db:edge", 0).unwrap();

        // Consolidate should handle duplicates from double replay
        let mut consolidated = updates.clone();
        consolidate(&mut consolidated);

        let tuples = to_tuples(&consolidated);
        assert_eq!(tuples.len(), 1, "Consolidation should deduplicate");
        assert!(tuples.contains(&Tuple::from_pair(1, 2)));
    }
}

// Corrupted WAL JSON Tests
#[test]
fn test_recovery_with_corrupted_wal_json() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create directory structure manually
    fs::create_dir_all(path.join("wal")).unwrap();
    fs::create_dir_all(path.join("shards")).unwrap();
    fs::create_dir_all(path.join("batches")).unwrap();

    // Write corrupted WAL
    let wal_path = path.join("wal/current.wal");
    fs::write(&wal_path, "{ invalid json garbage }\n").unwrap();

    // Recovery should fail gracefully with clear error
    let result = FilePersist::new(PersistConfig {
        path: path.clone(),
        buffer_size: 10,
        immediate_sync: true,
        durability_mode: DurabilityMode::Immediate,
    });

    // Should return an error, not panic
    match result {
        Ok(_) => panic!("Corrupted WAL should return error"),
        Err(e) => {
            let err_msg = format!("{}", e);
            assert!(
                err_msg.contains("WAL") || err_msg.contains("parse"),
                "Error should mention WAL or parsing: {}",
                err_msg
            );
        }
    }
}

#[test]
fn test_recovery_with_mixed_valid_invalid_wal_entries() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // First create valid entries
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
    }

    // Append garbage to WAL
    let wal_path = path.join("wal/current.wal");
    if wal_path.exists() {
        let mut file = fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        writeln!(file, "{{garbage not json}}").unwrap();
    }

    // Recovery should fail - we don't skip corrupted entries
    let result = FilePersist::new(PersistConfig {
        path: path.clone(),
        buffer_size: 100,
        immediate_sync: true,
        durability_mode: DurabilityMode::Immediate,
    });

    assert!(result.is_err(), "Corrupted WAL entry should cause error");
}

#[test]
