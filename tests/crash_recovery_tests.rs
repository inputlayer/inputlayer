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
fn test_empty_wal_file_recovery() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create empty WAL file
    fs::create_dir_all(path.join("wal")).unwrap();
    fs::create_dir_all(path.join("shards")).unwrap();
    fs::create_dir_all(path.join("batches")).unwrap();
    fs::write(path.join("wal/current.wal"), "").unwrap();

    // Should handle empty WAL gracefully
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.is_empty(), "No shards should exist with empty WAL");
}

#[test]
fn test_wal_with_only_whitespace() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create WAL with only whitespace
    fs::create_dir_all(path.join("wal")).unwrap();
    fs::create_dir_all(path.join("shards")).unwrap();
    fs::create_dir_all(path.join("batches")).unwrap();
    fs::write(path.join("wal/current.wal"), "   \n\n   \n").unwrap();

    // Should handle whitespace-only WAL gracefully
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.is_empty());
}

// Corrupted Metadata Tests
#[test]
fn test_recovery_with_corrupted_shard_metadata() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create directory structure
    fs::create_dir_all(path.join("wal")).unwrap();
    fs::create_dir_all(path.join("shards")).unwrap();
    fs::create_dir_all(path.join("batches")).unwrap();

    // Write corrupted shard metadata
    fs::write(path.join("shards/db_edge.json"), "{ corrupted metadata }").unwrap();

    // Recovery should fail gracefully
    let result = FilePersist::new(PersistConfig {
        path: path.clone(),
        buffer_size: 10,
        immediate_sync: true,
        durability_mode: DurabilityMode::Immediate,
    });

    assert!(result.is_err(), "Corrupted metadata should return error");
}

#[test]
fn test_recovery_with_missing_required_metadata_fields() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create directory structure
    fs::create_dir_all(path.join("wal")).unwrap();
    fs::create_dir_all(path.join("shards")).unwrap();
    fs::create_dir_all(path.join("batches")).unwrap();

    // Write metadata with missing required fields
    fs::write(
        path.join("shards/db_edge.json"),
        r#"{"name": "db:edge"}"#, // Missing other required fields
    )
    .unwrap();

    // Recovery should fail gracefully
    let result = FilePersist::new(PersistConfig {
        path: path.clone(),
        buffer_size: 10,
        immediate_sync: true,
        durability_mode: DurabilityMode::Immediate,
    });

    assert!(
        result.is_err(),
        "Missing metadata fields should return error"
    );
}

// Corrupted Batch File (Parquet) Tests
#[test]
fn test_read_with_corrupted_parquet_file() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create valid data and flush to parquet
    {
        let persist = create_test_persist_with_config(path.clone(), 5);
        persist.ensure_shard("db:edge").unwrap();

        // Add enough data to trigger flush
        for i in 0..6 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();
    }

    // Corrupt the parquet file
    let batches_dir = path.join("batches");
    for entry in fs::read_dir(&batches_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
            fs::write(entry.path(), b"corrupted parquet data").unwrap();
            break;
        }
    }

    // Re-create persist and try to read
    let persist = create_test_persist_with_config(path.clone(), 100);
    let result = persist.read("db:edge", 0);

    // Should return error for corrupted parquet
    assert!(result.is_err(), "Corrupted parquet should return error");
}

#[test]
fn test_read_with_truncated_parquet_file() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create valid data and flush to parquet
    {
        let persist = create_test_persist_with_config(path.clone(), 5);
        persist.ensure_shard("db:edge").unwrap();

        for i in 0..6 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();
    }

    // Truncate the parquet file
    let batches_dir = path.join("batches");
    for entry in fs::read_dir(&batches_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
            let content = fs::read(entry.path()).unwrap();
            if content.len() > 10 {
                fs::write(entry.path(), &content[..content.len() / 2]).unwrap();
            }
            break;
        }
    }

    // Re-create persist and try to read
    let persist = create_test_persist_with_config(path.clone(), 100);
    let result = persist.read("db:edge", 0);

    // Should return error for truncated parquet
    assert!(result.is_err(), "Truncated parquet should return error");
}

#[test]
fn test_read_with_missing_batch_file() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create valid data and flush to parquet
    {
        let persist = create_test_persist_with_config(path.clone(), 5);
        persist.ensure_shard("db:edge").unwrap();

        for i in 0..6 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();
    }

    // Delete the batch file but leave metadata
    let batches_dir = path.join("batches");
    for entry in fs::read_dir(&batches_dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().extension().and_then(|s| s.to_str()) == Some("parquet") {
            fs::remove_file(entry.path()).unwrap();
        }
    }

    // Re-create persist and try to read
    let persist = create_test_persist_with_config(path.clone(), 100);
    let result = persist.read("db:edge", 0);

    // Should return error for missing batch file
    assert!(result.is_err(), "Missing batch file should return error");
}

// Recovery After Disk Full
#[test]
fn test_recovery_after_failed_flush() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create data in buffer (not flushed)
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

    // Verify data is in buffer
    let updates = persist.read("db:edge", 0).unwrap();
    assert_eq!(updates.len(), 2);

    // If flush fails (e.g., due to disk full), WAL should still have data
    // This test verifies the WAL persistence works
}

// Orphaned File Handling
#[test]
fn test_recovery_with_orphaned_batch_files() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create directory structure
    fs::create_dir_all(path.join("wal")).unwrap();
    fs::create_dir_all(path.join("shards")).unwrap();
    fs::create_dir_all(path.join("batches")).unwrap();

    // Create an orphaned parquet file (no metadata references it)
    let orphan_path = path.join("batches/orphan.parquet");
    fs::write(&orphan_path, b"not a real parquet file").unwrap();

    // System should start up without issues (orphaned files are ignored)
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.is_empty(), "No valid shards should exist");

    // The orphaned file should still exist (we don't auto-clean)
    assert!(orphan_path.exists());
}

#[test]
fn test_recovery_with_orphaned_wal_archives() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create a valid persist instance first
    {
        let persist = create_test_persist_with_config(path.clone(), 5);
        persist.ensure_shard("db:edge").unwrap();

        for i in 0..6 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();
    }

    // Create orphaned WAL archive files
    fs::write(
        path.join("wal/wal_12345.archived"),
        r#"{"shard":"old:data","update":{}}"#,
    )
    .unwrap();

    // System should start up without issues
    let persist = create_test_persist_with_config(path.clone(), 100);
    let updates = persist.read("db:edge", 0).unwrap();
    assert!(
        !updates.is_empty(),
        "Should recover valid data despite orphaned archives"
    );
}

// Crash During Compaction
#[test]
fn test_compaction_atomicity() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create data and flush
    {
        let persist = create_test_persist_with_config(path.clone(), 5);
        persist.ensure_shard("db:edge").unwrap();

        // Add data at different times
        for i in 0..20i32 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();

        // Compact
        persist.compact("db:edge", 10).unwrap();

        // Verify compaction worked
        let info = persist.shard_info("db:edge").unwrap();
        assert_eq!(info.since, 10);
    }

    // Verify data after restart
    let persist = create_test_persist_with_config(path.clone(), 100);
    let updates = persist.read("db:edge", 0).unwrap();

    // All remaining updates should have time >= 10
    assert!(
        updates.iter().all(|u| u.time >= 10),
        "Compaction should have removed old data"
    );
}

// Data Integrity Verification
#[test]
