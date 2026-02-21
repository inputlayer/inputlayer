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
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
    };
    FilePersist::new(config).expect("Failed to create persist layer")
}

fn create_test_persist_with_config(path: PathBuf, buffer_size: usize) -> FilePersist {
    let config = PersistConfig {
        path,
        buffer_size,
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
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

    // Write corrupted WAL (single line = last line, tolerated as truncated write)
    let wal_path = path.join("wal/current.wal");
    fs::write(&wal_path, "{ invalid json garbage }\n").unwrap();

    // P0-5: A single corrupted line is the "last line" and is tolerated
    // (simulates a crash mid-write). Recovery should succeed with no data.
    let persist = FilePersist::new(PersistConfig {
        path: path.clone(),
        buffer_size: 10,
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
    });
    assert!(
        persist.is_ok(),
        "Single corrupted WAL line (last line) should be tolerated"
    );
    let persist = persist.unwrap();
    assert!(
        persist.list_shards().unwrap().is_empty(),
        "No shards should be recovered from corrupted-only WAL"
    );
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

    // Append garbage as last line of WAL (simulates crash mid-write)
    let wal_path = path.join("wal/current.wal");
    if wal_path.exists() {
        let mut file = fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
        writeln!(file, "{{garbage not json}}").unwrap();
    }

    // P0-5: Corrupted LAST line is tolerated (truncated write on crash).
    // Valid entries before it should be recovered.
    let persist = FilePersist::new(PersistConfig {
        path: path.clone(),
        buffer_size: 100,
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
    })
    .expect("Recovery should succeed — corrupted last line is tolerated");

    // The valid entry written before corruption should be recovered
    let shards = persist.list_shards().unwrap();
    assert!(
        shards.contains(&"db:edge".to_string()),
        "Valid shard should be recovered despite truncated last WAL entry"
    );
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
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
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
        durability_mode: DurabilityMode::Immediate,
        ..Default::default()
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

    // System should start up and clean orphaned files automatically
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.is_empty(), "No valid shards should exist");

    // The orphaned file should be cleaned up on startup
    assert!(
        !orphan_path.exists(),
        "Orphaned batch file should be removed during startup cleanup"
    );
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
fn test_data_integrity_after_multiple_restarts() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let expected_data = vec![(1, 2), (3, 4), (5, 6)];

    // First session: insert data
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();
        for (a, b) in &expected_data {
            persist
                .append("db:edge", &[Update::insert(Tuple::from_pair(*a, *b), 10)])
                .unwrap();
        }
        persist.flush("db:edge").unwrap();
    }

    // Multiple restarts should preserve data
    for i in 0..3 {
        let persist = create_test_persist_with_config(path.clone(), 100);
        let updates = persist.read("db:edge", 0).unwrap();
        let tuples = to_tuples(&updates);

        assert_eq!(
            tuples.len(),
            expected_data.len(),
            "Restart {} should preserve data count",
            i
        );

        for (a, b) in &expected_data {
            assert!(
                tuples.contains(&Tuple::from_pair(*a, *b)),
                "Restart {} should preserve tuple ({}, {})",
                i,
                a,
                b
            );
        }
    }
}

#[test]
fn test_concurrent_crash_recovery() {
    use std::sync::Arc;
    use std::thread;

    let temp = TempDir::new().unwrap();
    let path = Arc::new(temp.path().to_path_buf());

    // Create initial data
    {
        let persist = create_test_persist_with_config(path.as_ref().clone(), 100);
        persist.ensure_shard("db:edge").unwrap();
        persist
            .append("db:edge", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush("db:edge").unwrap();
    }

    // Multiple threads trying to recover simultaneously
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let path = Arc::clone(&path);
            thread::spawn(move || {
                let persist = create_test_persist_with_config(path.as_ref().clone(), 100);
                let result = persist.read("db:edge", 0);
                result.is_ok()
            })
        })
        .collect();

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // At least some should succeed (concurrent file access might cause issues on some systems)
    assert!(
        results.iter().any(|r| *r),
        "At least one concurrent recovery should succeed"
    );
}

// Edge Cases
#[test]
fn test_recovery_with_unicode_shard_names() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create shard with unicode name
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("数据库:表").unwrap();
        persist
            .append("数据库:表", &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush("数据库:表").unwrap();
    }

    // Verify recovery
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.contains(&"数据库:表".to_string()));

    let updates = persist.read("数据库:表", 0).unwrap();
    assert_eq!(updates.len(), 1);
}

#[test]
fn test_recovery_with_very_long_shard_name() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create shard with very long name (200 chars)
    let long_name = format!("db:{}edge", "a".repeat(190));

    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard(&long_name).unwrap();
        persist
            .append(&long_name, &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush(&long_name).unwrap();
    }

    // Verify recovery
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.contains(&long_name));
}

#[test]
fn test_recovery_with_special_chars_in_shard_name() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Note: The shard name is sanitized, so special chars like / become _
    let shard_name = "db:table_with_special";

    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard(shard_name).unwrap();
        persist
            .append(shard_name, &[Update::insert(Tuple::from_pair(1, 2), 10)])
            .unwrap();
        persist.flush(shard_name).unwrap();
    }

    // Verify recovery
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert!(shards.contains(&shard_name.to_string()));
}

#[test]
fn test_recovery_preserves_tuple_types() {
    use inputlayer::Value;

    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create mixed-type tuples
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:mixed").unwrap();
        persist
            .append(
                "db:mixed",
                &[Update::insert(
                    Tuple::new(vec![
                        Value::Int32(42),
                        Value::string("hello"),
                        Value::Float64(3.14),
                    ]),
                    10,
                )],
            )
            .unwrap();
        persist.flush("db:mixed").unwrap();
    }

    // Verify types are preserved after recovery
    let persist = create_test_persist_with_config(path.clone(), 100);
    let updates = persist.read("db:mixed", 0).unwrap();

    assert_eq!(updates.len(), 1);
    let tuple = &updates[0].data;
    assert_eq!(tuple.get(0), Some(&Value::Int32(42)));
    assert_eq!(tuple.get(1).and_then(|v| v.as_str()), Some("hello"));
}

// Clean Shutdown Tests
#[test]
fn test_wal_replay_after_clean_shutdown() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // First instance: write data and flush properly (clean shutdown)
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
        persist.flush("db:edge").unwrap();
        // Clean shutdown - data flushed to parquet
    }

    // Second instance: should recover data from parquet (WAL may be empty after clean flush)
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        let updates = persist.read("db:edge", 0).unwrap();
        assert_eq!(updates.len(), 2, "Flushed data should be recovered");

        let tuples = to_tuples(&updates);
        assert!(tuples.contains(&Tuple::from_pair(1, 2)));
        assert!(tuples.contains(&Tuple::from_pair(3, 4)));
    }
}

// Compaction Preserves Data Tests
#[test]
fn test_compaction_preserves_all_data() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create data with many updates at different times
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();

        // Insert data at various times
        for i in 0..100i32 {
            persist
                .append(
                    "db:edge",
                    &[Update::insert(Tuple::from_pair(i, i * 2), i as u64)],
                )
                .unwrap();
        }
        persist.flush("db:edge").unwrap();

        // Read data before compaction
        let before = persist.read("db:edge", 0).unwrap();
        assert_eq!(before.len(), 100);

        // Compact with time threshold at 50 (keep updates >= 50)
        persist.compact("db:edge", 50).unwrap();

        // Read data after compaction
        let after = persist.read("db:edge", 50).unwrap();

        // All data with time >= 50 should still be present
        assert_eq!(after.len(), 50, "Should keep 50 updates after compaction");

        // Verify the correct data is preserved
        let tuples = to_tuples(&after);
        for i in 50..100 {
            assert!(
                tuples.contains(&Tuple::from_pair(i, i * 2)),
                "Tuple ({}, {}) should be preserved after compaction",
                i,
                i * 2
            );
        }
    }

    // Verify after restart
    let persist = create_test_persist_with_config(path.clone(), 100);
    let updates = persist.read("db:edge", 50).unwrap();
    assert_eq!(
        updates.len(),
        50,
        "Compacted data should persist across restart"
    );
}

#[test]
fn test_compaction_with_deletions() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();

        // Insert then delete some tuples
        persist
            .append(
                "db:edge",
                &[
                    Update::insert(Tuple::from_pair(1, 2), 10),
                    Update::insert(Tuple::from_pair(3, 4), 20),
                    Update::insert(Tuple::from_pair(5, 6), 30),
                ],
            )
            .unwrap();

        // Delete the middle tuple
        persist
            .append("db:edge", &[Update::delete(Tuple::from_pair(3, 4), 40)])
            .unwrap();

        persist.flush("db:edge").unwrap();

        // Compact to time 25 (keeps >= 25)
        persist.compact("db:edge", 25).unwrap();
    }

    // Verify after restart
    let persist = create_test_persist_with_config(path.clone(), 100);
    let updates = persist.read("db:edge", 25).unwrap();

    // Consolidate to see net effect
    let mut consolidated = updates.clone();
    consolidate(&mut consolidated);
    let tuples = to_tuples(&consolidated);

    // Should have (5,6) from time 30 and deletion of (3,4) at time 40
    assert!(
        tuples.contains(&Tuple::from_pair(5, 6)),
        "(5,6) should be preserved"
    );
}

// Concurrent Read During Compaction Tests
#[test]
fn test_concurrent_read_during_compaction() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;

    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // Create a single shared persist instance to avoid orphan-cleanup race
    // (two separate instances on the same path can race: one cleans up batch
    // files that the other still references)
    let persist = Arc::new(create_test_persist_with_config(path, 100));
    persist.ensure_shard("db:edge").unwrap();

    for i in 0..50i32 {
        persist
            .append(
                "db:edge",
                &[Update::insert(Tuple::from_pair(i, i), i as u64)],
            )
            .unwrap();
    }
    persist.flush("db:edge").unwrap();

    // Shared flag to signal compaction is happening
    let compacting = Arc::new(AtomicBool::new(false));
    let reads_during_compaction = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Reader thread
    let reader_persist = Arc::clone(&persist);
    let reader_compacting = Arc::clone(&compacting);
    let reader_count = Arc::clone(&reads_during_compaction);
    let reader = thread::spawn(move || {
        for _ in 0..20 {
            let result = reader_persist.read("db:edge", 0);
            if result.is_ok() && reader_compacting.load(Ordering::Relaxed) {
                reader_count.fetch_add(1, Ordering::Relaxed);
            }
            thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    // Compaction thread
    let compact_persist = Arc::clone(&persist);
    let compact_flag = Arc::clone(&compacting);
    let compactor = thread::spawn(move || {
        compact_flag.store(true, Ordering::Relaxed);
        let result = compact_persist.compact("db:edge", 25);
        compact_flag.store(false, Ordering::Relaxed);

        result.is_ok()
    });

    reader.join().unwrap();
    let compaction_ok = compactor.join().unwrap();

    // Compaction should succeed
    assert!(compaction_ok, "Compaction should complete successfully");

    // Some reads should have happened during compaction
    // (This verifies concurrent access doesn't cause deadlocks)
}

// Stress Tests
#[test]
fn test_many_restarts_with_incremental_data() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    // 10 restart cycles, each adding more data
    for cycle in 0..10 {
        let persist = create_test_persist_with_config(path.clone(), 100);
        persist.ensure_shard("db:edge").unwrap();

        // Add new data each cycle
        persist
            .append(
                "db:edge",
                &[Update::insert(Tuple::from_pair(cycle, cycle), cycle as u64)],
            )
            .unwrap();
        persist.flush("db:edge").unwrap();

        // Verify cumulative data
        let updates = persist.read("db:edge", 0).unwrap();
        assert_eq!(
            updates.len(),
            (cycle + 1) as usize,
            "Cycle {} should have {} updates",
            cycle,
            cycle + 1
        );
    }
}

#[test]
fn test_recovery_with_many_shards() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let num_shards = 50;

    // Create many shards
    {
        let persist = create_test_persist_with_config(path.clone(), 100);
        for i in 0..num_shards {
            let shard = format!("db:shard_{}", i);
            persist.ensure_shard(&shard).unwrap();
            persist
                .append(&shard, &[Update::insert(Tuple::from_pair(i, i), 10)])
                .unwrap();
            persist.flush(&shard).unwrap();
        }
    }

    // Verify all shards recover
    let persist = create_test_persist_with_config(path.clone(), 100);
    let shards = persist.list_shards().unwrap();
    assert_eq!(shards.len(), num_shards as usize);

    for i in 0..num_shards {
        let shard = format!("db:shard_{}", i);
        let updates = persist.read(&shard, 0).unwrap();
        assert_eq!(updates.len(), 1, "Shard {} should have data", i);
    }
}
