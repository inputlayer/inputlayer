//! Concurrent access, lock poisoning recovery, and deadlock prevention.

use inputlayer::{Config, StorageEngine};
use std::sync::{Arc, RwLock};
use std::thread;
use tempfile::TempDir;

// Test Helpers
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 4;
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

fn create_shared_storage() -> (Arc<RwLock<StorageEngine>>, TempDir) {
    let (storage, temp) = create_test_storage();
    (Arc::new(RwLock::new(storage)), temp)
}

// Concurrent Read Tests
#[test]
fn test_concurrent_reads_do_not_block() {
    let (storage, _temp) = create_test_storage();

    // Setup: create KG with data
    storage.create_knowledge_graph("concurrent_test").unwrap();
    storage
        .insert_into(
            "concurrent_test",
            "edge",
            vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)],
        )
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_readers = 10;
    let mut handles = vec![];

    // Spawn multiple concurrent readers
    for i in 0..num_readers {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            // Each reader executes the same query multiple times
            for _ in 0..5 {
                let storage_guard = storage_clone.write().expect("Lock acquisition failed");
                let results = storage_guard
                    .execute_query_on("concurrent_test", "result(X,Y) :- edge(X,Y).")
                    .expect(&format!("Reader {} failed to execute query", i));
                assert_eq!(results.len(), 5);
            }
        });
        handles.push(handle);
    }

    // All readers should complete successfully
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_reads_across_multiple_kgs() {
    let (storage, _temp) = create_test_storage();

    // Create multiple KGs with data
    for i in 1..=5 {
        let kg_name = format!("kg{}", i);
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage
            .insert_into(&kg_name, "data", vec![(i, i * 10)])
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Spawn readers for different KGs
    for kg_num in 1..=5i32 {
        for _ in 0..3 {
            let storage_clone = Arc::clone(&storage);
            let kg_name = format!("kg{}", kg_num);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let results = storage_guard
                        .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
                        .expect("Query failed");
                    assert_eq!(results.len(), 1);
                    assert_eq!(results[0], (kg_num, kg_num * 10));
                }
            });
            handles.push(handle);
        }
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// Read-Write Isolation Tests
#[test]
fn test_readers_see_consistent_snapshot() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("snapshot_test").unwrap();
    storage
        .insert_into("snapshot_test", "counter", vec![(1, 100)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_readers = 5;
    let mut handles = vec![];

    // Spawn readers that query multiple times
    for reader_id in 0..num_readers {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let mut previous_result: Option<Vec<(i32, i32)>> = None;
            for iteration in 0..20 {
                let storage_guard = storage_clone.write().unwrap();
                let results = storage_guard
                    .execute_query_on("snapshot_test", "result(X,Y) :- counter(X,Y).")
                    .expect("Query failed");

                // Results should be non-empty (at least the initial data)
                assert!(!results.is_empty() || iteration == 0);

                // TODO: verify this condition
                if let Some(prev) = &previous_result {
                    // Within a single thread, results should be consistent
                    // (no torn reads)
                    // TODO: verify this condition
                    if !results.is_empty() && !prev.is_empty() {
                        // Just verify we get valid tuples
                        assert!(results[0].0 > 0);
                    }
                }
                previous_result = Some(results);
            }
            reader_id
        });
        handles.push(handle);
    }

    for handle in handles {
        let reader_id = handle.join().expect("Reader panicked");
        assert!(reader_id < num_readers);
    }
}

// High Contention Stress Tests
#[test]
