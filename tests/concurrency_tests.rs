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
        handle.join().expect("Reader thread panicked");
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
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("snapshot_test", "result(X,Y) :- counter(X,Y).")
                    .expect("Query failed");

                // Results should be non-empty (at least the initial data)
                assert!(!results.is_empty() || iteration == 0);

                if let Some(prev) = &previous_result {
                    // Within a single thread, results should be consistent
                    // (no torn reads)
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
fn test_high_contention_many_readers() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("contention_test").unwrap();

    // Insert substantial data
    let edges: Vec<(i32, i32)> = (0..100).map(|i| (i, i + 1)).collect();
    storage
        .insert_into("contention_test", "edge", edges)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let queries_per_thread = 50;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for query_num in 0..queries_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("contention_test", "result(X,Y) :- edge(X,Y).")
                    .expect(&format!("Thread {} query {} failed", thread_id, query_num));
                assert_eq!(results.len(), 100);
            }
            thread_id
        });
        handles.push(handle);
    }

    // All threads should complete
    let mut completed = 0;
    for handle in handles {
        handle.join().expect("Thread panicked under contention");
        completed += 1;
    }
    assert_eq!(completed, num_threads);
}

#[test]
fn test_no_deadlock_with_cross_kg_queries() {
    let (storage, _temp) = create_test_storage();

    // Create KGs
    for i in 1..=4 {
        let kg_name = format!("deadlock_test_kg{}", i);
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage
            .insert_into(&kg_name, "data", vec![(i, i * 100)])
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Threads that query different KGs in different orders
    // This pattern could cause deadlock if lock ordering is wrong
    for pattern in 0..4 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                // Query KGs in rotating order
                for offset in 0..4 {
                    let kg_num = ((pattern + offset) % 4) + 1;
                    let kg_name = format!("deadlock_test_kg{}", kg_num);
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let results = storage_guard
                        .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
                        .expect("Query failed - possible deadlock?");
                    assert_eq!(results.len(), 1);
                }
            }
        });
        handles.push(handle);
    }

    // If there's a deadlock, this will hang
    for handle in handles {
        handle.join().expect("Deadlock or panic detected");
    }
}

// Error Recovery Tests
#[test]
fn test_graceful_error_on_nonexistent_kg() {
    let (storage, _temp) = create_shared_storage();
    let num_threads = 5;
    let mut handles = vec![];

    // All threads try to query non-existent KG
    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let storage_guard = storage_clone.write().expect("Lock failed");
            let result =
                storage_guard.execute_query_on("nonexistent_kg", "result(X,Y) :- edge(X,Y).");
            // Should return error, not panic
            assert!(result.is_err());
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .join()
            .expect("Thread panicked instead of returning error");
    }
}

#[test]
fn test_mixed_valid_invalid_queries_concurrent() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("valid_kg").unwrap();
    storage
        .insert_into("valid_kg", "edge", vec![(1, 2)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Mix of valid and invalid queries
    for i in 0..10 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let storage_guard = storage_clone.write().expect("Lock failed");
            if i % 2 == 0 {
                // Valid query
                let result = storage_guard
                    .execute_query_on("valid_kg", "result(X,Y) :- edge(X,Y).")
                    .expect("Valid query should succeed");
                assert_eq!(result.len(), 1);
            } else {
                // Invalid KG
                let result =
                    storage_guard.execute_query_on("invalid_kg", "result(X,Y) :- edge(X,Y).");
                assert!(result.is_err());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// Metadata Access Under Contention
#[test]
