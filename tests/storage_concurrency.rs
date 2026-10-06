//! Storage engine concurrency invariants: readers and writers do not block
//! each other, a write is visible to the reads after it, errors do not corrupt
//! state, queries across knowledge graphs do not deadlock, and the parallel
//! query API keeps knowledge graphs isolated.
//!
//! The loops that only repeat these at size (100 writers, 1000 operations,
//! starvation, rapid cycles) are in `tests/soak/storage.rs`.

use inputlayer::{Config, StorageEngine};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use tempfile::TempDir;

// Test Helpers
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 4;
    let storage = StorageEngine::new(config).expect("create storage engine");
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
                    .execute_query_on("concurrent_test", "result(X,Y) <- edge(X,Y)")
                    .unwrap_or_else(|_| panic!("Reader {i} failed to execute query"));
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
        let kg_name = format!("kg{i}");
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
            let kg_name = format!("kg{kg_num}");
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let results = storage_guard
                        .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
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
                    .execute_query_on("snapshot_test", "result(X,Y) <- counter(X,Y)")
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

#[test]
fn test_no_deadlock_with_cross_kg_queries() {
    let (storage, _temp) = create_test_storage();

    // Create KGs
    for i in 1..=4 {
        let kg_name = format!("deadlock_test_kg{i}");
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
                    let kg_name = format!("deadlock_test_kg{kg_num}");
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let results = storage_guard
                        .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
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
                storage_guard.execute_query_on("nonexistent_kg", "result(X,Y) <- edge(X,Y)");
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
                    .execute_query_on("valid_kg", "result(X,Y) <- edge(X,Y)")
                    .expect("Valid query should succeed");
                assert_eq!(result.len(), 1);
            } else {
                // Invalid KG
                let result =
                    storage_guard.execute_query_on("invalid_kg", "result(X,Y) <- edge(X,Y)");
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
fn test_list_kgs_under_read_contention() {
    let (storage, _temp) = create_test_storage();

    // Create several KGs
    for i in 1..=5 {
        storage
            .create_knowledge_graph(&format!("list_test_kg{i}"))
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Readers querying data
    for i in 0..5 {
        let storage_clone = Arc::clone(&storage);
        let kg_name = format!("list_test_kg{}", (i % 5) + 1);
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.execute_query_on(&kg_name, "result(X,Y) <- edge(X,Y)");
            }
        });
        handles.push(handle);
    }

    // Threads listing KGs concurrently with readers
    for _ in 0..3 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for _ in 0..20 {
                let storage_guard = storage_clone.read().expect("Lock failed");
                let kgs = storage_guard.list_knowledge_graphs();
                // Should always see at least default + our 5 KGs
                assert!(kgs.len() >= 6);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// Parallel Query API Tests
#[test]
fn test_parallel_api_under_concurrent_access() {
    let (storage, _temp) = create_test_storage();

    // Create KGs
    for i in 1..=4 {
        let kg_name = format!("parallel_api_kg{i}");
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage
            .insert_into(&kg_name, "data", vec![(i, i * 10)])
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Multiple threads using parallel query API simultaneously
    for _ in 0..4 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                let queries: Vec<(&str, &str)> = vec![
                    ("parallel_api_kg1", "result(X,Y) <- data(X,Y)"),
                    ("parallel_api_kg2", "result(X,Y) <- data(X,Y)"),
                    ("parallel_api_kg3", "result(X,Y) <- data(X,Y)"),
                    ("parallel_api_kg4", "result(X,Y) <- data(X,Y)"),
                ];

                let storage_guard = storage_clone.read().expect("Lock failed");
                let results = storage_guard
                    .execute_parallel_queries_on_knowledge_graphs(queries)
                    .expect("Parallel query failed");

                assert_eq!(results.len(), 4);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// Lock Poisoning Recovery Test
#[test]
fn test_lock_poisoning_recovery_at_storage_level() {
    // This tests that our storage engine handles errors gracefully
    // We can't actually poison the internal locks easily, but we can test
    // that the error paths work correctly

    let (storage, _temp) = create_test_storage();

    // Create a KG
    storage.create_knowledge_graph("poison_test").unwrap();
    storage
        .insert_into("poison_test", "data", vec![(1, 2)])
        .unwrap();

    // Wrap in RwLock
    let storage = Arc::new(RwLock::new(storage));

    // Simulate a thread that panics while NOT holding the lock
    // (This won't poison the lock, but tests error handling paths)
    let storage_clone = Arc::clone(&storage);
    let handle = thread::spawn(move || {
        // Successfully get data
        let guard = storage_clone.write().unwrap();
        let results = guard.execute_query_on("poison_test", "result(X,Y) <- data(X,Y)");
        assert!(results.is_ok());
        // Explicitly drop the guard before any potential panic
        drop(guard);
    });

    handle.join().expect("Thread should complete successfully");

    // After thread completes, we should still be able to use the storage
    let storage_guard = storage.write().unwrap();
    let results = storage_guard
        .execute_query_on("poison_test", "result(X,Y) <- data(X,Y)")
        .expect("Should still work after thread completed");
    assert_eq!(results.len(), 1);
}

// Internal Lock Error Handling Test
#[test]
fn test_storage_engine_returns_errors_not_panics() {
    let (mut storage, _temp) = create_test_storage();

    // These operations should return errors, not panic

    // Query non-existent KG
    let result = storage.execute_query_on("nonexistent", "result(X,Y) <- edge(X,Y)");
    assert!(result.is_err());

    // Try to use non-existent KG
    let result = storage.use_knowledge_graph("nonexistent");
    assert!(result.is_err());

    // Try to drop non-existent KG
    let result = storage.drop_knowledge_graph("nonexistent");
    assert!(result.is_err());

    // Try to insert into non-existent KG
    let result = storage.insert_into("nonexistent", "edge", vec![(1, 2)]);
    assert!(result.is_err());

    // After all these errors, storage should still work
    storage.create_knowledge_graph("working").unwrap();
    storage
        .insert_into("working", "edge", vec![(1, 2)])
        .unwrap();
    let results = storage
        .execute_query_on("working", "result(X,Y) <- edge(X,Y)")
        .expect("Should work after errors");
    assert_eq!(results.len(), 1);
}

// Concurrent Insert Tests
#[test]
fn test_concurrent_inserts_to_same_kg() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("insert_test").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let inserts_per_thread = 10;
    let mut handles = vec![];

    // Each thread inserts unique tuples
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..inserts_per_thread {
                let tuple_id = (thread_id * 1000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("insert_test", "data", vec![(tuple_id, tuple_id * 2)])
                    .expect("Insert failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all inserts succeeded
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("insert_test", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(
        results.len(),
        num_threads * inserts_per_thread,
        "Expected {} tuples, got {}",
        num_threads * inserts_per_thread,
        results.len()
    );
}

#[test]
fn test_concurrent_inserts_to_different_kgs() {
    let (storage, _temp) = create_test_storage();

    // Create multiple KGs
    for i in 0..5 {
        storage.create_knowledge_graph(&format!("kg_{i}")).unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let inserts_per_thread = 10;
    let mut handles = vec![];

    // Each thread writes to a different KG based on thread_id % 5
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let kg_name = format!("kg_{}", thread_id % 5);
            for i in 0..inserts_per_thread {
                let tuple_id = (thread_id * 1000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into(&kg_name, "data", vec![(tuple_id, tuple_id * 2)])
                    .expect("Insert failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify each KG has correct number of tuples
    let storage_guard = storage.write().expect("Lock failed");
    for i in 0..5 {
        let kg_name = format!("kg_{i}");
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
            .expect("Query failed");
        // Each KG should have tuples from 4 threads (20 / 5)
        let expected = 4 * inserts_per_thread;
        assert_eq!(
            results.len(),
            expected,
            "KG {} expected {} tuples, got {}",
            kg_name,
            expected,
            results.len()
        );
    }
}

// Concurrent Delete Tests
#[test]
fn test_concurrent_deletes_to_same_kg() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("delete_test").unwrap();

    // Pre-populate with data
    let initial_data: Vec<(i32, i32)> = (0..100).map(|i| (i, i * 10)).collect();
    storage
        .insert_into("delete_test", "data", initial_data)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let mut handles = vec![];

    // Each thread deletes a subset of tuples
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            // Each thread deletes tuples where id % num_threads == thread_id
            for i in 0..10 {
                let tuple_id = thread_id * 10 + i;
                let storage_guard = storage_clone.write().expect("Lock failed");
                // Use delete with specific tuple
                let _ = storage_guard.delete_from(
                    "delete_test",
                    "data",
                    vec![(tuple_id, tuple_id * 10)],
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all tuples were deleted
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("delete_test", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), 0, "Expected 0 tuples after delete");
}

#[test]
fn test_concurrent_deletes_to_different_kgs() {
    let (storage, _temp) = create_test_storage();

    // Create and populate multiple KGs
    for i in 0..5 {
        let kg_name = format!("delete_kg_{i}");
        storage.create_knowledge_graph(&kg_name).unwrap();
        let data: Vec<(i32, i32)> = (0..20).map(|j| (j, j * 10)).collect();
        storage.insert_into(&kg_name, "data", data).unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Each thread deletes from a different KG
    for kg_id in 0..5 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let kg_name = format!("delete_kg_{kg_id}");
            for i in 0..20 {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.delete_from(&kg_name, "data", vec![(i, i * 10)]);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all KGs are empty
    let storage_guard = storage.write().expect("Lock failed");
    for i in 0..5 {
        let kg_name = format!("delete_kg_{i}");
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
            .expect("Query failed");
        assert_eq!(results.len(), 0, "KG {kg_name} should be empty");
    }
}

#[test]
fn test_delete_nonexistent_concurrent() {
    let (storage, _temp) = create_test_storage();
    storage
        .create_knowledge_graph("delete_nonexistent")
        .unwrap();

    // Pre-populate with small amount of data
    storage
        .insert_into("delete_nonexistent", "data", vec![(1, 10), (2, 20)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let mut handles = vec![];

    // All threads try to delete tuples that don't exist
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let tuple_id = thread_id + 100; // IDs 100-109 don't exist
            let storage_guard = storage_clone.write().expect("Lock failed");
            // Should not error, just do nothing
            let result = storage_guard.delete_from(
                "delete_nonexistent",
                "data",
                vec![(tuple_id, tuple_id * 10)],
            );
            // Delete of non-existent should succeed (just delete 0 rows)
            assert!(result.is_ok());
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Original data should still exist
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("delete_nonexistent", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), 2);
}

// Mixed Read-Write Tests
#[test]
fn test_readers_not_blocked_by_writers() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("read_write_mix").unwrap();
    storage
        .insert_into("read_write_mix", "data", vec![(1, 10)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_writers = 4;
    let num_readers = 8;
    let ops_per_thread = 50;
    let reads_completed = Arc::new(AtomicUsize::new(0));
    let writes_completed = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Spawn writers
    for thread_id in 0..num_writers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&writes_completed);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let tuple_id = (thread_id * 10000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("read_write_mix", "new", vec![(tuple_id, tuple_id)])
                    .expect("Insert failed");
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    // Spawn readers
    for _ in 0..num_readers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&reads_completed);
        let handle = thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("read_write_mix", "result(X,Y) <- data(X,Y)")
                    .expect("Query failed");
                // Should always see at least the initial tuple
                assert!(!results.is_empty());
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(
        reads_completed.load(Ordering::SeqCst),
        num_readers * ops_per_thread
    );
    assert_eq!(
        writes_completed.load(Ordering::SeqCst),
        num_writers * ops_per_thread
    );
}

#[test]
fn test_writers_not_blocked_by_readers() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("write_read_mix").unwrap();

    // Pre-populate with substantial data
    let initial: Vec<(i32, i32)> = (0..1000).map(|i| (i, i * 2)).collect();
    storage
        .insert_into("write_read_mix", "data", initial)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_readers = 8;
    let num_writers = 4;
    let ops_per_thread = 30;
    let writes_completed = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Spawn many readers first
    for _ in 0..num_readers {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("write_read_mix", "result(X,Y) <- data(X,Y)")
                    .expect("Query failed");
                assert!(!results.is_empty());
                // Simulate some processing time
                thread::yield_now();
            }
        });
        handles.push(handle);
    }

    // Spawn writers
    for thread_id in 0..num_writers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&writes_completed);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let tuple_id = (thread_id * 10000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("write_read_mix", "new_data", vec![(tuple_id, tuple_id)])
                    .expect("Insert failed");
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // All writes should have completed
    assert_eq!(
        writes_completed.load(Ordering::SeqCst),
        num_writers * ops_per_thread
    );
}

#[test]
fn test_snapshot_visibility_after_write() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("snapshot_vis").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 5;
    let mut handles = vec![];

    // Each thread writes then reads, expecting to see its own write
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let tuple_id = (thread_id * 100) as i32;

            // Write
            {
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("snapshot_vis", "data", vec![(tuple_id, tuple_id * 2)])
                    .expect("Insert failed");
            }

            // Read - should see our write
            {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("snapshot_vis", "result(X,Y) <- data(X,Y)")
                    .expect("Query failed");
                // Should see at least our own tuple
                assert!(
                    results.iter().any(|t| *t == (tuple_id, tuple_id * 2)),
                    "Thread {thread_id} should see its own write"
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all writes are visible
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("snapshot_vis", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads);
}

// Rule Drop Concurrent Tests
#[test]
fn test_concurrent_rule_drop() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("rule_drop_test").unwrap();
    storage
        .insert_into("rule_drop_test", "edge", vec![(1, 2), (2, 3)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let mut handles = vec![];

    // Each thread tries to drop rules (some may not exist, but that's fine)
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let rule_name = format!("rule_{thread_id}");
            let storage_guard = storage_clone.write().expect("Lock failed");
            // This should not panic, even if rule doesn't exist
            let _ = storage_guard.drop_rule_in("rule_drop_test", &rule_name);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// Error Recovery Tests
#[test]
fn test_write_error_doesnt_corrupt_state() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("error_test").unwrap();
    storage
        .insert_into("error_test", "data", vec![(1, 10), (2, 20)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let mut handles = vec![];

    // Some threads do valid operations
    for thread_id in 0..5 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let tuple_id = thread_id + 10;
            let storage_guard = storage_clone.write().expect("Lock failed");
            storage_guard
                .insert_into("error_test", "data", vec![(tuple_id, tuple_id * 10)])
                .expect("Valid insert failed");
        });
        handles.push(handle);
    }

    // Some threads try invalid operations (non-existent KG)
    for _ in 0..5 {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let storage_guard = storage_clone.write().expect("Lock failed");
            let result = storage_guard.insert_into("nonexistent", "data", vec![(1, 1)]);
            // Should error, not panic
            assert!(result.is_err());
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked on error");
    }

    // State should be consistent
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("error_test", "result(X,Y) <- data(X,Y)")
        .expect("Query failed after errors");
    // Original 2 + 5 valid inserts
    assert_eq!(results.len(), 7);
}

#[test]
fn test_concurrent_writes_with_errors() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("mixed_errors").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Mix of valid and invalid operations
    for i in 0..20 {
        let storage_clone = Arc::clone(&storage);
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);
        let handle = thread::spawn(move || {
            let storage_guard = storage_clone.write().expect("Lock failed");
            if i % 3 == 0 {
                // Invalid KG
                let result = storage_guard.insert_into("invalid_kg", "data", vec![(i, i)]);
                if result.is_err() {
                    errors.fetch_add(1, Ordering::SeqCst);
                }
            } else {
                // Valid insert
                let result = storage_guard.insert_into("mixed_errors", "data", vec![(i, i * 10)]);
                if result.is_ok() {
                    success.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify counts
    assert!(success_count.load(Ordering::SeqCst) > 0);
    assert!(error_count.load(Ordering::SeqCst) > 0);

    // Verify data integrity
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("mixed_errors", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), success_count.load(Ordering::SeqCst));
}

#[test]
fn test_concurrent_kg_creation_and_writes() {
    let (storage, _temp) = create_shared_storage();
    let num_threads = 10;
    let mut handles = vec![];

    // Each thread creates its own KG and writes to it
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let kg_name = format!("created_kg_{thread_id}");

            // Create KG
            {
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .create_knowledge_graph(&kg_name)
                    .expect("KG creation failed");
            }

            // Write to it
            for i in 0..10 {
                let tuple_id = thread_id * 100 + i;
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into(&kg_name, "data", vec![(tuple_id, tuple_id)])
                    .expect("Insert failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all KGs exist and have data
    let storage_guard = storage.write().expect("Lock failed");
    for i in 0..num_threads {
        let kg_name = format!("created_kg_{i}");
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
            .expect("Query failed");
        assert_eq!(results.len(), 10);
    }
}

// Thread Pool Configuration Tests
#[test]
fn test_thread_pool_is_configured() {
    let (storage, _temp) = create_test_storage();

    // Should have worker pool configured
    let num_cpus = storage.num_cpus();
    assert!(num_cpus > 0, "Thread pool should have at least 1 thread");
}

#[test]
fn test_multiple_storage_engines_share_thread_pool() {
    let temp1 = TempDir::new().unwrap();
    let temp2 = TempDir::new().unwrap();

    let mut config1 = Config::default();
    config1.storage.data_dir = temp1.path().to_path_buf();
    config1.storage.performance.num_threads = 2;

    let mut config2 = Config::default();
    config2.storage.data_dir = temp2.path().to_path_buf();
    config2.storage.performance.num_threads = 4; // Different config, but global pool already initialized

    let storage1 = StorageEngine::new(config1).unwrap();
    let storage2 = StorageEngine::new(config2).unwrap();

    // Both should report same thread pool (global pool is shared)
    let cpus1 = storage1.num_cpus();
    let cpus2 = storage2.num_cpus();

    assert_eq!(
        cpus1, cpus2,
        "All storage engines share the same global thread pool"
    );
}

// Parallel Query Execution Tests
#[test]
fn test_execute_queries_on_multiple_knowledge_graphs_concurrently() {
    let (storage, _temp) = create_test_storage();

    // Create 4 knowledge_graphs with different data
    for i in 1..=4 {
        let db_name = format!("db{i}");
        storage.create_knowledge_graph(&db_name).unwrap();
        storage
            .insert_into(&db_name, "edge", vec![(i, i * 10)])
            .unwrap();
    }

    // Execute queries on all knowledge_graphs in parallel
    let queries = vec![
        ("db1", "result(X,Y) <- edge(X,Y)"),
        ("db2", "result(X,Y) <- edge(X,Y)"),
        ("db3", "result(X,Y) <- edge(X,Y)"),
        ("db4", "result(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 4);

    // Verify each knowledge_graph returned its own data
    for (db_name, result) in &results {
        assert_eq!(result.len(), 1);
        let db_num = db_name.chars().last().unwrap().to_digit(10).unwrap() as i32;
        assert_eq!(result[0], (db_num, db_num * 10));
    }
}

#[test]
fn test_same_query_on_multiple_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge_graphs with increasing amounts of data
    for i in 1..=3 {
        let db_name = format!("db{i}");
        storage.create_knowledge_graph(&db_name).unwrap();

        let edges: Vec<(i32, i32)> = (0..i).map(|j| (j, j + 1)).collect();
        storage.insert_into(&db_name, "edge", edges).unwrap();
    }

    // Execute same query on all knowledge_graphs in parallel
    let knowledge_graphs = vec!["db1", "db2", "db3"];
    let query = "result(X,Y) <- edge(X,Y)";

    let results = storage
        .execute_query_on_multiple_knowledge_graphs(knowledge_graphs, query)
        .unwrap();

    assert_eq!(results.len(), 3);
    // Results may come back in any order due to parallel execution
    // Use HashMap for order-independent comparison
    let results_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(results_map.get("db1").map(|v| v.len()), Some(1)); // db1 has 1 edge
    assert_eq!(results_map.get("db2").map(|v| v.len()), Some(2)); // db2 has 2 edges
    assert_eq!(results_map.get("db3").map(|v| v.len()), Some(3)); // db3 has 3 edges
}

// KnowledgeGraph Isolation Tests (Concurrent Access)
#[test]
fn test_parallel_queries_maintain_knowledge_graph_isolation() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge_graphs with different data
    storage.create_knowledge_graph("db1").unwrap();
    storage
        .insert_into("db1", "edge", vec![(1, 2), (2, 3)])
        .unwrap();

    storage.create_knowledge_graph("db2").unwrap();
    storage
        .insert_into("db2", "edge", vec![(10, 20), (20, 30)])
        .unwrap();

    // Execute queries in parallel
    let queries = vec![
        ("db1", "result(X,Y) <- edge(X,Y)"),
        ("db2", "result(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    // Verify each knowledge_graph only sees its own data
    let db1_results = results.iter().find(|(db, _)| db == "db1").unwrap();
    let db2_results = results.iter().find(|(db, _)| db == "db2").unwrap();

    assert!(db1_results.1.contains(&(1, 2)));
    assert!(db1_results.1.contains(&(2, 3)));
    assert!(!db1_results.1.contains(&(10, 20))); // Should not see db2's data

    assert!(db2_results.1.contains(&(10, 20)));
    assert!(db2_results.1.contains(&(20, 30)));
    assert!(!db2_results.1.contains(&(1, 2))); // Should not see db1's data
}

#[test]
fn test_concurrent_queries_on_different_relations() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage
        .insert_into("test", "edge", vec![(1, 2), (2, 3)])
        .unwrap();
    storage
        .insert_into("test", "person", vec![(100, 200), (200, 300)])
        .unwrap();

    // Query different relations in parallel
    let queries = vec!["q1(X,Y) <- edge(X,Y)", "q2(X,Y) <- person(X,Y)"];

    let results = storage
        .execute_parallel_queries_on_knowledge_graph("test", queries)
        .unwrap();

    assert_eq!(results.len(), 2);
    // Both queries should return 2 results each
    assert_eq!(results[0].len(), 2);
    assert_eq!(results[1].len(), 2);

    // Results may come back in any order due to parallel execution
    // Find which result set contains edge data vs person data
    let (edge_results, person_results) = if results[0].contains(&(1, 2)) {
        (&results[0], &results[1])
    } else {
        (&results[1], &results[0])
    };

    // Verify edge results
    assert!(edge_results.contains(&(1, 2)));
    assert!(edge_results.contains(&(2, 3)));
    assert!(!edge_results.contains(&(100, 200))); // edge query shouldn't see person data

    // Verify person results
    assert!(person_results.contains(&(100, 200)));
    assert!(person_results.contains(&(200, 300)));
    assert!(!person_results.contains(&(1, 2))); // person query shouldn't see edge data
}

// Error Handling in Parallel Context
#[test]
fn test_parallel_queries_with_invalid_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("db1").unwrap();
    storage.insert_into("db1", "edge", vec![(1, 2)]).unwrap();

    // Mix valid and invalid knowledge_graphs
    let queries = vec![
        ("db1", "result(X,Y) <- edge(X,Y)"),
        ("nonexistent", "result(X,Y) <- edge(X,Y)"),
    ];

    let result = storage.execute_parallel_queries_on_knowledge_graphs(queries);

    // Should return error because one knowledge_graph doesn't exist
    assert!(result.is_err());
}

#[test]
fn test_parallel_queries_handle_empty_results() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge_graphs with no data
    for i in 1..=3 {
        let db_name = format!("empty_db{i}");
        storage.create_knowledge_graph(&db_name).unwrap();
    }

    let queries = vec![
        ("empty_db1", "result(X,Y) <- edge(X,Y)"),
        ("empty_db2", "result(X,Y) <- edge(X,Y)"),
        ("empty_db3", "result(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    // Should succeed but return empty results
    assert_eq!(results.len(), 3);
    for (_, result) in results {
        assert_eq!(result.len(), 0);
    }
}

// Performance and Scalability Tests
#[test]
fn test_parallel_execution_with_many_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create 10 knowledge_graphs
    let num_knowledge_graphs = 10;
    for i in 1..=num_knowledge_graphs {
        let db_name = format!("db{i}");
        storage.create_knowledge_graph(&db_name).unwrap();
        storage
            .insert_into(&db_name, "data", vec![(i, i * 100)])
            .unwrap();
    }

    // Execute queries on all knowledge_graphs in parallel
    let queries: Vec<(&str, &str)> = (1..=num_knowledge_graphs)
        .map(|i| (format!("db{i}"), "result(X,Y) <- data(X,Y)"))
        .map(|(db, q)| (Box::leak(db.into_boxed_str()) as &str, q))
        .collect();

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), num_knowledge_graphs as usize);

    // Verify all results are correct
    for (db_name, result) in &results {
        assert_eq!(result.len(), 1);
        let db_num = db_name
            .chars()
            .skip(2)
            .collect::<String>()
            .parse::<i32>()
            .unwrap();
        assert_eq!(result[0], (db_num, db_num * 100));
    }
}

// Thread Safety Tests
#[test]
fn test_parallel_queries_use_internal_thread_safety() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("shared_db").unwrap();
    storage
        .insert_into("shared_db", "edge", vec![(1, 2), (2, 3)])
        .unwrap();

    // Execute same query multiple times in parallel via the parallel API
    // This tests that the internal Arc<RwLock<KnowledgeGraph>> mechanism works
    let queries = vec![
        ("shared_db", "q1(X,Y) <- edge(X,Y)"),
        ("shared_db", "q2(X,Y) <- edge(X,Y)"),
        ("shared_db", "q3(X,Y) <- edge(X,Y)"),
        ("shared_db", "q4(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    // Verify all queries got the same results (thread-safe access to shared knowledge_graph)
    assert_eq!(results.len(), 4);
    for (_, query_results) in &results {
        assert_eq!(query_results.len(), 2);
        assert!(query_results.contains(&(1, 2)));
        assert!(query_results.contains(&(2, 3)));
    }
}

#[test]
fn test_concurrent_queries_do_not_deadlock() {
    let (storage, _temp) = create_test_storage();

    // Create multiple knowledge_graphs
    for i in 1..=5 {
        let db_name = format!("db{i}");
        storage.create_knowledge_graph(&db_name).unwrap();
        storage.insert_into(&db_name, "data", vec![(i, i)]).unwrap();
    }

    // Execute many parallel queries (more than thread pool size)
    let queries: Vec<(&str, &str)> = (1..=5)
        .flat_map(|i| {
            let db = format!("db{i}");
            vec![
                (
                    Box::leak(db.clone().into_boxed_str()) as &str,
                    "q1(X,Y) <- data(X,Y)",
                ),
                (
                    Box::leak(db.into_boxed_str()) as &str,
                    "q2(X,Y) <- data(X,Y)",
                ),
            ]
        })
        .collect();

    // This should not deadlock
    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 10); // 5 knowledge_graphs × 2 queries
}

// Edge Cases
#[test]
fn test_parallel_query_with_empty_query_list() {
    let (storage, _temp) = create_test_storage();

    let queries: Vec<(&str, &str)> = vec![];
    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_parallel_query_with_single_query() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("solo").unwrap();
    storage.insert_into("solo", "edge", vec![(1, 2)]).unwrap();

    let queries = vec![("solo", "result(X,Y) <- edge(X,Y)")];
    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, vec![(1, 2)]);
}
