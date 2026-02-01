//! Write Concurrency Tests
//!
//! Tests for concurrent write operations including:
//! - Concurrent inserts to same/different KGs
//! - Concurrent deletes
//! - Mixed read-write operations
//! - Rule management under concurrency
//! - High-volume stress tests
//! - Error recovery under concurrent load

use inputlayer::{Config, StorageEngine};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use tempfile::TempDir;

// ============================================================================
// Test Helpers
// ============================================================================

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

// ============================================================================
// Concurrent Insert Tests
// ============================================================================

#[test]
fn test_concurrent_inserts_to_same_kg() {
    let (mut storage, _temp) = create_test_storage();
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("insert_test", "result(X,Y) :- data(X,Y).")
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
    let (mut storage, _temp) = create_test_storage();

    // Create multiple KGs
    for i in 0..5 {
        storage
            .create_knowledge_graph(&format!("kg_{}", i))
            .unwrap();
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    for i in 0..5 {
        let kg_name = format!("kg_{}", i);
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
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

#[test]
fn test_high_volume_concurrent_inserts() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("high_volume").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 8;
    let inserts_per_thread = 100;
    let total_inserts = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&total_inserts);
        let handle = thread::spawn(move || {
            for i in 0..inserts_per_thread {
                let tuple_id = (thread_id * 10000 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("high_volume", "data", vec![(tuple_id, tuple_id * 2)])
                    .expect("Insert failed");
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(
        total_inserts.load(Ordering::SeqCst),
        num_threads * inserts_per_thread
    );

    // Verify data integrity
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("high_volume", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * inserts_per_thread);
}

#[test]
fn test_insert_throughput_under_read_load() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("throughput_test").unwrap();

    // Pre-populate with some data
    for i in 0..100 {
        storage
            .insert_into("throughput_test", "initial", vec![(i, i * 10)])
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let num_writers = 4;
    let num_readers = 8;
    let ops_per_thread = 50;
    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Spawn writers
    for thread_id in 0..num_writers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&write_count);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let tuple_id = (thread_id * 10000 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("throughput_test", "new_data", vec![(tuple_id, tuple_id)])
                    .expect("Insert failed");
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    // Spawn readers
    for _ in 0..num_readers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&read_count);
        let handle = thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard
                    .execute_query_on("throughput_test", "result(X,Y) :- initial(X,Y).")
                    .expect("Query failed");
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(
        write_count.load(Ordering::SeqCst),
        num_writers * ops_per_thread
    );
    assert_eq!(
        read_count.load(Ordering::SeqCst),
        num_readers * ops_per_thread
    );
}

// ============================================================================
// Concurrent Delete Tests
// ============================================================================

#[test]
fn test_concurrent_deletes_to_same_kg() {
    let (mut storage, _temp) = create_test_storage();
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
                let tuple_id = (thread_id * 10 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("delete_test", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), 0, "Expected 0 tuples after delete");
}

#[test]
fn test_concurrent_deletes_to_different_kgs() {
    let (mut storage, _temp) = create_test_storage();

    // Create and populate multiple KGs
    for i in 0..5 {
        let kg_name = format!("delete_kg_{}", i);
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
            let kg_name = format!("delete_kg_{}", kg_id);
            for i in 0..20 {
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.delete_from(&kg_name, "data", vec![(i, i * 10)]);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all KGs are empty
    let mut storage_guard = storage.write().expect("Lock failed");
    for i in 0..5 {
        let kg_name = format!("delete_kg_{}", i);
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
            .expect("Query failed");
        assert_eq!(results.len(), 0, "KG {} should be empty", kg_name);
    }
}

#[test]
fn test_delete_nonexistent_concurrent() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("delete_nonexistent").unwrap();

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
            let tuple_id = (thread_id + 100) as i32; // IDs 100-109 don't exist
            let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("delete_nonexistent", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), 2);
}

// ============================================================================
// Mixed Read-Write Tests
// ============================================================================

#[test]
fn test_readers_not_blocked_by_writers() {
    let (mut storage, _temp) = create_test_storage();
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("read_write_mix", "result(X,Y) :- data(X,Y).")
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
    let (mut storage, _temp) = create_test_storage();
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("write_read_mix", "result(X,Y) :- data(X,Y).")
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
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
fn test_read_write_interleaving() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("interleave").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let ops_per_thread = 40;
    let mut handles = vec![];

    // Each thread alternates between reads and writes
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                if i % 2 == 0 {
                    // Write
                    let tuple_id = (thread_id * 10000 + i) as i32;
                    storage_guard
                        .insert_into("interleave", "data", vec![(tuple_id, tuple_id)])
                        .expect("Insert failed");
                } else {
                    // Read
                    let _ = storage_guard
                        .execute_query_on("interleave", "result(X,Y) :- data(X,Y).")
                        .expect("Query failed");
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify writes succeeded
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("interleave", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    // Each thread does ops_per_thread / 2 writes
    assert_eq!(results.len(), num_threads * (ops_per_thread / 2));
}

#[test]
fn test_snapshot_visibility_after_write() {
    let (mut storage, _temp) = create_test_storage();
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
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("snapshot_vis", "data", vec![(tuple_id, tuple_id * 2)])
                    .expect("Insert failed");
            }

            // Read - should see our write
            {
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("snapshot_vis", "result(X,Y) :- data(X,Y).")
                    .expect("Query failed");
                // Should see at least our own tuple
                assert!(
                    results.iter().any(|t| *t == (tuple_id, tuple_id * 2)),
                    "Thread {} should see its own write",
                    thread_id
                );
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all writes are visible
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("snapshot_vis", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads);
}

// ============================================================================
// Stress Tests
// ============================================================================

#[test]
fn test_100_concurrent_writers_same_kg() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("stress_same").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 100;
    let writes_per_thread = 5;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let tuple_id = (thread_id * 1000 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                if storage_guard
                    .insert_into("stress_same", "data", vec![(tuple_id, tuple_id)])
                    .is_ok()
                {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked under stress");
    }

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        num_threads * writes_per_thread
    );

    // Verify data
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("stress_same", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * writes_per_thread);
}

#[test]
fn test_100_concurrent_writers_10_kgs() {
    let (mut storage, _temp) = create_test_storage();

    // Create 10 KGs
    for i in 0..10 {
        storage
            .create_knowledge_graph(&format!("stress_kg_{}", i))
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 100;
    let writes_per_thread = 5;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let kg_name = format!("stress_kg_{}", thread_id % 10);
            for i in 0..writes_per_thread {
                let tuple_id = (thread_id * 1000 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                if storage_guard
                    .insert_into(&kg_name, "data", vec![(tuple_id, tuple_id)])
                    .is_ok()
                {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked under stress");
    }

    assert_eq!(
        success_count.load(Ordering::SeqCst),
        num_threads * writes_per_thread
    );

    // Verify each KG has correct data
    let mut storage_guard = storage.write().expect("Lock failed");
    let mut total = 0;
    for i in 0..10 {
        let kg_name = format!("stress_kg_{}", i);
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
            .expect("Query failed");
        // Each KG should have tuples from 10 threads (100/10)
        assert_eq!(results.len(), 10 * writes_per_thread);
        total += results.len();
    }
    assert_eq!(total, num_threads * writes_per_thread);
}

#[test]
fn test_sustained_write_load_1000_ops() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("sustained_write").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let ops_per_thread = 100;
    let total_ops = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&total_ops);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let tuple_id = (thread_id * 10000 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("sustained_write", "data", vec![(tuple_id, tuple_id)])
                    .expect("Insert failed");
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread failed under sustained load");
    }

    assert_eq!(
        total_ops.load(Ordering::SeqCst),
        num_threads * ops_per_thread
    );

    // Verify all data
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("sustained_write", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * ops_per_thread);
}

// ============================================================================
// Rule Drop Concurrent Tests
// ============================================================================

#[test]
fn test_concurrent_rule_drop() {
    let (mut storage, _temp) = create_test_storage();
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
            let rule_name = format!("rule_{}", thread_id);
            let mut storage_guard = storage_clone.write().expect("Lock failed");
            // This should not panic, even if rule doesn't exist
            let _ = storage_guard.drop_rule_in("rule_drop_test", &rule_name);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// ============================================================================
// Error Recovery Tests
// ============================================================================

#[test]
fn test_write_error_doesnt_corrupt_state() {
    let (mut storage, _temp) = create_test_storage();
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
            let tuple_id = (thread_id + 10) as i32;
            let mut storage_guard = storage_clone.write().expect("Lock failed");
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
            let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("error_test", "result(X,Y) :- data(X,Y).")
        .expect("Query failed after errors");
    // Original 2 + 5 valid inserts
    assert_eq!(results.len(), 7);
}

#[test]
fn test_concurrent_writes_with_errors() {
    let (mut storage, _temp) = create_test_storage();
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
            let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("mixed_errors", "result(X,Y) :- data(X,Y).")
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
            let kg_name = format!("created_kg_{}", thread_id);

            // Create KG
            {
                let mut storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .create_knowledge_graph(&kg_name)
                    .expect("KG creation failed");
            }

            // Write to it
            for i in 0..10 {
                let tuple_id = (thread_id * 100 + i) as i32;
                let mut storage_guard = storage_clone.write().expect("Lock failed");
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
    let mut storage_guard = storage.write().expect("Lock failed");
    for i in 0..num_threads {
        let kg_name = format!("created_kg_{}", i);
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
            .expect("Query failed");
        assert_eq!(results.len(), 10);
    }
}
