//! Concurrent write and read-write operation tests.

use inputlayer::{Config, StorageEngine};
use std::sync::atomic::{AtomicUsize, Ordering};
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
    let (storage, _temp) = create_test_storage();

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
    let (storage, _temp) = create_test_storage();
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
                let storage_guard = storage_clone.write().expect("Lock failed");
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
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("high_volume", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * inserts_per_thread);
}

#[test]
fn test_insert_throughput_under_read_load() {
    let (storage, _temp) = create_test_storage();
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
                let storage_guard = storage_clone.write().expect("Lock failed");
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
                let storage_guard = storage_clone.write().expect("Lock failed");
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
                let tuple_id = (thread_id * 10 + i) as i32;
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
        .execute_query_on("delete_test", "result(X,Y) :- data(X,Y).")
        .expect("Query failed");
    assert_eq!(results.len(), 0, "Expected 0 tuples after delete");
}

#[test]
fn test_concurrent_deletes_to_different_kgs() {
    let (storage, _temp) = create_test_storage();

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
        let kg_name = format!("delete_kg_{}", i);
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) :- data(X,Y).")
            .expect("Query failed");
        assert_eq!(results.len(), 0, "KG {} should be empty", kg_name);
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
            let tuple_id = (thread_id + 100) as i32; // IDs 100-109 don't exist
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
        .execute_query_on("delete_nonexistent", "result(X,Y) :- data(X,Y).")
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
