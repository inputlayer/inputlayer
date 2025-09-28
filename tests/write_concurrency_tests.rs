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
