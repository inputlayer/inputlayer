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
    let storage_guard = storage.write().unwrap();
    let results = storage_guard
        .execute_query_on("insert_test", "result(X,Y) :- data(X,Y).")
        .unwrap();
    assert_eq!(
        results.len(),
        num_threads * inserts_per_thread,
        "Expected {} tuples, got {}",
        num_threads * inserts_per_thread,
        results.len()
    );
}

#[test]
