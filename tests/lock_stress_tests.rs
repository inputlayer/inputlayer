//! Lock contention and concurrent KG stress tests.

use inputlayer::{Config, StorageEngine};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, Instant};
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

// Lock Contention Tests
#[test]
fn test_100_concurrent_readers_during_write() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("reader_stress").unwrap();

    // Pre-populate with data
    let data: Vec<(i32, i32)> = (0..100).map(|i| (i, i * 10)).collect();
    storage.insert_into("reader_stress", "data", data).unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_readers = 100;
    let num_writers = 5;
    let reads_completed = Arc::new(AtomicUsize::new(0));
    let writes_completed = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));
    let mut handles = vec![];

    // Spawn many concurrent readers
    for _ in 0..num_readers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&reads_completed);
        let barrier_clone = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier_clone.wait(); // Synchronize start
            for _ in 0..10 {
                let storage_guard = storage_clone.write().unwrap();
                let results = storage_guard
                    .execute_query_on("reader_stress", "result(X,Y) :- data(X,Y).")
                    .expect("Query failed");
                // Should always see data (at least initial 100)
                assert!(results.len() >= 100);
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    // Spawn writers during heavy read load
    for writer_id in 0..num_writers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&writes_completed);
        let barrier_clone = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier_clone.wait(); // Synchronize start
            for i in 0..20 {
                let tuple_id = (writer_id * 10000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("reader_stress", "new_data", vec![(tuple_id, tuple_id)])
                    .unwrap();
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(reads_completed.load(Ordering::SeqCst), num_readers * 10);
    assert_eq!(writes_completed.load(Ordering::SeqCst), num_writers * 20);
}

#[test]
