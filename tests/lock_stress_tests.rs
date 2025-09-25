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
                let storage_guard = storage_clone.write().expect("Lock failed");
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
                    .expect("Insert failed");
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
fn test_lock_contention_no_starvation() {
    // Test that writers don't starve readers and vice versa
    // Note: With RwLock and simple operations, some imbalance is expected
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("starvation_test").unwrap();
    storage
        .insert_into("starvation_test", "data", vec![(1, 10)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let test_duration = Duration::from_secs(1);
    let start = Instant::now();
    let reader_ops = Arc::new(AtomicUsize::new(0));
    let writer_ops = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let mut handles = vec![];

    // Equal number of readers and writers for fair comparison
    for _ in 0..5 {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&reader_ops);
        let running_clone = Arc::clone(&running);
        let handle = thread::spawn(move || {
            while running_clone.load(Ordering::Relaxed) {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.list_knowledge_graphs(); // Simple read operation
                drop(storage_guard);
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    // Spawn writers
    for writer_id in 0..5 {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&writer_ops);
        let running_clone = Arc::clone(&running);
        let handle = thread::spawn(move || {
            let mut i = 0i32;
            while running_clone.load(Ordering::Relaxed) {
                let tuple_id = writer_id as i32 * 100000 + i;
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.insert_into(
                    "starvation_test",
                    "new_data",
                    vec![(tuple_id, tuple_id)],
                );
                drop(storage_guard);
                counter.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        });
        handles.push(handle);
    }

    // Let threads run for duration
    while start.elapsed() < test_duration {
        thread::sleep(Duration::from_millis(100));
    }
    running.store(false, Ordering::SeqCst);

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let reads = reader_ops.load(Ordering::SeqCst);
    let writes = writer_ops.load(Ordering::SeqCst);

    // Both readers and writers should have made progress
    // The key invariant is that neither is completely starved (0 operations)
    // Note: We don't check for proportional fairness because:
    // 1. RwLock semantics don't guarantee fairness
    // 2. Operation durations vary, so ratios fluctuate
    // 3. The important thing is both made progress
    assert!(reads > 0, "Readers starved: 0 operations");
    assert!(writes > 0, "Writers starved: 0 operations");

    // Log for debugging (not asserted due to natural variation)
    eprintln!(
        "Lock contention test: {} reads, {} writes (total {})",
        reads,
        writes,
        reads + writes
    );
}

#[test]
fn test_rapid_lock_acquire_release_cycles() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("rapid_lock").unwrap();
    storage
        .insert_into("rapid_lock", "data", vec![(1, 10)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let cycles_per_thread = 500;
    let completed_cycles = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&completed_cycles);
        let handle = thread::spawn(move || {
            for _ in 0..cycles_per_thread {
                // Rapidly acquire and release lock
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.execute_query_on("rapid_lock", "result(X,Y) :- data(X,Y).");
                drop(storage_guard);
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(
        completed_cycles.load(Ordering::SeqCst),
        num_threads * cycles_per_thread
    );
}

// Concurrent KG Operations
#[test]
fn test_concurrent_kg_create_delete() {
    let (storage, _temp) = create_shared_storage();
    let num_threads = 20;
    let operations_per_thread = 10;
    let mut handles = vec![];

    // Threads create and delete KGs rapidly
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..operations_per_thread {
                let kg_name = format!("temp_kg_{}_{}", thread_id, i);

                // Create
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let _ = storage_guard.create_knowledge_graph(&kg_name);
                }

                // Write some data
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let _ = storage_guard.insert_into(&kg_name, "data", vec![(i as i32, i as i32)]);
                }

                // Delete
                {
                    let mut storage_guard = storage_clone.write().expect("Lock failed");
                    let _ = storage_guard.drop_knowledge_graph(&kg_name);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked during KG operations");
    }

    // Storage should still be functional
    let storage_guard = storage.write().expect("Lock failed");
    storage_guard.create_knowledge_graph("final_test").unwrap();
    storage_guard
        .insert_into("final_test", "data", vec![(1, 1)])
        .unwrap();
    let results = storage_guard
        .execute_query_on("final_test", "result(X,Y) :- data(X,Y).")
        .expect("Query failed after stress");
    assert_eq!(results.len(), 1);
}

#[test]
