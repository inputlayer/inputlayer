//! Storage engine concurrency loops: many readers and writers, sustained
//! load, starvation and rapid lock cycles. The invariants they repeat at size
//! are on the PR gate in `tests/storage_concurrency.rs`.
//!
//! Each loop runs `INPUTLAYER_SOAK_STORAGE_SCALE` times (default 10) the
//! iterations it ran on the PR gate, with the same number of threads.

use inputlayer::{Config, StorageEngine};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, RwLock};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

// Test Helpers
/// The factor on every loop's iterations, or `None` outside the soak.
fn soak_scale() -> Option<usize> {
    if std::env::var("INPUTLAYER_SOAK").as_deref() != Ok("1") {
        eprintln!("skipped: set INPUTLAYER_SOAK=1 to run the storage concurrency loops");
        return None;
    }
    Some(match std::env::var("INPUTLAYER_SOAK_STORAGE_SCALE") {
        Ok(raw) => raw
            .parse()
            .ok()
            .filter(|scale| *scale > 0)
            .unwrap_or_else(|| {
                panic!("INPUTLAYER_SOAK_STORAGE_SCALE={raw} is not a positive integer")
            }),
        Err(_) => 10,
    })
}

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

// High Contention Stress Tests
#[test]
fn test_high_contention_many_readers() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("contention_test").unwrap();

    // Insert substantial data
    let edges: Vec<(i32, i32)> = (0..100).map(|i| (i, i + 1)).collect();
    storage
        .insert_into("contention_test", "edge", edges)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let queries_per_thread = 50 * scale;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for query_num in 0..queries_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("contention_test", "result(X,Y) <- edge(X,Y)")
                    .unwrap_or_else(|_| panic!("Thread {thread_id} query {query_num} failed"));
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

// Stress Test: Sustained Load
#[test]
fn test_sustained_concurrent_load() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("sustained_test").unwrap();

    // Insert data
    let data: Vec<(i32, i32)> = (0..50).map(|i| (i, i * 2)).collect();
    storage
        .insert_into("sustained_test", "values", data)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 8;
    let iterations = 100 * scale;
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for iter in 0..iterations {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("sustained_test", "result(X,Y) <- values(X,Y)")
                    .unwrap_or_else(|_| panic!("Thread {thread_id} iteration {iter} failed"));
                assert_eq!(results.len(), 50);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread failed under sustained load");
    }
}

// Lock Contention Tests
#[test]
fn test_100_concurrent_readers_during_write() {
    let Some(scale) = soak_scale() else {
        return;
    };
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
            for _ in 0..10 * scale {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("reader_stress", "result(X,Y) <- data(X,Y)")
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
            for i in 0..20 * scale {
                let tuple_id = (writer_id * 1_000_000 + i) as i32;
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

    assert_eq!(
        reads_completed.load(Ordering::SeqCst),
        num_readers * 10 * scale
    );
    assert_eq!(
        writes_completed.load(Ordering::SeqCst),
        num_writers * 20 * scale
    );
}

#[test]
fn test_lock_contention_no_starvation() {
    let Some(scale) = soak_scale() else {
        return;
    };
    // Test that writers don't starve readers and vice versa
    // Note: With RwLock and simple operations, some imbalance is expected
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("starvation_test").unwrap();
    storage
        .insert_into("starvation_test", "data", vec![(1, 10)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let test_duration = Duration::from_secs(scale as u64);
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
                let tuple_id = writer_id * 100_000_000 + i;
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
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("rapid_lock").unwrap();
    storage
        .insert_into("rapid_lock", "data", vec![(1, 10)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let cycles_per_thread = 500 * scale;
    let completed_cycles = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&completed_cycles);
        let handle = thread::spawn(move || {
            for _ in 0..cycles_per_thread {
                // Rapidly acquire and release lock
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.execute_query_on("rapid_lock", "result(X,Y) <- data(X,Y)");
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
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_shared_storage();
    let num_threads = 20;
    let operations_per_thread = 10 * scale as i32;
    let mut handles = vec![];

    // Threads create and delete KGs rapidly
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..operations_per_thread {
                let kg_name = format!("temp_kg_{thread_id}_{i}");

                // Create
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let _ = storage_guard.create_knowledge_graph(&kg_name);
                }

                // Write some data
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let _ = storage_guard.insert_into(&kg_name, "data", vec![(i, i)]);
                }

                // Delete
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
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
        .execute_query_on("final_test", "result(X,Y) <- data(X,Y)")
        .expect("Query failed after stress");
    assert_eq!(results.len(), 1);
}

#[test]
fn test_concurrent_kg_switch_under_load() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();

    // Create multiple KGs with different data
    for i in 0..5 {
        let kg_name = format!("switch_kg_{i}");
        storage.create_knowledge_graph(&kg_name).unwrap();
        let data: Vec<(i32, i32)> = (0..=i).map(|j| (j, j * 10)).collect();
        storage.insert_into(&kg_name, "data", data).unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let switches_per_thread = 50 * scale;
    let successful_queries = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&successful_queries);
        let handle = thread::spawn(move || {
            for i in 0..switches_per_thread {
                // Switch between KGs based on iteration
                let kg_idx = (thread_id + i) % 5;
                let kg_name = format!("switch_kg_{kg_idx}");

                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
                    .expect("Query failed");

                // Each KG should have (kg_idx + 1) tuples
                assert_eq!(
                    results.len(),
                    kg_idx + 1,
                    "KG {} should have {} tuples, got {}",
                    kg_name,
                    kg_idx + 1,
                    results.len()
                );
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(
        successful_queries.load(Ordering::SeqCst),
        num_threads * switches_per_thread
    );
}

#[test]
fn test_parallel_queries_with_different_complexities() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("complexity_test").unwrap();

    // Create data for various query complexities
    let edges: Vec<(i32, i32)> = (0..100).map(|i| (i, i + 1)).collect();
    storage
        .insert_into("complexity_test", "edge", edges)
        .unwrap();

    let nodes: Vec<(i32, i32)> = (0..50).map(|i| (i, 0)).collect();
    storage
        .insert_into("complexity_test", "node", nodes)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 12;
    let successful_queries = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&successful_queries);
        let handle = thread::spawn(move || {
            for i in 0..30 * scale {
                let storage_guard = storage_clone.write().expect("Lock failed");

                // Alternate between different query types
                // All queries should succeed (not panic)
                let result = match (thread_id + i) % 3 {
                    0 => {
                        // Simple single-relation query
                        storage_guard
                            .execute_query_on("complexity_test", "result(X,Y) <- edge(X,Y)")
                    }
                    1 => {
                        // Node query
                        storage_guard
                            .execute_query_on("complexity_test", "result(X,Y) <- node(X,Y)")
                    }
                    _ => {
                        // Join query
                        storage_guard.execute_query_on(
                            "complexity_test",
                            "result(X,Y,Z) <- edge(X,Y), edge(Y,Z)",
                        )
                    }
                };

                // Query should succeed - we're testing concurrent execution, not result correctness
                assert!(result.is_ok(), "Query failed: {:?}", result.err());
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // All queries should have completed successfully
    assert_eq!(
        successful_queries.load(Ordering::SeqCst),
        num_threads * 30 * scale
    );
}

// Thread Pool Behavior Tests
#[test]
fn test_many_short_lived_operations() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("short_ops").unwrap();
    storage
        .insert_into("short_ops", "data", vec![(1, 1)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 50;
    let ops_per_thread = 100 * scale;
    let completed = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Spawn many threads doing very short operations
    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&completed);
        let handle = thread::spawn(move || {
            for _ in 0..ops_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let _ = storage_guard.execute_query_on("short_ops", "result(X,Y) <- data(X,Y)");
                drop(storage_guard);
                counter.fetch_add(1, Ordering::Relaxed);
                // No sleep - rapid fire operations
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    assert_eq!(
        completed.load(Ordering::SeqCst),
        num_threads * ops_per_thread
    );
}

#[test]
fn test_burst_traffic_pattern() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("burst_test").unwrap();
    storage
        .insert_into("burst_test", "data", vec![(1, 10), (2, 20), (3, 30)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_bursts = 5 * scale;
    let threads_per_burst = 20;
    let ops_per_thread = 10;

    for _burst in 0..num_bursts {
        let mut handles = vec![];

        // Create burst of activity
        for _ in 0..threads_per_burst {
            let storage_clone = Arc::clone(&storage);
            let handle = thread::spawn(move || {
                for _ in 0..ops_per_thread {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let results = storage_guard
                        .execute_query_on("burst_test", "result(X,Y) <- data(X,Y)")
                        .expect("Query failed");
                    assert_eq!(results.len(), 3);
                }
            });
            handles.push(handle);
        }

        // Wait for burst to complete
        for handle in handles {
            handle.join().expect("Thread panicked during burst");
        }

        // Brief pause between bursts
        thread::sleep(Duration::from_millis(10));
    }
}

// Data Integrity Under Concurrency
#[test]
fn test_data_integrity_under_heavy_concurrent_writes() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("integrity_test").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let writes_per_thread = 50 * scale;
    let mut handles = vec![];

    // Each thread writes unique tuples
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let key = thread_id * 1_000_000 + i;
                let value = key * 2; // Deterministic value
                let storage_guard = storage_clone.write().expect("Lock failed");
                storage_guard
                    .insert_into("integrity_test", "data", vec![(key as i32, value as i32)])
                    .expect("Insert failed");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify data integrity
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("integrity_test", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");

    assert_eq!(
        results.len(),
        num_threads * writes_per_thread,
        "Missing tuples"
    );

    // Verify each tuple has correct value
    for (key, value) in results {
        assert_eq!(
            value,
            key * 2,
            "Data corruption: key {} has value {}, expected {}",
            key,
            value,
            key * 2
        );
    }
}

#[test]
fn test_no_data_loss_during_concurrent_operations() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("no_loss_test").unwrap();

    // Pre-populate with initial data
    let initial_data: Vec<(i32, i32)> = (0..100).map(|i| (i, i * 10)).collect();
    storage
        .insert_into("no_loss_test", "initial", initial_data)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 15;
    let mut handles = vec![];

    // Half threads read, half threads write to different relation
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..30 * scale as i32 {
                let storage_guard = storage_clone.write().expect("Lock failed");
                if thread_id % 2 == 0 {
                    // Reader - verify initial data is intact
                    let results = storage_guard
                        .execute_query_on("no_loss_test", "result(X,Y) <- initial(X,Y)")
                        .expect("Query failed");
                    assert_eq!(
                        results.len(),
                        100,
                        "Data loss detected! Expected 100, got {}",
                        results.len()
                    );
                } else {
                    // Writer - add to different relation
                    let tuple_id = thread_id * 1_000_000 + i;
                    storage_guard
                        .insert_into("no_loss_test", "new_data", vec![(tuple_id, tuple_id)])
                        .expect("Insert failed");
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Final verification
    let storage_guard = storage.write().expect("Lock failed");
    let initial_results = storage_guard
        .execute_query_on("no_loss_test", "result(X,Y) <- initial(X,Y)")
        .expect("Query failed");
    assert_eq!(initial_results.len(), 100, "Initial data was corrupted");
}

// Edge Cases
#[test]
fn test_concurrent_empty_query_results() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("empty_results").unwrap();
    // Don't insert any data

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let mut handles = vec![];

    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for _ in 0..50 * scale {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("empty_results", "result(X,Y) <- data(X,Y)")
                    .expect("Query failed");
                // Empty result is valid
                assert!(results.is_empty());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked on empty results");
    }
}

#[test]
fn test_concurrent_single_tuple_contention() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("single_tuple").unwrap();
    storage
        .insert_into("single_tuple", "data", vec![(42, 84)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 50;
    let queries_per_thread = 100 * scale;
    let correct_results = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&correct_results);
        let handle = thread::spawn(move || {
            for _ in 0..queries_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                let results = storage_guard
                    .execute_query_on("single_tuple", "result(X,Y) <- data(X,Y)")
                    .expect("Query failed");
                if results.len() == 1 && results[0] == (42, 84) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // All queries should have returned the correct single tuple
    assert_eq!(
        correct_results.load(Ordering::SeqCst),
        num_threads * queries_per_thread
    );
}

#[test]
fn test_high_volume_concurrent_inserts() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("high_volume").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 8;
    let inserts_per_thread = 100 * scale;
    let total_inserts = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&total_inserts);
        let handle = thread::spawn(move || {
            for i in 0..inserts_per_thread {
                let tuple_id = (thread_id * 1_000_000 + i) as i32;
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
        .execute_query_on("high_volume", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * inserts_per_thread);
}

#[test]
fn test_insert_throughput_under_read_load() {
    let Some(scale) = soak_scale() else {
        return;
    };
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
    let ops_per_thread = 50 * scale;
    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Spawn writers
    for thread_id in 0..num_writers {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&write_count);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let tuple_id = (thread_id * 1_000_000 + i) as i32;
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
                    .execute_query_on("throughput_test", "result(X,Y) <- initial(X,Y)")
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

// Stress Tests
#[test]
fn test_100_concurrent_writers_same_kg() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("stress_same").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 100;
    let writes_per_thread = 5 * scale;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let tuple_id = (thread_id * 1_000_000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
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
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("stress_same", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * writes_per_thread);
}

#[test]
fn test_100_concurrent_writers_10_kgs() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();

    // Create 10 KGs
    for i in 0..10 {
        storage
            .create_knowledge_graph(&format!("stress_kg_{i}"))
            .unwrap();
    }

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 100;
    let writes_per_thread = 5 * scale;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let kg_name = format!("stress_kg_{}", thread_id % 10);
            for i in 0..writes_per_thread {
                let tuple_id = (thread_id * 1_000_000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
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
    let storage_guard = storage.write().expect("Lock failed");
    let mut total = 0;
    for i in 0..10 {
        let kg_name = format!("stress_kg_{i}");
        let results = storage_guard
            .execute_query_on(&kg_name, "result(X,Y) <- data(X,Y)")
            .expect("Query failed");
        // Each KG should have tuples from 10 threads (100/10)
        assert_eq!(results.len(), 10 * writes_per_thread);
        total += results.len();
    }
    assert_eq!(total, num_threads * writes_per_thread);
}

#[test]
fn test_sustained_write_load_1000_ops() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("sustained_write").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let ops_per_thread = 100 * scale;
    let total_ops = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&total_ops);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let tuple_id = (thread_id * 1_000_000 + i) as i32;
                let storage_guard = storage_clone.write().expect("Lock failed");
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
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("sustained_write", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    assert_eq!(results.len(), num_threads * ops_per_thread);
}

#[test]
fn test_concurrent_rule_modification() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("rule_mod").unwrap();
    storage
        .insert_into("rule_mod", "edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let mut handles = vec![];

    // Threads add and query rules concurrently
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..10 * scale {
                // Query (uses implicit rule)
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let results = storage_guard
                        .execute_query_on("rule_mod", "result(X,Y) <- edge(X,Y)")
                        .expect("Query failed");
                    assert_eq!(results.len(), 3);
                }

                // Try to drop non-existent rules (should not error)
                {
                    let storage_guard = storage_clone.write().expect("Lock failed");
                    let rule_name = format!("test_rule_{thread_id}_{i}");
                    let _ = storage_guard.drop_rule_in("rule_mod", &rule_name);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

// Concurrent Query Tests
#[test]
fn test_concurrent_recursive_queries() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("recursive_stress").unwrap();

    // Create graph for transitive closure
    let edges: Vec<(i32, i32)> = (0..20).map(|i| (i, i + 1)).collect();
    storage
        .insert_into("recursive_stress", "edge", edges)
        .unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 8;
    let queries_per_thread = 20 * scale;
    let successful_queries = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let counter = Arc::clone(&successful_queries);
        let handle = thread::spawn(move || {
            for _ in 0..queries_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                // Simple query (not actually recursive, but exercises query path)
                let results = storage_guard
                    .execute_query_on("recursive_stress", "result(X,Y) <- edge(X,Y)")
                    .expect("Query failed");
                assert_eq!(results.len(), 20);
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .join()
            .expect("Thread panicked during recursive queries");
    }

    assert_eq!(
        successful_queries.load(Ordering::SeqCst),
        num_threads * queries_per_thread
    );
}

#[test]
fn test_read_write_interleaving() {
    let Some(scale) = soak_scale() else {
        return;
    };
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("interleave").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 10;
    let ops_per_thread = 40 * scale;
    let mut handles = vec![];

    // Each thread alternates between reads and writes
    for thread_id in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let storage_guard = storage_clone.write().expect("Lock failed");
                if i % 2 == 0 {
                    // Write
                    let tuple_id = (thread_id * 1_000_000 + i) as i32;
                    storage_guard
                        .insert_into("interleave", "data", vec![(tuple_id, tuple_id)])
                        .expect("Insert failed");
                } else {
                    // Read
                    let _ = storage_guard
                        .execute_query_on("interleave", "result(X,Y) <- data(X,Y)")
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
    let storage_guard = storage.write().expect("Lock failed");
    let results = storage_guard
        .execute_query_on("interleave", "result(X,Y) <- data(X,Y)")
        .expect("Query failed");
    // Each thread does ops_per_thread / 2 writes
    assert_eq!(results.len(), num_threads * (ops_per_thread / 2));
}
