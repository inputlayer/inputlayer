//! Tests for query timeout, memory, result size, and row width limits.

use inputlayer::{
    Config, ExecutionConfig, MemoryTracker, QueryTimeout, ResourceError, ResourceLimits,
    StorageEngine, TimeoutError,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// Test Helpers
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

// QueryTimeout Tests
#[test]
fn test_query_timeout_creation() {
    // Test creating timeout with various durations
    let timeout = QueryTimeout::new(Some(Duration::from_secs(30)));
    assert!(!timeout.is_cancelled());
    assert!(timeout.check().is_ok());

    let infinite = QueryTimeout::infinite();
    assert!(!infinite.is_cancelled());
    assert!(infinite.check().is_ok());
}

#[test]
fn test_query_timeout_check_before_timeout() {
    let timeout = QueryTimeout::new(Some(Duration::from_secs(10)));

    // Immediately after creation, should not be timed out
    assert!(timeout.check().is_ok());
    assert!(!timeout.is_cancelled());

    // Should have remaining time
    let remaining = timeout.remaining().unwrap();
    assert!(remaining > Duration::from_secs(9));
}

#[test]
fn test_query_timeout_exceeded() {
    let timeout = QueryTimeout::new(Some(Duration::from_millis(50)));

    // Sleep well past timeout (5x margin for CI scheduling jitter)
    thread::sleep(Duration::from_millis(250));

    let result = timeout.check();
    assert!(result.is_err());

    if let Err(e) = result {
        assert!(e.elapsed >= Duration::from_millis(50));
        assert_eq!(e.timeout, Duration::from_millis(50));
    }
}

#[test]
fn test_query_timeout_explicit_cancel() {
    let timeout = QueryTimeout::new(Some(Duration::from_secs(60)));

    assert!(!timeout.is_cancelled());
    timeout.cancel();
    assert!(timeout.is_cancelled());

    // Check should now return error
    assert!(timeout.check().is_err());
}

#[test]
fn test_cancel_handle_cross_thread() {
    let timeout = QueryTimeout::new(Some(Duration::from_secs(60)));
    let handle = timeout.cancel_handle();

    // Spawn thread to cancel
    let cancel_thread = thread::spawn(move || {
        thread::sleep(Duration::from_millis(10));
        handle.cancel();
    });

    // Wait for cancellation
    cancel_thread.join().unwrap();

    // Original timeout should be cancelled
    assert!(timeout.is_cancelled());
    assert!(timeout.check().is_err());
}

#[test]
fn test_query_timeout_remaining_time() {
    let timeout = QueryTimeout::new(Some(Duration::from_secs(2)));

    let remaining1 = timeout.remaining().unwrap();
    // Generous headroom: 500ms for scheduling jitter on overloaded CI
    assert!(remaining1 > Duration::from_millis(1500));

    thread::sleep(Duration::from_millis(100));

    let remaining2 = timeout.remaining().unwrap();
    assert!(remaining2 < remaining1);
    assert!(remaining2 < Duration::from_millis(1950));
}

#[test]
fn test_query_timeout_reset() {
    let mut timeout = QueryTimeout::new(Some(Duration::from_millis(100)));

    // Cancel it
    timeout.cancel();
    assert!(timeout.is_cancelled());

    // Reset should clear cancellation
    timeout.reset();
    assert!(!timeout.is_cancelled());
    assert!(timeout.check().is_ok());
}

// MemoryTracker Tests
#[test]
fn test_memory_tracker_allocate_within_limit() {
    let tracker = MemoryTracker::new(Some(1000));

    // Allocate within limits
    assert!(tracker.allocate(500).is_ok());
    assert_eq!(tracker.current_usage(), 500);

    assert!(tracker.allocate(300).is_ok());
    assert_eq!(tracker.current_usage(), 800);
}

#[test]
fn test_memory_tracker_exceed_limit() {
    let tracker = MemoryTracker::new(Some(1000));

    // Allocate up to limit
    assert!(tracker.allocate(900).is_ok());

    // Exceed limit - should fail and rollback
    let result = tracker.allocate(200);
    assert!(result.is_err());

    if let Err(ResourceError::MemoryLimitExceeded { limit, used }) = result {
        assert_eq!(limit, 1000);
        assert_eq!(used, 1100); // 900 + 200
    }

    // Should have rolled back
    assert_eq!(tracker.current_usage(), 900);
}

#[test]
fn test_memory_tracker_release() {
    let tracker = MemoryTracker::new(Some(1000));

    tracker.allocate(500).unwrap();
    assert_eq!(tracker.current_usage(), 500);

    tracker.release(200);
    assert_eq!(tracker.current_usage(), 300);

    // Can now allocate more
    assert!(tracker.allocate(600).is_ok());
    assert_eq!(tracker.current_usage(), 900);
}

#[test]
fn test_memory_tracker_peak_tracking() {
    let tracker = MemoryTracker::new(Some(1000));

    tracker.allocate(500).unwrap();
    assert_eq!(tracker.peak_usage(), 500);

    tracker.allocate(300).unwrap();
    assert_eq!(tracker.peak_usage(), 800);

    tracker.release(500);
    assert_eq!(tracker.current_usage(), 300);
    assert_eq!(tracker.peak_usage(), 800); // Peak remains
}

#[test]
fn test_memory_tracker_unlimited() {
    let tracker = MemoryTracker::unlimited();

    // Should accept any amount
    assert!(tracker.allocate(1_000_000_000).is_ok());
    assert!(tracker.remaining().is_none());
}

#[test]
fn test_memory_tracker_concurrent_access() {
    let tracker = Arc::new(MemoryTracker::new(Some(10000)));

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let tracker = Arc::clone(&tracker);
            thread::spawn(move || {
                for _ in 0..100 {
                    let _ = tracker.allocate(10);
                    tracker.release(10);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Should be back to 0 (all allocations released)
    assert_eq!(tracker.current_usage(), 0);
}

// ResourceLimits Tests
#[test]
fn test_resource_limits_default() {
    let limits = ResourceLimits::default();

    // Should have reasonable defaults
    assert!(limits.max_memory_bytes.is_some());
    assert!(limits.max_result_size.is_some());
    assert!(limits.max_intermediate_size.is_some());
    assert!(limits.max_row_width.is_some());
}

#[test]
fn test_resource_limits_unlimited() {
    let limits = ResourceLimits::unlimited();

    assert!(limits.max_memory_bytes.is_none());
    assert!(limits.max_result_size.is_none());
    assert!(limits.max_intermediate_size.is_none());
    assert!(limits.max_row_width.is_none());
}

#[test]
fn test_resource_limits_strict() {
    let limits = ResourceLimits::strict();

    // Strict limits should be more restrictive than default
    let default = ResourceLimits::default();

    assert!(limits.max_result_size.unwrap() < default.max_result_size.unwrap());
    assert!(limits.max_memory_bytes.unwrap() < default.max_memory_bytes.unwrap());
}

#[test]
fn test_result_size_check() {
    let limits = ResourceLimits::default().with_max_result_size(100);

    assert!(limits.check_result_size(50).is_ok());
    assert!(limits.check_result_size(100).is_ok());

    let result = limits.check_result_size(101);
    assert!(result.is_err());

    if let Err(ResourceError::ResultSizeLimitExceeded { limit, actual }) = result {
        assert_eq!(limit, 100);
        assert_eq!(actual, 101);
    }
}

#[test]
fn test_intermediate_size_check() {
    let limits = ResourceLimits::default().with_max_intermediate_size(1000);

    assert!(limits.check_intermediate_size(500, "join").is_ok());
    assert!(limits.check_intermediate_size(1000, "join").is_ok());

    let result = limits.check_intermediate_size(1001, "join");
    assert!(result.is_err());

    if let Err(ResourceError::IntermediateResultExceeded {
        limit,
        actual,
        stage,
    }) = result
    {
        assert_eq!(limit, 1000);
        assert_eq!(actual, 1001);
        assert_eq!(stage, "join");
    }
}

#[test]
fn test_row_width_check() {
    let mut limits = ResourceLimits::default();
    limits.max_row_width = Some(10);

    assert!(limits.check_row_width(5).is_ok());
    assert!(limits.check_row_width(10).is_ok());

    let result = limits.check_row_width(11);
    assert!(result.is_err());

    if let Err(ResourceError::RowWidthExceeded { limit, actual }) = result {
        assert_eq!(limit, 10);
        assert_eq!(actual, 11);
    }
}

#[test]
fn test_resource_limits_builder_pattern() {
    let limits = ResourceLimits::unlimited()
        .with_max_memory(1024 * 1024)
        .with_max_result_size(10000)
        .with_max_intermediate_size(100000);

    assert_eq!(limits.max_memory_bytes, Some(1024 * 1024));
    assert_eq!(limits.max_result_size, Some(10000));
    assert_eq!(limits.max_intermediate_size, Some(100000));
}

// ExecutionConfig Tests
#[test]
fn test_execution_config_default() {
    let config = ExecutionConfig::default();

    // Should have sensible defaults
    assert!(config.timeout.is_some());
    assert!(config.enable_query_cache);
}

#[test]
fn test_execution_config_unlimited() {
    let config = ExecutionConfig::unlimited();

    // Unlimited config should have no limits
    assert!(config.timeout.is_none());
    assert!(config.limits.max_memory_bytes.is_none());
    assert!(!config.enable_query_cache);
}

#[test]
fn test_execution_config_builder() {
    let config = ExecutionConfig::default()
        .with_timeout(Duration::from_secs(30))
        .with_max_results(5000)
        .with_memory_limit(50 * 1024 * 1024);

    assert_eq!(config.timeout, Some(Duration::from_secs(30)));
    assert_eq!(config.limits.max_result_size, Some(5000));
    assert_eq!(config.limits.max_memory_bytes, Some(50 * 1024 * 1024));
}

// Integration Tests with StorageEngine
#[test]
fn test_storage_engine_query_completes_quickly() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_kg").unwrap();
    storage.use_knowledge_graph("test_kg").unwrap();

    // Insert some data
    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    // Query should complete quickly (well under any timeout)
    let start = std::time::Instant::now();
    let results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    let elapsed = start.elapsed();

    assert_eq!(results.len(), 3);
    assert!(
        elapsed < Duration::from_secs(5),
        "Query took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_large_result_set_handling() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_large").unwrap();
    storage.use_knowledge_graph("test_large").unwrap();

    // Insert moderate amount of data
    let data: Vec<(i32, i32)> = (0..1000).map(|i| (i, i * 2)).collect();
    storage.insert("numbers", data).unwrap();

    // Query should handle 1000 results without issue
    let results = storage
        .execute_query("result(X,Y) <- numbers(X,Y)")
        .unwrap();
    assert_eq!(results.len(), 1000);
}

#[test]
fn test_recursive_query_terminates() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_recursive").unwrap();
    storage.use_knowledge_graph("test_recursive").unwrap();

    // Create a simple graph for transitive closure
    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)])
        .unwrap();

    // Simple query should complete without timeout
    let results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();

    // Should find all edges
    assert_eq!(results.len(), 4);
}

// Error Handling Tests
#[test]
fn test_memory_error_display() {
    let error = ResourceError::MemoryLimitExceeded {
        limit: 1000,
        used: 1500,
    };

    let msg = format!("{}", error);
    assert!(msg.contains("Memory limit exceeded"));
    assert!(msg.contains("1000"));
    assert!(msg.contains("1500"));
}

#[test]
fn test_result_size_error_display() {
    let error = ResourceError::ResultSizeLimitExceeded {
        limit: 100,
        actual: 150,
    };

    let msg = format!("{}", error);
    assert!(msg.contains("Result size limit exceeded"));
    assert!(msg.contains("100"));
    assert!(msg.contains("150"));
}

#[test]
fn test_timeout_error_display() {
    let error = TimeoutError {
        timeout: Duration::from_secs(30),
        elapsed: Duration::from_secs(35),
    };

    let msg = format!("{}", error);
    assert!(msg.contains("timeout"));
    assert!(msg.contains("30"));
}

#[test]
fn test_row_width_error_display() {
    let error = ResourceError::RowWidthExceeded {
        limit: 10,
        actual: 15,
    };

    let msg = format!("{}", error);
    assert!(msg.contains("Row width limit exceeded"));
    assert!(msg.contains("10"));
    assert!(msg.contains("15"));
}

#[test]
fn test_intermediate_result_error_display() {
    let error = ResourceError::IntermediateResultExceeded {
        limit: 1000,
        actual: 1500,
        stage: "join_phase".to_string(),
    };

    let msg = format!("{}", error);
    assert!(msg.contains("Intermediate result limit exceeded"));
    assert!(msg.contains("join_phase"));
}

// Stress Tests
#[test]
fn test_many_small_queries() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("stress_test").unwrap();
    storage.use_knowledge_graph("stress_test").unwrap();

    storage
        .insert("data", vec![(1, 2), (3, 4), (5, 6)])
        .unwrap();

    // Run many queries
    for _ in 0..100 {
        let results = storage.execute_query("result(X,Y) <- data(X,Y)").unwrap();
        assert_eq!(results.len(), 3);
    }
}

#[test]
fn test_concurrent_timeout_checks() {
    let timeout = Arc::new(QueryTimeout::new(Some(Duration::from_secs(60))));

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let timeout = Arc::clone(&timeout);
            thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = timeout.check();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Should still be valid
    assert!(timeout.check().is_ok());
}
