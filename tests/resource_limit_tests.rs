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
    let timeout = QueryTimeout::new(Some(Duration::from_millis(10)));

    // Sleep to exceed timeout
    thread::sleep(Duration::from_millis(50));

    let result = timeout.check();
    assert!(result.is_err());

    if let Err(e) = result {
        assert!(e.elapsed >= Duration::from_millis(10));
        assert_eq!(e.timeout, Duration::from_millis(10));
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
    let timeout = QueryTimeout::new(Some(Duration::from_millis(100)));

    let remaining1 = timeout.remaining().unwrap();
    assert!(remaining1 > Duration::from_millis(90));

    thread::sleep(Duration::from_millis(30));

    let remaining2 = timeout.remaining().unwrap();
    assert!(remaining2 < remaining1);
    assert!(remaining2 < Duration::from_millis(80));
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
