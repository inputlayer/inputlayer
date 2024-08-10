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
