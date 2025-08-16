//! Handler creation, query execution, and concurrent access tests.

use inputlayer::protocol::Handler;
use inputlayer::value::{Tuple, Value};
use inputlayer::{Config, StorageEngine};
use std::sync::Arc;
use tempfile::TempDir;

// Test Helpers
fn create_test_handler() -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    let handler = Handler::new(storage);
    (handler, temp)
}

fn create_handler_with_config(config: Config) -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = config;
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    let handler = Handler::new(storage);
    (handler, temp)
}

/// Helper to create tuples for testing
fn make_tuples(values: &[i64]) -> Vec<Tuple> {
    values
        .iter()
        .map(|v| Tuple::new(vec![Value::Int64(*v)]))
        .collect()
}

/// Helper to create 2-column tuples
fn make_tuples_2col(values: &[(i64, i64)]) -> Vec<Tuple> {
    values
        .iter()
        .map(|(a, b)| Tuple::new(vec![Value::Int64(*a), Value::Int64(*b)]))
        .collect()
}

// Handler Creation Tests
#[test]
fn test_handler_creation() {
    let (handler, _temp) = create_test_handler();

    // Handler should start with zero counters
    assert_eq!(handler.total_queries(), 0);
    assert_eq!(handler.total_inserts(), 0);

    // Uptime should be very small (just created)
    assert!(handler.uptime_seconds() < 5);
}

#[test]
