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
fn test_handler_from_config() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();

    let handler = Handler::from_config(config);
    assert!(handler.is_ok());

    let handler = handler.unwrap();
    assert_eq!(handler.total_queries(), 0);
}

#[test]
fn test_handler_with_custom_config() {
    let mut config = Config::default();
    config.storage.performance.batch_size = 500;

    let (handler, _temp) = create_handler_with_config(config);

    // Handler should be created successfully
    assert_eq!(handler.total_queries(), 0);
}

// Storage Access Tests
#[test]
fn test_get_storage_read() {
    let (handler, _temp) = create_test_handler();

    // Should be able to get read access to storage
    let storage = handler.get_storage();
    assert!(storage.current_knowledge_graph().is_some());
}

#[test]
fn test_get_storage_write() {
    let (handler, _temp) = create_test_handler();

    // Should be able to get write access to storage
    let storage = handler.get_storage_mut();

    // Insert some data using the storage API (use 2-column data for binary tuple return type)
    let tuples = make_tuples_2col(&[(1, 10), (2, 20), (3, 30)]);
    let result = storage.insert_tuples("test", tuples);
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());

    // Verify data was inserted (use rule-style query for binary tuple result)
    let result = storage.execute_query("result(X, Y) :- test(X, Y).");
    assert!(result.is_ok(), "Query failed: {:?}", result.err());
    let rows = result.unwrap();
    assert_eq!(rows.len(), 3);
}

#[test]
