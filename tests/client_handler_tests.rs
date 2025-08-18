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
fn test_storage_multiple_reads() {
    let (handler, _temp) = create_test_handler();

    // Multiple sequential read accesses should work
    let kg1 = {
        let storage = handler.get_storage();
        storage.current_knowledge_graph().map(|s| s.to_string())
    };

    let kg2 = {
        let storage = handler.get_storage();
        storage.current_knowledge_graph().map(|s| s.to_string())
    };

    assert_eq!(kg1, kg2);
}

// Query Counter Tests
#[tokio::test]
async fn test_query_count_increments() {
    let (handler, _temp) = create_test_handler();

    assert_eq!(handler.total_queries(), 0);

    // Execute a query via query_program
    let result = handler.query_program(None, "?- foo(X).".to_string()).await;
    // Query might fail if relation doesn't exist, but counter should still increment
    let _ = result;

    assert_eq!(handler.total_queries(), 1);

    // Execute another query
    let _ = handler.query_program(None, "?- bar(X).".to_string()).await;

    assert_eq!(handler.total_queries(), 2);
}

// Schema Validation Tests
#[test]
fn test_validate_tuples_no_schema() {
    let (handler, _temp) = create_test_handler();

    // Without a schema, validation should pass
    let tuples = vec![
        Tuple::new(vec![Value::Int64(1), Value::string("Alice")]),
        Tuple::new(vec![Value::Int64(2), Value::string("Bob")]),
    ];

    // Per-KG schema validation: pass the knowledge graph name
    let result =
        handler.validate_tuples_against_schema("default", "unregistered_relation", &tuples);
    assert!(result.is_ok());
}

#[test]
fn test_validate_tuples_empty() {
    let (handler, _temp) = create_test_handler();

    // Empty batch should validate
    let tuples: Vec<Tuple> = vec![];
    // Per-KG schema validation: pass the knowledge graph name
    let result = handler.validate_tuples_against_schema("default", "any_relation", &tuples);
    assert!(result.is_ok());
}

// Query Program Tests
#[tokio::test]
async fn test_query_program_simple() {
    let (handler, _temp) = create_test_handler();

    // Insert some data first
    {
        let storage = handler.get_storage_mut();
        storage
            .insert_tuples("numbers", make_tuples(&[1, 2, 3]))
            .unwrap();
    }

    // Query the data
    let result = handler
        .query_program(None, "?- numbers(X).".to_string())
        .await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 3);
}

#[tokio::test]
async fn test_query_program_with_knowledge_graph() {
    let (handler, _temp) = create_test_handler();

    // Create a new knowledge graph
    {
        let mut storage = handler.get_storage_mut();
        storage.create_knowledge_graph("test_kg").unwrap();
        storage.use_knowledge_graph("test_kg").unwrap();
        storage
            .insert_tuples("kg_data", make_tuples(&[1, 2]))
            .unwrap();
    }

    // Query the specific knowledge graph
    let result = handler
        .query_program(Some("test_kg".to_string()), "?- kg_data(X).".to_string())
        .await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 2);
}

#[tokio::test]
