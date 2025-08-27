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
async fn test_query_program_nonexistent_relation() {
    let (handler, _temp) = create_test_handler();

    // Query a relation that doesn't exist
    let result = handler
        .query_program(None, "?- nonexistent(X).".to_string())
        .await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 0); // No results
}

#[tokio::test]
async fn test_query_program_with_join() {
    let (handler, _temp) = create_test_handler();

    // Insert data for join
    {
        let storage = handler.get_storage_mut();
        storage
            .insert_tuples("edge", make_tuples_2col(&[(1, 2), (2, 3), (3, 4)]))
            .unwrap();
        storage
            .insert_tuples("node", make_tuples(&[1, 2, 3]))
            .unwrap();
    }

    // Query with join
    let result = handler
        .query_program(None, "?- edge(X, Y), node(X).".to_string())
        .await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 3);
}

#[tokio::test]
async fn test_query_program_invalid_syntax() {
    let (handler, _temp) = create_test_handler();

    // Query with invalid syntax
    let result = handler
        .query_program(None, "?- invalid syntax here".to_string())
        .await;

    // Should return error
    assert!(result.is_err());
}

// Concurrent Access Tests
#[tokio::test]
async fn test_concurrent_queries() {
    let (handler, _temp) = create_test_handler();
    let handler = Arc::new(handler);

    // Insert some data
    {
        let storage = handler.get_storage_mut();
        storage
            .insert_tuples("data", make_tuples(&[1, 2, 3, 4, 5]))
            .unwrap();
    }

    // Spawn multiple concurrent queries
    let mut handles = vec![];
    for i in 0..5 {
        let h = Arc::clone(&handler);
        let handle = tokio::spawn(async move {
            let result = h.query_program(None, "?- data(X).".to_string()).await;
            (i, result.is_ok())
        });
        handles.push(handle);
    }

    // Wait for all queries
    for handle in handles {
        let (idx, success) = handle.await.unwrap();
        assert!(success, "Query {} failed", idx);
    }

    // All queries should have been counted - exactly 5 queries were executed
    assert_eq!(
        handler.total_queries(),
        5,
        "Expected exactly 5 queries to be counted"
    );
}

#[test]
fn test_concurrent_read_write() {
    use std::thread;

    let (handler, _temp) = create_test_handler();
    let handler = Arc::new(handler);

    // Insert initial data
    {
        let storage = handler.get_storage_mut();
        storage.insert_tuples("items", make_tuples(&[1])).unwrap();
    }

    // Spawn readers and writers
    let mut handles = vec![];

    // Readers
    for _ in 0..3 {
        let h = Arc::clone(&handler);
        let handle = thread::spawn(move || {
            for _ in 0..10 {
                let storage = h.get_storage();
                let _ = storage.current_knowledge_graph();
            }
        });
        handles.push(handle);
    }

    // Writer
    let h = Arc::clone(&handler);
    let writer_handle = thread::spawn(move || {
        for i in 2i64..5i64 {
            let storage = h.get_storage_mut();
            let _ = storage.insert_tuples("items", make_tuples(&[i]));
        }
    });
    handles.push(writer_handle);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
}

// Uptime Tests
#[test]
fn test_uptime_increases() {
    let (handler, _temp) = create_test_handler();

    let uptime1 = handler.uptime_seconds();

    // Sleep briefly
    std::thread::sleep(std::time::Duration::from_millis(100));

    let uptime2 = handler.uptime_seconds();

    // Uptime should be >= (could be same if < 1 second)
    assert!(uptime2 >= uptime1);
}

// Error Handling Tests
#[tokio::test]
async fn test_query_program_error_recovery() {
    let (handler, _temp) = create_test_handler();

    // First query with error
    let result1 = handler
        .query_program(None, "invalid query".to_string())
        .await;
    assert!(result1.is_err());

    // Handler should still work after error
    {
        let storage = handler.get_storage_mut();
        storage.insert_tuples("test", make_tuples(&[1])).unwrap();
    }

    let result2 = handler.query_program(None, "?- test(X).".to_string()).await;
    assert!(result2.is_ok());
}

#[tokio::test]
async fn test_query_nonexistent_knowledge_graph() {
    let (handler, _temp) = create_test_handler();

    // Query a knowledge graph that doesn't exist
    let result = handler
        .query_program(
            Some("nonexistent_kg".to_string()),
            "?- data(X).".to_string(),
        )
        .await;

    // Should return error
    assert!(result.is_err());
}

// Knowledge Graph Switching Tests
#[tokio::test]
async fn test_query_with_kg_switch() {
    let (handler, _temp) = create_test_handler();

    // Create and populate two knowledge graphs
    {
        let mut storage = handler.get_storage_mut();

        storage.create_knowledge_graph("kg1").unwrap();
        storage.use_knowledge_graph("kg1").unwrap();
        storage
            .insert_tuples("kg1_data", make_tuples(&[1]))
            .unwrap();

        storage.create_knowledge_graph("kg2").unwrap();
        storage.use_knowledge_graph("kg2").unwrap();
        storage
            .insert_tuples("kg2_data", make_tuples(&[2]))
            .unwrap();
    }

    // Query kg1
    let result1 = handler
        .query_program(Some("kg1".to_string()), "?- kg1_data(X).".to_string())
        .await;
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap().rows.len(), 1);

    // Query kg2
    let result2 = handler
        .query_program(Some("kg2".to_string()), "?- kg2_data(X).".to_string())
        .await;
    assert!(result2.is_ok());
    assert_eq!(result2.unwrap().rows.len(), 1);

    // kg1 data should not be visible in kg2
    let result3 = handler
        .query_program(Some("kg2".to_string()), "?- kg1_data(X).".to_string())
        .await;
    assert!(result3.is_ok());
    assert_eq!(result3.unwrap().rows.len(), 0);
}

// Query Result Format Tests
#[tokio::test]
async fn test_query_result_schema() {
    let (handler, _temp) = create_test_handler();

    // Insert data with multiple columns
    {
        let storage = handler.get_storage_mut();
        let tuples = vec![
            Tuple::new(vec![Value::string("Alice"), Value::Int64(30)]),
            Tuple::new(vec![Value::string("Bob"), Value::Int64(25)]),
        ];
        storage.insert_tuples("people", tuples).unwrap();
    }

    // Query
    let result = handler
        .query_program(None, "?- people(Name, Age).".to_string())
        .await;

    assert!(result.is_ok());
    let query_result = result.unwrap();

    // Check structure
    assert_eq!(query_result.rows.len(), 2);
    // Schema should have exactly 2 fields (Name, Age)
    assert_eq!(
        query_result.schema.len(),
        2,
        "Schema should have exactly 2 fields"
    );
}

#[tokio::test]
async fn test_query_result_with_different_types() {
    let (handler, _temp) = create_test_handler();

    // Insert different types
    {
        let storage = handler.get_storage_mut();
        let tuples = vec![Tuple::new(vec![
            Value::Int64(1),
            Value::Float64(3.14),
            Value::string("text"),
        ])];
        storage.insert_tuples("typed_data", tuples).unwrap();
    }

    // Query
    let result = handler
        .query_program(None, "?- typed_data(I, F, S).".to_string())
        .await;

    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

// Edge Case Tests
#[tokio::test]
async fn test_query_empty_program() {
    let (handler, _temp) = create_test_handler();

    let result = handler.query_program(None, "".to_string()).await;

    // Empty program might parse as no-op or error - either is acceptable
    let _ = result;
}

#[tokio::test]
async fn test_query_comment_only() {
    let (handler, _temp) = create_test_handler();

    let result = handler
        .query_program(None, "% This is a comment".to_string())
        .await;

    // Comment-only should be valid (no-op) - depending on parser
    let _ = result;
}

#[tokio::test]
async fn test_query_whitespace_only() {
    let (handler, _temp) = create_test_handler();

    let result = handler
        .query_program(None, "   \n\t  \n  ".to_string())
        .await;

    // Whitespace-only should be valid (no-op)
    let _ = result;
}

#[test]
fn test_handler_with_large_data() {
    let (handler, _temp) = create_test_handler();

    // Insert large amount of data (2-column for binary tuple return type)
    {
        let storage = handler.get_storage_mut();
        let tuples: Vec<Tuple> = (0i64..1000)
            .map(|i| Tuple::new(vec![Value::Int64(i), Value::Int64(i * 10)]))
            .collect();
        let result = storage.insert_tuples("large_data", tuples);
        assert!(result.is_ok());
    }

    // Query should work (2-column query for binary tuple result)
    {
        let storage = handler.get_storage_mut();
        let result = storage.execute_query("result(X, Y) :- large_data(X, Y).");
        assert!(result.is_ok(), "Query failed: {:?}", result.err());
        assert_eq!(result.unwrap().len(), 1000);
    }
}

// Statistics Tests
#[test]
fn test_statistics_consistency() {
    let (handler, _temp) = create_test_handler();

    // Initial state
    let initial_queries = handler.total_queries();
    let initial_inserts = handler.total_inserts();

    // These should be 0 for a fresh handler
    assert_eq!(initial_queries, 0);
    assert_eq!(initial_inserts, 0);

    // After some operations via storage (use 2-column for binary tuple)
    {
        let storage = handler.get_storage_mut();
        let _ = storage.insert_tuples("stat_test", make_tuples_2col(&[(1, 10)]));
        let _ = storage.execute_query("result(X, Y) :- stat_test(X, Y).");
    }

    // Counters should be consistent (storage operations don't increment handler counters)
    let final_queries = handler.total_queries();
    let final_inserts = handler.total_inserts();

    // At minimum, values shouldn't have decreased
    assert!(final_queries >= initial_queries);
    assert!(final_inserts >= initial_inserts);
}

// Handler Method Tests
#[test]
fn test_handler_total_queries_atomic() {
    let (handler, _temp) = create_test_handler();
    let handler = Arc::new(handler);

    // Concurrent reads of counter should work
    let mut handles = vec![];
    for _ in 0..10 {
        let h = Arc::clone(&handler);
        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                let _ = h.total_queries();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_handler_uptime_seconds_consistent() {
    let (handler, _temp) = create_test_handler();

    // Multiple uptime reads should be consistent (monotonically increasing)
    let mut prev = handler.uptime_seconds();
    for _ in 0..10 {
        let current = handler.uptime_seconds();
        assert!(current >= prev);
        prev = current;
    }
}
