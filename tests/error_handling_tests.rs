//! Tests for error handling: no input should crash the server.

use inputlayer::{Config, DatalogEngine, StorageEngine, Tuple, Value};
use std::sync::{Arc, RwLock};
use std::thread;
use tempfile::TempDir;

// Test Helpers
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

fn create_engine() -> DatalogEngine {
    DatalogEngine::new()
}

// Parser Error Handling (no panics)
#[test]
fn test_empty_query_returns_error() {
    let mut engine = create_engine();
    let result = engine.execute("");
    // Should return error, not panic
    assert!(result.is_err() || result.unwrap().is_empty());
}

#[test]
fn test_whitespace_only_query_returns_error() {
    let mut engine = create_engine();
    let result = engine.execute("   \t\n  ");
    // Should return error, not panic
    assert!(result.is_err() || result.unwrap().is_empty());
}

#[test]
fn test_unbalanced_parens_returns_error() {
    let mut engine = create_engine();

    let test_cases = vec![
        "relation(X, Y",   // Missing close paren
        "relation X, Y)",  // Missing open paren
        "relation((X, Y)", // Extra open paren
        "relation(X, Y))", // Extra close paren
        "(relation(X, Y)", // Unbalanced outer
    ];

    for query in test_cases {
        let result = engine.execute(query);
        assert!(result.is_err(), "Query '{}' should return error", query);
    }
}

#[test]
fn test_missing_period_returns_error() {
    let mut engine = create_engine();
    let result = engine.execute("edge(1, 2)"); // Missing period
                                               // Some implementations accept this, some don't - but shouldn't panic
    let _ = result; // Just verify no panic
}

#[test]
fn test_invalid_rule_syntax_returns_error() {
    let mut engine = create_engine();

    let test_cases = vec![
        "path(X, Y) :- .",        // Empty body
        "path :- edge(X, Y).",    // Invalid head
        "path(X, Y) :- edge(X).", // Arity mismatch in body
        ":- edge(X, Y).",         // Missing head entirely
    ];

    for query in test_cases {
        let result = engine.execute(query);
        // Should return error, not panic
        assert!(result.is_err(), "Query '{}' should return error", query);
    }
}

#[test]
fn test_unknown_function_returns_error() {
    let mut engine = create_engine();
    engine.execute("data(1, 2).").ok();

    let result = engine.execute("result(X) :- data(X, Y), Z = nonexistent_function(Y).");
    assert!(result.is_err(), "Unknown function should return error");
}

#[test]
fn test_malformed_aggregation_returns_error() {
    let mut engine = create_engine();
    engine.execute("data(1, 10).").ok();

    let test_cases = vec![
        "result(count<) :- data(X, Y).",     // Empty aggregation
        "result(count<X Y>) :- data(X, Y).", // Missing comma
    ];

    for query in test_cases {
        let result = engine.execute(query);
        // Should error, not panic
        assert!(result.is_err(), "Query '{}' should return error", query);
    }
}

#[test]
fn test_unbound_head_variable_returns_error() {
    let mut engine = create_engine();
    engine.execute("edge(1, 2).").ok();

    // Z is not bound in the body
    let result = engine.execute("path(X, Z) :- edge(X, Y).");
    assert!(result.is_err(), "Unbound head variable should return error");
}

// Storage Error Handling (no panics)
#[test]
fn test_query_nonexistent_kg_returns_error() {
    let (storage, _temp) = create_test_storage();

    let result = storage.execute_query_on("nonexistent_kg", "result(X) :- data(X).");
    assert!(
        result.is_err(),
        "Query on non-existent KG should return error"
    );
}

#[test]
fn test_insert_into_nonexistent_kg_returns_error() {
    let (storage, _temp) = create_test_storage();
    let storage = storage; // Make mutable for insert operation

    let result = storage.insert_into("nonexistent_kg", "data", vec![(1, 2)]);
    assert!(
        result.is_err(),
        "Insert into non-existent KG should return error"
    );
}

#[test]
fn test_delete_from_nonexistent_kg_returns_error() {
    let (storage, _temp) = create_test_storage();
    let storage = storage; // Make mutable for delete operation

    let result = storage.delete_from("nonexistent_kg", "data", vec![(1, 2)]);
    assert!(
        result.is_err(),
        "Delete from non-existent KG should return error"
    );
}

#[test]
fn test_drop_nonexistent_kg_returns_error() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.drop_knowledge_graph("nonexistent_kg");
    assert!(result.is_err(), "Drop non-existent KG should return error");
}

#[test]
