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
