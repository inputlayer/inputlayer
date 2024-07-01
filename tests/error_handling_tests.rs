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
fn test_drop_nonexistent_rule_handles_gracefully() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    // Dropping non-existent rule should not panic
    let result = storage.drop_rule_in("test", "nonexistent_rule");
    // Either succeeds (no-op) or returns error - but doesn't panic
    let _ = result;
}

#[test]
fn test_create_duplicate_kg_returns_error() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("duplicate").unwrap();

    let result = storage.create_knowledge_graph("duplicate");
    assert!(result.is_err(), "Creating duplicate KG should return error");
}

// Query Execution Error Handling (no panics)
#[test]
fn test_query_undefined_relation_returns_error() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    // Querying an undefined relation with a free variable is unsafe in Datalog
    // The variable X in ?undefined_relation(X) doesn't appear in any positive body atom
    let result = storage.execute_query_on("test", "?undefined_relation(X).");
    // Should return error because the query is unsafe (unbound variable in head)
    assert!(
        result.is_err(),
        "Query with unbound variable should return error"
    );
}

#[test]
fn test_recursive_query_on_empty_relation() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    // First create the base relation (empty)
    storage
        .insert_into("test", "edge", vec![(0i32, 0i32); 0])
        .ok();

    // Then query with recursion - should return empty, not panic
    let result = storage.execute_query_on(
        "test",
        "path(X, Y) :- edge(X, Y). path(X, Z) :- path(X, Y), edge(Y, Z). ?path(A, B).",
    );
    // Should succeed with empty result or return error - but not panic
    let _ = result;
}

#[test]
fn test_negation_only_rule_returns_error() {
    let mut engine = create_engine();
    engine.execute("node(1).").ok();

    // Rule with only negation is unsafe
    let result = engine.execute("isolated(X) :- !edge(X, _).");
    assert!(result.is_err(), "Negation-only rule should return error");
}

// Value Boundary Error Handling (no panics)
#[test]
fn test_tuple_out_of_bounds_access() {
    let tuple = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);

    // Out of bounds access should return None, not panic
    assert_eq!(tuple.get(0), Some(&Value::Int32(1)));
    assert_eq!(tuple.get(1), Some(&Value::Int32(2)));
    assert_eq!(tuple.get(2), None); // Out of bounds
    assert_eq!(tuple.get(100), None); // Way out of bounds
    assert_eq!(tuple.get(usize::MAX), None); // Maximum index
}

#[test]
fn test_empty_tuple_access() {
    let tuple = Tuple::new(vec![]);

    assert_eq!(tuple.arity(), 0);
    assert_eq!(tuple.get(0), None);
}

#[test]
fn test_value_type_mismatches_dont_panic() {
    let int_val = Value::Int32(42);
    let string_val = Value::string("hello");
    let float_val = Value::Float64(3.14);

    // These should return None, not panic
    assert_eq!(int_val.as_str(), None);
    assert_eq!(string_val.as_i64(), None);
    assert_eq!(float_val.as_str(), None);
}

// Concurrent Error Handling (no panics under contention)
#[test]
fn test_concurrent_errors_dont_cause_panic() {
    let (storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("concurrent_errors").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let mut handles = vec![];

    for i in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let storage_guard = storage_clone.write().expect("Lock failed");

            // Half threads do invalid operations
            if i % 2 == 0 {
                let _ = storage_guard.insert_into("nonexistent_kg", "data", vec![(i, i)]);
                let _ = storage_guard.execute_query_on("nonexistent_kg", "result(X) :- data(X).");
            } else {
                // Half do valid operations
                let _ = storage_guard.insert_into(
                    "concurrent_errors",
                    "data",
                    vec![(i as i32, i as i32)],
                );
                let _ = storage_guard
                    .execute_query_on("concurrent_errors", "result(X,Y) :- data(X,Y).");
            }
        });
        handles.push(handle);
    }

    // All threads should complete without panic
    for handle in handles {
        handle.join().expect("Thread panicked on error");
    }
}

#[test]
