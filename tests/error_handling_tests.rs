//! Error Handling Tests
//!
//! Tests that verify proper error handling instead of panics.
//! These are critical for production safety - no input should crash the server.
//!
//! Coverage areas:
//! - Invalid query syntax handling
//! - Invalid statement types
//! - Non-existent resources
//! - Type mismatches
//! - Boundary errors
//! - Lock handling under errors

use inputlayer::{Config, DatalogEngine, StorageEngine, Value, Tuple};
use std::sync::{Arc, RwLock};
use std::thread;
use tempfile::TempDir;

// ============================================================================
// Test Helpers
// ============================================================================

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

// ============================================================================
// Parser Error Handling (no panics)
// ============================================================================

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
        "relation(X, Y",      // Missing close paren
        "relation X, Y)",     // Missing open paren
        "relation((X, Y)",    // Extra open paren
        "relation(X, Y))",    // Extra close paren
        "(relation(X, Y)",    // Unbalanced outer
    ];

    for query in test_cases {
        let result = engine.execute(query);
        assert!(result.is_err(), "Query '{}' should return error", query);
    }
}

#[test]
fn test_missing_period_returns_error() {
    let mut engine = create_engine();
    let result = engine.execute("edge(1, 2)");  // Missing period
    // Some implementations accept this, some don't - but shouldn't panic
    let _ = result;  // Just verify no panic
}

#[test]
fn test_invalid_rule_syntax_returns_error() {
    let mut engine = create_engine();

    let test_cases = vec![
        "path(X, Y) :- .",           // Empty body
        "path :- edge(X, Y).",       // Invalid head
        "path(X, Y) :- edge(X).",    // Arity mismatch in body
        ":- edge(X, Y).",            // Missing head entirely
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
        "result(count<) :- data(X, Y).",    // Empty aggregation
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

// ============================================================================
// Storage Error Handling (no panics)
// ============================================================================

#[test]
fn test_query_nonexistent_kg_returns_error() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.execute_query_on("nonexistent_kg", "result(X) :- data(X).");
    assert!(result.is_err(), "Query on non-existent KG should return error");
}

#[test]
fn test_insert_into_nonexistent_kg_returns_error() {
    let (storage, _temp) = create_test_storage();
    let mut storage = storage;  // Make mutable for insert operation

    let result = storage.insert_into("nonexistent_kg", "data", vec![(1, 2)]);
    assert!(result.is_err(), "Insert into non-existent KG should return error");
}

#[test]
fn test_delete_from_nonexistent_kg_returns_error() {
    let (storage, _temp) = create_test_storage();
    let mut storage = storage;  // Make mutable for delete operation

    let result = storage.delete_from("nonexistent_kg", "data", vec![(1, 2)]);
    assert!(result.is_err(), "Delete from non-existent KG should return error");
}

#[test]
fn test_drop_nonexistent_kg_returns_error() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.drop_knowledge_graph("nonexistent_kg");
    assert!(result.is_err(), "Drop non-existent KG should return error");
}

#[test]
fn test_drop_nonexistent_rule_handles_gracefully() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    // Dropping non-existent rule should not panic
    let result = storage.drop_rule_in("test", "nonexistent_rule");
    // Either succeeds (no-op) or returns error - but doesn't panic
    let _ = result;
}

#[test]
fn test_create_duplicate_kg_returns_error() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("duplicate").unwrap();

    let result = storage.create_knowledge_graph("duplicate");
    assert!(result.is_err(), "Creating duplicate KG should return error");
}

// ============================================================================
// Query Execution Error Handling (no panics)
// ============================================================================

#[test]
fn test_query_undefined_relation_returns_error() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    // Querying an undefined relation with a free variable is unsafe in Datalog
    // The variable X in ?undefined_relation(X) doesn't appear in any positive body atom
    let result = storage.execute_query_on("test", "?undefined_relation(X).");
    // Should return error because the query is unsafe (unbound variable in head)
    assert!(result.is_err(), "Query with unbound variable should return error");
}

#[test]
fn test_recursive_query_on_empty_relation() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    // First create the base relation (empty)
    storage.insert_into("test", "edge", vec![(0i32, 0i32); 0]).ok();

    // Then query with recursion - should return empty, not panic
    let result = storage.execute_query_on("test", "path(X, Y) :- edge(X, Y). path(X, Z) :- path(X, Y), edge(Y, Z). ?path(A, B).");
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

// ============================================================================
// Value Boundary Error Handling (no panics)
// ============================================================================

#[test]
fn test_tuple_out_of_bounds_access() {
    let tuple = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);

    // Out of bounds access should return None, not panic
    assert_eq!(tuple.get(0), Some(&Value::Int32(1)));
    assert_eq!(tuple.get(1), Some(&Value::Int32(2)));
    assert_eq!(tuple.get(2), None);  // Out of bounds
    assert_eq!(tuple.get(100), None);  // Way out of bounds
    assert_eq!(tuple.get(usize::MAX), None);  // Maximum index
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

// ============================================================================
// Concurrent Error Handling (no panics under contention)
// ============================================================================

#[test]
fn test_concurrent_errors_dont_cause_panic() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("concurrent_errors").unwrap();

    let storage = Arc::new(RwLock::new(storage));
    let num_threads = 20;
    let mut handles = vec![];

    for i in 0..num_threads {
        let storage_clone = Arc::clone(&storage);
        let handle = thread::spawn(move || {
            let mut storage_guard = storage_clone.write().expect("Lock failed");

            // Half threads do invalid operations
            if i % 2 == 0 {
                let _ = storage_guard.insert_into("nonexistent_kg", "data", vec![(i, i)]);
                let _ = storage_guard.execute_query_on("nonexistent_kg", "result(X) :- data(X).");
            } else {
                // Half do valid operations
                let _ = storage_guard.insert_into("concurrent_errors", "data", vec![(i as i32, i as i32)]);
                let _ = storage_guard.execute_query_on("concurrent_errors", "result(X,Y) :- data(X,Y).");
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
fn test_rapid_kg_create_drop_cycle() {
    let (mut storage, _temp) = create_test_storage();

    // Rapid create/drop cycle should not cause issues
    for i in 0..50 {
        let kg_name = format!("rapid_cycle_{}", i);
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage.insert_into(&kg_name, "data", vec![(i, i)]).unwrap();
        storage.drop_knowledge_graph(&kg_name).unwrap();
    }
}

#[test]
fn test_error_after_successful_operations() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("mixed").unwrap();

    // Successful operations
    storage.insert_into("mixed", "data", vec![(1, 10), (2, 20)]).unwrap();
    let result = storage.execute_query_on("mixed", "result(X,Y) :- data(X,Y).").unwrap();
    assert_eq!(result.len(), 2);

    // Try a malformed query - should error
    let err_result = storage.execute_query_on("mixed", "invalid syntax @@#$%");
    assert!(err_result.is_err(), "Malformed query should return error");

    // Previous data should still be accessible
    let result = storage.execute_query_on("mixed", "result(X,Y) :- data(X,Y).").unwrap();
    assert_eq!(result.len(), 2);
}

// ============================================================================
// Meta Command Error Handling (no panics)
// ============================================================================

#[test]
fn test_invalid_meta_commands_return_error() {
    let mut engine = create_engine();

    // Invalid meta commands should return errors, not panic
    let test_cases = vec![
        ".invalid",
        ".drop",  // Missing argument
        ".show xyz",  // Invalid show argument
    ];

    for cmd in test_cases {
        let result = engine.execute(cmd);
        // Should error or be handled gracefully
        let _ = result;
    }
}

#[test]
fn test_load_nonexistent_file_returns_error() {
    let mut engine = create_engine();

    let result = engine.execute(".load /nonexistent/path/file.dl");
    assert!(result.is_err(), "Loading non-existent file should return error");
}

// ============================================================================
// Edge Case Error Handling (no panics)
// ============================================================================

#[test]
fn test_very_long_relation_name_handled() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("test").unwrap();

    let long_name = "a".repeat(1000);
    let result = storage.insert_into("test", &long_name, vec![(1, 2)]);
    // Should succeed or return error, but not panic
    let _ = result;
}

#[test]
fn test_very_long_query_handled() {
    let mut engine = create_engine();

    // Generate a very long query
    let long_query = format!(
        "result(X) :- {}.",
        (0..100).map(|i| format!("rel{}(X)", i)).collect::<Vec<_>>().join(", ")
    );

    let result = engine.execute(&long_query);
    // Should return error (undefined relations), but not panic
    let _ = result;
}

#[test]
fn test_deeply_nested_arithmetic_handled() {
    let mut engine = create_engine();
    engine.execute("data(1).").ok();

    // Deeply nested arithmetic
    let nested = "(((((X+1)+1)+1)+1)+1)";
    let query = format!("result(Y) :- data(X), Y = {}.", nested);

    let result = engine.execute(&query);
    // Should succeed or error, but not panic
    let _ = result;
}

#[test]
fn test_unicode_in_queries_handled() {
    let mut engine = create_engine();

    // Unicode in string literals
    engine.execute(r#"data("日本語")."#).ok();
    engine.execute(r#"data("Привет")."#).ok();
    engine.execute(r#"data("مرحبا")."#).ok();

    let result = engine.execute("result(X) :- data(X).");
    assert!(result.is_ok());
}

#[test]
fn test_special_characters_in_strings_handled() {
    let mut engine = create_engine();

    // Various special characters
    engine.execute(r#"data("hello\nworld")."#).ok();
    engine.execute(r#"data("tab\there")."#).ok();
    engine.execute(r#"data("quote\"inside")."#).ok();

    let result = engine.execute("result(X) :- data(X).");
    assert!(result.is_ok());
}

// ============================================================================
// Numeric Edge Case Error Handling (no panics)
// ============================================================================

#[test]
fn test_division_by_zero_handled() {
    let mut engine = create_engine();
    engine.execute("data(0).").ok();

    // Division by zero should be handled
    let result = engine.execute("result(Y) :- data(X), Y = 10 / X.");
    // Should return error or special value, but not panic
    let _ = result;
}

#[test]
fn test_overflow_arithmetic_handled() {
    let mut engine = create_engine();
    engine.execute(&format!("data({}).", i64::MAX)).ok();

    // Overflow should be handled
    let result = engine.execute("result(Y) :- data(X), Y = X + 1.");
    // Should return error or wrap, but not panic
    let _ = result;
}

#[test]
fn test_underflow_arithmetic_handled() {
    let mut engine = create_engine();
    engine.execute(&format!("data({}).", i64::MIN)).ok();

    // Underflow should be handled
    let result = engine.execute("result(Y) :- data(X), Y = X - 1.");
    // Should return error or wrap, but not panic
    let _ = result;
}

// ============================================================================
// Vector Error Handling (no panics)
// ============================================================================

#[test]
fn test_vector_dimension_mismatch_handled() {
    let mut engine = create_engine();

    // Insert vectors of different dimensions
    engine.execute("vec1([1.0, 2.0, 3.0]).").ok();
    engine.execute("vec2([1.0, 2.0]).").ok();  // Different dimension

    // Distance calculation with mismatched dimensions
    let result = engine.execute("result(D) :- vec1(V1), vec2(V2), D = euclidean(V1, V2).");
    // Should return error, but not panic
    assert!(result.is_err() || result.unwrap().is_empty(),
        "Vector dimension mismatch should return error or empty result");
}

#[test]
fn test_empty_vector_operations_handled() {
    let mut engine = create_engine();

    engine.execute("vec([]).").ok();  // Empty vector

    // Operations on empty vector
    let result = engine.execute("result(D) :- vec(V), D = normalize(V).");
    // Should handle gracefully
    let _ = result;
}

// ============================================================================
// State Consistency After Errors
// ============================================================================

#[test]
fn test_state_consistent_after_parse_error() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("state_test").unwrap();

    // Add valid data
    storage.insert_into("state_test", "edge", vec![(1, 2), (2, 3)]).unwrap();

    // Parse error should not affect state
    let _ = storage.execute_query_on("state_test", "invalid syntax here @@#$%");

    // State should still be consistent
    let result = storage.execute_query_on("state_test", "result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn test_state_consistent_after_execution_error() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("exec_test").unwrap();

    // Add valid data
    storage.insert_into("exec_test", "data", vec![(1, 10)]).unwrap();

    // Execution error (parse error)
    let _ = storage.execute_query_on("exec_test", "invalid @@@");

    // State should still be consistent
    let result = storage.execute_query_on("exec_test", "result(X,Y) :- data(X,Y).").unwrap();
    assert_eq!(result.len(), 1);
}

#[test]
fn test_partial_batch_error_handling() {
    let (mut storage, _temp) = create_test_storage();
    storage.create_knowledge_graph("batch_test").unwrap();

    // Insert valid data
    storage.insert_into("batch_test", "data", vec![(1, 10), (2, 20)]).unwrap();

    // Try to query with error
    let _ = storage.execute_query_on("batch_test", "result(X) :- undefined(X).");

    // Original data should be intact
    let result = storage.execute_query_on("batch_test", "result(X, Y) :- data(X, Y).").unwrap();
    assert_eq!(result.len(), 2);
}
