//! Example Execution Tests
//!
//! Integration tests that verify the core functionality demonstrated in the examples.
//! These tests ensure the examples work correctly as part of the test suite.

use inputlayer::DatalogEngine;
use std::collections::HashSet;

fn to_set(results: Vec<(i32, i32)>) -> HashSet<(i32, i32)> {
    results.into_iter().collect()
}

// =============================================================================
// simple_query.rs tests
// =============================================================================

#[test]
fn test_simple_scan() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine.execute("result(X, Y) :- edge(X, Y).").unwrap();

    assert_eq!(results.len(), 5);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(3, 4)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
}

#[test]
#[ignore] // Constraint syntax (X > 2) no longer supported - Constraint type removed
fn test_filter_greater_than() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), X > 2.")
        .unwrap();

    assert_eq!(results.len(), 3);
    let result_set = to_set(results);
    assert!(result_set.contains(&(3, 4)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
}

#[test]
#[ignore] // Constraint syntax (X > 1, Y < 5) no longer supported - Constraint type removed
fn test_multiple_filters() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), X > 1, Y < 5.")
        .unwrap();

    assert_eq!(results.len(), 2);
    let result_set = to_set(results);
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(3, 4)));
}

#[test]
fn test_projection_column_swap() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine.execute("result(Y, X) :- edge(X, Y).").unwrap();

    assert_eq!(results.len(), 5);
    let result_set = to_set(results);
    assert!(result_set.contains(&(2, 1)));
    assert!(result_set.contains(&(3, 2)));
    assert!(result_set.contains(&(4, 3)));
    assert!(result_set.contains(&(5, 4)));
    assert!(result_set.contains(&(6, 5)));
}

#[test]
#[ignore] // Constraint syntax (X != 3) no longer supported - Constraint type removed
fn test_inequality_filter() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), X != 3.")
        .unwrap();

    assert_eq!(results.len(), 4);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
    assert!(!result_set.contains(&(3, 4)));
}

// =============================================================================
// join_query.rs tests
// =============================================================================

#[test]
fn test_two_hop_path() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    let results = engine
        .execute("result(X, Z) :- edge(X, Y), edge(Y, Z).")
        .unwrap();

    assert_eq!(results.len(), 3);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 3)));
    assert!(result_set.contains(&(2, 4)));
    assert!(result_set.contains(&(3, 5)));
}

#[test]
fn test_three_hop_path() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    let results = engine
        .execute("result(X, W) :- edge(X, Y), edge(Y, Z), edge(Z, W).")
        .unwrap();

    assert_eq!(results.len(), 2);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 4)));
    assert!(result_set.contains(&(2, 5)));
}

#[test]
#[ignore] // Constraint syntax (X < 3) no longer supported - Constraint type removed
fn test_join_with_filter() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    let results = engine
        .execute("result(X, Z) :- edge(X, Y), edge(Y, Z), X < 3.")
        .unwrap();

    assert_eq!(results.len(), 2);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 3)));
    assert!(result_set.contains(&(2, 4)));
}

#[test]
fn test_bidirectional_edges() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 1), (2, 3), (4, 5), (5, 4)]);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), edge(Y, X).")
        .unwrap();

    // Should find bidirectional pairs
    assert!(results.len() >= 2);
}

#[test]
fn test_triangle_detection() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 1), (4, 5), (5, 6)]);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), edge(Y, Z), edge(Z, X).")
        .unwrap();

    // Triangle 1-2-3 should be detected
    assert!(results.len() >= 1);
}

// =============================================================================
// pipeline_demo.rs tests
// =============================================================================

#[test]
#[ignore] // Constraint syntax (X > 1) no longer supported - Constraint type removed
fn test_pipeline_stages() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Parse
    let program = engine
        .parse("result(X, Z) :- edge(X, Y), edge(Y, Z), X > 1.")
        .unwrap();
    assert!(!program.rules.is_empty());

    // Build IR
    engine.build_ir().unwrap();
    let ir_nodes = engine.ir_nodes();
    assert!(!ir_nodes.is_empty());

    // Optimize
    engine.optimize_ir().unwrap();

    // Execute
    let results = engine.execute_ir(&engine.ir_nodes()[0]).unwrap();
    assert_eq!(results.len(), 1);
    assert!(to_set(results).contains(&(2, 4)));
}

#[test]
fn test_pipeline_with_trace() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let (results, trace) = engine
        .execute_with_trace("result(X, Y) :- edge(X, Y).")
        .unwrap();

    // Verify trace contains information
    let trace_str = trace.to_string();
    assert!(!trace_str.is_empty());

    // Verify results
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Comparison operators tests
// =============================================================================

#[test]
#[ignore] // Constraint syntax (X > 2, etc.) no longer supported - Constraint type removed
fn test_all_comparison_operators() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

    // Greater than
    let results = engine
        .execute("result(X, Y) :- data(X, Y), X > 2.")
        .unwrap();
    assert_eq!(results.len(), 3);

    // Less than
    let results = engine
        .execute("result(X, Y) :- data(X, Y), X < 3.")
        .unwrap();
    assert_eq!(results.len(), 2);

    // Greater or equal
    let results = engine
        .execute("result(X, Y) :- data(X, Y), X >= 3.")
        .unwrap();
    assert_eq!(results.len(), 3);

    // Less or equal
    let results = engine
        .execute("result(X, Y) :- data(X, Y), X <= 3.")
        .unwrap();
    assert_eq!(results.len(), 3);

    // Not equal
    let results = engine
        .execute("result(X, Y) :- data(X, Y), X != 3.")
        .unwrap();
    assert_eq!(results.len(), 4);
}

// =============================================================================
// Edge cases
// =============================================================================

#[test]
#[ignore] // Constraint syntax (X > 100) no longer supported - Constraint type removed
fn test_empty_result() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), X > 100.")
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_empty_relation() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![]);

    let results = engine.execute("result(X, Y) :- edge(X, Y).").unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_single_tuple() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    let results = engine.execute("result(X, Y) :- edge(X, Y).").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (1, 2));
}

#[test]
#[ignore] // Constraint syntax (X > 50) no longer supported - Constraint type removed
fn test_large_dataset() {
    let mut engine = DatalogEngine::new();
    let data: Vec<(i32, i32)> = (1..=100).map(|i| (i, i + 1)).collect();
    engine.add_fact("edge", data);

    let results = engine
        .execute("result(X, Y) :- edge(X, Y), X > 50.")
        .unwrap();
    assert_eq!(results.len(), 50);
}
