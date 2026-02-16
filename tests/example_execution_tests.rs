//! Tests for the example .idl files work correctly.

use inputlayer::DatalogEngine;
use std::collections::HashSet;

fn to_set(results: Vec<(i32, i32)>) -> HashSet<(i32, i32)> {
    results.into_iter().collect()
}

// simple_query.rs tests
#[test]
fn test_simple_scan() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();

    assert_eq!(results.len(), 5);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(3, 4)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
}

#[test]
fn test_projection_column_swap() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);

    let results = engine.execute("result(Y, X) <- edge(X, Y)").unwrap();

    assert_eq!(results.len(), 5);
    let result_set = to_set(results);
    assert!(result_set.contains(&(2, 1)));
    assert!(result_set.contains(&(3, 2)));
    assert!(result_set.contains(&(4, 3)));
    assert!(result_set.contains(&(5, 4)));
    assert!(result_set.contains(&(6, 5)));
}

// join_query.rs tests
#[test]
fn test_two_hop_path() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    let results = engine
        .execute("result(X, Z) <- edge(X, Y), edge(Y, Z)")
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
        .execute("result(X, W) <- edge(X, Y), edge(Y, Z), edge(Z, W)")
        .unwrap();

    assert_eq!(results.len(), 2);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 4)));
    assert!(result_set.contains(&(2, 5)));
}

#[test]
fn test_bidirectional_edges() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 1), (2, 3), (4, 5), (5, 4)]);

    let results = engine
        .execute("result(X, Y) <- edge(X, Y), edge(Y, X)")
        .unwrap();

    // Should find bidirectional pairs
    assert!(results.len() >= 2);
}

#[test]
fn test_triangle_detection() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 1), (4, 5), (5, 6)]);

    let results = engine
        .execute("result(X, Y) <- edge(X, Y), edge(Y, Z), edge(Z, X)")
        .unwrap();

    // Triangle 1-2-3 should be detected
    assert!(results.len() >= 1);
}

// pipeline_demo.rs tests

#[test]
fn test_pipeline_with_trace() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let (results, trace) = engine
        .execute_with_trace("result(X, Y) <- edge(X, Y)")
        .unwrap();

    // Verify trace contains information
    let trace_str = trace.to_string();
    assert!(!trace_str.is_empty());

    // Verify results
    assert_eq!(results.len(), 3);
}

// Comparison operators tests

// Edge cases

#[test]
fn test_empty_relation() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![]);

    let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_single_tuple() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (1, 2));
}
