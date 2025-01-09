//! Multi-hop joins, self-joins, and complex filter combinations.

use inputlayer::DatalogEngine;
use std::collections::HashSet;

/// Helper to convert results to set for easy comparison
fn to_set(results: Vec<(i32, i32)>) -> HashSet<(i32, i32)> {
    results.into_iter().collect()
}

#[test]
fn test_chained_joins_3hop() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    // 3-hop paths
    let query = "path3(X, W) :- edge(X, Y), edge(Y, Z), edge(Z, W).";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 2);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 4)));
    assert!(result_set.contains(&(2, 5)));
}

#[test]
fn test_self_join_with_filter() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 2), (2, 3), (3, 4), (5, 6)]);

    // Find pairs where x connects to z through y, with X < Z
    let query = "result(X, Z) :- data(X, Y), data(Y, Z), X < Z.";
    let results = engine.execute(query).unwrap();

    assert!(!results.is_empty());
    // All results should satisfy x < z
    for (x, z) in &results {
        assert!(x < z, "Expected x < z, got {} < {}", x, z);
    }
}

#[test]
#[ignore] // Constraint syntax (X >= 2, etc.) no longer supported - Constraint type removed
fn test_complex_filter_combination() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

    // x in range [2, 4], y in range [20, 40]
    let query = "result(X, Y) :- data(X, Y), X >= 2, X <= 4, Y >= 20, Y <= 40.";
    let results = engine.execute(query).unwrap();

    let result_set = to_set(results);
    assert_eq!(result_set.len(), 3);
    assert!(result_set.contains(&(2, 20)));
    assert!(result_set.contains(&(3, 30)));
    assert!(result_set.contains(&(4, 40)));
}

#[test]
fn test_column_swap_projection() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (3, 4), (5, 6)]);

    // Swap columns: result(Y, X) :- edge(X, Y)
    let query = "result(Y, X) :- edge(X, Y).";
    let results = engine.execute(query).unwrap();

    let result_set = to_set(results);
    assert!(result_set.contains(&(2, 1)));
    assert!(result_set.contains(&(4, 3)));
    assert!(result_set.contains(&(6, 5)));
}

#[test]
fn test_join_with_column_equality() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("r", vec![(1, 2), (2, 3), (3, 3)]);
    engine.add_fact("s", vec![(2, 10), (3, 20), (3, 30)]);

    // Join where y from r equals x from s
    let query = "result(X, Z) :- r(X, Y), s(Y, Z).";
    let results = engine.execute(query).unwrap();

    assert!(!results.is_empty());
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 10))); // r(1,2), s(2,10)
    assert!(result_set.contains(&(2, 20)) || result_set.contains(&(2, 30))); // r(2,3), s(3,*)
}

#[test]
#[ignore] // Constraint syntax (A < C) no longer supported - Constraint type removed
fn test_cartesian_product_filtered() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("r", vec![(1, 2), (2, 3)]);
    engine.add_fact("s", vec![(10, 20), (30, 40)]);

    // Cartesian product filtered by constraint
    let query = "result(A, C) :- r(A, B), s(C, D), A < C.";
    let results = engine.execute(query).unwrap();

    // Should have results where a < c
    for (a, c) in &results {
        assert!(a < c);
    }
}

#[test]
fn test_variable_reuse_in_body() {
    let mut engine = DatalogEngine::new();
    // Add edges including bidirectional ones to form cycles
    engine.add_fact("edge", vec![(1, 2), (2, 1), (2, 3), (3, 1), (4, 4)]);

    // Find cycles: edge(X, Y), edge(Y, X)
    let query = "cycle(X, Y) :- edge(X, Y), edge(Y, X).";
    let results = engine.execute(query).unwrap();

    let result_set = to_set(results);
    // (1,2) and (2,1) form a cycle (both directions exist)
    // (4,4) is self-loop
    assert!(
        result_set.contains(&(1, 2)) || result_set.contains(&(2, 1)),
        "Expected (1,2) or (2,1) cycle but got {:?}",
        result_set
    );
    assert!(
        result_set.contains(&(4, 4)),
        "Expected self-loop (4,4) but got {:?}",
        result_set
    );
}

#[test]
#[ignore] // Constraint syntax (X > 10) no longer supported - Constraint type removed
