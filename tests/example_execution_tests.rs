//! Tests for the example .dl files work correctly.

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

    let results = engine.execute("result(X, Y) :- edge(X, Y).").unwrap();

    assert_eq!(results.len(), 5);
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 3.clone())));
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
    let result_set = to_set(results.clone());
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

// join_query.rs tests
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
