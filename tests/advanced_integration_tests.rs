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
fn test_empty_result_set() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Filter that excludes everything
    let query = "result(X, Y) :- edge(X, Y), X > 10.";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 0, "Expected empty result set");
}

#[test]
#[ignore] // Constraint syntax (X = Y) no longer supported - Constraint type removed
fn test_single_variable_constraint() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 1), (2, 2), (3, 4), (5, 5)]);

    // Only pairs where X == Y
    let query = "result(X, Y) :- data(X, Y), X = Y.";
    let results = engine.execute(query).unwrap();

    for (x, y) in &results {
        assert_eq!(x, y, "Expected x == y");
    }

    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 1)));
    assert!(result_set.contains(&(2, 2)));
    assert!(result_set.contains(&(5, 5)));
    assert!(!result_set.contains(&(3, 4)));
}

#[test]
#[ignore] // Constraint syntax (X != Y) no longer supported - Constraint type removed
fn test_multiple_inequality_constraints() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 1), (2, 3), (4, 5), (6, 6)]);

    // X != Y
    let query = "result(X, Y) :- data(X, Y), X != Y.";
    let results = engine.execute(query).unwrap();

    for (x, y) in &results {
        assert_ne!(x, y, "Expected x != y");
    }

    assert_eq!(results.len(), 2); // (2,3) and (4,5)
}

#[test]
fn test_join_with_multiple_constraints() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("r", vec![(1, 2), (2, 3), (3, 4)]);
    engine.add_fact("s", vec![(2, 5), (3, 6), (4, 7)]);

    // Join with both join condition and filter
    let query = "result(X, Z) :- r(X, Y), s(Y, Z), X < Y, Y < Z.";
    let results = engine.execute(query).unwrap();

    for (x, z) in &results {
        // We can't check y directly, but results should exist
        assert!(x < z);
    }
}

#[test]
#[ignore] // Constraint syntax (X > 50) no longer supported - Constraint type removed
fn test_large_dataset_performance() {
    let mut engine = DatalogEngine::new();

    // Create larger dataset
    let mut data = Vec::new();
    for i in 1..=100 {
        data.push((i, i + 1));
    }
    engine.add_fact("edge", data);

    let query = "result(X, Y) :- edge(X, Y), X > 50.";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 50); // 51 to 100
}

#[test]
fn test_nested_join_complex() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("a", vec![(1, 2), (2, 3)]);
    engine.add_fact("b", vec![(2, 4), (3, 5)]);
    engine.add_fact("c", vec![(4, 6), (5, 7)]);

    // Three-way join: a JOIN b JOIN c
    let query = "result(X, W) :- a(X, Y), b(Y, Z), c(Z, W).";
    let results = engine.execute(query).unwrap();

    assert!(!results.is_empty());
}

#[test]
#[ignore] // Constraint syntax (X > 1) no longer supported - Constraint type removed
fn test_pipeline_with_trace() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let query = "result(X, Y) :- edge(X, Y), X > 1.";
    let (results, trace) = engine.execute_with_trace(query).unwrap();

    // Verify trace captured all stages
    assert!(trace.ast.is_some());
    assert!(!trace.ir_before.is_empty());
    assert!(!trace.ir_after.is_empty());
    assert!(!trace.results.is_empty());

    // Verify results
    assert_eq!(results.len(), 1);
    assert!(results.contains(&(2, 3)));
}

#[test]
#[ignore] // Constraint syntax (X = 3, etc.) no longer supported - Constraint type removed
fn test_all_comparison_operators_comprehensive() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

    // Test each operator
    let tests = vec![
        ("result(X, Y) :- data(X, Y), X = 3.", 1),  // Equal
        ("result(X, Y) :- data(X, Y), X != 3.", 4), // Not equal
        ("result(X, Y) :- data(X, Y), X < 3.", 2),  // Less than
        ("result(X, Y) :- data(X, Y), X > 3.", 2),  // Greater than
        ("result(X, Y) :- data(X, Y), X <= 3.", 3), // Less or equal
        ("result(X, Y) :- data(X, Y), X >= 3.", 3), // Greater or equal
    ];

    for (query, expected_count) in tests {
        let results = engine.execute(query).unwrap();
        assert_eq!(
            results.len(),
            expected_count,
            "Query '{}' expected {} results, got {}",
            query,
            expected_count,
            results.len()
        );
    }
}

#[test]
fn test_safety_violation_detection() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    // Unsafe: z not in body
    let query = "result(X, Z) :- edge(X, Y).";
    let result = engine.execute(query);

    assert!(result.is_err(), "Expected safety violation error");
    let err = result.unwrap_err();
    assert!(
        err.contains("Unsafe") || err.contains("not found"),
        "Error should mention safety: {}",
        err
    );
}

#[test]
fn test_empty_knowledge_graph_query() {
    let mut engine = DatalogEngine::new();
    // No facts added

    let query = "result(X, Y) :- edge(X, Y).";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
