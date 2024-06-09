//! End-to-end pipeline tests: Parser -> IR -> Optimizer -> Codegen.

use inputlayer::DatalogEngine;

#[test]
fn test_engine_initialization() {
    let engine = DatalogEngine::new();
    assert!(engine.program().is_none());
    assert_eq!(engine.ir_nodes().len(), 0);
}

#[test]
fn test_add_multiple_relations() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);
    engine.add_fact("label", vec![(1, 10), (2, 20), (3, 30)]);

    // Verify catalog tracks both relations
    assert!(engine.catalog().has_relation("edge"));
    assert!(engine.catalog().has_relation("label"));
}

#[test]
fn test_simple_scan_query() {
    let mut engine = DatalogEngine::new();

    // Add base facts
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Query: result(X, Y) :- edge(X, Y)
    let program = "result(X, Y) :- edge(X, Y).";

    let results = engine.execute(program).unwrap();

    // Should return all edges
    assert_eq!(results.len(), 3);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(2, 3)));
    assert!(results.contains(&(3, 4)));
}

#[test]
#[ignore] // Constraint syntax (Y > 3) no longer supported - Constraint type removed
fn test_filter_query() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 5), (3, 10), (4, 1)]);

    // Query: result(X, Y) :- edge(X, Y), Y > 3
    let program = "result(X, Y) :- edge(X, Y), Y > 3.";

    let results = engine.execute(program.clone()).unwrap();

    // Should only return edges where y > 3
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(2, 5)));
    assert!(results.contains(&(3, 10)));
}

#[test]
