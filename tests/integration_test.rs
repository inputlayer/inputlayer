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

    let results = engine.execute(program).unwrap();

    // Should only return edges where y > 3
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(2, 5)));
    assert!(results.contains(&(3, 10)));
}

#[test]
fn test_projection_query() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Query: result(Y, X) :- edge(X, Y) (swap columns)
    let program = "result(Y, X) :- edge(X, Y).";

    let results = engine.execute(program).unwrap();

    // Should return swapped edges
    assert_eq!(results.len(), 3);
    assert!(results.contains(&(2, 1)));
    assert!(results.contains(&(3, 2)));
    assert!(results.contains(&(4, 3)));
}

#[test]
fn test_join_query() {
    let mut engine = DatalogEngine::new();

    // Create a simple graph: 1->2->3
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    // Query: result(X, Z) :- edge(X, Y), edge(Y, Z)
    // This computes 2-hop paths
    let program = "result(X, Z) :- edge(X, Y), edge(Y, Z).";

    let results = engine.execute(program).unwrap();

    // Should find the 2-hop path: 1->2->3
    assert_eq!(results.len(), 1);
    assert!(results.contains(&(1, 3)));
}

#[test]
#[ignore] // Constraint syntax (X > 1, Y < 20) no longer supported - Constraint type removed
fn test_multiple_filters() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 5), (2, 10), (3, 15), (4, 20)]);

    // Query: result(X, Y) :- edge(X, Y), X > 1, Y < 20
    let program = "result(X, Y) :- edge(X, Y), X > 1, Y < 20.";

    let results = engine.execute(program).unwrap();

    // Should filter: x > 1 AND y < 20
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(2, 10)));
    assert!(results.contains(&(3, 15)));
}

#[test]
fn test_self_join() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 1), (2, 2), (3, 4)]);

    // Query: result(X, Y) :- edge(X, Y), edge(Y, X)
    // Finds bidirectional edges (including self-loops)
    let program = "result(X, Y) :- edge(X, Y), edge(Y, X).";

    let results = engine.execute(program).unwrap();

    // Should find self-loops: (1,1) and (2,2)
    assert!(results.contains(&(1, 1)));
    assert!(results.contains(&(2, 2)));
}

#[test]
#[ignore] // Constraint syntax (X != Y) no longer supported - Constraint type removed
fn test_inequality_constraint() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 1), (1, 2), (2, 2), (2, 3)]);

    // Query: result(X, Y) :- edge(X, Y), X != Y
    let program = "result(X, Y) :- edge(X, Y), X != Y.";

    let results = engine.execute(program).unwrap();

    // Should exclude self-loops
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(2, 3)));
}

#[test]
#[ignore] // Constraint syntax (X < 3) no longer supported - Constraint type removed
fn test_complex_join_with_filter() {
    let mut engine = DatalogEngine::new();

    // Graph: 1->2, 2->3, 3->4, 4->5
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    // Query: result(X, Z) :- edge(X, Y), edge(Y, Z), X < 3
    let program = "result(X, Z) :- edge(X, Y), edge(Y, Z), X < 3.";

    let results = engine.execute(program).unwrap();

    // 2-hop paths where x < 3: 1->2->3, 2->3->4
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 3)));
    assert!(results.contains(&(2, 4)));
}

#[test]
fn test_parse_with_comments() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let program = "
        % This is a comment
        % This is also a comment (Prolog-style)
        result(X, Y) :- edge(X, Y).

        % Another comment
    ";

    let results = engine.execute(program).unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_multiple_rules() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let program = "
        path1(X, Y) :- edge(X, Y).
        path2(X, Z) :- edge(X, Y), edge(Y, Z).
    ";

    // Execute returns results from the LAST rule (query semantics)
    // All intermediate rules are computed and their results become available
    // as input data for subsequent rules
    let results = engine.execute(program).unwrap();

    // Last rule (path2): 2-hop paths
    assert_eq!(results.len(), 2);

    // Test that we can execute all rules
    // With SIP enabled, additional intermediate rules may be generated
    let all_results = engine.execute_all_rules(program).unwrap();
    assert!(all_results.len() >= 2); // At least two rules (may include SIP intermediates)

    // First rule results (path1  -  single atom, not SIP-rewritten)
    let rule0_results = &all_results[&0];
    assert_eq!(rule0_results.len(), 3);

    // Last rule results (path2  -  2-hop paths)
    let last_idx = all_results.len() - 1;
    let rule_last_results = &all_results[&last_idx];
    assert_eq!(rule_last_results.len(), 2);
    assert!(rule_last_results.contains(&(1, 3)));
    assert!(rule_last_results.contains(&(2, 4)));
}

#[test]
fn test_safety_validation() {
    let mut engine = DatalogEngine::new();

    // Unsafe rule: z appears in head but not in body
    let program = "result(X, Z) :- edge(X, Y).";

    let result = engine.execute(program);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unsafe rule"));
}

#[test]
fn test_empty_relation() {
    let mut engine = DatalogEngine::new();

    // No facts added
    let program = "result(X, Y) :- edge(X, Y).";

    let results = engine.execute(program).unwrap();

    // Should return empty results
    assert_eq!(results.len(), 0);
}

#[test]
#[ignore] // Constraint syntax (X = 2) no longer supported - Constraint type removed
fn test_constant_in_body() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Query with constant equality: result(X, Y) :- edge(X, Y), X = 2
    let program = "result(X, Y) :- edge(X, Y), X = 2.";

    let results = engine.execute(program).unwrap();

    // Should only return edges where x = 2
    assert_eq!(results.len(), 1);
    assert!(results.contains(&(2, 3)));
}

#[test]
fn test_catalog_schema_inference() {
    let mut engine = DatalogEngine::new();

    // Add facts - catalog should track schema
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let catalog = engine.catalog();

    assert!(catalog.has_relation("edge"));
    let schema = catalog.get_schema("edge").unwrap();
    assert_eq!(schema, &["col0", "col1"]);
}

#[test]
fn test_optimization_removes_identity_projection() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    // This query has identity projection (x, y) :- edge(X, Y)
    let program = "result(X, Y) :- edge(X, Y).";

    engine.parse(program).unwrap();
    engine.build_ir().unwrap();

    // After optimization, identity maps should be removed
    engine.optimize_ir().unwrap();

    let ir_after = &engine.ir_nodes()[0];

    // Should be a Scan node (Map removed by optimization)
    assert!(ir_after.is_scan());
}

#[test]
fn test_large_dataset() {
    let mut engine = DatalogEngine::new();

    // Create a larger dataset
    let mut edges = Vec::new();
    for i in 1..100 {
        edges.push((i, i + 1));
    }
    engine.add_fact("edge", edges);

    // Count edges
    let program = "result(X, Y) :- edge(X, Y).";

    let results = engine.execute(program).unwrap();

    assert_eq!(results.len(), 99);
}

#[test]
fn test_triangles_query() {
    let mut engine = DatalogEngine::new();

    // Create a triangle: 1->2, 2->3, 3->1
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 1)]);

    // Find triangles: result(X, Z) :- edge(X, Y), edge(Y, Z), edge(Z, X)
    let program = "result(X, Z) :- edge(X, Y), edge(Y, Z), edge(Z, X).";

    let results = engine.execute(program).unwrap();

    // Should find the triangle edges
    assert!(results.len() > 0);
}

#[test]
#[ignore] // Constraint syntax (X != Z) no longer supported - Constraint type removed
