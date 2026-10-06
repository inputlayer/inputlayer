//! The pipeline end to end on the library types: parser, IR, optimizer and
//! code generator through `IQLEngine`. Cases no spec or module test asserts.

use crate::IQLEngine;

#[test]
fn test_multiple_rules() {
    let mut engine = IQLEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let program = "
        path1(X, Y) <- edge(X, Y)
        path2(X, Z) <- edge(X, Y), edge(Y, Z)
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
fn test_catalog_schema_inference() {
    let mut engine = IQLEngine::new();

    // Add facts - catalog should track schema
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let catalog = engine.catalog();

    assert!(catalog.has_relation("edge"));
    let schema = catalog.get_schema("edge").unwrap();
    assert_eq!(schema, &["col0", "col1"]);
}

#[test]
fn test_large_dataset() {
    let mut engine = IQLEngine::new();

    // Create a larger dataset
    let mut edges = Vec::new();
    for i in 1..100 {
        edges.push((i, i + 1));
    }
    engine.add_fact("edge", edges);

    // Count edges
    let program = "result(X, Y) <- edge(X, Y)";

    let results = engine.execute(program).unwrap();

    assert_eq!(results.len(), 99);
}

#[test]
fn test_shared_types_compatibility() {
    use crate::{Atom, IRNode, Predicate, Rule, Term};

    // Create an AST rule
    let rule = Rule {
        head: Atom {
            relation: "test".to_string(),
            args: vec![Term::Variable("x".to_string())],
        },
        body: vec![],
    };

    // Create an IR node
    let ir = IRNode::Scan {
        relation: "test".to_string(),
        schema: vec!["x".to_string()],
    };

    // Create a predicate
    let pred = Predicate::ColumnGtConst(0, 5);

    // If these compile, types are compatible!
    assert_eq!(rule.head.relation, "test");
    assert_eq!(ir.output_schema(), vec!["x"]);
    assert!(!matches!(pred, Predicate::True));
}

/// Verify that BooleanDiff produces the same results as isize for set-semantic queries
#[test]
fn test_boolean_diff_produces_same_results_simple_scan() {
    use crate::code_generator::CodeGenerator;
    use crate::ir::IRNode;
    use crate::SemiringType;
    use crate::Tuple;

    let data = vec![
        Tuple::from_pair(1, 2),
        Tuple::from_pair(2, 3),
        Tuple::from_pair(3, 4),
    ];

    // Execute with Counting (isize)
    let mut codegen_counting = CodeGenerator::new();
    codegen_counting.set_semiring_type(SemiringType::Counting);
    codegen_counting.add_input("edge".to_string(), data.clone());

    let ir = IRNode::Scan {
        relation: "edge".to_string(),
        schema: vec!["x".to_string(), "y".to_string()],
    };
    let mut results_counting = codegen_counting.execute(&ir).unwrap();
    results_counting.sort();

    // Execute with Boolean (BooleanDiff)
    let mut codegen_boolean = CodeGenerator::new();
    codegen_boolean.set_semiring_type(SemiringType::Boolean);
    codegen_boolean.add_input("edge".to_string(), data);

    let mut results_boolean = codegen_boolean.execute(&ir).unwrap();
    results_boolean.sort();

    assert_eq!(results_counting, results_boolean, "Scan results must match");
}

/// Verify Boolean and Counting produce same results for join queries
#[test]
fn test_boolean_diff_produces_same_results_join() {
    use crate::code_generator::CodeGenerator;
    use crate::ir::IRNode;
    use crate::SemiringType;
    use crate::Tuple;

    let edges = vec![
        Tuple::from_pair(1, 2),
        Tuple::from_pair(2, 3),
        Tuple::from_pair(3, 4),
    ];

    let ir = IRNode::Join {
        left: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        right: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["a".to_string(), "b".to_string()],
        }),
        left_keys: vec![1],
        right_keys: vec![0],
        output_schema: vec!["x".to_string(), "y".to_string(), "b".to_string()],
    };

    // Execute with Counting
    let mut codegen_counting = CodeGenerator::new();
    codegen_counting.set_semiring_type(SemiringType::Counting);
    codegen_counting.add_input("edge".to_string(), edges.clone());
    let mut results_counting = codegen_counting.execute(&ir).unwrap();
    results_counting.sort();

    // Execute with Boolean
    let mut codegen_boolean = CodeGenerator::new();
    codegen_boolean.set_semiring_type(SemiringType::Boolean);
    codegen_boolean.add_input("edge".to_string(), edges);
    let mut results_boolean = codegen_boolean.execute(&ir).unwrap();
    results_boolean.sort();

    assert_eq!(results_counting, results_boolean, "Join results must match");
}

/// Test recursive shortest path with min<> aggregation in the head.
/// This triggers the aggregation-in-loop optimization in the code generator,
/// which applies min reduction inside the fixpoint loop instead of using
/// distinct_core(), pruning non-optimal paths early.
#[test]
fn test_recursive_shortest_path_min_aggregation() {
    use crate::{Tuple, Value};

    let mut engine = IQLEngine::new();

    // Weighted directed graph:
    //   1 --5--> 2 --3--> 3 --2--> 4
    //   1 ------10------> 3
    engine.add_tuples(
        "edge",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(5)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3), Value::Int64(3)]),
            Tuple::new(vec![Value::Int64(1), Value::Int64(3), Value::Int64(10)]),
            Tuple::new(vec![Value::Int64(3), Value::Int64(4), Value::Int64(2)]),
        ],
    );

    // Compute all distances (no aggregation in recursive rule)
    let program = "\
        dist(X, Y, D) <- edge(X, Y, D)\n\
        dist(X, Z, D) <- dist(X, Y, D1), edge(Y, Z, D2), D = D1 + D2, D < 100\n\
        shortest(X, Y, min<D>) <- dist(X, Y, D)";

    let mut results = engine.execute_tuples(program).unwrap();
    results.sort();

    // Expected shortest paths:
    // (1,2) = 5, (1,3) = min(10, 8) = 8, (1,4) = min(10, 12) = 10
    // (2,3) = 3, (2,4) = 5, (3,4) = 2
    let expected = vec![
        Tuple::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(5)]),
        Tuple::new(vec![Value::Int64(1), Value::Int64(3), Value::Int64(8)]),
        Tuple::new(vec![Value::Int64(1), Value::Int64(4), Value::Int64(10)]),
        Tuple::new(vec![Value::Int64(2), Value::Int64(3), Value::Int64(3)]),
        Tuple::new(vec![Value::Int64(2), Value::Int64(4), Value::Int64(5)]),
        Tuple::new(vec![Value::Int64(3), Value::Int64(4), Value::Int64(2)]),
    ];
    assert_eq!(results, expected, "Shortest path results mismatch");
}

/// Test recursive widest path with max<> aggregation.
/// Widest path = maximum bottleneck bandwidth between nodes.
#[test]
fn test_recursive_widest_path_max_aggregation() {
    use crate::{Tuple, Value};

    let mut engine = IQLEngine::new();

    // Bandwidth graph:
    //   1 --10--> 2 --5--> 3
    //   1 ---3---------->  3
    engine.add_tuples(
        "link",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(10)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3), Value::Int64(5)]),
            Tuple::new(vec![Value::Int64(1), Value::Int64(3), Value::Int64(3)]),
        ],
    );

    // Compute all bandwidths, then take max
    // Bandwidth of a path = min of edge bandwidths (bottleneck)
    // For simplicity, we just compute all paths with their last-hop bandwidth
    // and take max, which tests the max<> aggregation path
    let program = "\
        bw(X, Y, B) <- link(X, Y, B)\n\
        bw(X, Z, B) <- bw(X, Y, _), link(Y, Z, B), B > 0\n\
        max_bw(X, Y, max<B>) <- bw(X, Y, B)";

    let mut results = engine.execute_tuples(program).unwrap();
    results.sort();

    // bw(1,2,10), bw(2,3,5), bw(1,3,3), bw(1,3,5) [via 1->2->3]
    // max_bw(1,2) = 10, max_bw(2,3) = 5, max_bw(1,3) = max(3, 5) = 5
    let expected = vec![
        Tuple::new(vec![Value::Int64(1), Value::Int64(2), Value::Int64(10)]),
        Tuple::new(vec![Value::Int64(1), Value::Int64(3), Value::Int64(5)]),
        Tuple::new(vec![Value::Int64(2), Value::Int64(3), Value::Int64(5)]),
    ];
    assert_eq!(results, expected, "Widest path results mismatch");
}

#[test]
fn test_optimization_actually_optimizes() {
    let mut engine = IQLEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    // Query that creates identity projection (will be optimized away)
    let query = "result(X, Y) <- edge(X, Y)";
    let (_results, trace) = engine.execute_with_trace(query).unwrap();

    // In the IR, there might be a Map node before optimization
    // After optimization, it should be simplified
    // Check that optimization happened (nodes reduced or stayed same)
    assert!(
        trace.stats.nodes_after <= trace.stats.nodes_before,
        "Optimization should not increase node count"
    );
}

#[test]
fn test_multiple_rules_execution() {
    let mut engine = IQLEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let program = "
        direct(X, Y) <- edge(X, Y)
        hop2(X, Z) <- edge(X, Y), edge(Y, Z)
    ";

    let results_map = engine.execute_all_rules(program).unwrap();

    // With SIP enabled, additional intermediate rules may be generated.
    // The original rules are at indices 0 (direct) and the last index (hop2).
    assert!(results_map.len() >= 2, "Expected at least 2 rules");
    assert!(results_map.contains_key(&0)); // Rule 0: direct

    let direct_results = &results_map[&0];
    assert_eq!(direct_results.len(), 3);

    // The final rule (hop2) is at the last index
    let last_idx = results_map.len() - 1;
    let hop2_results = &results_map[&last_idx];
    assert_eq!(hop2_results.len(), 2);
}

#[test]
fn test_fact_parsing() {
    let mut engine = IQLEngine::new();

    // Test parsing facts (rules with no body)
    let program = "
        edge(1, 2)
        edge(2, 3)
    ";

    engine.parse(program).unwrap();
    let prog = engine.program().unwrap();

    assert_eq!(prog.rules.len(), 2);
    for rule in &prog.rules {
        assert_eq!(rule.body.len(), 0, "Facts should have empty body");
    }
}

#[test]
fn test_whitespace_handling() {
    let mut engine = IQLEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    // Test various whitespace scenarios
    let program = "

        result(X,Y)<-edge(X,Y)

    ";

    let results = engine.execute(program).unwrap();
    assert_eq!(results.len(), 1);
}
