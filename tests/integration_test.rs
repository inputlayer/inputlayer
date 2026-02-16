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

    // Query: result(X, Y) <- edge(X, Y)
    let program = "result(X, Y) <- edge(X, Y)";

    let results = engine.execute(program).unwrap();

    // Should return all edges
    assert_eq!(results.len(), 3);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(2, 3)));
    assert!(results.contains(&(3, 4)));
}

#[test]
fn test_projection_query() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Query: result(Y, X) <- edge(X, Y) (swap columns)
    let program = "result(Y, X) <- edge(X, Y)";

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

    // Query: result(X, Z) <- edge(X, Y), edge(Y, Z)
    // This computes 2-hop paths
    let program = "result(X, Z) <- edge(X, Y), edge(Y, Z)";

    let results = engine.execute(program).unwrap();

    // Should find the 2-hop path: 1->2->3
    assert_eq!(results.len(), 1);
    assert!(results.contains(&(1, 3)));
}

#[test]
fn test_self_join() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 1), (2, 2), (3, 4)]);

    // Query: result(X, Y) <- edge(X, Y), edge(Y, X)
    // Finds bidirectional edges (including self-loops)
    let program = "result(X, Y) <- edge(X, Y), edge(Y, X)";

    let results = engine.execute(program).unwrap();

    // Should find self-loops: (1,1) and (2,2)
    assert!(results.contains(&(1, 1)));
    assert!(results.contains(&(2, 2)));
}

#[test]
fn test_parse_with_comments() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let program = "
        // This is a comment
        // This is also a comment (Prolog-style)
        result(X, Y) <- edge(X, Y)

        // Another comment
    ";

    let results = engine.execute(program).unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_multiple_rules() {
    let mut engine = DatalogEngine::new();

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
fn test_safety_validation() {
    let mut engine = DatalogEngine::new();

    // Unsafe rule: z appears in head but not in body
    let program = "result(X, Z) <- edge(X, Y)";

    let result = engine.execute(program);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unsafe rule"));
}

#[test]
fn test_empty_relation() {
    let mut engine = DatalogEngine::new();

    // No facts added
    let program = "result(X, Y) <- edge(X, Y)";

    let results = engine.execute(program).unwrap();

    // Should return empty results
    assert_eq!(results.len(), 0);
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

    // This query has identity projection (x, y) <- edge(X, Y)
    let program = "result(X, Y) <- edge(X, Y)";

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
    let program = "result(X, Y) <- edge(X, Y)";

    let results = engine.execute(program).unwrap();

    assert_eq!(results.len(), 99);
}

#[test]
fn test_triangles_query() {
    let mut engine = DatalogEngine::new();

    // Create a triangle: 1->2, 2->3, 3->1
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 1)]);

    // Find triangles: result(X, Z) <- edge(X, Y), edge(Y, Z), edge(Z, X)
    let program = "result(X, Z) <- edge(X, Y), edge(Y, Z), edge(Z, X)";

    let results = engine.execute(program).unwrap();

    // Should find the triangle edges
    assert!(results.len() > 0);
}

#[test]
fn test_parse_simple_rule() {
    let mut engine = DatalogEngine::new();

    let program = "result(X, Y) <- edge(X, Y)";

    // Test parsing
    let parse_result = engine.parse(program);

    assert!(parse_result.is_ok());
    let program = parse_result.unwrap();
    assert_eq!(program.rules.len(), 1);
    assert_eq!(program.rules[0].head.relation, "result");
}

#[test]
fn test_shared_types_compatibility() {
    use inputlayer::{Atom, IRNode, Predicate, Rule, Term};

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

#[test]
fn test_optimization_config() {
    use inputlayer::OptimizationConfig;

    let config = OptimizationConfig {
        enable_join_planning: true,
        enable_sip_rewriting: true,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };

    let engine = DatalogEngine::with_config(config.clone());
    assert_eq!(engine.config().enable_join_planning, true);
    assert_eq!(engine.config().enable_subplan_sharing, false);
}

// Negation (Antijoin) Tests
#[test]
fn test_simple_negation() {
    let mut engine = DatalogEngine::new();

    // All employees
    engine.add_fact(
        "employee",
        vec![(1, 10), (2, 10), (3, 20), (4, 20), (5, 30)],
    );
    // Employees on leave
    engine.add_fact("on_leave", vec![(2, 0), (4, 0)]);

    // Query: active employees (not on leave)
    // active(EmpId, DeptId) <- employee(EmpId, DeptId), !on_leave(EmpId, _)
    let program = "active(X, Y) <- employee(X, Y), !on_leave(X, _)";

    let results = engine.execute(program).unwrap();

    // Should return employees 1, 3, 5 (those NOT on leave)
    assert_eq!(
        results.len(),
        3,
        "Expected 3 active employees, got {:?}",
        results
    );
    assert!(results.contains(&(1, 10)), "Employee 1 should be active");
    assert!(results.contains(&(3, 20)), "Employee 3 should be active");
    assert!(results.contains(&(5, 30)), "Employee 5 should be active");

    // Should NOT contain on_leave employees
    assert!(
        !results.contains(&(2, 10)),
        "Employee 2 is on leave, should not be in results"
    );
    assert!(
        !results.contains(&(4, 20)),
        "Employee 4 is on leave, should not be in results"
    );
}

#[test]
fn test_negation_with_join() {
    let mut engine = DatalogEngine::new();

    // Employees with department
    engine.add_fact(
        "employee",
        vec![(1, 10), (2, 10), (3, 20), (4, 20), (5, 30)],
    );
    // Departments with managers
    engine.add_fact("department", vec![(10, 100), (20, 200), (30, 300)]);
    // Employees on leave
    engine.add_fact("on_leave", vec![(2, 0), (4, 0)]);

    // Query: active employees with their manager
    // active_mgr(EmpId, MgrId) <- employee(EmpId, DeptId), department(DeptId, MgrId), !on_leave(EmpId, _)
    let program = "active_mgr(X, Z) <- employee(X, Y), department(Y, Z), !on_leave(X, _)";

    let results = engine.execute(program).unwrap();

    // Should return (1, 100), (3, 200), (5, 300) - employees NOT on leave with their managers
    assert_eq!(
        results.len(),
        3,
        "Expected 3 active employee-manager pairs, got {:?}",
        results
    );
    assert!(
        results.contains(&(1, 100)),
        "Employee 1 with manager 100 should be active"
    );
    assert!(
        results.contains(&(3, 200)),
        "Employee 3 with manager 200 should be active"
    );
    assert!(
        results.contains(&(5, 300)),
        "Employee 5 with manager 300 should be active"
    );

    // Should NOT contain on_leave employees
    assert!(
        !results.contains(&(2, 100)),
        "Employee 2 is on leave, should not be in results"
    );
    assert!(
        !results.contains(&(4, 200)),
        "Employee 4 is on leave, should not be in results"
    );
}

#[test]
fn test_negation_on_view() {
    // Test case: negation where the negated relation is another rule's result (a view)
    // This mimics the failing snapshot test 06_negation_self_relation.idl
    let mut engine = DatalogEngine::new();

    // Base relation: edges in a graph
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (1, 3), (2, 4)]);

    // Program with multiple rules (views):
    // source_node(X, Y) <- edge(X, Y)             -- wraps edge
    // target_node(Y, X) <- edge(X, Y)             -- swaps X and Y from edge
    // pure_source(X, Y) <- source_node(X, Y), !target_node(X, _)   -- negation on a view
    let program = r#"
        source_node(X, Y) <- edge(X, Y)
        target_node(Y, X) <- edge(X, Y)
        pure_source(X, Y) <- source_node(X, Y), !target_node(X, _)
    "#;

    let results = engine.execute(program).unwrap();

    // Analysis:
    // edge = [(1,2), (2,3), (3,4), (1,3), (2,4)]
    // source_node = edge = [(1,2), (2,3), (3,4), (1,3), (2,4)]
    // target_node(Y, X) <- edge(X, Y) gives: [(2,1), (3,2), (4,3), (3,1), (4,2)]
    // target_node's first column values: {2, 3, 4}
    //
    // pure_source = source_node where X NOT in target_node's first column
    // Source node X values: 1, 2, 3, 1, 2
    // Filter: keep where X NOT in {2, 3, 4}
    // Only X=1 passes: (1,2) and (1,3)
    //
    // Expected: [(1, 2), (1, 3)]
    assert_eq!(
        results.len(),
        2,
        "Expected 2 pure source nodes, got {:?}",
        results
    );
    assert!(
        results.contains(&(1, 2)),
        "Node (1,2) should be a pure source"
    );
    assert!(
        results.contains(&(1, 3)),
        "Node (1,3) should be a pure source"
    );

    // These should NOT be in results (their X value is in target_node's first column)
    assert!(
        !results.contains(&(2, 3)),
        "Node (2,3) has X=2 which is in target_node"
    );
    assert!(
        !results.contains(&(3, 4)),
        "Node (3,4) has X=3 which is in target_node"
    );
    assert!(
        !results.contains(&(2, 4)),
        "Node (2,4) has X=2 which is in target_node"
    );
}

#[test]
fn test_sip_four_way_join() {
    use inputlayer::{DatalogEngine, OptimizationConfig, Tuple, Value};

    let config = OptimizationConfig {
        enable_sip_rewriting: true,
        ..OptimizationConfig::default()
    };
    let mut engine = DatalogEngine::with_config(config);

    engine.add_tuples(
        "users",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::from("alice")]),
            Tuple::new(vec![Value::Int64(2), Value::from("bob")]),
        ],
    );
    engine.add_tuples(
        "emails",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::from("alice@mail.com")]),
            Tuple::new(vec![Value::Int64(2), Value::from("bob@mail.com")]),
        ],
    );
    engine.add_tuples(
        "roles",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::from("admin")]),
            Tuple::new(vec![Value::Int64(2), Value::from("user")]),
        ],
    );
    engine.add_tuples(
        "departments",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::from("engineering")]),
            Tuple::new(vec![Value::Int64(2), Value::from("sales")]),
        ],
    );

    let program = "full_user_info(Name, Email, Role, Dept) <- users(Id, Name), emails(Id, Email), roles(Id, Role), departments(Id, Dept)";

    let results = engine.execute_tuples(program).unwrap();
    eprintln!("SIP four-way join results: {:?}", results);
    assert_eq!(
        results.len(),
        2,
        "Expected 2 results from four-way join, got {}",
        results.len()
    );
}

#[test]
fn test_sip_self_join() {
    use inputlayer::{DatalogEngine, OptimizationConfig, Tuple, Value};

    let config = OptimizationConfig {
        enable_sip_rewriting: true,
        ..OptimizationConfig::default()
    };
    let mut engine = DatalogEngine::with_config(config);

    engine.add_tuples(
        "edge",
        vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
            Tuple::new(vec![Value::Int64(3), Value::Int64(4)]),
        ],
    );

    // Self-join: edge(X, Y), edge(Y, Z) -> 2-hop paths
    let program = "connected(X, Z) <- edge(X, Y), edge(Y, Z)";

    let results = engine.execute_tuples(program).unwrap();
    eprintln!("SIP self-join results: {:?}", results);
    assert_eq!(
        results.len(),
        2,
        "Expected 2 two-hop paths, got {}",
        results.len()
    );
}

// Boolean Diff Type Integration Tests
/// Verify that BooleanDiff produces the same results as isize for set-semantic queries
#[test]
fn test_boolean_diff_produces_same_results_simple_scan() {
    use inputlayer::code_generator::CodeGenerator;
    use inputlayer::ir::IRNode;
    use inputlayer::SemiringType;
    use inputlayer::Tuple;

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
    use inputlayer::code_generator::CodeGenerator;
    use inputlayer::ir::IRNode;
    use inputlayer::SemiringType;
    use inputlayer::Tuple;

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

/// Verify Boolean and Counting produce same results for full pipeline queries
#[test]
fn test_boolean_diff_full_pipeline() {
    // Execute same query with boolean specialization enabled (default)
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let mut results = engine.execute("path(X, Y) <- edge(X, Y)").unwrap();
    results.sort();

    // Boolean specialization is always on; verify results are correct
    assert_eq!(results, vec![(1, 2), (2, 3), (3, 4)]);
}

/// Verify Boolean diff works for recursive transitive closure
#[test]
fn test_boolean_diff_transitive_closure() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let mut results = engine
        .execute(
            "path(X, Y) <- edge(X, Y)\n\
             path(X, Z) <- path(X, Y), edge(Y, Z)",
        )
        .unwrap();
    results.sort();

    // Should find all transitive pairs
    let expected = vec![(1, 2), (1, 3), (1, 4), (2, 3), (2, 4), (3, 4)];
    assert_eq!(results, expected);
}

/// Verify aggregation queries correctly fall back to Counting semiring
#[test]
fn test_counting_fallback_for_aggregation() {
    use inputlayer::ir::{AggregateFunction, IRNode};
    use inputlayer::{BooleanSpecializer, SemiringType};

    let mut specializer = BooleanSpecializer::new();
    let ir = IRNode::Aggregate {
        input: Box::new(IRNode::Scan {
            relation: "R".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        group_by: vec![0],
        aggregations: vec![(AggregateFunction::Count, 1)],
        output_schema: vec!["x".to_string(), "count".to_string()],
    };

    let (_, annotation) = specializer.specialize(ir);
    assert_eq!(
        annotation.semiring,
        SemiringType::Counting,
        "Aggregation must use Counting semiring"
    );
}

/// Verify set-semantic queries correctly select Boolean semiring
#[test]
fn test_boolean_selection_for_set_queries() {
    use inputlayer::ir::IRNode;
    use inputlayer::{BooleanSpecializer, SemiringType};

    let mut specializer = BooleanSpecializer::new();
    let ir = IRNode::Distinct {
        input: Box::new(IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "R".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "S".to_string(),
                schema: vec!["a".to_string(), "b".to_string()],
            }),
            left_keys: vec![1],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "y".to_string(), "b".to_string()],
        }),
    };

    let (_, annotation) = specializer.specialize(ir);
    assert_eq!(
        annotation.semiring,
        SemiringType::Boolean,
        "Set-semantic query must use Boolean semiring"
    );
}

// Min/Max Recursive Aggregation Tests
/// Test recursive shortest path with min<> aggregation in the head.
/// This triggers the aggregation-in-loop optimization in the code generator,
/// which applies min reduction inside the fixpoint loop instead of using
/// distinct_core(), pruning non-optimal paths early.
#[test]
fn test_recursive_shortest_path_min_aggregation() {
    use inputlayer::{Tuple, Value};

    let mut engine = DatalogEngine::new();

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
    use inputlayer::{Tuple, Value};

    let mut engine = DatalogEngine::new();

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

/// Verify Min semiring annotation is correctly detected for recursive min aggregation
#[test]
fn test_min_semiring_annotation() {
    use inputlayer::ir::{AggregateFunction, IRNode};
    use inputlayer::{BooleanSpecializer, SemiringType};

    let mut specializer = BooleanSpecializer::new();
    let ir = IRNode::Aggregate {
        input: Box::new(IRNode::Scan {
            relation: "R".to_string(),
            schema: vec!["x".to_string(), "y".to_string(), "d".to_string()],
        }),
        group_by: vec![0, 1],
        aggregations: vec![(AggregateFunction::Min, 2)],
        output_schema: vec!["x".to_string(), "y".to_string(), "min_d".to_string()],
    };

    let (_, annotation) = specializer.specialize(ir);
    assert_eq!(
        annotation.semiring,
        SemiringType::Min,
        "Min aggregation must produce Min semiring"
    );
}

/// Verify Max semiring annotation is correctly detected for recursive max aggregation
#[test]
fn test_max_semiring_annotation() {
    use inputlayer::ir::{AggregateFunction, IRNode};
    use inputlayer::{BooleanSpecializer, SemiringType};

    let mut specializer = BooleanSpecializer::new();
    let ir = IRNode::Aggregate {
        input: Box::new(IRNode::Scan {
            relation: "R".to_string(),
            schema: vec!["x".to_string(), "y".to_string(), "d".to_string()],
        }),
        group_by: vec![0, 1],
        aggregations: vec![(AggregateFunction::Max, 2)],
        output_schema: vec!["x".to_string(), "y".to_string(), "max_d".to_string()],
    };

    let (_, annotation) = specializer.specialize(ir);
    assert_eq!(
        annotation.semiring,
        SemiringType::Max,
        "Max aggregation must produce Max semiring"
    );
}
