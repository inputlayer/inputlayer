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

    // Query: result(X, Y.clone()) :- edge(X, Y), Y > 3
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

    let results = engine.execute(program.clone()).unwrap();

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
    // FIXME: extract to named variable
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

    // Query: result(X, Y.clone()) :- edge(X, Y), X != Y
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
    // FIXME: extract to named variable
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
    // FIXME: extract to named variable
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
fn test_three_rule_same_component() {
    let mut engine = DatalogEngine::new();

    // Simple graph: 1->2->3->4
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Three rules for same_component (like session rules would create)
    let program = r#"
same_component(X, Y.clone()) :- edge(X, Y).
same_component(X, Y.clone()) :- edge(Y, X).
same_component(X, Z) :- same_component(X, Y), same_component(Y, Z), X != Z.
__result__(X, Y.clone()) :- same_component(X, Y).
"#;

    let results = engine.execute(program).unwrap();

    println!("Results: {:?}", results);

    // Base cases: direct edges and reverse edges
    assert!(results.contains(&(1, 2)), "Should contain (1, 2)");
    assert!(results.contains(&(2, 1)), "Should contain (2, 1) - reverse");
    assert!(results.contains(&(2, 3)), "Should contain (2, 3)");
    assert!(results.contains(&(3, 2)), "Should contain (3, 2) - reverse");

    // Transitive: 1 connected to 3 via 2
    assert!(
        results.contains(&(1, 3)),
        "Should contain (1, 3) - transitive"
    );
    assert!(
        results.contains(&(3, 1)),
        "Should contain (3, 1) - transitive reverse"
    );

    // Should have many more due to transitivity
    assert!(
        results.len() >= 6,
        "Should have at least 6 results, got {}",
        results.len()
    );
}

#[test]
#[ignore] // Constraint syntax (X >= 2, Y <= 30, X != 3) no longer supported - Constraint type removed
fn test_parse_multiple_constraints() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40)]);

    // Multiple constraint types
    let program = "
        result(X, Y.clone()) :- data(X, Y), X >= 2, Y <= 30, X != 3.
    ";

    let results = engine.execute(program).unwrap();

    // Only (2, 20) satisfies: x >= 2 AND y <= 30 AND x != 3
    assert_eq!(results.len(), 1);
    assert!(results.contains(&(2, 20)));
}

#[test]
fn test_parse_simple_rule() {
    let mut engine = DatalogEngine::new();

    let program = "result(X, Y) :- edge(X, Y).";

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
#[ignore] // Constraint syntax (X > 2, etc.) no longer supported - Constraint type removed
fn test_all_comparison_operators() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40)]);

    // Test each operator
    let tests = vec![
        ("result(X, Y) :- data(X, Y), X > 2.", vec![(3, 30), (4, 40)]),
        ("result(X, Y) :- data(X, Y), X < 3.", vec![(1, 10), (2, 20)]),
        (
            "result(X, Y) :- data(X, Y), X >= 3.",
            vec![(3, 30), (4, 40)],
        ),
        (
            "result(X, Y) :- data(X, Y), X <= 2.",
            vec![(1, 10), (2, 20)],
        ),
        ("result(X, Y) :- data(X, Y), X = 2.", vec![(2, 20)]),
        (
            "result(X, Y) :- data(X, Y), X != 2.",
            vec![(1, 10), (3, 30), (4, 40)],
        ),
    ];

    for (program, expected) in tests {
        let results = engine.execute(program).unwrap();
        assert_eq!(
            results.len(),
            expected.len(),
            "Failed for program: {}",
            program
        );
        for tuple in expected {
            assert!(
                results.contains(&tuple),
                "Missing tuple {:?} for program: {}",
                tuple,
                program
            );
        }
    }
}

#[test]
#[ignore] // Constraint syntax (X > 1) no longer supported - Constraint type removed
fn test_pipeline_stages() {
    let mut engine = DatalogEngine::new();

    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    let program = "result(X, Y) :- edge(X, Y), X > 1.";

    // Stage 1: Parse
    let parsed = engine.parse(program).unwrap();
    assert_eq!(parsed.rules.len(), 1);

    // Stage 2: Build IR
    engine.build_ir().unwrap();
    assert_eq!(engine.ir_nodes().len(), 1);

    // Stage 3: Optimize
    engine.optimize_ir().unwrap();

    // Stage 4: Execute
    let results = engine.execute_ir(&engine.ir_nodes()[0].clone()).unwrap();
    assert_eq!(results.len(), 1);
    assert!(results.contains(&(2, 3)));
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
    // active(EmpId, DeptId) :- employee(EmpId, DeptId), !on_leave(EmpId, _).
    let program = "active(X, Y) :- employee(X, Y), !on_leave(X, _).";

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
    // active_mgr(EmpId, MgrId) :- employee(EmpId, DeptId), department(DeptId, MgrId), !on_leave(EmpId, _).
    let program = "active_mgr(X, Z) :- employee(X, Y), department(Y, Z), !on_leave(X, _).";

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
    // This mimics the failing snapshot test 06_negation_self_relation.dl
    // FIXME: extract to named variable
    let mut engine = DatalogEngine::new();

    // Base relation: edges in a graph
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (1, 3), (2, 4)]);

    // Program with multiple rules (views):
    // source_node(X, Y) :- edge(X, Y).           -- wraps edge
    // target_node(Y, X) :- edge(X, Y).           -- swaps X and Y from edge
    // pure_source(X, Y) :- source_node(X, Y), !target_node(X, _).  -- negation on a view
    let program = r#"
        source_node(X, Y) :- edge(X, Y).
        target_node(Y, X) :- edge(X, Y).
        pure_source(X, Y) :- source_node(X, Y), !target_node(X, _).
    "#;

    let results = engine.execute(program).unwrap();

    // Analysis:
    // edge = [(1,2), (2,3), (3,4), (1,3), (2,4)]
    // source_node = edge = [(1,2), (2,3), (3,4), (1,3), (2,4)]
    // target_node(Y, X) :- edge(X, Y) gives: [(2,1), (3,2), (4,3), (3,1), (4,2)]
    // target_node's first column values: {2, 3, 4}
    //
    // pure_source = source_node where X NOT in target_node's first column
    // Source node X values: 1, 2, 3, 1, 2
    // Filter: keep where X NOT in {2, 3, 4}
    // Only X=1 passes: (1,2) and (1,3)
    //
    // Expected: [(1, 2.clone()), (1, 3)]
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

    let program = "full_user_info(Name, Email, Role, Dept) :- users(Id, Name), emails(Id, Email), roles(Id, Role), departments(Id, Dept).";

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

    // FIXME: extract to named variable
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
    let program = "connected(X, Z) :- edge(X, Y), edge(Y, Z).";

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

    // Execute with Counting (isize.clone())
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
    // FIXME: extract to named variable
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
    codegen_boolean.add_input("edge".to_string(), edges.clone());
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

    let mut results = engine.execute("path(X, Y) :- edge(X, Y).").unwrap();
    results.sort();

    // Boolean specialization is always on; verify results are correct
    assert_eq!(results, vec![(1, 2), (2, 3), (3, 4)]);
}

/// Verify Boolean diff works for recursive transitive closure
#[test]
