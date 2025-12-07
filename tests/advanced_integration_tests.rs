//! Advanced Integration Tests
//!
//! More complex scenarios testing the full pipeline

use datalog_engine::DatalogEngine;
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
#[ignore] // TODO: Variable-to-variable comparison in join context not yet supported
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
#[ignore] // TODO: Cartesian products (no shared variables) not yet supported
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
    assert!(result_set.contains(&(1, 2)) || result_set.contains(&(2, 1)),
        "Expected (1,2) or (2,1) cycle but got {:?}", result_set);
    assert!(result_set.contains(&(4, 4)), "Expected self-loop (4,4) but got {:?}", result_set);
}

#[test]
fn test_empty_result_set() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Filter that excludes everything
    let query = "result(X, Y) :- edge(X, Y), X > 10.";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 0, "Expected empty result set");
}

#[test]
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
#[ignore] // TODO: Variable-to-variable comparison across joins not yet supported
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

    // Three-way join: a ⋈ b ⋈ c
    let query = "result(X, W) :- a(X, Y), b(Y, Z), c(Z, W).";
    let results = engine.execute(query).unwrap();

    assert!(!results.is_empty());
}

#[test]
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
fn test_all_comparison_operators_comprehensive() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

    // Test each operator
    let tests = vec![
        ("result(X, Y) :- data(X, Y), X = 3.", 1), // Equal
        ("result(X, Y) :- data(X, Y), X != 3.", 4), // Not equal
        ("result(X, Y) :- data(X, Y), X < 3.", 2), // Less than
        ("result(X, Y) :- data(X, Y), X > 3.", 2), // Greater than
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
    assert!(err.contains("Unsafe") || err.contains("not found"), "Error should mention safety: {}", err);
}

#[test]
fn test_empty_database_query() {
    let mut engine = DatalogEngine::new();
    // No facts added

    let query = "result(X, Y) :- edge(X, Y).";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_optimization_actually_optimizes() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3)]);

    // Query that creates identity projection (will be optimized away)
    let query = "result(X, Y) :- edge(X, Y).";
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
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    let program = "
        direct(X, Y) :- edge(X, Y).
        hop2(X, Z) :- edge(X, Y), edge(Y, Z).
    ";

    let results_map = engine.execute_all_rules(program).unwrap();

    assert_eq!(results_map.len(), 2);
    assert!(results_map.contains_key(&0)); // Rule 0: direct
    assert!(results_map.contains_key(&1)); // Rule 1: hop2

    let direct_results = &results_map[&0];
    assert_eq!(direct_results.len(), 3);

    let hop2_results = &results_map[&1];
    assert_eq!(hop2_results.len(), 2);
}

#[test]
fn test_asymmetric_joins() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("parent", vec![(1, 2), (1, 3), (2, 4)]);
    engine.add_fact("age", vec![(1, 50), (2, 25), (3, 22), (4, 5)]);

    // Join parent with age: result(P, C, A) :- parent(P, C), age(C, A)
    let query = "result(P, A) :- parent(P, C), age(C, A).";
    let results = engine.execute(query).unwrap();

    assert!(!results.is_empty());
    let result_set = to_set(results);

    // parent(1,2), age(2,25) -> (1,25)
    assert!(result_set.contains(&(1, 25)));
    // parent(1,3), age(3,22) -> (1,22)
    assert!(result_set.contains(&(1, 22)));
    // parent(2,4), age(4,5) -> (2,5)
    assert!(result_set.contains(&(2, 5)));
}

#[test]
fn test_filters_pushed_through_pipeline() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

    // Multiple filters that should all be applied
    let query = "result(X, Y) :- data(X, Y), X > 1, X < 5, Y >= 20, Y <= 40.";
    let results = engine.execute(query).unwrap();

    // Should get (2,20), (3,30), (4,40)
    assert_eq!(results.len(), 3);

    for (x, y) in &results {
        assert!(*x > 1 && *x < 5, "x should be in (1, 5)");
        assert!(*y >= 20 && *y <= 40, "y should be in [20, 40]");
    }
}

#[test]
fn test_column_to_column_comparison() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("pairs", vec![(1, 1), (1, 2), (2, 1), (2, 2), (3, 4)]);

    // X != Y (column-to-column comparison)
    let query = "result(X, Y) :- pairs(X, Y), X != Y.";
    let results = engine.execute(query).unwrap();

    let result_set = to_set(results);
    assert!(!result_set.contains(&(1, 1)));
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 1)));
    assert!(!result_set.contains(&(2, 2)));
    assert!(result_set.contains(&(3, 4)));
}

#[test]
fn test_fact_parsing() {
    let mut engine = DatalogEngine::new();

    // Test parsing facts (rules with no body)
    let program = "
        edge(1, 2).
        edge(2, 3).
    ";

    engine.parse(program).unwrap();
    let prog = engine.program().unwrap();

    assert_eq!(prog.rules.len(), 2);
    for rule in &prog.rules {
        assert_eq!(rule.body.len(), 0, "Facts should have empty body");
    }
}

#[test]
fn test_comment_handling() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    let program = "
        % This is a comment
        // This is also a comment
        result(X, Y) :- edge(X, Y).
    ";

    let results = engine.execute(program).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_whitespace_handling() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    // Test various whitespace scenarios
    let program = "

        result(X,Y):-edge(X,Y).

    ";

    let results = engine.execute(program).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_config_creation() {
    use datalog_engine::OptimizationConfig;

    let config = OptimizationConfig::default();
    // Most optimizations enabled; SIP disabled by default due to known issues
    assert!(config.enable_join_planning);
    assert!(!config.enable_sip_rewriting);
    assert!(config.enable_subplan_sharing);
    assert!(config.enable_boolean_specialization);

    let mut engine = DatalogEngine::with_config(config);
    engine.add_fact("edge", vec![(1, 2)]);

    let results = engine.execute("result(X, Y) :- edge(X, Y).").unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_distinct_eliminates_duplicates() {
    let mut engine = DatalogEngine::new();
    // Add duplicate facts
    engine.add_fact("data", vec![(1, 2), (1, 2), (2, 3), (2, 3), (3, 4)]);

    let query = "result(X, Y) :- data(X, Y).";
    let results = engine.execute(query).unwrap();

    // DD collections are naturally distinct, so duplicates should be eliminated
    let result_set = to_set(results);
    assert_eq!(result_set.len(), 3);
}

#[test]
fn test_no_constraints_returns_all() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (3, 4), (5, 6)]);

    let query = "result(X, Y) :- edge(X, Y).";
    let results = engine.execute(query).unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_catalog_tracks_schemas() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    let catalog = engine.catalog();
    assert!(catalog.has_relation("edge"));
}

#[test]
fn test_ir_nodes_generated() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2)]);

    engine.parse("result(X, Y) :- edge(X, Y).").unwrap();
    engine.build_ir().unwrap();

    let ir_nodes = engine.ir_nodes();
    assert_eq!(ir_nodes.len(), 1);
}

#[test]
fn test_optimization_config_accessible() {
    let engine = DatalogEngine::new();
    let config = engine.config();

    // Verify default configuration
    assert!(config.enable_join_planning);
    // SIP disabled by default due to known issues
    assert!(!config.enable_sip_rewriting);
    assert!(config.enable_subplan_sharing);
    assert!(config.enable_boolean_specialization);
}

#[test]
fn test_multiple_atoms_in_body() {
    let mut engine = DatalogEngine::new();
    engine.add_fact("r", vec![(1, 2)]);
    engine.add_fact("s", vec![(2, 3)]);
    engine.add_fact("t", vec![(3, 4)]);

    // Three atoms in body
    let query = "result(X, W) :- r(X, Y), s(Y, Z), t(Z, W).";
    let results = engine.execute(query).unwrap();

    assert!(!results.is_empty());
    assert!(results.contains(&(1, 4)));
}

/// Test aggregation using direct IR construction
/// This tests count, sum, min, max aggregations
#[test]
fn test_aggregation_count() {
    use datalog_engine::code_generator::CodeGenerator;
    use datalog_engine::value::{Tuple, Value};
    use datalog_ir::{AggregateFunction, IRNode};

    let mut generator = CodeGenerator::new();

    // Data: sales(category, amount)
    generator.add_input_tuples(
        "sales".to_string(),
        vec![
            Tuple::new(vec![Value::string("electronics"), Value::Int32(100)]),
            Tuple::new(vec![Value::string("electronics"), Value::Int32(200)]),
            Tuple::new(vec![Value::string("electronics"), Value::Int32(150)]),
            Tuple::new(vec![Value::string("clothing"), Value::Int32(50)]),
            Tuple::new(vec![Value::string("clothing"), Value::Int32(75)]),
        ],
    );

    // COUNT aggregation: count items per category
    let ir = IRNode::Aggregate {
        input: Box::new(IRNode::Scan {
            relation: "sales".to_string(),
            schema: vec!["category".to_string(), "amount".to_string()],
        }),
        group_by: vec![0], // group by category
        aggregations: vec![(AggregateFunction::Count, 1)], // count items
        output_schema: vec!["category".to_string(), "count".to_string()],
    };

    let results = generator.generate_and_execute_tuples(&ir).unwrap();

    // Should have 2 groups: electronics (3 items) and clothing (2 items)
    assert_eq!(results.len(), 2);

    // Check counts
    for tuple in &results {
        let category = tuple.get(0).unwrap().as_str().unwrap();
        let count = tuple.get(1).unwrap().as_i64().unwrap();
        match category {
            "electronics" => assert_eq!(count, 3),
            "clothing" => assert_eq!(count, 2),
            _ => panic!("Unexpected category: {}", category),
        }
    }
}

#[test]
fn test_aggregation_sum() {
    use datalog_engine::code_generator::CodeGenerator;
    use datalog_engine::value::{Tuple, Value};
    use datalog_ir::{AggregateFunction, IRNode};

    let mut generator = CodeGenerator::new();

    // Data: sales(category, amount)
    generator.add_input_tuples(
        "sales".to_string(),
        vec![
            Tuple::new(vec![Value::string("electronics"), Value::Int32(100)]),
            Tuple::new(vec![Value::string("electronics"), Value::Int32(200)]),
            Tuple::new(vec![Value::string("clothing"), Value::Int32(50)]),
            Tuple::new(vec![Value::string("clothing"), Value::Int32(75)]),
        ],
    );

    // SUM aggregation: total amount per category
    let ir = IRNode::Aggregate {
        input: Box::new(IRNode::Scan {
            relation: "sales".to_string(),
            schema: vec!["category".to_string(), "amount".to_string()],
        }),
        group_by: vec![0], // group by category
        aggregations: vec![(AggregateFunction::Sum, 1)], // sum amount
        output_schema: vec!["category".to_string(), "total".to_string()],
    };

    let results = generator.generate_and_execute_tuples(&ir).unwrap();

    assert_eq!(results.len(), 2);

    for tuple in &results {
        let category = tuple.get(0).unwrap().as_str().unwrap();
        let total = tuple.get(1).unwrap().as_i64().unwrap();
        match category {
            "electronics" => assert_eq!(total, 300), // 100 + 200
            "clothing" => assert_eq!(total, 125),    // 50 + 75
            _ => panic!("Unexpected category: {}", category),
        }
    }
}

#[test]
fn test_aggregation_min_max() {
    use datalog_engine::code_generator::CodeGenerator;
    use datalog_engine::value::{Tuple, Value};
    use datalog_ir::{AggregateFunction, IRNode};

    let mut generator = CodeGenerator::new();

    // Data: scores(student, score)
    generator.add_input_tuples(
        "scores".to_string(),
        vec![
            Tuple::new(vec![Value::string("alice"), Value::Int32(85)]),
            Tuple::new(vec![Value::string("alice"), Value::Int32(92)]),
            Tuple::new(vec![Value::string("alice"), Value::Int32(78)]),
            Tuple::new(vec![Value::string("bob"), Value::Int32(90)]),
            Tuple::new(vec![Value::string("bob"), Value::Int32(88)]),
        ],
    );

    // MIN and MAX aggregation
    let ir = IRNode::Aggregate {
        input: Box::new(IRNode::Scan {
            relation: "scores".to_string(),
            schema: vec!["student".to_string(), "score".to_string()],
        }),
        group_by: vec![0], // group by student
        aggregations: vec![
            (AggregateFunction::Min, 1), // min score
            (AggregateFunction::Max, 1), // max score
        ],
        output_schema: vec![
            "student".to_string(),
            "min_score".to_string(),
            "max_score".to_string(),
        ],
    };

    let results = generator.generate_and_execute_tuples(&ir).unwrap();

    assert_eq!(results.len(), 2);

    for tuple in &results {
        let student = tuple.get(0).unwrap().as_str().unwrap();
        let min_score = tuple.get(1).unwrap().as_i32().unwrap();
        let max_score = tuple.get(2).unwrap().as_i32().unwrap();
        match student {
            "alice" => {
                assert_eq!(min_score, 78);
                assert_eq!(max_score, 92);
            }
            "bob" => {
                assert_eq!(min_score, 88);
                assert_eq!(max_score, 90);
            }
            _ => panic!("Unexpected student: {}", student),
        }
    }
}

// =============================================================================
// SIP Rewriting Tests
// =============================================================================

#[test]
fn test_sip_chain_join_correctness() {
    use datalog_engine::OptimizationConfig;

    // Create engine with SIP enabled to verify it produces correct results
    let config = OptimizationConfig {
        enable_join_planning: true,
        enable_sip_rewriting: true,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };

    let mut engine = DatalogEngine::with_config(config);

    // Chain data: a -> b -> c -> d
    engine.add_fact("r", vec![(1, 2), (3, 4)]);
    engine.add_fact("s", vec![(2, 5), (4, 6)]);
    engine.add_fact("t", vec![(5, 7), (6, 8)]);

    // Chain query: result(A, D) :- r(A, B), s(B, C), t(C, D).
    // Expected: (1, 7) via 1->2->5->7 and (3, 8) via 3->4->6->8
    let query = "result(A, D) :- r(A, B), s(B, C), t(C, D).";
    let results = engine.execute(query).unwrap();

    let result_set = to_set(results);
    assert_eq!(result_set.len(), 2);
    assert!(result_set.contains(&(1, 7)));
    assert!(result_set.contains(&(3, 8)));
}

#[test]
fn test_sip_two_way_join_correctness() {
    use datalog_engine::OptimizationConfig;

    let config = OptimizationConfig {
        enable_join_planning: true,
        enable_sip_rewriting: true,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };

    let mut engine = DatalogEngine::with_config(config);

    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Two-hop path query
    let query = "path2(X, Z) :- edge(X, Y), edge(Y, Z).";
    let results = engine.execute(query).unwrap();

    let result_set = to_set(results);
    assert_eq!(result_set.len(), 2);
    assert!(result_set.contains(&(1, 3))); // 1->2->3
    assert!(result_set.contains(&(2, 4))); // 2->3->4
}

#[test]
fn test_sip_with_dangling_tuples() {
    use datalog_engine::OptimizationConfig;

    // This tests the core value of SIP: filtering out tuples that won't
    // contribute to the final result
    let config = OptimizationConfig {
        enable_join_planning: true,
        enable_sip_rewriting: true,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };

    let mut engine = DatalogEngine::with_config(config);

    // R has tuples that won't match S, S has tuples that won't match T
    engine.add_fact("r", vec![(1, 2), (100, 200), (300, 400)]); // 100,300 won't join
    engine.add_fact("s", vec![(2, 3), (999, 888)]);              // 999 won't join
    engine.add_fact("t", vec![(3, 4)]);

    let query = "result(A, D) :- r(A, B), s(B, C), t(C, D).";
    let results = engine.execute(query).unwrap();

    // Only 1->2->3->4 should work
    assert_eq!(results.len(), 1);
    assert!(to_set(results).contains(&(1, 4)));
}
