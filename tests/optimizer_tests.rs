//! Optimizer Tests
//!
//! Tests for IR optimization passes.

use inputlayer::ir::{IRNode, Predicate};
use inputlayer::Optimizer;

#[test]
fn test_identity_map_single() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Map {
        input: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        projection: vec![0, 1],
        output_schema: vec!["x".to_string(), "y".to_string()],
    };

    let optimized = optimizer.optimize(ir);

    assert!(optimized.is_scan(), "Identity map should be eliminated");
}

#[test]
fn test_identity_map_nested() {
    let optimizer = Optimizer::new();

    // Map(Map(Scan, identity), identity) - both should be removed
    let ir = IRNode::Map {
        input: Box::new(IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![0, 1],
            output_schema: vec!["x".to_string(), "y".to_string()],
        }),
        projection: vec![0, 1],
        output_schema: vec!["x".to_string(), "y".to_string()],
    };

    let optimized = optimizer.optimize(ir);

    assert!(
        optimized.is_scan(),
        "Nested identity maps should be eliminated"
    );
}

#[test]
fn test_non_identity_map_preserved() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Map {
        input: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        projection: vec![1, 0], // Swap - NOT identity
        output_schema: vec!["y".to_string(), "x".to_string()],
    };

    let optimized = optimizer.optimize(ir);

    // Check that it's still a Map node
    assert!(
        matches!(optimized, IRNode::Map { .. }),
        "Non-identity map should be preserved"
    );
}

#[test]
fn test_always_true_filter() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Filter {
        input: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        predicate: Predicate::True,
    };

    let optimized = optimizer.optimize(ir);

    assert!(
        optimized.is_scan(),
        "Always-true filter should be eliminated"
    );
}

#[test]
fn test_always_false_filter() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Filter {
        input: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        predicate: Predicate::False,
    };

    let optimized = optimizer.optimize(ir);

    // Should become empty union
    match optimized {
        IRNode::Union { inputs } => assert_eq!(inputs.len(), 0),
        _ => panic!("Expected empty union for always-false filter"),
    }
}

#[test]
fn test_nested_filters_with_true() {
    let optimizer = Optimizer::new();

    // Filter(True, Filter(x > 5, Scan))
    let ir = IRNode::Filter {
        input: Box::new(IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::ColumnGtConst(0, 5),
        }),
        predicate: Predicate::True,
    };

    let optimized = optimizer.optimize(ir);

    // Should eliminate the True filter, keeping only the real filter
    match optimized {
        IRNode::Filter { predicate, .. } => {
            assert!(matches!(predicate, Predicate::ColumnGtConst(0, 5)));
        }
        _ => panic!("Expected single filter after optimization"),
    }
}

#[test]
fn test_real_predicate_preserved() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Filter {
        input: Box::new(IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }),
        predicate: Predicate::ColumnGtConst(0, 5),
    };

    let optimized = optimizer.optimize(ir);

    // Check that it's still a Filter node
    assert!(
        matches!(optimized, IRNode::Filter { .. }),
        "Real filter should be preserved"
    );
}

#[test]
fn test_join_children_optimized() {
    let optimizer = Optimizer::new();

    // Join with identity maps on both sides
    let ir = IRNode::Join {
        left: Box::new(IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "r".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![0, 1], // Identity
            output_schema: vec!["x".to_string(), "y".to_string()],
        }),
        right: Box::new(IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "s".to_string(),
                schema: vec!["y".to_string(), "z".to_string()],
            }),
            projection: vec![0, 1], // Identity
            output_schema: vec!["y".to_string(), "z".to_string()],
        }),
        left_keys: vec![1],
        right_keys: vec![0],
        output_schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
    };

    let optimized = optimizer.optimize(ir);

    // Check that join children are optimized
    match optimized {
        IRNode::Join { left, right, .. } => {
            assert!(left.is_scan(), "Left child should be optimized to Scan");
            assert!(right.is_scan(), "Right child should be optimized to Scan");
        }
        _ => panic!("Expected Join node"),
    }
}

#[test]
fn test_distinct_child_optimized() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Distinct {
        input: Box::new(IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::True, // Will be eliminated
        }),
    };

    let optimized = optimizer.optimize(ir);

    // Distinct should remain, but child should be optimized
    match optimized {
        IRNode::Distinct { input } => {
            assert!(input.is_scan(), "Child should be optimized to Scan");
        }
        _ => panic!("Expected Distinct node"),
    }
}

#[test]
fn test_union_children_optimized() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Union {
        inputs: vec![
            IRNode::Filter {
                input: Box::new(IRNode::Scan {
                    relation: "r1".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                predicate: Predicate::True,
            },
            IRNode::Map {
                input: Box::new(IRNode::Scan {
                    relation: "r2".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                projection: vec![0, 1], // Identity
                output_schema: vec!["x".to_string(), "y".to_string()],
            },
        ],
    };

    let optimized = optimizer.optimize(ir);

    // Both union children should be optimized to Scans
    match optimized {
        IRNode::Union { inputs } => {
            assert_eq!(inputs.len(), 2);
            assert!(inputs[0].is_scan());
            assert!(inputs[1].is_scan());
        }
        _ => panic!("Expected Union node"),
    }
}

#[test]
fn test_complex_nested_optimization() {
    let optimizer = Optimizer::new();

    // Complex: Filter(True, Map(identity, Join(
    //   Filter(True, Scan),
    //   Map(identity, Scan)
    // )))
    let ir = IRNode::Filter {
        input: Box::new(IRNode::Map {
            input: Box::new(IRNode::Join {
                left: Box::new(IRNode::Filter {
                    input: Box::new(IRNode::Scan {
                        relation: "r".to_string(),
                        schema: vec!["x".to_string(), "y".to_string()],
                    }),
                    predicate: Predicate::True,
                }),
                right: Box::new(IRNode::Map {
                    input: Box::new(IRNode::Scan {
                        relation: "s".to_string(),
                        schema: vec!["y".to_string(), "z".to_string()],
                    }),
                    projection: vec![0, 1],
                    output_schema: vec!["y".to_string(), "z".to_string()],
                }),
                left_keys: vec![1],
                right_keys: vec![0],
                output_schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
            }),
            projection: vec![0, 1, 2], // Identity
            output_schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
        }),
        predicate: Predicate::True,
    };

    let optimized = optimizer.optimize(ir);

    // Should be optimized to just a Join with Scan children
    match optimized {
        IRNode::Join { left, right, .. } => {
            assert!(left.is_scan());
            assert!(right.is_scan());
        }
        _ => panic!("Expected optimized Join"),
    }
}

#[test]
fn test_fixpoint_reaches_stable_state() {
    let optimizer = Optimizer::with_max_iterations(5);

    // Create deeply nested identity maps
    let mut ir = IRNode::Scan {
        relation: "edge".to_string(),
        schema: vec!["x".to_string(), "y".to_string()],
    };

    for _ in 0..5 {
        ir = IRNode::Map {
            input: Box::new(ir),
            projection: vec![0, 1],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };
    }

    let optimized = optimizer.optimize(ir);

    // Should eliminate all identity maps
    assert!(optimized.is_scan());
}

#[test]
fn test_empty_union_not_further_optimized() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Union { inputs: vec![] };

    let optimized = optimizer.optimize(ir);

    // Empty union stays empty union
    match optimized {
        IRNode::Union { inputs } => assert_eq!(inputs.len(), 0),
        _ => panic!("Expected empty union"),
    }
}

#[test]
fn test_scan_not_modified() {
    let optimizer = Optimizer::new();

    let ir = IRNode::Scan {
        relation: "edge".to_string(),
        schema: vec!["x".to_string(), "y".to_string()],
    };

    let optimized = optimizer.optimize(ir.clone());

    // Scans should pass through unchanged
    assert!(optimized.is_scan());
}

#[test]
fn test_multiple_real_filters_preserved() {
    let optimizer = Optimizer::new();

    // Filter(x > 5, Filter(y < 10, Scan))
    let ir = IRNode::Filter {
        input: Box::new(IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::ColumnLtConst(1, 10),
        }),
        predicate: Predicate::ColumnGtConst(0, 5),
    };

    let optimized = optimizer.optimize(ir);

    // Filters should be fused into a single filter with And predicate
    match optimized {
        IRNode::Filter { input, predicate } => {
            // Input should be the scan
            assert!(input.is_scan(), "After fusion, input should be scan");
            // Predicate should be And of both conditions
            assert!(
                matches!(predicate, Predicate::And(_, _)),
                "Filters should be fused with And"
            );
        }
        _ => panic!("Expected fused filter"),
    }
}

// Advanced Optimizer Tests (join planning, subplan sharing, etc.)
#[test]
