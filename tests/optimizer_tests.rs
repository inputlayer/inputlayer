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
