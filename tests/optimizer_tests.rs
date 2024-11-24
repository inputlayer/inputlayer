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
