//! Simple Query Example
//!
//! Demonstrates basic scanning and filtering with the Datalog engine.
//! Shows the full pipeline: Parser → IR Builder → Optimizer → Code Generator

use inputlayer::DatalogEngine;
use std::collections::HashSet;

fn to_set(results: Vec<(i32, i32)>) -> HashSet<(i32, i32)> {
    results.into_iter().collect()
}

fn main() {
    println!("=== Simple Query Example ===\n");

    // Create engine
    let mut engine = DatalogEngine::new();

    // Add some base facts: edges in a graph
    println!("Adding base facts (edges):");
    let edges = vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)];
    for (src, dst) in &edges {
        println!("  edge({}, {})", src, dst);
    }
    engine.add_fact("edge", edges);
    println!();

    // Example 1: Simple scan (identity query)
    println!("Example 1: Simple scan");
    println!("Query: result(x, y) :- edge(x, y).");
    let program1 = "result(x, y) :- edge(x, y).";

    let results = engine.execute(program1).expect("Query 1 failed");
    println!("Results ({} tuples):", results.len());
    for (x, y) in &results {
        println!("  result({}, {})", x, y);
    }
    // Assertion: Should return all 5 edges
    assert_eq!(results.len(), 5, "Simple scan should return 5 tuples");
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(3, 4)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
    println!();

    // Example 2: Filter query
    println!("Example 2: Filter query");
    println!("Query: result(x, y) :- edge(x, y), x > 2.");
    let program2 = "result(x, y) :- edge(x, y), x > 2.";

    let results = engine.execute(program2).expect("Query 2 failed");
    println!("Results ({} tuples):", results.len());
    for (x, y) in &results {
        println!("  result({}, {})", x, y);
    }
    // Assertion: Should return edges where x > 2: (3,4), (4,5), (5,6)
    assert_eq!(results.len(), 3, "Filter x > 2 should return 3 tuples");
    let result_set = to_set(results);
    assert!(result_set.contains(&(3, 4)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
    println!();

    // Example 3: Multiple filters
    println!("Example 3: Multiple filters");
    println!("Query: result(x, y) :- edge(x, y), x > 1, y < 5.");
    let program3 = "result(x, y) :- edge(x, y), x > 1, y < 5.";

    let results = engine.execute(program3).expect("Query 3 failed");
    println!("Results ({} tuples):", results.len());
    for (x, y) in &results {
        println!("  result({}, {})", x, y);
    }
    // Assertion: Should return edges where x > 1 AND y < 5: (2,3), (3,4)
    assert_eq!(results.len(), 2, "Multiple filters should return 2 tuples");
    let result_set = to_set(results);
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(3, 4)));
    println!();

    // Example 4: Projection (column swap)
    println!("Example 4: Projection (swap columns)");
    println!("Query: result(y, x) :- edge(x, y).");
    let program4 = "result(y, x) :- edge(x, y).";

    let results = engine.execute(program4).expect("Query 4 failed");
    println!("Results ({} tuples):", results.len());
    for (y, x) in &results {
        println!("  result({}, {})", y, x);
    }
    // Assertion: Should return 5 tuples with columns swapped
    assert_eq!(results.len(), 5, "Projection should return 5 tuples");
    let result_set = to_set(results);
    assert!(result_set.contains(&(2, 1)));
    assert!(result_set.contains(&(3, 2)));
    assert!(result_set.contains(&(4, 3)));
    assert!(result_set.contains(&(5, 4)));
    assert!(result_set.contains(&(6, 5)));
    println!();

    // Example 5: Inequality constraint
    println!("Example 5: Inequality constraint");
    println!("Query: result(x, y) :- edge(x, y), x != 3.");
    let program5 = "result(x, y) :- edge(x, y), x != 3.";

    let results = engine.execute(program5).expect("Query 5 failed");
    println!("Results ({} tuples):", results.len());
    for (x, y) in &results {
        println!("  result({}, {})", x, y);
    }
    // Assertion: Should return edges where x != 3: all except (3,4)
    assert_eq!(results.len(), 4, "Inequality should return 4 tuples");
    let result_set = to_set(results);
    assert!(result_set.contains(&(1, 2)));
    assert!(result_set.contains(&(2, 3)));
    assert!(result_set.contains(&(4, 5)));
    assert!(result_set.contains(&(5, 6)));
    assert!(!result_set.contains(&(3, 4)), "Should NOT contain (3, 4)");

    println!("\n=== Example Complete - All assertions passed! ===");
}
