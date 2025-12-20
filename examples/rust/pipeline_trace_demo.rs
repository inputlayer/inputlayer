//! Pipeline Trace Demo
//!
//! Demonstrates the full query processing pipeline with detailed tracing.
//! Shows each stage: Parse → IR Build → Optimize → Execute
//!
//! This is extremely valuable for students to understand how the engine works!

use inputlayer::DatalogEngine;

fn main() {
    println!("\n🔍 InputLayer Pipeline Trace Demo\n");
    println!("This example shows exactly what happens at each stage of query processing.\n");

    // Example 1: Simple query with optimization opportunities
    example_1_simple_with_optimization();

    // Example 2: Join query
    example_2_join_query();

    // Example 3: Complex query with multiple optimizations
    example_3_complex_query();
}

fn example_1_simple_with_optimization() {
    println!("═══════════════════════════════════════════════════════════");
    println!("Example 1: Simple Query with Identity Map (will be optimized)");
    println!("═══════════════════════════════════════════════════════════\n");

    let mut engine = DatalogEngine::new();

    // Add base facts
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);

    // Query that will produce identity projection
    // result(x, y) :- edge(x, y).
    // This creates Map with identity projection, which optimizer will remove!
    let query = "result(x, y) :- edge(x, y).";

    match engine.execute_with_trace(query) {
        Ok((results, trace)) => {
            // Display the full trace
            println!("{}", trace);

            println!("✅ Pipeline execution successful!");
            println!("   Results: {} tuples\n", results.len());
        }
        Err(e) => println!("❌ Error: {}\n", e),
    }
}

fn example_2_join_query() {
    println!("═══════════════════════════════════════════════════════════");
    println!("Example 2: Join Query (2-hop paths)");
    println!("═══════════════════════════════════════════════════════════\n");

    let mut engine = DatalogEngine::new();

    // Add base facts - a simple path
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Query: find 2-hop paths
    // path2(x, z) :- edge(x, y), edge(y, z).
    let query = "path2(x, z) :- edge(x, y), edge(y, z).";

    match engine.execute_with_trace(query) {
        Ok((results, trace)) => {
            println!("{}", trace);

            println!("✅ Join query executed!");
            println!("   Found {} 2-hop paths\n", results.len());
        }
        Err(e) => println!("❌ Error: {}\n", e),
    }
}

fn example_3_complex_query() {
    println!("═══════════════════════════════════════════════════════════");
    println!("Example 3: Complex Query with Filters");
    println!("═══════════════════════════════════════════════════════════\n");

    let mut engine = DatalogEngine::new();

    // Add base facts
    engine.add_fact("data", vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);

    // Query with multiple filters
    // result(x, y) :- data(x, y), x > 1, y < 45, x != 3.
    let query = "result(x, y) :- data(x, y), x > 1, y < 45, x != 3.";

    match engine.execute_with_trace(query) {
        Ok((results, trace)) => {
            println!("{}", trace);

            println!("✅ Complex query with filters executed!");
            println!("   Matching tuples: {}\n", results.len());

            // Show which tuples passed all filters
            println!("   Tuples that passed all filters:");
            for (x, y) in &results {
                println!("     ({}, {})", x, y);
            }
            println!();
        }
        Err(e) => println!("❌ Error: {}\n", e),
    }
}
