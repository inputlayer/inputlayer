//! Debug recursive union execution
//!
//! Tests the 3-rule recursive same_component query

use inputlayer::DatalogEngine;

fn main() {
    println!("=== Debug Recursive Union ===\n");

    let mut engine = DatalogEngine::new();

    // Simple graph: 1->2->3->4
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // First test - simple single rule
    println!("=== Test 1: Single base rule ===");
    let program1 = "sc(X, Y) :- edge(X, Y). __result__(X, Y) :- sc(X, Y).";
    match engine.execute(program1) {
        Ok(results) => {
            println!("Results: {:?}", results);
        }
        Err(e) => println!("Error: {}", e),
    }

    // Reset
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Test 2 - two rules for same head (non-recursive union)
    println!("\n=== Test 2: Two base rules (union) ===");
    let program2 = r#"
sc(X, Y) :- edge(X, Y).
sc(X, Y) :- edge(Y, X).
__result__(X, Y) :- sc(X, Y).
"#;
    match engine.execute(program2) {
        Ok(results) => {
            println!("Results: {:?}", results);
        }
        Err(e) => println!("Error: {}", e),
    }

    // Reset
    let mut engine = DatalogEngine::new();
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

    // Test 3 - recursive rule with union detection
    println!("\n=== Test 3: Three rules (recursive union) ===");
    let program3 = r#"
same_component(X, Y) :- edge(X, Y).
same_component(X, Y) :- edge(Y, X).
same_component(X, Z) :- same_component(X, Y), same_component(Y, Z), X != Z.
__result__(X, Y) :- same_component(X, Y).
"#;
    match engine.execute(program3) {
        Ok(results) => {
            println!("Results ({} rows): {:?}", results.len(), results);
        }
        Err(e) => println!("Error: {}", e),
    }
}
