//! # Datalog REPL
//!
//! A simple Read-Eval-Print Loop for the `InputLayer` Datalog engine.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --bin datalog-repl
//! ```
//!
//! Then enter Datalog programs or commands:
//! - `.facts <relation> <tuples>` - Add facts
//! - `.query <datalog>` - Execute a query
//! - `.quit` - Exit

use inputlayer::DatalogEngine;
use std::io::{self, Write};

fn main() {
    println!("InputLayer Datalog Engine REPL");
    println!("============================\n");
    println!("Commands:");
    println!("  .facts <relation> - Add facts (comma-separated tuples)");
    println!("  .query <datalog>  - Execute Datalog query");
    println!("  .help             - Show this help");
    println!("  .quit             - Exit\n");

    let mut engine = DatalogEngine::new();

    // Add some example data
    engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4), (4, 5)]);
    println!("Loaded example facts:");
    println!("  edge(1, 2), edge(2, 3), edge(3, 4), edge(4, 5)\n");

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read input");

        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        if input.starts_with(".quit") || input.starts_with(".exit") {
            println!("Goodbye!");
            break;
        }

        if input.starts_with(".help") {
            println!("\nCommands:");
            println!("  .facts <relation> - Add facts");
            println!("  .query <datalog>  - Execute query");
            println!("  .help             - Show help");
            println!("  .quit             - Exit\n");
            continue;
        }

        if input.starts_with(".query") {
            let query = input.trim_start_matches(".query").trim();
            println!("Executing: {query}");

            match engine.execute(query) {
                Ok(results) => {
                    println!("Results:");
                    for tuple in results {
                        println!("  {tuple:?}");
                    }
                }
                Err(e) => {
                    println!("Error: {e}");
                    println!("\nNote: The full pipeline requires:");
                    println!("  - Parser");
                    println!("  - IR Builder");
                    println!("  - Optimizer");
                    println!("  - Code Generator");
                }
            }
            continue;
        }

        if input.starts_with(".simple") {
            // Simplified query that bypasses parsing
            // Format: .simple <relation> [projection]
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() >= 2 {
                let relation = parts[1];
                let projection = if parts.len() > 2 {
                    vec![
                        parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0),
                        parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(1),
                    ]
                } else {
                    vec![0, 1]
                };

                match engine.execute_simple_query(relation, projection) {
                    Ok(results) => {
                        println!("Results:");
                        for tuple in results {
                            println!("  {tuple:?}");
                        }
                    }
                    Err(e) => {
                        println!("Error: {e}");
                    }
                }
            } else {
                println!("Usage: .simple <relation> [col1] [col2]");
            }
            continue;
        }

        println!("Unknown command: {input}");
        println!("Type .help for available commands");
    }
}
