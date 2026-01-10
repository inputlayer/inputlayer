//! Pipeline Demonstration
//!
//! This example demonstrates each stage of the Datalog pipeline:
//! 1. Parsing (source text → AST)
//! 2. IR Building (AST → IR with catalog)
//! 3. Optimization (IR → optimized IR)
//! 4. Code Generation & Execution (IR → Differential Dataflow → results)

use inputlayer::{DatalogEngine, IRNode};

fn main() {
    println!("=== Datalog Pipeline Demonstration ===\n");
    println!("This example shows each stage of the query processing pipeline.\n");

    // Create engine and add base data
    let mut engine = DatalogEngine::new();

    println!("Step 0: Adding Base Facts");
    println!("-------------------------");
    let edges = vec![(1, 2), (2, 3), (3, 4)];
    engine.add_fact("edge", edges.clone());
    println!("Added {} edge facts:", edges.len());
    for (x, y) in &edges {
        println!("  edge({}, {})", x, y);
    }
    println!();

    // Define the Datalog program
    let program = "
        % Find 2-hop paths where starting node > 1
        result(x, z) :- edge(x, y), edge(y, z), x > 1.
    ";

    println!("Datalog Program:");
    println!("----------------");
    println!("{}", program.trim());
    println!();

    // Stage 1: Parsing
    println!("Stage 1: PARSING");
    println!("----------------");
    println!("Converting source text to Abstract Syntax Tree (AST)...");

    match engine.parse(program) {
        Ok(parsed) => {
            println!("✓ Parsing successful!");
            println!("  Rules parsed: {}", parsed.rules.len());

            for (i, rule) in parsed.rules.iter().enumerate() {
                println!("\n  Rule {}:", i);
                println!(
                    "    Head: {} with {} args",
                    rule.head.relation,
                    rule.head.args.len()
                );
                println!("    Body: {} predicates", rule.body.len());
                println!("    Constraints: {} filters", rule.constraints.len());

                // Show rule is safe
                if rule.is_safe() {
                    println!("    ✓ Rule is safe (all head variables in positive body)");
                } else {
                    println!("    ✗ Rule is unsafe!");
                }
            }
        }
        Err(e) => {
            println!("✗ Parsing failed: {}", e);
            return;
        }
    }
    println!();

    // Stage 2: IR Building
    println!("Stage 2: IR BUILDING");
    println!("--------------------");
    println!("Converting AST to Intermediate Representation (IR)...");

    match engine.build_ir() {
        Ok(_) => {
            println!("✓ IR building successful!");
            let ir_nodes = engine.ir_nodes();
            println!("  IR nodes created: {}", ir_nodes.len());

            for (i, ir) in ir_nodes.iter().enumerate() {
                println!("\n  IR Node {}:", i);
                print_ir_structure(ir, 4);
            }

            // Show catalog state
            println!("\n  Catalog state:");
            let catalog = engine.catalog();
            for relation in catalog.all_relations() {
                if let Some(schema) = catalog.get_schema(&relation) {
                    println!("    {} → {:?}", relation, schema);
                }
            }
        }
        Err(e) => {
            println!("✗ IR building failed: {}", e);
            return;
        }
    }
    println!();

    // Stage 3: Optimization
    println!("Stage 3: OPTIMIZATION");
    println!("---------------------");
    println!("Applying optimizations to IR...");

    let ir_before = engine.ir_nodes()[0].clone();

    match engine.optimize_ir() {
        Ok(_) => {
            println!("✓ Optimization successful!");

            let ir_after = &engine.ir_nodes()[0];

            println!("\n  Optimizations applied:");
            println!("    - Identity projection removal");
            println!("    - Always-true filter elimination");
            println!("    - Constant folding");

            println!("\n  IR before optimization:");
            print_ir_structure(&ir_before, 4);

            println!("\n  IR after optimization:");
            print_ir_structure(ir_after, 4);
        }
        Err(e) => {
            println!("✗ Optimization failed: {}", e);
            return;
        }
    }
    println!();

    // Stage 4: Code Generation & Execution
    println!("Stage 4: CODE GENERATION & EXECUTION");
    println!("-------------------------------------");
    println!("Generating Differential Dataflow code and executing...");

    match engine.execute_ir(&engine.ir_nodes()[0].clone()) {
        Ok(results) => {
            println!("✓ Execution successful!");
            println!("\n  Results ({} tuples):", results.len());

            if results.is_empty() {
                println!("    (no results)");
            } else {
                for (x, y) in &results {
                    println!("    result({}, {})", x, y);
                }
            }

            println!("\n  Explanation:");
            println!("    Query finds 2-hop paths (x→y→z) where x > 1");
            println!("    From edges: 1→2, 2→3, 3→4");
            println!("    Paths: 1→2→3 (excluded, 1 ≤ 1), 2→3→4 (included, 2 > 1)");
            println!("    Result: (2, 4)");
        }
        Err(e) => {
            println!("✗ Execution failed: {}", e);
            return;
        }
    }

    println!("\n=== Pipeline Complete ===");
    println!("\nSummary: All stages executed successfully!");
    println!("  Parse → Build IR → Optimize → Execute → Results");
}

/// Helper function to print IR structure in a readable format
fn print_ir_structure(ir: &IRNode, indent: usize) {
    let prefix = " ".repeat(indent);

    match ir {
        IRNode::Scan { relation, schema } => {
            println!("{}Scan({})", prefix, relation);
            println!("{}  schema: {:?}", prefix, schema);
        }
        IRNode::Map {
            input,
            projection,
            output_schema,
        } => {
            println!("{}Map", prefix);
            println!("{}  projection: {:?}", prefix, projection);
            println!("{}  output: {:?}", prefix, output_schema);
            println!("{}  input:", prefix);
            print_ir_structure(input, indent + 4);
        }
        IRNode::Filter { input, predicate } => {
            println!("{}Filter({:?})", prefix, predicate);
            println!("{}  input:", prefix);
            print_ir_structure(input, indent + 4);
        }
        IRNode::Join {
            left,
            right,
            left_keys,
            right_keys,
            output_schema,
        } => {
            println!("{}Join", prefix);
            println!("{}  left_keys: {:?}", prefix, left_keys);
            println!("{}  right_keys: {:?}", prefix, right_keys);
            println!("{}  output: {:?}", prefix, output_schema);
            println!("{}  left:", prefix);
            print_ir_structure(left, indent + 4);
            println!("{}  right:", prefix);
            print_ir_structure(right, indent + 4);
        }
        IRNode::Distinct { input } => {
            println!("{}Distinct", prefix);
            println!("{}  input:", prefix);
            print_ir_structure(input, indent + 4);
        }
        IRNode::Union { inputs } => {
            println!("{}Union", prefix);
            for (i, input) in inputs.iter().enumerate() {
                println!("{}  input {}:", prefix, i);
                print_ir_structure(input, indent + 4);
            }
        }
        IRNode::Aggregate {
            input,
            group_by,
            aggregations,
            ..
        } => {
            println!(
                "{}Aggregate group_by={:?} aggs={:?}",
                prefix, group_by, aggregations
            );
            println!("{}  input:", prefix);
            print_ir_structure(input, indent + 4);
        }
        IRNode::Antijoin {
            left,
            right,
            left_keys,
            right_keys,
            output_schema,
        } => {
            println!("{}Antijoin", prefix);
            println!("{}  left_keys: {:?}", prefix, left_keys);
            println!("{}  right_keys: {:?}", prefix, right_keys);
            println!("{}  output: {:?}", prefix, output_schema);
            println!("{}  left:", prefix);
            print_ir_structure(left, indent + 4);
            println!("{}  right:", prefix);
            print_ir_structure(right, indent + 4);
        }
        IRNode::Compute { input, expressions } => {
            println!("{}Compute", prefix);
            println!(
                "{}  expressions: {:?}",
                prefix,
                expressions.iter().map(|(n, _)| n).collect::<Vec<_>>()
            );
            println!("{}  input:", prefix);
            print_ir_structure(input, indent + 4);
        }
    }
}
