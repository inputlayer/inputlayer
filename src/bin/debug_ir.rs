use inputlayer::{DatalogEngine, OptimizationConfig};

fn main() {
    let query = "path2(x, z) :- edge(x, y), edge(y, z).";
    let edges: Vec<(i32, i32)> = vec![(1, 2), (2, 3), (3, 4), (4, 5)];

    // Test with NO optimizations - show IR
    println!("=== NO Optimizations ===");
    let config = OptimizationConfig {
        enable_join_planning: false,
        enable_sip_rewriting: false,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };
    let mut engine = DatalogEngine::with_config(config);
    engine.add_fact("edge", edges.clone());
    engine.parse(query).unwrap();
    engine.build_ir().unwrap();
    println!("IR before optimize: {:#?}", engine.ir_nodes());
    engine.optimize_ir().unwrap();
    println!("IR after optimize: {:#?}", engine.ir_nodes());

    // Test with ONLY join planning - show IR
    println!("\n\n=== ONLY Join Planning ===");
    let config = OptimizationConfig {
        enable_join_planning: true,
        enable_sip_rewriting: false,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };
    let mut engine2 = DatalogEngine::with_config(config);
    engine2.add_fact("edge", edges.clone());
    engine2.parse(query).unwrap();
    engine2.build_ir().unwrap();
    println!("IR before optimize: {:#?}", engine2.ir_nodes());
    engine2.optimize_ir().unwrap();
    println!("IR after optimize: {:#?}", engine2.ir_nodes());
}
