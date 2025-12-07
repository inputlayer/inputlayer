use datalog_engine::{DatalogEngine, OptimizationConfig};

fn main() {
    let base_query = "path2(x, z) :- edge(x, y), edge(y, z).";
    let edges: Vec<(i32, i32)> = vec![(1, 2), (2, 3), (3, 4), (4, 5)];
    
    // Test with NO optimizations
    println!("Testing with NO optimizations...");
    let config_none = OptimizationConfig {
        enable_join_planning: false,
        enable_sip_rewriting: false,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };
    let mut engine = DatalogEngine::with_config(config_none);
    engine.add_fact("edge", edges.clone());
    let results = engine.execute(base_query).unwrap();
    println!("  Results: {:?}", results);
    
    // Test with ONLY join planning
    println!("\nTesting with ONLY join planning...");
    let config_jp = OptimizationConfig {
        enable_join_planning: true,
        enable_sip_rewriting: false,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };
    let mut engine = DatalogEngine::with_config(config_jp);
    engine.add_fact("edge", edges.clone());
    let results = engine.execute(base_query).unwrap();
    println!("  Results: {:?}", results);
    
    // Test with ONLY SIP
    println!("\nTesting with ONLY SIP rewriting...");
    let config_sip = OptimizationConfig {
        enable_join_planning: false,
        enable_sip_rewriting: true,
        enable_subplan_sharing: false,
        enable_boolean_specialization: false,
    };
    let mut engine = DatalogEngine::with_config(config_sip);
    engine.add_fact("edge", edges.clone());
    let results = engine.execute(base_query).unwrap();
    println!("  Results: {:?}", results);
    
    // Test with ONLY subplan sharing
    println!("\nTesting with ONLY subplan sharing...");
    let config_ss = OptimizationConfig {
        enable_join_planning: false,
        enable_sip_rewriting: false,
        enable_subplan_sharing: true,
        enable_boolean_specialization: false,
    };
    let mut engine = DatalogEngine::with_config(config_ss);
    engine.add_fact("edge", edges.clone());
    let results = engine.execute(base_query).unwrap();
    println!("  Results: {:?}", results);
    
    // Test with ONLY boolean specialization
    println!("\nTesting with ONLY boolean specialization...");
    let config_bs = OptimizationConfig {
        enable_join_planning: false,
        enable_sip_rewriting: false,
        enable_subplan_sharing: false,
        enable_boolean_specialization: true,
    };
    let mut engine = DatalogEngine::with_config(config_bs);
    engine.add_fact("edge", edges.clone());
    let results = engine.execute(base_query).unwrap();
    println!("  Results: {:?}", results);
}
