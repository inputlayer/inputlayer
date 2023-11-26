//! Property-based arithmetic tests (proptest).

use proptest::prelude::*;
use std::collections::HashMap;

use inputlayer::{parser::parse_rule, Catalog, CodeGenerator, IRBuilder, Tuple, Value};

/// Helper function to execute a Datalog rule with 2-column input relations
fn execute_two_column_rule(
    data_values: Vec<(i32, i32)>,
    link_values: Vec<(i32, i32)>,
    rule_str: &str,
) -> Result<Vec<Tuple>, String> {
    // Setup catalog with relations
    let mut catalog = Catalog::new();
    catalog.register_relation("data".to_string(), vec!["X".to_string(), "D".to_string()]);
    catalog.register_relation("link".to_string(), vec!["X".to_string(), "Y".to_string()]);
    catalog.register_relation("result".to_string(), vec!["Y".to_string(), "V".to_string()]);

    // Build IR
    let rule = parse_rule(rule_str).map_err(|e| format!("Parse error: {:?}", e))?;
    let builder = IRBuilder::new(catalog);
    let ir = builder.build_ir(&rule)?;

    // Setup input data
    let mut input_data: HashMap<String, Vec<Tuple>> = HashMap::new();

    input_data.insert(
        "data".to_string(),
        data_values
            .iter()
            .map(|(x, d)| Tuple::new(vec![Value::Int32(*x), Value::Int32(*d)]))
            .collect(),
    );

    input_data.insert(
        "link".to_string(),
        link_values
            .iter()
            .map(|(x, y)| Tuple::new(vec![Value::Int32(*x), Value::Int32(*y)]))
            .collect(),
    );

    // Execute
    let mut codegen = CodeGenerator::new();
    for (rel, tuples) in &input_data {
        codegen.add_input_tuples(rel.clone(), tuples.clone());
    }

    codegen.generate_and_execute_tuples(&ir)
}

/// Helper function to execute a simple single-relation arithmetic rule
