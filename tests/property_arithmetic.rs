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
fn _execute_simple_arithmetic(
    input_values: Vec<(i32, i32)>,
    rule_str: &str,
) -> Result<Vec<Tuple>, String> {
    // Setup catalog with relations
    let mut catalog = Catalog::new();
    catalog.register_relation("input".to_string(), vec!["X".to_string(), "V".to_string()]);
    catalog.register_relation("output".to_string(), vec!["X".to_string(), "R".to_string()]);

    // Build IR
    let rule = parse_rule(rule_str).map_err(|e| format!("Parse error: {:?}", e))?;
    let builder = IRBuilder::new(catalog);
    let ir = builder.build_ir(&rule)?;

    // Setup input data
    let mut input_data: HashMap<String, Vec<Tuple>> = HashMap::new();

    input_data.insert(
        "input".to_string(),
        input_values
            .iter()
            .map(|(x, v)| Tuple::new(vec![Value::Int32(*x), Value::Int32(*v)]))
            .collect(),
    );

    // Execute
    let mut codegen = CodeGenerator::new();
    for (rel, tuples) in &input_data {
        codegen.add_input_tuples(rel.clone(), tuples.clone());
    }

    codegen.generate_and_execute_tuples(&ir)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    /// Test that D+1 produces correct results for any input value
    #[test]
    fn prop_increment_correct(base in -1000i32..1000i32) {
        let results = execute_two_column_rule(
            vec![(1, base)],
            vec![(1, 100)],
            "result(Y, D+1) :- data(X, D), link(X, Y).",
        ).expect("Execution should succeed");

        prop_assert_eq!(results.len(), 1, "Should have exactly one result");

        let result = &results[0];
        let values = result.values();
        prop_assert_eq!(values.len(), 2, "Result should have 2 columns");

        let y = values[0].as_i32().expect("First column should be i32");
        let computed = values[1].as_i32().expect("Second column should be i32");

        prop_assert_eq!(y, 100, "Y should be 100 from link");
        prop_assert_eq!(computed, base + 1, "D+1 should equal base + 1");
    }

    /// Test that D+constant produces correct results
    #[test]
    fn prop_add_constant_correct(base in -1000i32..1000, constant in 1i32..100) {
        let rule = format!("result(Y, D+{}) :- data(X, D), link(X, Y).", constant);
        let results = execute_two_column_rule(
            vec![(1, base)],
            vec![(1, 100)],
            &rule,
        ).expect("Execution should succeed");

        prop_assert_eq!(results.len(), 1, "Should have exactly one result");

        let result = &results[0];
        let values = result.values();
        prop_assert_eq!(values.len(), 2, "Result should have 2 columns");

        let computed = values[1].as_i32().expect("Second column should be i32");
        prop_assert_eq!(computed, base + constant, "D+constant should be computed correctly");
    }

    /// Test that A-B produces correct results for variable subtraction
    #[test]
    fn prop_subtraction_correct(a in 0i32..1000, b in 0i32..1000) {
        // Use two relations that join on X to compute A-B
        let mut catalog = Catalog::new();
        catalog.register_relation("left".to_string(), vec!["X".to_string(), "A".to_string()]);
        catalog.register_relation("right".to_string(), vec!["X".to_string(), "B".to_string()]);
        catalog.register_relation("result".to_string(), vec!["X".to_string(), "D".to_string()]);

        let rule = parse_rule("result(X, A-B) :- left(X, A), right(X, B).").unwrap();
        let builder = IRBuilder::new(catalog);
        let ir = builder.build_ir(&rule).unwrap();

        let mut input_data: HashMap<String, Vec<Tuple>> = HashMap::new();
        input_data.insert(
            "left".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(a)])],
        );
        input_data.insert(
            "right".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(b)])],
        );

        let mut codegen = CodeGenerator::new();
        for (rel, tuples) in &input_data {
            codegen.add_input_tuples(rel.clone(), tuples.clone());
        }

        let results = codegen.generate_and_execute_tuples(&ir).expect("Should execute");

        prop_assert_eq!(results.len(), 1);
        let values = results[0].values();
        prop_assert_eq!(values.len(), 2);

        let computed = values[1].as_i32().expect("Should be i32");
        prop_assert_eq!(computed, a - b);
    }

    /// Test that D*constant produces correct results for multiplication
