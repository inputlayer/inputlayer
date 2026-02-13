//! Test arithmetic expressions in rule heads

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::code_generator::CodeGenerator;
    use crate::ir_builder::IRBuilder;
    use crate::parser::parse_rule;
    use crate::value::{Tuple, Value};
    use std::collections::HashMap;

    #[test]
    fn test_shortest_path_arithmetic() {
        // Setup catalog with relations
        let mut catalog = Catalog::new();
        catalog.register_relation("edge".to_string(), vec!["X".to_string(), "Y".to_string()]);
        catalog.register_relation("source".to_string(), vec!["X".to_string(), "D".to_string()]);
        catalog.register_relation("dist".to_string(), vec!["X".to_string(), "D".to_string()]);

        // Build IR for: dist(Y, D+1) <- dist(X, D), edge(X, Y)
        let rule = parse_rule("dist(Y, D+1) <- dist(X, D), edge(X, Y)").unwrap();
        let builder = IRBuilder::new(catalog);
        let ir = builder.build_ir(&rule).unwrap();

        eprintln!("IR output schema: {:?}", ir.output_schema());
        eprintln!("Full IR: {:#?}", ir);

        // Setup input data
        let mut input_data: HashMap<String, Vec<Tuple>> = HashMap::new();

        // dist contains (1, 0) - node 1 is at distance 0
        input_data.insert(
            "dist".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(0)])],
        );

        // edge contains (1, 2) - edge from node 1 to node 2
        input_data.insert(
            "edge".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );

        // Execute the IR
        let mut codegen = CodeGenerator::new();
        for (rel, tuples) in &input_data {
            codegen.add_input_tuples(rel.clone(), tuples.clone());
        }

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        eprintln!("Results: {:?}", results);

        // Expected: (2, 1) - node 2 is at distance 1 (which is 0+1)
        assert_eq!(results.len(), 1);
        let result = &results[0];

        // Check values
        let values: Vec<i32> = result
            .values()
            .iter()
            .map(|v| v.as_i32().unwrap_or(-999))
            .collect();

        eprintln!("Result values: {:?}", values);

        // Y=2 (from edge.Y), D+1=1 (from dist.D=0 + 1)
        assert_eq!(values[0], 2, "First column should be Y=2");
        assert_eq!(values[1], 1, "Second column should be D+1=1");
    }

    #[test]
    fn test_simple_join_schema() {
        // Test that join produces correct schema order
        let mut catalog = Catalog::new();
        catalog.register_relation("dist".to_string(), vec!["X".to_string(), "D".to_string()]);
        catalog.register_relation("edge".to_string(), vec!["X".to_string(), "Y".to_string()]);

        // Build IR for: result(X, D, Y) <- dist(X, D), edge(X, Y)
        let rule = parse_rule("result(X, D, Y) <- dist(X, D), edge(X, Y)").unwrap();
        let builder = IRBuilder::new(catalog);
        let ir = builder.build_ir(&rule).unwrap();

        eprintln!("Join IR output schema: {:?}", ir.output_schema());

        // Setup input data
        let mut input_data: HashMap<String, Vec<Tuple>> = HashMap::new();
        input_data.insert(
            "dist".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(0)])],
        );
        input_data.insert(
            "edge".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );

        // Execute
        let mut codegen = CodeGenerator::new();
        for (rel, tuples) in &input_data {
            codegen.add_input_tuples(rel.clone(), tuples.clone());
        }

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        eprintln!("Join results: {:?}", results);

        assert_eq!(results.len(), 1);
        let values: Vec<i32> = results[0]
            .values()
            .iter()
            .map(|v| v.as_i32().unwrap_or(-999))
            .collect();

        // Expected: X=1, D=0, Y=2
        assert_eq!(values[0], 1, "First column should be X=1");
        assert_eq!(values[1], 0, "Second column should be D=0");
        assert_eq!(values[2], 2, "Third column should be Y=2");
    }
}
