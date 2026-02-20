//! Session and Schema Tests
//!
//! Tests for:
//! - Session fact and rule lifecycle
//! - Session isolation and cleanup
//! - Schema type and arity enforcement
//! - Schema validation engine

use inputlayer::schema::{
    validator::ViolationType, ColumnSchema, RelationSchema, SchemaCatalog, SchemaType,
    ValidationEngine, ValidationError,
};
use inputlayer::value::{Tuple, Value};
use std::sync::Arc;

// Session Data Structure Pattern Tests
//
// NOTE: These are unit tests for the expected behavior patterns of session-related
// data structures (Vec operations, clearing, isolation). They test the data structure
// patterns that session management relies on.
//
// For integration tests of actual StorageEngine session functionality, see:
// - examples/datalog/04_session/*.idl (snapshot tests for session lifecycle)
// - tests/storage_engine_tests.rs (integration tests)
mod session_data_pattern_tests {
    use super::*;

    /// Test that session rules are tracked separately from persistent rules
    /// (Tests Vec isolation pattern that session management relies on)
    #[test]
    fn test_session_rules_tracking() {
        // Session rules should be stored in a separate Vec
        // This tests the data structure pattern
        let mut session_rules: Vec<String> = Vec::new();
        let mut persistent_rules: Vec<String> = Vec::new();

        // Add session rule
        session_rules.push("path(X, Y) <- edge(X, Y)".to_string());

        // Add persistent rule
        persistent_rules.push("+connected(X, Y) <- path(X, Y)".to_string());

        assert_eq!(session_rules.len(), 1);
        assert_eq!(persistent_rules.len(), 1);

        // Session rules can be cleared independently
        session_rules.clear();
        assert!(session_rules.is_empty());
        assert_eq!(persistent_rules.len(), 1);
    }

    /// Test that session facts are tracked separately
    #[test]
    fn test_session_facts_tracking() {
        let mut session_facts: Vec<Tuple> = Vec::new();

        // Add session facts
        session_facts.push(Tuple::new(vec![Value::Int64(1), Value::Int64(2)]));
        session_facts.push(Tuple::new(vec![Value::Int64(3), Value::Int64(4)]));

        assert_eq!(session_facts.len(), 2);

        // Session facts can be individually removed
        session_facts.remove(0);
        assert_eq!(session_facts.len(), 1);

        // Session facts can be cleared
        session_facts.clear();
        assert!(session_facts.is_empty());
    }

    /// Test session data clearing behavior
    #[test]
    fn test_session_clear_behavior() {
        let mut session_facts: Vec<Tuple> = Vec::new();
        let mut session_rules: Vec<String> = Vec::new();

        // Add some session data
        session_facts.push(Tuple::new(vec![Value::Int64(1)]));
        session_facts.push(Tuple::new(vec![Value::Int64(2)]));
        session_rules.push("rule1(X) <- fact(X)".to_string());
        session_rules.push("rule2(X) <- rule1(X)".to_string());

        // Clear all
        let facts_count = session_facts.len();
        let rules_count = session_rules.len();
        session_facts.clear();
        session_rules.clear();

        assert_eq!(facts_count, 2);
        assert_eq!(rules_count, 2);
        assert!(session_facts.is_empty());
        assert!(session_rules.is_empty());
    }

    /// Test session rule removal by index
    #[test]
    fn test_session_rule_removal_by_index() {
        let mut session_rules: Vec<String> = Vec::new();

        session_rules.push("rule1(X) <- fact(X)".to_string());
        session_rules.push("rule2(X) <- rule1(X)".to_string());
        session_rules.push("rule3(X) <- rule2(X)".to_string());

        // Remove by 0-based index (simulating 1-based UI)
        let removed = session_rules.remove(1); // Remove rule2
        assert_eq!(removed, "rule2(X) <- rule1(X)");
        assert_eq!(session_rules.len(), 2);
        assert_eq!(session_rules[0], "rule1(X) <- fact(X)");
        assert_eq!(session_rules[1], "rule3(X) <- rule2(X)");
    }

    /// Test session data isolation between logical sessions
    #[test]
    fn test_session_isolation() {
        // Simulate two separate sessions
        struct Session {
            facts: Vec<Tuple>,
            rules: Vec<String>,
        }

        let mut session1 = Session {
            facts: Vec::new(),
            rules: Vec::new(),
        };
        let mut session2 = Session {
            facts: Vec::new(),
            rules: Vec::new(),
        };

        // Add data to session 1
        session1.facts.push(Tuple::new(vec![Value::Int64(1)]));
        session1.rules.push("s1_rule(X) <- fact(X)".to_string());

        // Add data to session 2
        session2.facts.push(Tuple::new(vec![Value::Int64(2)]));
        session2.rules.push("s2_rule(X) <- fact(X)".to_string());

        // Sessions are isolated
        assert_eq!(session1.facts.len(), 1);
        assert_eq!(session2.facts.len(), 1);
        assert_ne!(
            session1.facts[0].get(0).unwrap(),
            session2.facts[0].get(0).unwrap()
        );
    }

    /// Test that session data is temporary and can be rebuilt
    #[test]
    fn test_session_rebuild() {
        let mut session_rules: Vec<String> = Vec::new();

        // Add rules
        session_rules.push("rule1(X) <- a(X)".to_string());
        session_rules.push("rule2(X) <- b(X)".to_string());

        // Build program text from session rules
        let program_text = session_rules.join("\n");
        assert!(program_text.contains("rule1"));
        assert!(program_text.contains("rule2"));

        // Clear and rebuild
        session_rules.clear();
        session_rules.push("new_rule(X) <- c(X)".to_string());

        let new_program_text = session_rules.join("\n");
        assert!(!new_program_text.contains("rule1"));
        assert!(new_program_text.contains("new_rule"));
    }
}

// Schema Type Tests
mod schema_type_tests {
    use super::*;

    #[test]
    fn test_int_type_matching() {
        // Int matches both Int32 and Int64
        assert!(SchemaType::Int.matches(&Value::Int32(0)));
        assert!(SchemaType::Int.matches(&Value::Int32(i32::MAX)));
        assert!(SchemaType::Int.matches(&Value::Int32(i32::MIN)));
        assert!(SchemaType::Int.matches(&Value::Int64(0)));
        assert!(SchemaType::Int.matches(&Value::Int64(i64::MAX)));
        assert!(SchemaType::Int.matches(&Value::Int64(i64::MIN)));

        // Int does not match other types
        assert!(!SchemaType::Int.matches(&Value::Float64(1.0)));
        assert!(!SchemaType::Int.matches(&Value::string("1")));
        assert!(!SchemaType::Int.matches(&Value::Bool(true)));
    }

    #[test]
    fn test_float_type_with_coercion() {
        // Float matches Float64
        assert!(SchemaType::Float.matches(&Value::Float64(0.0)));
        assert!(SchemaType::Float.matches(&Value::Float64(f64::MAX)));
        assert!(SchemaType::Float.matches(&Value::Float64(f64::MIN)));
        assert!(SchemaType::Float.matches(&Value::Float64(f64::NAN)));
        assert!(SchemaType::Float.matches(&Value::Float64(f64::INFINITY)));

        // Float also matches Int (coercion)
        assert!(SchemaType::Float.matches(&Value::Int32(42)));
        assert!(SchemaType::Float.matches(&Value::Int64(42)));

        // Float does not match string or bool
        assert!(!SchemaType::Float.matches(&Value::string("1.0")));
        assert!(!SchemaType::Float.matches(&Value::Bool(true)));
    }

    #[test]
    fn test_string_type_matching() {
        assert!(SchemaType::String.matches(&Value::string("")));
        assert!(SchemaType::String.matches(&Value::string("hello")));
        assert!(SchemaType::String.matches(&Value::string("hello world with spaces")));

        // String does not match other types
        assert!(!SchemaType::String.matches(&Value::Int64(42)));
        assert!(!SchemaType::String.matches(&Value::Float64(3.14)));
        assert!(!SchemaType::String.matches(&Value::Bool(true)));
    }

    #[test]
    fn test_symbol_type_matching() {
        // Symbol is currently an alias for String in type matching
        assert!(SchemaType::Symbol.matches(&Value::string("alice")));
        assert!(SchemaType::Symbol.matches(&Value::string("bob")));

        assert!(!SchemaType::Symbol.matches(&Value::Int64(42)));
    }

    #[test]
    fn test_bool_type_matching() {
        assert!(SchemaType::Bool.matches(&Value::Bool(true)));
        assert!(SchemaType::Bool.matches(&Value::Bool(false)));

        assert!(!SchemaType::Bool.matches(&Value::Int64(0)));
        assert!(!SchemaType::Bool.matches(&Value::Int64(1)));
        assert!(!SchemaType::Bool.matches(&Value::string("true")));
    }

    #[test]
    fn test_timestamp_type_matching() {
        assert!(SchemaType::Timestamp.matches(&Value::Timestamp(0)));
        assert!(SchemaType::Timestamp.matches(&Value::Timestamp(1705349600000)));

        // Timestamp also accepts Int64 (for convenience)
        assert!(SchemaType::Timestamp.matches(&Value::Int64(1705349600000)));

        assert!(!SchemaType::Timestamp.matches(&Value::string("2024-01-15")));
        assert!(!SchemaType::Timestamp.matches(&Value::Float64(1705349600000.0)));
    }

    #[test]
    fn test_vector_type_matching() {
        // dim: None accepts any dimension
        assert!(
            SchemaType::Vector { dim: None }.matches(&Value::Vector(Arc::new(vec![1.0, 2.0, 3.0])))
        );
        assert!(SchemaType::Vector { dim: None }.matches(&Value::Vector(Arc::new(vec![]))));
        assert!(
            SchemaType::Vector { dim: None }.matches(&Value::VectorInt8(Arc::new(vec![1, 2, 3])))
        );

        assert!(!SchemaType::Vector { dim: None }.matches(&Value::string("[1,2,3]")));
        assert!(!SchemaType::Vector { dim: None }.matches(&Value::Int64(123)));

        // dim: Some(n) enforces exact dimension
        assert!(SchemaType::Vector { dim: Some(3) }
            .matches(&Value::Vector(Arc::new(vec![1.0, 2.0, 3.0]))));
        assert!(
            !SchemaType::Vector { dim: Some(3) }.matches(&Value::Vector(Arc::new(vec![1.0, 2.0])))
        );
        assert!(!SchemaType::Vector { dim: Some(3) }
            .matches(&Value::Vector(Arc::new(vec![1.0, 2.0, 3.0, 4.0]))));
        assert!(SchemaType::Vector { dim: Some(3) }
            .matches(&Value::VectorInt8(Arc::new(vec![1, 2, 3]))));
        assert!(
            !SchemaType::Vector { dim: Some(3) }.matches(&Value::VectorInt8(Arc::new(vec![1, 2])))
        );
    }

    #[test]
    fn test_any_type_matches_all() {
        assert!(SchemaType::Any.matches(&Value::Int32(42)));
        assert!(SchemaType::Any.matches(&Value::Int64(42)));
        assert!(SchemaType::Any.matches(&Value::Float64(3.14)));
        assert!(SchemaType::Any.matches(&Value::string("anything")));
        assert!(SchemaType::Any.matches(&Value::Bool(true)));
        assert!(SchemaType::Any.matches(&Value::Null));
        assert!(SchemaType::Any.matches(&Value::Vector(Arc::new(vec![1.0]))));
        assert!(SchemaType::Any.matches(&Value::Timestamp(0)));
    }

    #[test]
    fn test_named_type_parsing() {
        // Named types start with uppercase
        assert_eq!(
            SchemaType::from_str("Email"),
            Some(SchemaType::Named("Email".to_string()))
        );
        assert_eq!(
            SchemaType::from_str("UserId"),
            Some(SchemaType::Named("UserId".to_string()))
        );
        assert_eq!(
            SchemaType::from_str("Age"),
            Some(SchemaType::Named("Age".to_string()))
        );

        // Lowercase is not a named type (it's parsed as base type or None)
        assert_eq!(SchemaType::from_str("email"), None);
    }

    #[test]
    fn test_schema_type_display() {
        assert_eq!(format!("{}", SchemaType::Int), "int");
        assert_eq!(format!("{}", SchemaType::Float), "float");
        assert_eq!(format!("{}", SchemaType::String), "string");
        assert_eq!(format!("{}", SchemaType::Symbol), "symbol");
        assert_eq!(format!("{}", SchemaType::Bool), "bool");
        assert_eq!(format!("{}", SchemaType::Timestamp), "timestamp");
        assert_eq!(format!("{}", SchemaType::Vector { dim: None }), "vector");
        assert_eq!(format!("{}", SchemaType::Any), "any");
        assert_eq!(
            format!("{}", SchemaType::Named("Email".to_string())),
            "Email"
        );
    }
}

// Schema Validation Tests
mod validation_tests {
    use super::*;

    fn make_user_schema() -> RelationSchema {
        RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String))
            .with_column(ColumnSchema::new("age", SchemaType::Int))
    }

    #[test]
    fn test_validate_valid_single_tuple() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::string("Alice"),
            Value::Int64(30),
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().validated_count, 1);
    }

    #[test]
    fn test_validate_empty_batch() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let result = engine.validate_batch(&schema, &[]);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().validated_count, 0);
    }

    #[test]
    fn test_validate_multiple_valid_tuples() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![
                Value::Int64(1),
                Value::string("Alice"),
                Value::Int64(30),
            ]),
            Tuple::new(vec![
                Value::Int64(2),
                Value::string("Bob"),
                Value::Int64(25),
            ]),
            Tuple::new(vec![
                Value::Int64(3),
                Value::string("Carol"),
                Value::Int64(35),
            ]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().validated_count, 3);
    }

    #[test]
    fn test_arity_mismatch_too_few_columns() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![Value::Int64(1), Value::string("Alice")]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());

        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert_eq!(violations.len(), 1);
            assert_eq!(violations[0].violation_type, ViolationType::ArityMismatch);
            assert!(violations[0].message.contains("Expected 3 columns, got 2"));
        } else {
            panic!("Expected BatchRejected error");
        }
    }

    #[test]
    fn test_arity_mismatch_too_many_columns() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::string("Alice"),
            Value::Int64(30),
            Value::string("extra"),
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());

        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert_eq!(violations.len(), 1);
            assert_eq!(violations[0].violation_type, ViolationType::ArityMismatch);
            assert!(violations[0].message.contains("Expected 3 columns, got 4"));
        }
    }

    #[test]
    fn test_type_mismatch_single_column() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::Int64(1),
            Value::string("Alice"),
            Value::string("thirty"), // Should be Int
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());

        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert_eq!(violations.len(), 1);
            assert_eq!(violations[0].violation_type, ViolationType::TypeMismatch);
            assert_eq!(violations[0].column, Some("age".to_string()));
        }
    }

    #[test]
    fn test_type_mismatch_multiple_columns() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string("not_an_id"), // Should be Int
            Value::Int64(123),          // Should be String
            Value::string("thirty"),    // Should be Int
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());

        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            // Should have violations for all 3 columns
            assert_eq!(violations.len(), 3);
            assert!(violations
                .iter()
                .all(|v| v.violation_type == ViolationType::TypeMismatch));
        }
    }

    #[test]
    fn test_batch_rejected_all_or_nothing() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![
                Value::Int64(1),
                Value::string("Alice"),
                Value::Int64(30),
            ]),
            Tuple::new(vec![
                Value::Int64(2),
                Value::string("Bob"),
                Value::string("invalid"), // Type error
            ]),
            Tuple::new(vec![
                Value::Int64(3),
                Value::string("Carol"),
                Value::Int64(35),
            ]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_err());

        // All-or-nothing: the entire batch is rejected
        if let Err(ValidationError::BatchRejected {
            total_tuples,
            violations,
            ..
        }) = result
        {
            assert_eq!(total_tuples, 3);
            assert_eq!(violations.len(), 1);
            assert_eq!(violations[0].tuple_index, 1); // Second tuple (index 1)
        }
    }

    #[test]
    fn test_violation_contains_tuple_data() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::Int64(999),
            Value::string("Test"),
            Value::string("not_int"),
        ]);

        let result = engine.validate_batch(&schema, &[tuple.clone()]);

        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            // Violation should contain the original tuple
            assert_eq!(violations[0].tuple.arity(), 3);
            assert_eq!(violations[0].tuple.get(0), Some(&Value::Int64(999)));
        }
    }

    #[test]
    fn test_int32_and_int64_both_valid_for_int_schema() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![
                Value::Int32(1),
                Value::string("Alice"),
                Value::Int32(30),
            ]),
            Tuple::new(vec![
                Value::Int64(2),
                Value::string("Bob"),
                Value::Int64(25),
            ]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_float_schema_accepts_int_coercion() {
        let schema = RelationSchema::new("Numbers")
            .with_column(ColumnSchema::new("value", SchemaType::Float));
        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![Value::Float64(3.14)]),
            Tuple::new(vec![Value::Int32(42)]),
            Tuple::new(vec![Value::Int64(100)]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_ok());
    }
}

// Schema Catalog Tests
mod catalog_tests {
    use super::*;

    #[test]
    fn test_register_and_get_schema() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));

        catalog.register(schema.clone()).unwrap();

        let retrieved = catalog.get("User");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "User");
        assert_eq!(retrieved.unwrap().arity(), 2);
    }

    #[test]
    fn test_get_nonexistent_schema() {
        let catalog = SchemaCatalog::new();
        assert!(catalog.get("NonExistent").is_none());
    }

    #[test]
    fn test_remove_schema() {
        let mut catalog = SchemaCatalog::new();

        let schema =
            RelationSchema::new("ToRemove").with_column(ColumnSchema::new("id", SchemaType::Int));

        catalog.register(schema).unwrap();
        assert!(catalog.get("ToRemove").is_some());

        catalog.remove("ToRemove");
        assert!(catalog.get("ToRemove").is_none());
    }

    #[test]
    fn test_list_schemas() {
        let mut catalog = SchemaCatalog::new();

        catalog
            .register(
                RelationSchema::new("Alpha").with_column(ColumnSchema::new("a", SchemaType::Int)),
            )
            .unwrap();
        catalog
            .register(
                RelationSchema::new("Beta").with_column(ColumnSchema::new("b", SchemaType::Int)),
            )
            .unwrap();
        catalog
            .register(
                RelationSchema::new("Gamma").with_column(ColumnSchema::new("c", SchemaType::Int)),
            )
            .unwrap();

        let names = catalog.relations();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"Alpha"));
        assert!(names.contains(&"Beta"));
        assert!(names.contains(&"Gamma"));
    }

    #[test]
    fn test_clear_catalog() {
        let mut catalog = SchemaCatalog::new();

        catalog
            .register(
                RelationSchema::new("One").with_column(ColumnSchema::new("a", SchemaType::Int)),
            )
            .unwrap();
        catalog
            .register(
                RelationSchema::new("Two").with_column(ColumnSchema::new("b", SchemaType::Int)),
            )
            .unwrap();

        assert_eq!(catalog.relations().len(), 2);

        catalog.clear();
        assert_eq!(catalog.relations().len(), 0);
    }

    #[test]
    fn test_schema_overwrite() {
        let mut catalog = SchemaCatalog::new();

        // Register initial schema
        catalog
            .register(
                RelationSchema::new("User").with_column(ColumnSchema::new("id", SchemaType::Int)),
            )
            .unwrap();

        assert_eq!(catalog.get("User").unwrap().arity(), 1);

        // Register new schema with same name (use register_or_update)
        catalog
            .register_or_update(
                RelationSchema::new("User")
                    .with_column(ColumnSchema::new("id", SchemaType::Int))
                    .with_column(ColumnSchema::new("name", SchemaType::String)),
            )
            .unwrap();

        assert_eq!(catalog.get("User").unwrap().arity(), 2);
    }
}

// Relation Schema Tests
mod relation_schema_tests {
    use super::*;

    #[test]
    fn test_schema_arity() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("a", SchemaType::Int))
            .with_column(ColumnSchema::new("b", SchemaType::String))
            .with_column(ColumnSchema::new("c", SchemaType::Float));

        assert_eq!(schema.arity(), 3);
    }

    #[test]
    fn test_empty_schema_arity() {
        let schema = RelationSchema::new("Empty");
        assert_eq!(schema.arity(), 0);
    }

    #[test]
    fn test_column_by_index() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("first", SchemaType::Int))
            .with_column(ColumnSchema::new("second", SchemaType::String));

        assert_eq!(schema.column(0).unwrap().name, "first");
        assert_eq!(schema.column(1).unwrap().name, "second");
        assert!(schema.column(2).is_none());
    }

    #[test]
    fn test_column_by_name() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));

        let col = schema.column_by_name("id");
        assert!(col.is_some());
        assert_eq!(col.unwrap().data_type, SchemaType::Int);

        assert!(schema.column_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_column_index_by_name() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("a", SchemaType::Int))
            .with_column(ColumnSchema::new("b", SchemaType::String))
            .with_column(ColumnSchema::new("c", SchemaType::Float));

        assert_eq!(schema.column_index("a"), Some(0));
        assert_eq!(schema.column_index("b"), Some(1));
        assert_eq!(schema.column_index("c"), Some(2));
        assert_eq!(schema.column_index("d"), None);
    }

    #[test]
    fn test_column_names() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("x", SchemaType::Int))
            .with_column(ColumnSchema::new("y", SchemaType::Int))
            .with_column(ColumnSchema::new("z", SchemaType::Int));

        let names = schema.column_names();
        assert_eq!(names, vec!["x", "y", "z"]);
    }

    #[test]
    fn test_schema_display_format() {
        let schema = RelationSchema::new("Person")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String))
            .with_column(ColumnSchema::new("active", SchemaType::Bool));

        let display = format!("{}", schema);
        assert_eq!(display, "Person(id: int, name: string, active: bool)");
    }

    #[test]
    fn test_to_tuple_schema_conversion() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));

        let tuple_schema = schema.to_tuple_schema();
        assert_eq!(tuple_schema.arity(), 2);
        assert_eq!(tuple_schema.field_name(0), Some("id"));
        assert_eq!(tuple_schema.field_name(1), Some("name"));
    }
}

// Edge Case Tests
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_validate_tuple_with_null_value() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("optional", SchemaType::Any));

        let mut engine = ValidationEngine::new();

        // Null is allowed for 'any' type
        let tuple = Tuple::new(vec![Value::Int64(1), Value::Null]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty_string() {
        let schema =
            RelationSchema::new("Test").with_column(ColumnSchema::new("text", SchemaType::String));

        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![Value::string("")]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_special_float_values() {
        let schema =
            RelationSchema::new("Test").with_column(ColumnSchema::new("val", SchemaType::Float));

        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![Value::Float64(f64::NAN)]),
            Tuple::new(vec![Value::Float64(f64::INFINITY)]),
            Tuple::new(vec![Value::Float64(f64::NEG_INFINITY)]),
            Tuple::new(vec![Value::Float64(0.0)]),
            Tuple::new(vec![Value::Float64(-0.0)]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_boundary_int_values() {
        let schema =
            RelationSchema::new("Test").with_column(ColumnSchema::new("val", SchemaType::Int));

        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![Value::Int32(i32::MAX)]),
            Tuple::new(vec![Value::Int32(i32::MIN)]),
            Tuple::new(vec![Value::Int64(i64::MAX)]),
            Tuple::new(vec![Value::Int64(i64::MIN)]),
            Tuple::new(vec![Value::Int64(0)]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty_vector() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("vec", SchemaType::Vector { dim: None }));

        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![Value::Vector(Arc::new(vec![]))]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_large_vector() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("vec", SchemaType::Vector { dim: None }));

        let mut engine = ValidationEngine::new();

        // 1024-dimensional vector (common for embeddings)
        let large_vec: Vec<f32> = (0..1024).map(|i| i as f32 / 1024.0).collect();
        let tuple = Tuple::new(vec![Value::Vector(Arc::new(large_vec))]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_unicode_strings() {
        let schema =
            RelationSchema::new("Test").with_column(ColumnSchema::new("text", SchemaType::String));

        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![Value::string("Hello 世界")]),
            Tuple::new(vec![Value::string("Привет мир")]),
            Tuple::new(vec![Value::string("مرحبا بالعالم")]),
            Tuple::new(vec![Value::string("🎉🚀💻")]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_single_column_schema() {
        let schema =
            RelationSchema::new("Single").with_column(ColumnSchema::new("only", SchemaType::Int));

        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![Value::Int64(42)]);
        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_wide_schema_many_columns() {
        // Schema with many columns
        let mut schema = RelationSchema::new("Wide");
        for i in 0..20 {
            schema = schema.with_column(ColumnSchema::new(format!("col{}", i), SchemaType::Int));
        }

        let mut engine = ValidationEngine::new();

        let values: Vec<Value> = (0..20).map(|i| Value::Int64(i)).collect();
        let tuple = Tuple::new(values);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }
}
