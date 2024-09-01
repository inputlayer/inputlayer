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
// - examples/datalog/04_session/*.dl (snapshot tests for session lifecycle)
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
        session_rules.push("path(X, Y) :- edge(X, Y).".to_string());

        // Add persistent rule
        persistent_rules.push("+connected(X, Y) :- path(X, Y).".to_string());

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
        session_rules.push("rule1(X) :- fact(X).".to_string());
        session_rules.push("rule2(X) :- rule1(X).".to_string());

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

        session_rules.push("rule1(X) :- fact(X).".to_string());
        session_rules.push("rule2(X) :- rule1(X).".to_string());
        session_rules.push("rule3(X) :- rule2(X).".to_string());

        // Remove by 0-based index (simulating 1-based UI)
        let removed = session_rules.remove(1); // Remove rule2
        assert_eq!(removed, "rule2(X) :- rule1(X).");
        assert_eq!(session_rules.len(), 2);
        assert_eq!(session_rules[0], "rule1(X) :- fact(X).");
        assert_eq!(session_rules[1], "rule3(X) :- rule2(X).");
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
        session1.rules.push("s1_rule(X) :- fact(X).".to_string());

        // Add data to session 2
        session2.facts.push(Tuple::new(vec![Value::Int64(2)]));
        session2.rules.push("s2_rule(X) :- fact(X).".to_string());

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
        session_rules.push("rule1(X) :- a(X).".to_string());
        session_rules.push("rule2(X) :- b(X).".to_string());

        // Build program text from session rules
        let program_text = session_rules.join("\n");
        assert!(program_text.contains("rule1"));
        assert!(program_text.contains("rule2"));

        // Clear and rebuild
        session_rules.clear();
        session_rules.push("new_rule(X) :- c(X).".to_string());

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
        assert!(SchemaType::Vector.matches(&Value::Vector(Arc::new(vec![1.0, 2.0, 3.0]))));
        assert!(SchemaType::Vector.matches(&Value::Vector(Arc::new(vec![]))));
        assert!(SchemaType::Vector.matches(&Value::VectorInt8(Arc::new(vec![1, 2, 3]))));

        assert!(!SchemaType::Vector.matches(&Value::string("[1,2,3]")));
        assert!(!SchemaType::Vector.matches(&Value::Int64(123)));
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
        assert_eq!(format!("{}", SchemaType::Vector), "vector");
        assert_eq!(format!("{}", SchemaType::Any), "any");
        assert_eq!(
            format!("{}", SchemaType::Named("Email".to_string())),
            "Email"
        );
    }
}

// Schema Validation Tests
