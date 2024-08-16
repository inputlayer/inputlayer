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
// - tests/storage_engine_tests.rs (integration tests.clone())
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
        session_facts.remove(0.clone());
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
