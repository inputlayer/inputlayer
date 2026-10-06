//! Statement parser tests for the IQL-native syntax: meta commands, data
//! operations, rules, queries, serializable rules and unquoted-atom rejection.

use crate::parser::parse_rule;
use crate::statement::{
    parse_rule_definition, parse_statement, DeletePattern, MetaCommand, SerializableRule, Statement,
};

// Statement Parser Tests
#[test]
fn test_parse_meta_commands() {
    // Knowledge graph commands
    assert!(matches!(
        parse_statement(".kg").unwrap(),
        Statement::Meta(MetaCommand::KgShow)
    ));
    assert!(matches!(
        parse_statement(".kg list").unwrap(),
        Statement::Meta(MetaCommand::KgList)
    ));
    assert!(matches!(
        parse_statement(".kg create mykg").unwrap(),
        Statement::Meta(MetaCommand::KgCreate(name)) if name == "mykg"
    ));
    assert!(matches!(
        parse_statement(".kg use mykg").unwrap(),
        Statement::Meta(MetaCommand::KgUse(name)) if name == "mykg"
    ));
    assert!(matches!(
        parse_statement(".kg drop mykg").unwrap(),
        Statement::Meta(MetaCommand::KgDrop(name)) if name == "mykg"
    ));

    // Relation commands
    assert!(matches!(
        parse_statement(".rel").unwrap(),
        Statement::Meta(MetaCommand::RelList)
    ));
    assert!(matches!(
        parse_statement(".rel edge").unwrap(),
        Statement::Meta(MetaCommand::RelDescribe(name)) if name == "edge"
    ));

    // Rule commands
    assert!(matches!(
        parse_statement(".rule").unwrap(),
        Statement::Meta(MetaCommand::RuleList)
    ));
    assert!(matches!(
        parse_statement(".rule path").unwrap(),
        Statement::Meta(MetaCommand::RuleQuery(name)) if name == "path"
    ));
    assert!(matches!(
        parse_statement(".rule def path").unwrap(),
        Statement::Meta(MetaCommand::RuleShowDef(name)) if name == "path"
    ));
    assert!(matches!(
        parse_statement(".rule drop path").unwrap(),
        Statement::Meta(MetaCommand::RuleDrop(name)) if name == "path"
    ));

    // System commands
    assert!(matches!(
        parse_statement(".compact").unwrap(),
        Statement::Meta(MetaCommand::Compact)
    ));
    assert!(matches!(
        parse_statement(".status").unwrap(),
        Statement::Meta(MetaCommand::Status)
    ));
    assert!(matches!(
        parse_statement(".help").unwrap(),
        Statement::Meta(MetaCommand::Help)
    ));
    assert!(matches!(
        parse_statement(".quit").unwrap(),
        Statement::Meta(MetaCommand::Quit)
    ));
    assert!(matches!(
        parse_statement(".exit").unwrap(),
        Statement::Meta(MetaCommand::Quit)
    ));
}

#[test]
fn test_parse_insert_operations() {
    // Single insert
    let stmt = parse_statement("+edge(1, 2)").unwrap();
    if let Statement::Insert(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert_eq!(op.tuples.len(), 1);
    } else {
        panic!("Expected Insert statement");
    }

    // Bulk insert
    let stmt = parse_statement("+edge[(1, 2), (3, 4), (5, 6)]").unwrap();
    if let Statement::Insert(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert_eq!(op.tuples.len(), 3);
    } else {
        panic!("Expected Insert statement");
    }
}

#[test]
fn test_parse_delete_operations() {
    // Single delete
    let stmt = parse_statement("-edge(1, 2)").unwrap();
    if let Statement::Delete(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert!(matches!(op.pattern, DeletePattern::SingleTuple(_)));
    } else {
        panic!("Expected Delete statement");
    }

    // Conditional delete - use valid atom syntax instead of constraint
    let stmt = parse_statement("-edge(X, Y) <- source(X)").unwrap();
    if let Statement::Delete(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert!(matches!(op.pattern, DeletePattern::Conditional { .. }));
    } else {
        panic!("Expected Delete statement");
    }
}

#[test]
fn test_parse_persistent_rule() {
    // Simple persistent rule (new syntax using + prefix)
    let stmt = parse_statement("+path(X, Y) <- edge(X, Y)").unwrap();
    if let Statement::PersistentRule(rule) = stmt {
        assert_eq!(rule.head.relation, "path");
    } else {
        panic!("Expected PersistentRule statement");
    }

    // Persistent rule with join - use valid atom syntax instead of constraint
    let stmt = parse_statement("+adult(N, A) <- person(N, A), ages(N, A)").unwrap();
    if let Statement::PersistentRule(rule) = stmt {
        assert_eq!(rule.head.relation, "adult");
    } else {
        panic!("Expected PersistentRule statement");
    }
}

#[test]
fn test_parse_transient_rule() {
    // Use valid atom syntax instead of constraint
    let stmt = parse_statement("result(X, Y) <- edge(X, Y), node(X)").unwrap();
    if let Statement::SessionRule(rule) = stmt {
        assert_eq!(rule.head.relation, "result");
    } else {
        panic!("Expected TransientRule statement");
    }
}

#[test]
fn test_parse_query() {
    let stmt = parse_statement("?edge(1, X)").unwrap();
    if let Statement::Query(goal) = stmt {
        assert_eq!(goal.goal.as_ref().unwrap().relation, "edge");
    } else {
        panic!("Expected Query statement");
    }
}

#[test]
fn test_parse_update_operation() {
    // Use valid atom syntax instead of constraint
    let stmt = parse_statement(
        "-person(X, OldAge), +person(X, NewAge) <- person(X, OldAge), newage(X, NewAge)",
    )
    .unwrap();
    if let Statement::Update(op) = stmt {
        assert_eq!(op.deletes.len(), 1);
        assert_eq!(op.inserts.len(), 1);
        assert_eq!(op.deletes[0].relation, "person");
        assert_eq!(op.inserts[0].relation, "person");
    } else {
        panic!("Expected Update statement");
    }
}

#[test]
fn test_parse_rule_definition_function() {
    let rule_def = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
    assert_eq!(rule_def.name, "path");

    let rule = rule_def.rule.to_rule();
    assert_eq!(rule.head.relation, "path");
    assert_eq!(rule.body.len(), 1);
}

// Error Handling Tests
#[test]
fn test_parse_invalid_statements() {
    // Invalid meta command
    assert!(parse_statement(".unknown").is_err());

    // Missing arguments
    assert!(parse_statement(".kg create").is_err());

    // Invalid syntax
    assert!(parse_statement("this is not valid").is_err());

    // Empty input
    assert!(parse_statement("").is_err());
}

// Serializable Rule Tests
#[test]
fn test_serializable_rule_roundtrip() {
    // Use valid atom syntax instead of constraint
    let rule_str = "path(X, Y) <- edge(X, Y), node(X)";
    let rule = parse_rule(rule_str).unwrap();

    // Convert to serializable
    let serializable = SerializableRule::from_rule(&rule);

    // Convert back
    let restored = serializable.to_rule();

    assert_eq!(rule.head.relation, restored.head.relation);
    assert_eq!(rule.body.len(), restored.body.len());
}

#[test]
fn test_serializable_rule_json() {
    let rule_str = "result(X, Y) <- edge(X, Y)";
    let rule = parse_rule(rule_str).unwrap();

    let serializable = SerializableRule::from_rule(&rule);

    // Serialize to JSON
    let json = serde_json::to_string(&serializable).unwrap();

    // Deserialize from JSON
    let restored: SerializableRule = serde_json::from_str(&json).unwrap();

    assert_eq!(serializable.head_relation, restored.head_relation);
}

// Unquoted Atom Rejection Tests
#[test]
fn test_unquoted_atom_rejected_in_insert() {
    // Unquoted lowercase identifier should be rejected - use quoted strings instead
    let result = parse_statement("+person(alice, 30)");
    assert!(result.is_err(), "Unquoted atom 'alice' should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.contains("Unquoted atom") || err.contains("alice"),
        "Error should mention unquoted atom: {err}"
    );
}

#[test]
fn test_quoted_string_accepted_in_insert() {
    // Quoted string should work
    let result = parse_statement("+person(\"alice\", 30)");
    assert!(
        result.is_ok(),
        "Quoted string should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_unquoted_atom_rejected_in_fact() {
    // Session fact with unquoted atom should be rejected
    let result = parse_statement("person(bob, 25)");
    assert!(result.is_err(), "Unquoted atom 'bob' should be rejected");
}

#[test]
fn test_quoted_string_accepted_in_fact() {
    // Session fact with quoted string should work
    let result = parse_statement("person(\"bob\", 25)");
    assert!(
        result.is_ok(),
        "Quoted string should be accepted: {:?}",
        result.err()
    );
}

#[test]
fn test_unquoted_atom_rejected_in_query() {
    // Query with unquoted atom constant should be rejected
    let result = parse_statement("?person(alice, X)");
    assert!(
        result.is_err(),
        "Unquoted atom 'alice' in query should be rejected"
    );
}

#[test]
fn test_variables_still_work_in_query() {
    // Variables (uppercase) should still work fine
    let result = parse_statement("?person(X, Y)");
    assert!(result.is_ok(), "Variables should work: {:?}", result.err());
}

#[test]
fn test_mixed_quoted_and_variables() {
    // Mix of quoted strings and variables should work
    let result = parse_statement("?person(\"alice\", Age)");
    assert!(
        result.is_ok(),
        "Mix of quoted string and variable should work: {:?}",
        result.err()
    );
}

#[test]
fn test_unquoted_atom_error_message_helpful() {
    let result = parse_statement("+person(alice, 30)");
    let err = result.unwrap_err();
    // Error should suggest using quotes
    assert!(
        err.contains("\"alice\"") || err.contains("quoted"),
        "Error should suggest using quotes: {err}"
    );
}
