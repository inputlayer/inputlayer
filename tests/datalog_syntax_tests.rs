//! Integration Tests for Datalog-Native Syntax
//!
//! Tests for:
//! - Statement parser integration
//! - Rule catalog operations
//! - REPL statement handling
//! - RPC rule operations

use inputlayer::{
    statement::{parse_rule_definition, parse_statement, DeletePattern, MetaCommand, Statement},
    Config, RuleCatalog, StorageEngine,
};
use tempfile::TempDir;

// Test Helpers
fn create_test_config(data_dir: std::path::PathBuf) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = data_dir;
    config.storage.performance.num_threads = 2;
    config
}

fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let config = create_test_config(temp.path().to_path_buf());
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

// Statement Parser Integration Tests
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
    let stmt = parse_statement("+edge(1, 2).").unwrap();
    if let Statement::Insert(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert_eq!(op.tuples.len(), 1);
    } else {
        panic!("Expected Insert statement");
    }

    // Bulk insert
    let stmt = parse_statement("+edge[(1, 2), (3, 4), (5, 6)].").unwrap();
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
    let stmt = parse_statement("-edge(1, 2).").unwrap();
    if let Statement::Delete(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert!(matches!(op.pattern, DeletePattern::SingleTuple(_)));
    } else {
        panic!("Expected Delete statement");
    }

    // Conditional delete - use valid atom syntax instead of constraint
    let stmt = parse_statement("-edge(X, Y) :- source(X).").unwrap();
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
    let stmt = parse_statement("+path(X, Y) :- edge(X, Y).").unwrap();
    if let Statement::PersistentRule(rule) = stmt {
        assert_eq!(rule.head.relation, "path");
    } else {
        panic!("Expected PersistentRule statement");
    }

    // Persistent rule with join - use valid atom syntax instead of constraint
    let stmt = parse_statement("+adult(N, A) :- person(N, A), ages(N, A).").unwrap();
    if let Statement::PersistentRule(rule) = stmt {
        assert_eq!(rule.head.relation, "adult");
    } else {
        panic!("Expected PersistentRule statement");
    }
}

#[test]
fn test_parse_transient_rule() {
    // Use valid atom syntax instead of constraint
    let stmt = parse_statement("result(X, Y) :- edge(X, Y), node(X).").unwrap();
    if let Statement::SessionRule(rule) = stmt {
        assert_eq!(rule.head.relation, "result");
    } else {
        panic!("Expected TransientRule statement");
    }
}

#[test]
fn test_parse_query() {
    let stmt = parse_statement("?- edge(1, X).").unwrap();
    if let Statement::Query(goal) = stmt {
        assert_eq!(goal.goal.relation, "edge");
    } else {
        panic!("Expected Query statement");
    }
}

#[test]
fn test_parse_update_operation() {
    // Use valid atom syntax instead of constraint
    let stmt = parse_statement(
        "-person(X, OldAge), +person(X, NewAge) :- person(X, OldAge), newage(X, NewAge).",
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
    let rule_def = parse_rule_definition("path(X, Y) :-edge(X, Y).").unwrap();
    assert_eq!(rule_def.name, "path");

    let rule = rule_def.rule.to_rule();
    assert_eq!(rule.head.relation, "path");
    assert_eq!(rule.body.len(), 1);
}

// Rule Catalog Integration Tests
#[test]
fn test_rule_catalog_with_storage_engine() {
    let (mut storage, _temp) = create_test_storage();

    // Create knowledge_graph
    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert base data
    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    // Register a rule
    let rule_def = parse_rule_definition("path(X, Y) :-edge(X, Y).").unwrap();
    storage.register_rule(&rule_def).unwrap();

    // List rules
    let rules = storage.list_rules().unwrap();
    assert!(rules.contains(&"path".to_string()));

    // Describe rule
    let desc = storage.describe_rule("path").unwrap();
    assert!(desc.is_some());
    assert!(desc.unwrap().contains("path"));

    // Execute query using rule
    let results = storage
        .execute_query_with_rules("result(X, Y) :- path(X, Y).")
        .unwrap();
    assert_eq!(results.len(), 3);

    // Drop rule
    storage.drop_rule("path").unwrap();
    let rules = storage.list_rules().unwrap();
    assert!(!rules.contains(&"path".to_string()));
}

#[test]
