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
    // TODO: verify this condition
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
fn test_recursive_rule_definition() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("graph").unwrap();
    storage.use_knowledge_graph("graph").unwrap();

    // Insert edges
    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    // Register recursive rule (two clauses)
    let base_rule = parse_rule_definition("path(X, Y) :-edge(X, Y).").unwrap();
    storage.register_rule(&base_rule).unwrap();

    let recursive_rule = parse_rule_definition("path(X, Z) :-edge(X, Y), path(Y, Z).").unwrap();
    storage.register_rule(&recursive_rule).unwrap();

    // The rule should have 2 clauses
    let desc = storage.describe_rule("path").unwrap().unwrap();
    assert!(desc.contains("path"));
}

#[test]
fn test_rule_persistence() {
    let temp = TempDir::new().unwrap();

    // Create storage, add rule, save
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("mydb").unwrap();
        storage.use_knowledge_graph("mydb").unwrap();

        let rule_def = parse_rule_definition("derived(X) :-base(X).").unwrap();
        storage.register_rule(&rule_def).unwrap();

        storage.save_all().unwrap();
    }

    // Reload storage, rule should still exist
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_knowledge_graph("mydb").unwrap();

        let rules = storage.list_rules().unwrap();
        assert!(rules.contains(&"derived".to_string()));
    }
}

#[test]
fn test_rule_catalog_standalone() {
    let temp = TempDir::new().unwrap();

    let mut catalog = RuleCatalog::new(temp.path().to_path_buf()).unwrap();

    // Register rule
    let rule_def = parse_rule_definition("path(X, Y) :-edge(X, Y).").unwrap();
    catalog.register_rule(&rule_def).unwrap();

    assert!(catalog.exists("path"));
    assert_eq!(catalog.len(), 1);

    // Get rules
    let rules = catalog.all_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].head.relation, "path");

    // Reload and verify persistence
    let catalog2 = RuleCatalog::new(temp.path().to_path_buf()).unwrap();
    assert!(catalog2.exists("path"));
}

// Storage Engine Rule Integration Tests
#[test]
fn test_storage_engine_list_relations() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Initially empty
    let relations = storage.list_relations().unwrap();
    assert!(relations.is_empty());

    // Add some data
    storage.insert("edge", vec![(1, 2)]).unwrap();
    storage.insert("node", vec![(1, 1)]).unwrap();

    let relations = storage.list_relations().unwrap();
    assert!(relations.contains(&"edge".to_string()));
    assert!(relations.contains(&"node".to_string()));
}

#[test]
fn test_execute_query_with_rules() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert data
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Register rule
    let rule_def = parse_rule_definition("path(X, Y) :-edge(X, Y).").unwrap();
    storage.register_rule(&rule_def).unwrap();

    // Query that uses the rule
    let results = storage
        .execute_query_with_rules("result(X, Y) :- path(X, Y).")
        .unwrap();
    assert_eq!(results.len(), 2);
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

#[test]
fn test_drop_nonexistent_view() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    let result = storage.drop_rule("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_view_operations_on_default_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    // StorageEngine creates a "default" knowledge_graph by default
    // So list_rules should succeed even without explicit knowledge_graph creation
    let result = storage.list_rules();
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // No views registered yet
}

// Serializable Rule Tests
#[test]
fn test_serializable_rule_roundtrip() {
    use inputlayer::parser::parse_rule;
    use inputlayer::statement::SerializableRule;

    // Use valid atom syntax instead of constraint
    let rule_str = "path(X, Y) :- edge(X, Y), node(X).";
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
    use inputlayer::parser::parse_rule;
    use inputlayer::statement::SerializableRule;

    let rule_str = "result(X, Y) :- edge(X, Y).";
    let rule = parse_rule(rule_str).unwrap();

    let serializable = SerializableRule::from_rule(&rule);

    // Serialize to JSON
    let json = serde_json::to_string(&serializable).unwrap();

    // Deserialize from JSON
    let restored: SerializableRule = serde_json::from_str(&json).unwrap();

    assert_eq!(serializable.head_relation, restored.head_relation);
}

// Complex Query Tests
#[test]
#[ignore] // Constraint syntax (Age >= 18) no longer supported - Constraint type removed
fn test_view_with_constraints() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert data
    storage
        .insert("person", vec![(1, 25), (2, 17), (3, 30), (4, 16)])
        .unwrap();

    // Register view with constraint
    let view_def = parse_rule_definition("adult(Id, Age) :-person(Id, Age), Age >= 18.").unwrap();
    storage.register_rule(&view_def).unwrap();

    // Query the view
    let results = storage
        .execute_query_with_rules("result(Id, Age) :- adult(Id, Age).")
        .unwrap();

    // Should only get people 18 or older (id 1 age 25, id 3 age 30)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_multiple_views() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert data
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Register multiple views
    let view1 = parse_rule_definition("path(X, Y) :-edge(X, Y).").unwrap();
    storage.register_rule(&view1).unwrap();

    let view2 = parse_rule_definition("reach(X) :-path(1, X).").unwrap();
    storage.register_rule(&view2).unwrap();

    let views = storage.list_rules().unwrap();
    assert_eq!(views.len(), 2);
}

// Query with Constants Tests (Phase 0 fix)
#[test]
