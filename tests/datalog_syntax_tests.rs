//! Integration Tests for Datalog-Native Syntax
//!
//! Tests for:
//! - Statement parser integration
//! - View catalog operations
//! - REPL statement handling
//! - RPC view operations

use inputlayer::{
    statement::{
        parse_statement, parse_view_definition, DeletePattern, MetaCommand, Statement,
    },
    Config, StorageEngine, ViewCatalog,
};
use tempfile::TempDir;

// ============================================================================
// Test Helpers
// ============================================================================

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

// ============================================================================
// Statement Parser Integration Tests
// ============================================================================

#[test]
fn test_parse_meta_commands() {
    // Database commands
    assert!(matches!(parse_statement(".db").unwrap(), Statement::Meta(MetaCommand::DbShow)));
    assert!(matches!(parse_statement(".db list").unwrap(), Statement::Meta(MetaCommand::DbList)));
    assert!(matches!(
        parse_statement(".db create mydb").unwrap(),
        Statement::Meta(MetaCommand::DbCreate(name)) if name == "mydb"
    ));
    assert!(matches!(
        parse_statement(".db use mydb").unwrap(),
        Statement::Meta(MetaCommand::DbUse(name)) if name == "mydb"
    ));
    assert!(matches!(
        parse_statement(".db drop mydb").unwrap(),
        Statement::Meta(MetaCommand::DbDrop(name)) if name == "mydb"
    ));

    // Relation commands
    assert!(matches!(parse_statement(".rel").unwrap(), Statement::Meta(MetaCommand::RelList)));
    assert!(matches!(
        parse_statement(".rel edge").unwrap(),
        Statement::Meta(MetaCommand::RelDescribe(name)) if name == "edge"
    ));

    // View commands
    assert!(matches!(parse_statement(".view").unwrap(), Statement::Meta(MetaCommand::ViewList)));
    assert!(matches!(
        parse_statement(".view path").unwrap(),
        Statement::Meta(MetaCommand::ViewQuery(name)) if name == "path"
    ));
    assert!(matches!(
        parse_statement(".view def path").unwrap(),
        Statement::Meta(MetaCommand::ViewDef(name)) if name == "path"
    ));
    assert!(matches!(
        parse_statement(".view drop path").unwrap(),
        Statement::Meta(MetaCommand::ViewDrop(name)) if name == "path"
    ));

    // System commands
    assert!(matches!(parse_statement(".compact").unwrap(), Statement::Meta(MetaCommand::Compact)));
    assert!(matches!(parse_statement(".status").unwrap(), Statement::Meta(MetaCommand::Status)));
    assert!(matches!(parse_statement(".help").unwrap(), Statement::Meta(MetaCommand::Help)));
    assert!(matches!(parse_statement(".quit").unwrap(), Statement::Meta(MetaCommand::Quit)));
    assert!(matches!(parse_statement(".exit").unwrap(), Statement::Meta(MetaCommand::Quit)));
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

    // Conditional delete
    let stmt = parse_statement("-edge(X, Y) :- X > 5.").unwrap();
    if let Statement::Delete(op) = stmt {
        assert_eq!(op.relation, "edge");
        assert!(matches!(op.pattern, DeletePattern::Conditional { .. }));
    } else {
        panic!("Expected Delete statement");
    }
}

#[test]
fn test_parse_view_definition() {
    // Simple view
    let stmt = parse_statement("view path(x: int, y: int) :- edge(x, y).").unwrap();
    if let Statement::ViewDecl(def) = stmt {
        assert_eq!(def.name, "path");
    } else {
        panic!("Expected View statement");
    }

    // View with filter
    let stmt = parse_statement("view adult(n: string, a: int) :- person(n, a), a >= 18.").unwrap();
    if let Statement::ViewDecl(def) = stmt {
        assert_eq!(def.name, "adult");
    } else {
        panic!("Expected View statement");
    }
}

#[test]
fn test_parse_transient_rule() {
    let stmt = parse_statement("result(X, Y) :- edge(X, Y), X < Y.").unwrap();
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
    let stmt = parse_statement("-person(X, OldAge), +person(X, NewAge) :- person(X, OldAge), NewAge = OldAge.").unwrap();
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
fn test_parse_view_definition_function() {
    let view_def = parse_view_definition("path(X, Y) := edge(X, Y).").unwrap();
    assert_eq!(view_def.name, "path");

    let rule = view_def.rule.to_rule();
    assert_eq!(rule.head.relation, "path");
    assert_eq!(rule.body.len(), 1);
}

// ============================================================================
// View Catalog Integration Tests
// ============================================================================

#[test]
fn test_view_catalog_with_storage_engine() {
    let (mut storage, _temp) = create_test_storage();

    // Create database
    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert base data
    storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();

    // Register a view
    let view_def = parse_view_definition("path(X, Y) := edge(X, Y).").unwrap();
    storage.register_view(&view_def).unwrap();

    // List views
    let views = storage.list_views().unwrap();
    assert!(views.contains(&"path".to_string()));

    // Describe view
    let desc = storage.describe_view("path").unwrap();
    assert!(desc.is_some());
    assert!(desc.unwrap().contains("path"));

    // Execute query using view
    let results = storage.execute_query_with_views("result(X, Y) :- path(X, Y).").unwrap();
    assert_eq!(results.len(), 3);

    // Drop view
    storage.drop_view("path").unwrap();
    let views = storage.list_views().unwrap();
    assert!(!views.contains(&"path".to_string()));
}

#[test]
fn test_recursive_view_definition() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("graph").unwrap();
    storage.use_database("graph").unwrap();

    // Insert edges
    storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();

    // Register recursive view (two rules)
    let base_rule = parse_view_definition("path(X, Y) := edge(X, Y).").unwrap();
    storage.register_view(&base_rule).unwrap();

    let recursive_rule = parse_view_definition("path(X, Z) := edge(X, Y), path(Y, Z).").unwrap();
    storage.register_view(&recursive_rule).unwrap();

    // The view should have 2 rules
    let desc = storage.describe_view("path").unwrap().unwrap();
    assert!(desc.contains("path"));
}

#[test]
fn test_view_persistence() {
    let temp = TempDir::new().unwrap();

    // Create storage, add view, save
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("mydb").unwrap();
        storage.use_database("mydb").unwrap();

        let view_def = parse_view_definition("derived(X) := base(X).").unwrap();
        storage.register_view(&view_def).unwrap();

        storage.save_all().unwrap();
    }

    // Reload storage, view should still exist
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_database("mydb").unwrap();

        let views = storage.list_views().unwrap();
        assert!(views.contains(&"derived".to_string()));
    }
}

#[test]
fn test_view_catalog_standalone() {
    let temp = TempDir::new().unwrap();

    let mut catalog = ViewCatalog::new(temp.path().to_path_buf()).unwrap();

    // Register view
    let view_def = parse_view_definition("path(X, Y) := edge(X, Y).").unwrap();
    catalog.register_view(&view_def).unwrap();

    assert!(catalog.exists("path"));
    assert_eq!(catalog.len(), 1);

    // Get rules
    let rules = catalog.all_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].head.relation, "path");

    // Reload and verify persistence
    let catalog2 = ViewCatalog::new(temp.path().to_path_buf()).unwrap();
    assert!(catalog2.exists("path"));
}

// ============================================================================
// Storage Engine View Integration Tests
// ============================================================================

#[test]
fn test_storage_engine_list_relations() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

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
fn test_execute_query_with_views() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert data
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Register view
    let view_def = parse_view_definition("path(X, Y) := edge(X, Y).").unwrap();
    storage.register_view(&view_def).unwrap();

    // Query that uses the view
    let results = storage.execute_query_with_views("result(X, Y) :- path(X, Y).").unwrap();
    assert_eq!(results.len(), 2);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_parse_invalid_statements() {
    // Invalid meta command
    assert!(parse_statement(".unknown").is_err());

    // Missing arguments
    assert!(parse_statement(".db create").is_err());

    // Invalid syntax
    assert!(parse_statement("this is not valid").is_err());

    // Empty input
    assert!(parse_statement("").is_err());
}

#[test]
fn test_drop_nonexistent_view() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    let result = storage.drop_view("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_view_operations_on_default_database() {
    let (storage, _temp) = create_test_storage();

    // StorageEngine creates a "default" database by default
    // So list_views should succeed even without explicit database creation
    let result = storage.list_views();
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty()); // No views registered yet
}

// ============================================================================
// Serializable Rule Tests
// ============================================================================

#[test]
fn test_serializable_rule_roundtrip() {
    use inputlayer::statement::SerializableRule;
    use inputlayer::parser::parse_rule;

    let rule_str = "path(X, Y) :- edge(X, Y), X < Y.";
    let rule = parse_rule(rule_str).unwrap();

    // Convert to serializable
    let serializable = SerializableRule::from_rule(&rule);

    // Convert back
    let restored = serializable.to_rule();

    assert_eq!(rule.head.relation, restored.head.relation);
    assert_eq!(rule.body.len(), restored.body.len());
    assert_eq!(rule.constraints.len(), restored.constraints.len());
}

#[test]
fn test_serializable_rule_json() {
    use inputlayer::statement::SerializableRule;
    use inputlayer::parser::parse_rule;

    let rule_str = "result(X, Y) :- edge(X, Y).";
    let rule = parse_rule(rule_str).unwrap();

    let serializable = SerializableRule::from_rule(&rule);

    // Serialize to JSON
    let json = serde_json::to_string(&serializable).unwrap();

    // Deserialize from JSON
    let restored: SerializableRule = serde_json::from_str(&json).unwrap();

    assert_eq!(serializable.head_relation, restored.head_relation);
}

// ============================================================================
// Complex Query Tests
// ============================================================================

#[test]
fn test_view_with_constraints() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert data
    storage.insert("person", vec![(1, 25), (2, 17), (3, 30), (4, 16)]).unwrap();

    // Register view with constraint
    let view_def = parse_view_definition("adult(Id, Age) := person(Id, Age), Age >= 18.").unwrap();
    storage.register_view(&view_def).unwrap();

    // Query the view
    let results = storage.execute_query_with_views("result(Id, Age) :- adult(Id, Age).").unwrap();

    // Should only get people 18 or older (id 1 age 25, id 3 age 30)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_multiple_views() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert data
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Register multiple views
    let view1 = parse_view_definition("path(X, Y) := edge(X, Y).").unwrap();
    storage.register_view(&view1).unwrap();

    let view2 = parse_view_definition("reach(X) := path(1, X).").unwrap();
    storage.register_view(&view2).unwrap();

    let views = storage.list_views().unwrap();
    assert_eq!(views.len(), 2);
}

// ============================================================================
// Query with Constants Tests (Phase 0 fix)
// ============================================================================

#[test]
fn test_query_with_constant_first_arg() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert parent relationships
    storage.insert("parent", vec![(1, 2), (1, 3), (2, 4)]).unwrap();

    // Query: ?- parent(1, X). transformed to query with constraint
    // This mimics the client's handle_query transformation
    let results = storage
        .execute_query_with_views("__query__(_c0, X) :- parent(_c0, X), _c0 = 1.")
        .unwrap();

    // Should return (1, 2) and (1, 3) - children of parent 1
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(1, 3)));
}

#[test]
fn test_query_with_all_constants() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert data
    storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();

    // Query: ?- edge(1, 2). transformed to query with constraints
    let results = storage
        .execute_query_with_views("__query__(_c0, _c1) :- edge(_c0, _c1), _c0 = 1, _c1 = 2.")
        .unwrap();

    // Should return (1, 2) since that fact exists
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (1, 2));

    // Query: ?- edge(1, 99). - fact doesn't exist
    let results = storage
        .execute_query_with_views("__query__(_c0, _c1) :- edge(_c0, _c1), _c0 = 1, _c1 = 99.")
        .unwrap();

    // Should return empty since (1, 99) doesn't exist
    assert_eq!(results.len(), 0);
}

#[test]
fn test_query_with_constant_on_base_relation() {
    // Test query with constant directly on base relation (no view)
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert edges
    storage.insert("edge", vec![(1, 2), (1, 3), (2, 4)]).unwrap();

    // Query: ?- edge(1, X). - find direct edges from 1
    let results = storage
        .execute_query_with_views("__query__(_c0, X) :- edge(_c0, X), _c0 = 1.")
        .unwrap();

    // From 1, direct edges are: (1,2), (1,3)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(1, 3)));
}

#[test]
fn test_query_constant_second_arg() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    // Insert edges
    storage.insert("edge", vec![(1, 3), (2, 3), (4, 5)]).unwrap();

    // Query: ?- edge(X, 3). - find all sources pointing to 3
    let results = storage
        .execute_query_with_views("__query__(X, _c1) :- edge(X, _c1), _c1 = 3.")
        .unwrap();

    // Should return (1, 3) and (2, 3)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 3)));
    assert!(results.contains(&(2, 3)));
}
