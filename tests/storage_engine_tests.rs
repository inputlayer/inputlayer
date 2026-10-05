//! Storage engine integration tests: multi-KG ops, persistence, concurrency.

use inputlayer::{statement::parse_rule_definition, Config, RuleCatalog, StorageEngine};
use tempfile::TempDir;

// Test Helpers
fn create_test_config(data_dir: std::path::PathBuf) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = data_dir;
    config.storage.performance.num_threads = 2; // Use 2 threads for tests
    config
}

fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let config = create_test_config(temp.path().to_path_buf());
    let storage = StorageEngine::new(config).expect("create storage engine");
    (storage, temp)
}

// Configuration Tests
#[test]
fn test_config_default() {
    let config = Config::default();
    assert_eq!(config.storage.default_knowledge_graph, "default");
    assert_eq!(config.storage.data_dir, std::path::PathBuf::from("./data"));
}

#[test]
fn test_config_thread_pool() {
    let config = Config::default();
    assert_eq!(config.storage.performance.num_threads, 0); // 0 = all CPUs
}

// Basic Storage Engine Tests
#[test]
fn test_storage_engine_creation() {
    let (storage, _temp) = create_test_storage();

    // Should have default knowledge graph
    let knowledge_graphs = storage.list_knowledge_graphs();
    assert!(knowledge_graphs.contains(&"default".to_string()));

    // Should be using default knowledge graph
    assert_eq!(storage.current_knowledge_graph(), Some("default"));
}

#[test]
fn test_create_multiple_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("kg1").unwrap();
    storage.create_knowledge_graph("kg2").unwrap();
    storage.create_knowledge_graph("kg3").unwrap();

    let knowledge_graphs = storage.list_knowledge_graphs();
    assert_eq!(knowledge_graphs.len(), 4); // default + 3 new
    assert!(knowledge_graphs.contains(&"kg1".to_string()));
    assert!(knowledge_graphs.contains(&"kg2".to_string()));
    assert!(knowledge_graphs.contains(&"kg3".to_string()));
}

#[test]
fn test_knowledge_graph_already_exists_error() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    let result = storage.create_knowledge_graph("test");

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[test]
fn test_use_nonexistent_knowledge_graph() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.use_knowledge_graph("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_drop_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("temp_kg").unwrap();
    assert!(storage
        .list_knowledge_graphs()
        .contains(&"temp_kg".to_string()));

    storage.drop_knowledge_graph("temp_kg").unwrap();
    assert!(!storage
        .list_knowledge_graphs()
        .contains(&"temp_kg".to_string()));
}

#[test]
fn test_cannot_drop_default_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    let result = storage.drop_knowledge_graph("default");
    assert!(result.is_err());
}

#[test]
fn test_cannot_drop_current_knowledge_graph() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    let result = storage.drop_knowledge_graph("test");
    assert!(result.is_err());
}

// Data Operation Tests
#[test]
fn test_insert_and_query() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_kg").unwrap();
    storage.use_knowledge_graph("test_kg").unwrap();

    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    let results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(2, 3)));
    assert!(results.contains(&(3, 4)));
}

#[test]
fn test_insert_multiple_relations() {
    let (mut storage, _temp) = create_test_storage();

    storage.use_knowledge_graph("default").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();
    storage.insert("person", vec![(1, 100), (2, 200)]).unwrap();

    let edge_results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    let person_results = storage.execute_query("result(X,Y) <- person(X,Y)").unwrap();

    assert_eq!(edge_results.len(), 2);
    assert_eq!(person_results.len(), 2);
}

#[test]
fn test_delete_tuples() {
    let (mut storage, _temp) = create_test_storage();

    storage.use_knowledge_graph("default").unwrap();
    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    storage.delete("edge", vec![(2, 3)]).unwrap();

    let results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    assert_eq!(results.len(), 2);
    assert!(!results.contains(&(2, 3)));
}

#[test]
fn test_knowledge_graph_isolation() {
    let (mut storage, _temp) = create_test_storage();

    // Insert data in kg1
    storage.create_knowledge_graph("kg1").unwrap();
    storage.use_knowledge_graph("kg1").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Check kg2 doesn't see kg1's data
    storage.create_knowledge_graph("kg2").unwrap();
    storage.use_knowledge_graph("kg2").unwrap();

    let results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    assert_eq!(results.len(), 0); // No data in kg2
}

// Explicit API Tests
#[test]
fn test_insert_into_specific_knowledge_graph() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("kg1").unwrap();
    storage.create_knowledge_graph("kg2").unwrap();

    // Insert without switching knowledge graphs
    storage.use_knowledge_graph("default").unwrap();
    storage.insert_into("kg1", "edge", vec![(1, 2)]).unwrap();
    storage.insert_into("kg2", "edge", vec![(3, 4)]).unwrap();

    // Verify data in correct knowledge graphs
    let kg1_results = storage
        .execute_query_on("kg1", "result(X,Y) <- edge(X,Y)")
        .unwrap();
    let kg2_results = storage
        .execute_query_on("kg2", "result(X,Y) <- edge(X,Y)")
        .unwrap();

    assert_eq!(kg1_results, vec![(1, 2)]);
    assert_eq!(kg2_results, vec![(3, 4)]);

    // Current knowledge graph should still be default
    assert_eq!(storage.current_knowledge_graph(), Some("default"));
}

#[test]
fn test_execute_query_on_specific_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage
        .insert_into("test", "edge", vec![(1, 2), (2, 3)])
        .unwrap();

    // Query without switching knowledge graphs
    let results = storage
        .execute_query_on("test", "result(X,Y) <- edge(X,Y)")
        .unwrap();
    assert_eq!(results.len(), 2);

    // Current knowledge graph unchanged
    assert_eq!(storage.current_knowledge_graph(), Some("default"));
}

// Persistence Tests
#[test]
fn test_save_and_load_knowledge_graph() {
    let temp = TempDir::new().unwrap();

    // Create and populate knowledge graph
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("persist_test").unwrap();
        storage.use_knowledge_graph("persist_test").unwrap();
        storage
            .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
            .unwrap();
        storage.insert("person", vec![(1, 100), (2, 200)]).unwrap();

        storage.save_knowledge_graph("persist_test").unwrap();
    }

    // Load knowledge graph in new storage engine instance
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_knowledge_graph("persist_test").unwrap();

        let edge_results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
        let person_results = storage.execute_query("result(X,Y) <- person(X,Y)").unwrap();

        assert_eq!(edge_results.len(), 3);
        assert_eq!(person_results.len(), 2);
        assert!(edge_results.contains(&(1, 2)));
        assert!(person_results.contains(&(1, 100)));
    }
}

#[test]
fn test_save_all_knowledge_graphs() {
    let temp = TempDir::new().unwrap();

    // Create multiple knowledge graphs
    {
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("kg1").unwrap();
        storage.insert_into("kg1", "data", vec![(1, 1)]).unwrap();

        storage.create_knowledge_graph("kg2").unwrap();
        storage.insert_into("kg2", "data", vec![(2, 2)]).unwrap();

        storage.save_all().unwrap();
    }

    // Load and verify
    {
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let kg1_results = storage
            .execute_query_on("kg1", "result(X,Y) <- data(X,Y)")
            .unwrap();
        let kg2_results = storage
            .execute_query_on("kg2", "result(X,Y) <- data(X,Y)")
            .unwrap();

        assert_eq!(kg1_results, vec![(1, 1)]);
        assert_eq!(kg2_results, vec![(2, 2)]);
    }
}

#[test]
fn test_persistence_metadata() {
    let temp = TempDir::new().unwrap();

    {
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("test").unwrap();
        storage.insert_into("test", "edge", vec![(1, 2)]).unwrap();
        storage.save_all().unwrap();
    }

    // Check metadata files exist (DD-native persistence layout)
    // - metadata/knowledge_graphs.json: knowledge graph registry
    // - persist/shards/test%3Aedge.json: shard metadata (percent-encoded "test:edge")
    // - persist/batches/*.parquet: batch files (created on flush)
    // - persist/wal/: WAL directory
    assert!(temp.path().join("metadata/knowledge_graphs.json").exists());
    assert!(temp.path().join("persist/shards/test%3Aedge.json").exists());
    assert!(temp.path().join("persist/batches").exists());
}

// Parallel Execution Tests
#[test]
fn test_parallel_queries_on_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create multiple knowledge graphs with data
    for i in 1..=3 {
        let kg_name = format!("kg{i}");
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage
            .insert_into(&kg_name, "edge", vec![(i, i + 1)])
            .unwrap();
    }

    // Execute queries in parallel
    let queries = vec![
        ("kg1", "result(X,Y) <- edge(X,Y)"),
        ("kg2", "result(X,Y) <- edge(X,Y)"),
        ("kg3", "result(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 3);
    // Results may come back in any order due to parallel execution
    // Collect into HashMap for order-independent comparison
    let results_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(results_map.get("kg1"), Some(&vec![(1, 2)]));
    assert_eq!(results_map.get("kg2"), Some(&vec![(2, 3)]));
    assert_eq!(results_map.get("kg3"), Some(&vec![(3, 4)]));
}

#[test]
fn test_same_query_on_multiple_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge graphs with different data
    for i in 1..=3 {
        let kg_name = format!("kg{i}");
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage
            .insert_into(&kg_name, "edge", vec![(i * 10, i * 10 + 1)])
            .unwrap();
    }

    // Execute same query on all knowledge graphs
    let knowledge_graphs = vec!["kg1", "kg2", "kg3"];
    let results = storage
        .execute_query_on_multiple_knowledge_graphs(knowledge_graphs, "result(X,Y) <- edge(X,Y)")
        .unwrap();

    assert_eq!(results.len(), 3);
    // Results may come back in any order due to parallel execution
    // Collect into HashMap for order-independent comparison
    let results_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(results_map.get("kg1"), Some(&vec![(10, 11)]));
    assert_eq!(results_map.get("kg2"), Some(&vec![(20, 21)]));
    assert_eq!(results_map.get("kg3"), Some(&vec![(30, 31)]));
}

#[test]
fn test_worker_pool_configuration() {
    let (storage, _temp) = create_test_storage();

    // Should have configured worker pool
    let num_cpus = storage.num_cpus();
    assert!(num_cpus > 0); // At least 1 CPU
}

// Error Handling Tests
#[test]
fn test_query_nonexistent_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    let result = storage.execute_query_on("nonexistent", "result(X,Y) <- edge(X,Y)");
    assert!(result.is_err());
}

#[test]
fn test_insert_without_current_knowledge_graph() {
    let temp = TempDir::new().unwrap();
    let config = create_test_config(temp.path().to_path_buf());
    let storage = StorageEngine::new(config).unwrap();

    // Drop default knowledge graph and try to insert (should use current_knowledge_graph)
    // This should work because default is current
    let result = storage.insert("edge", vec![(1, 2)]);
    assert!(result.is_ok());
}

// Complex Scenario Tests
#[test]
fn test_multi_knowledge_graph_workflow() {
    let (mut storage, _temp) = create_test_storage();

    // Create staging and production knowledge graphs
    storage.create_knowledge_graph("staging").unwrap();
    storage.create_knowledge_graph("production").unwrap();

    // Add data to staging
    storage.use_knowledge_graph("staging").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Verify staging
    let staging_results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    assert_eq!(staging_results.len(), 2);

    // Add different data to production
    storage.use_knowledge_graph("production").unwrap();
    storage.insert("edge", vec![(10, 20), (20, 30)]).unwrap();

    // Verify production
    let prod_results = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    assert_eq!(prod_results.len(), 2);
    assert!(prod_results.contains(&(10, 20)));

    // Verify isolation
    storage.use_knowledge_graph("staging").unwrap();
    let staging_results2 = storage.execute_query("result(X,Y) <- edge(X,Y)").unwrap();
    assert!(!staging_results2.contains(&(10, 20))); // Production data not in staging
}

#[test]
fn test_persistence_with_updates() {
    let temp = TempDir::new().unwrap();

    // Initial save
    {
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        storage.create_knowledge_graph("test").unwrap();
        storage.insert_into("test", "edge", vec![(1, 2)]).unwrap();
        storage.save_knowledge_graph("test").unwrap();
    }

    // Load and update
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_knowledge_graph("test").unwrap();
        storage.insert("edge", vec![(2, 3), (3, 4)]).unwrap();
        storage.save_knowledge_graph("test").unwrap();
    }

    // Load and verify all data
    {
        let config = create_test_config(temp.path().to_path_buf());
        let storage = StorageEngine::new(config).unwrap();

        let results = storage
            .execute_query_on("test", "result(X,Y) <- edge(X,Y)")
            .unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&(1, 2)));
        assert!(results.contains(&(2, 3)));
        assert!(results.contains(&(3, 4)));
    }
}

// IQL Rule and Query Tests
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
    let rule_def = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
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
        .execute_query_with_rules("result(X, Y) <- path(X, Y)")
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
    let base_rule = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
    storage.register_rule(&base_rule).unwrap();

    let recursive_rule = parse_rule_definition("path(X, Z) <- edge(X, Y), path(Y, Z)").unwrap();
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

        let rule_def = parse_rule_definition("derived(X) <- base(X)").unwrap();
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
    let rule_def = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
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
    let rule_def = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
    storage.register_rule(&rule_def).unwrap();

    // Query that uses the rule
    let results = storage
        .execute_query_with_rules("result(X, Y) <- path(X, Y)")
        .unwrap();
    assert_eq!(results.len(), 2);
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

#[test]
fn test_multiple_views() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert data
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Register multiple views
    let view1 = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
    storage.register_rule(&view1).unwrap();

    let view2 = parse_rule_definition("reach(X) <- path(1, X)").unwrap();
    storage.register_rule(&view2).unwrap();

    let views = storage.list_rules().unwrap();
    assert_eq!(views.len(), 2);
}

// Query with Constants Tests (Phase 0 fix)
#[test]
fn test_query_with_constant_first_arg() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert parent relationships
    storage
        .insert("parent", vec![(1, 2), (1, 3), (2, 4)])
        .unwrap();

    // Query: ?parent(1, X). - use constant directly in atom
    let results = storage
        .execute_query_with_rules("__query__(1, X) <- parent(1, X)")
        .unwrap();

    // Should return (1, 2) and (1, 3) - children of parent 1
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(1, 3)));
}

#[test]
fn test_query_with_all_constants() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert data
    storage
        .insert("edge", vec![(1, 2), (2, 3), (3, 4)])
        .unwrap();

    // Query: ?edge(1, 2). - use constants directly in atom
    let results = storage
        .execute_query_with_rules("__query__(1, 2) <- edge(1, 2)")
        .unwrap();

    // Should return (1, 2) since that fact exists
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (1, 2));

    // Query: ?edge(1, 99). - fact doesn't exist
    let results = storage
        .execute_query_with_rules("__query__(1, 99) <- edge(1, 99)")
        .unwrap();

    // Should return empty since (1, 99) doesn't exist
    assert_eq!(results.len(), 0);
}

#[test]
fn test_query_with_constant_on_base_relation() {
    // Test query with constant directly on base relation (no view)
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert edges
    storage
        .insert("edge", vec![(1, 2), (1, 3), (2, 4)])
        .unwrap();

    // Query: ?edge(1, X). - use constant directly in atom
    let results = storage
        .execute_query_with_rules("__query__(1, X) <- edge(1, X)")
        .unwrap();

    // From 1, direct edges are: (1,2), (1,3)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(1, 3)));
}

#[test]
fn test_query_constant_second_arg() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage.use_knowledge_graph("test").unwrap();

    // Insert edges
    storage
        .insert("edge", vec![(1, 3), (2, 3), (4, 5)])
        .unwrap();

    // Query: ?edge(X, 3). - use constant directly in atom
    let results = storage
        .execute_query_with_rules("__query__(X, 3) <- edge(X, 3)")
        .unwrap();

    // Should return (1, 3) and (2, 3)
    assert_eq!(results.len(), 2);
    assert!(results.contains(&(1, 3)));
    assert!(results.contains(&(2, 3)));
}
