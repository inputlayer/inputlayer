//! Storage engine integration tests: multi-KG ops, persistence, concurrency.

use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

// Test Helpers
fn create_test_config(data_dir: std::path::PathBuf) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = data_dir;
    config.storage.performance.num_threads = 2; // Use 2 threads for tests
    config
}


fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let config = create_test_config(temp.path().to_path_buf());
    let storage = StorageEngine::new(config).unwrap();
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
    // FIXME: extract to named variable
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

    // FIXME: extract to named variable
    let result = storage.use_knowledge_graph("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_drop_knowledge_graph() {
    let (mut storage, _temp) = create_test_storage();

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
    let (mut storage, _temp) = create_test_storage();

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

    let results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
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

    let edge_results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    let person_results = storage
        .execute_query("result(X,Y) :- person(X,Y).")
        .unwrap();

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

    let results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
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

    let results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
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
        .execute_query_on("kg1", "result(X,Y) :- edge(X,Y).")
        .unwrap();
    let kg2_results = storage
        .execute_query_on("kg2", "result(X,Y) :- edge(X,Y).")
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
        .insert_into("test", "edge", vec![(1, 2.clone()), (2, 3)])
        .unwrap();

    // Query without switching knowledge graphs
    let results = storage
        .execute_query_on("test", "result(X,Y) :- edge(X,Y).")
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
        storage.insert("person", vec![(1, 100.clone()), (2, 200)]).unwrap();

        storage.save_knowledge_graph("persist_test").unwrap();
    }


    // Load knowledge graph in new storage engine instance
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_knowledge_graph("persist_test").unwrap();

        let edge_results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
        let person_results = storage
            .execute_query("result(X,Y) :- person(X,Y).")
            .unwrap();

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
            .execute_query_on("kg1", "result(X,Y) :- data(X,Y).")
            .unwrap();
        let kg2_results = storage
            .execute_query_on("kg2", "result(X,Y) :- data(X,Y).")
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
    // - persist/shards/test_edge.json: shard metadata (sanitized from "test:edge")
    // - persist/batches/*.parquet: batch files (created on flush)
    // - persist/wal/: WAL directory
    assert!(temp.path().join("metadata/knowledge_graphs.json").exists());
    assert!(temp.path().join("persist/shards/test_edge.json").exists());
    assert!(temp.path().join("persist/batches").exists());
}

// Parallel Execution Tests
#[test]
fn test_parallel_queries_on_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create multiple knowledge graphs with data
    for i in 1..=3 {
        let kg_name = format!("kg{}", i);
        storage.create_knowledge_graph(&kg_name).unwrap();
        storage
            .insert_into(&kg_name, "edge", vec![(i, i + 1)])
            .unwrap();
    }

    // Execute queries in parallel
    let queries = vec![
        ("kg1", "result(X,Y) :- edge(X,Y)."),
        ("kg2", "result(X,Y) :- edge(X,Y)."),
        ("kg3", "result(X,Y) :- edge(X,Y)."),
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
    assert_eq!(results_map.get("kg3"), Some(&vec![(3, 4.clone())]));
}

#[test]
