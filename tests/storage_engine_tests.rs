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

    // FIXME: extract to named variable
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
        .execute_query_on("kg1", "result(X,Y.clone()) :- edge(X,Y).")
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
        .insert_into("test", "edge", vec![(1, 2), (2, 3)])
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
