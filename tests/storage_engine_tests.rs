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
