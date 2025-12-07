//! Comprehensive Storage Engine Integration Tests
//!
//! Tests for:
//! - Multi-database operations
//! - Persistence and recovery
//! - Parallel query execution
//! - Configuration loading
//! - Error handling
//! - Thread safety

use datalog_engine::{Config, StorageEngine};
use std::fs;
use tempfile::TempDir;

// ============================================================================
// Test Helpers
// ============================================================================

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

// ============================================================================
// Configuration Tests
// ============================================================================

#[test]
fn test_config_default() {
    let config = Config::default();
    assert_eq!(config.storage.default_database, "default");
    assert_eq!(config.storage.data_dir, std::path::PathBuf::from("./data"));
}

#[test]
fn test_config_thread_pool() {
    let config = Config::default();
    assert_eq!(config.storage.performance.num_threads, 0); // 0 = all CPUs
}

// ============================================================================
// Basic Storage Engine Tests
// ============================================================================

#[test]
fn test_storage_engine_creation() {
    let (storage, _temp) = create_test_storage();

    // Should have default database
    let databases = storage.list_databases();
    assert!(databases.contains(&"default".to_string()));

    // Should be using default database
    assert_eq!(storage.current_database(), Some("default"));
}

#[test]
fn test_create_multiple_databases() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("db1").unwrap();
    storage.create_database("db2").unwrap();
    storage.create_database("db3").unwrap();

    let databases = storage.list_databases();
    assert_eq!(databases.len(), 4); // default + 3 new
    assert!(databases.contains(&"db1".to_string()));
    assert!(databases.contains(&"db2".to_string()));
    assert!(databases.contains(&"db3".to_string()));
}

#[test]
fn test_database_already_exists_error() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    let result = storage.create_database("test");

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("already exists"));
}

#[test]
fn test_use_nonexistent_database() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.use_database("nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_drop_database() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("temp_db").unwrap();
    assert!(storage.list_databases().contains(&"temp_db".to_string()));

    storage.drop_database("temp_db").unwrap();
    assert!(!storage.list_databases().contains(&"temp_db".to_string()));
}

#[test]
fn test_cannot_drop_default_database() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.drop_database("default");
    assert!(result.is_err());
}

#[test]
fn test_cannot_drop_current_database() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.use_database("test").unwrap();

    let result = storage.drop_database("test");
    assert!(result.is_err());
}

// ============================================================================
// Data Operation Tests
// ============================================================================

#[test]
fn test_insert_and_query() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test_db").unwrap();
    storage.use_database("test_db").unwrap();

    storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();

    let results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.contains(&(1, 2)));
    assert!(results.contains(&(2, 3)));
    assert!(results.contains(&(3, 4)));
}

#[test]
fn test_insert_multiple_relations() {
    let (mut storage, _temp) = create_test_storage();

    storage.use_database("default").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();
    storage.insert("person", vec![(1, 100), (2, 200)]).unwrap();

    let edge_results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    let person_results = storage.execute_query("result(X,Y) :- person(X,Y).").unwrap();

    assert_eq!(edge_results.len(), 2);
    assert_eq!(person_results.len(), 2);
}

#[test]
fn test_delete_tuples() {
    let (mut storage, _temp) = create_test_storage();

    storage.use_database("default").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();

    storage.delete("edge", vec![(2, 3)]).unwrap();

    let results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(results.len(), 2);
    assert!(!results.contains(&(2, 3)));
}

#[test]
fn test_database_isolation() {
    let (mut storage, _temp) = create_test_storage();

    // Insert data in db1
    storage.create_database("db1").unwrap();
    storage.use_database("db1").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Check db2 doesn't see db1's data
    storage.create_database("db2").unwrap();
    storage.use_database("db2").unwrap();

    let results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(results.len(), 0); // No data in db2
}

// ============================================================================
// Explicit API Tests
// ============================================================================

#[test]
fn test_insert_into_specific_database() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("db1").unwrap();
    storage.create_database("db2").unwrap();

    // Insert without switching databases
    storage.use_database("default").unwrap();
    storage.insert_into("db1", "edge", vec![(1, 2)]).unwrap();
    storage.insert_into("db2", "edge", vec![(3, 4)]).unwrap();

    // Verify data in correct databases
    let db1_results = storage.execute_query_on("db1", "result(X,Y) :- edge(X,Y).").unwrap();
    let db2_results = storage.execute_query_on("db2", "result(X,Y) :- edge(X,Y).").unwrap();

    assert_eq!(db1_results, vec![(1, 2)]);
    assert_eq!(db2_results, vec![(3, 4)]);

    // Current database should still be default
    assert_eq!(storage.current_database(), Some("default"));
}

#[test]
fn test_execute_query_on_specific_database() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.insert_into("test", "edge", vec![(1, 2), (2, 3)]).unwrap();

    // Query without switching databases
    let results = storage.execute_query_on("test", "result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(results.len(), 2);

    // Current database unchanged
    assert_eq!(storage.current_database(), Some("default"));
}

// ============================================================================
// Persistence Tests
// ============================================================================

#[test]
fn test_save_and_load_database() {
    let temp = TempDir::new().unwrap();

    // Create and populate database
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("persist_test").unwrap();
        storage.use_database("persist_test").unwrap();
        storage.insert("edge", vec![(1, 2), (2, 3), (3, 4)]).unwrap();
        storage.insert("person", vec![(1, 100), (2, 200)]).unwrap();

        storage.save_database("persist_test").unwrap();
    }

    // Load database in new storage engine instance
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_database("persist_test").unwrap();

        let edge_results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
        let person_results = storage.execute_query("result(X,Y) :- person(X,Y).").unwrap();

        assert_eq!(edge_results.len(), 3);
        assert_eq!(person_results.len(), 2);
        assert!(edge_results.contains(&(1, 2)));
        assert!(person_results.contains(&(1, 100)));
    }
}

#[test]
fn test_save_all_databases() {
    let temp = TempDir::new().unwrap();

    // Create multiple databases
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("db1").unwrap();
        storage.insert_into("db1", "data", vec![(1, 1)]).unwrap();

        storage.create_database("db2").unwrap();
        storage.insert_into("db2", "data", vec![(2, 2)]).unwrap();

        storage.save_all().unwrap();
    }

    // Load and verify
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        let db1_results = storage.execute_query_on("db1", "result(X,Y) :- data(X,Y).").unwrap();
        let db2_results = storage.execute_query_on("db2", "result(X,Y) :- data(X,Y).").unwrap();

        assert_eq!(db1_results, vec![(1, 1)]);
        assert_eq!(db2_results, vec![(2, 2)]);
    }
}

#[test]
fn test_persistence_metadata() {
    let temp = TempDir::new().unwrap();

    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("test").unwrap();
        storage.insert_into("test", "edge", vec![(1, 2)]).unwrap();
        storage.save_all().unwrap();
    }

    // Check metadata files exist (DD-native persistence layout)
    // - metadata/databases.json: database registry
    // - persist/shards/test_edge.json: shard metadata (sanitized from "test:edge")
    // - persist/batches/*.parquet: batch files (created on flush)
    // - persist/wal/: WAL directory
    assert!(temp.path().join("metadata/databases.json").exists());
    assert!(temp.path().join("persist/shards/test_edge.json").exists());
    assert!(temp.path().join("persist/batches").exists());
}

// ============================================================================
// Parallel Execution Tests
// ============================================================================

#[test]
fn test_parallel_queries_on_databases() {
    let (mut storage, _temp) = create_test_storage();

    // Create multiple databases with data
    for i in 1..=3 {
        let db_name = format!("db{}", i);
        storage.create_database(&db_name).unwrap();
        storage.insert_into(&db_name, "edge", vec![(i, i + 1)]).unwrap();
    }

    // Execute queries in parallel
    let queries = vec![
        ("db1", "result(X,Y) :- edge(X,Y)."),
        ("db2", "result(X,Y) :- edge(X,Y)."),
        ("db3", "result(X,Y) :- edge(X,Y)."),
    ];

    let results = storage.execute_parallel_queries_on_databases(queries).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, vec![(1, 2)]);
    assert_eq!(results[1].1, vec![(2, 3)]);
    assert_eq!(results[2].1, vec![(3, 4)]);
}

#[test]
fn test_same_query_on_multiple_databases() {
    let (mut storage, _temp) = create_test_storage();

    // Create databases with different data
    for i in 1..=3 {
        let db_name = format!("db{}", i);
        storage.create_database(&db_name).unwrap();
        storage.insert_into(&db_name, "edge", vec![(i * 10, i * 10 + 1)]).unwrap();
    }

    // Execute same query on all databases
    let databases = vec!["db1", "db2", "db3"];
    let results = storage.execute_query_on_multiple_databases(
        databases,
        "result(X,Y) :- edge(X,Y)."
    ).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].1, vec![(10, 11)]);
    assert_eq!(results[1].1, vec![(20, 21)]);
    assert_eq!(results[2].1, vec![(30, 31)]);
}

#[test]
fn test_multiple_queries_on_same_database() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_database("test").unwrap();
    storage.insert_into("test", "edge", vec![
        (1, 2), (2, 3), (3, 4), (4, 5), (5, 6)
    ]).unwrap();

    // Execute multiple queries in parallel
    let queries = vec![
        "q1(X,Y) :- edge(X,Y).",
        "q2(X,Y) :- edge(X,Y), X > 2.",
        "q3(X,Y) :- edge(X,Y), X < 4.",
    ];

    let results = storage.execute_parallel_queries_on_database("test", queries).unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].len(), 5); // All edges
    assert_eq!(results[1].len(), 3); // x > 2
    assert_eq!(results[2].len(), 3); // x < 4
}

#[test]
fn test_worker_pool_configuration() {
    let (storage, _temp) = create_test_storage();

    // Should have configured worker pool
    let num_cpus = storage.num_cpus();
    assert!(num_cpus > 0); // At least 1 CPU
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_query_nonexistent_database() {
    let (mut storage, _temp) = create_test_storage();

    let result = storage.execute_query_on("nonexistent", "result(X,Y) :- edge(X,Y).");
    assert!(result.is_err());
}

#[test]
fn test_insert_without_current_database() {
    let temp = TempDir::new().unwrap();
    let config = create_test_config(temp.path().to_path_buf());
    let mut storage = StorageEngine::new(config).unwrap();

    // Drop default database and try to insert (should use current_database)
    // This should work because default is current
    let result = storage.insert("edge", vec![(1, 2)]);
    assert!(result.is_ok());
}

// ============================================================================
// Complex Scenario Tests
// ============================================================================

#[test]
fn test_multi_database_workflow() {
    let (mut storage, _temp) = create_test_storage();

    // Create staging and production databases
    storage.create_database("staging").unwrap();
    storage.create_database("production").unwrap();

    // Add data to staging
    storage.use_database("staging").unwrap();
    storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();

    // Verify staging
    let staging_results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(staging_results.len(), 2);

    // Add different data to production
    storage.use_database("production").unwrap();
    storage.insert("edge", vec![(10, 20), (20, 30)]).unwrap();

    // Verify production
    let prod_results = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    assert_eq!(prod_results.len(), 2);
    assert!(prod_results.contains(&(10, 20)));

    // Verify isolation
    storage.use_database("staging").unwrap();
    let staging_results2 = storage.execute_query("result(X,Y) :- edge(X,Y).").unwrap();
    assert!(!staging_results2.contains(&(10, 20))); // Production data not in staging
}

#[test]
fn test_persistence_with_updates() {
    let temp = TempDir::new().unwrap();

    // Initial save
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.create_database("test").unwrap();
        storage.insert_into("test", "edge", vec![(1, 2)]).unwrap();
        storage.save_database("test").unwrap();
    }

    // Load and update
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        storage.use_database("test").unwrap();
        storage.insert("edge", vec![(2, 3), (3, 4)]).unwrap();
        storage.save_database("test").unwrap();
    }

    // Load and verify all data
    {
        let config = create_test_config(temp.path().to_path_buf());
        let mut storage = StorageEngine::new(config).unwrap();

        let results = storage.execute_query_on("test", "result(X,Y) :- edge(X,Y).").unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&(1, 2)));
        assert!(results.contains(&(2, 3)));
        assert!(results.contains(&(3, 4)));
    }
}
