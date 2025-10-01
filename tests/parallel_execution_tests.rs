//! Worker pool and parallel query execution tests.

use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

// Test Helpers
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 4; // Use 4 threads for tests
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

// Thread Pool Configuration Tests
#[test]
fn test_thread_pool_is_configured() {
    let (storage, _temp) = create_test_storage();

    // Should have worker pool configured
    let num_cpus = storage.num_cpus();
    assert!(num_cpus > 0, "Thread pool should have at least 1 thread");
}

#[test]
fn test_multiple_storage_engines_share_thread_pool() {
    let temp1 = TempDir::new().unwrap();
    let temp2 = TempDir::new().unwrap();

    let mut config1 = Config::default();
    config1.storage.data_dir = temp1.path().to_path_buf();
    config1.storage.performance.num_threads = 2;

    let mut config2 = Config::default();
    config2.storage.data_dir = temp2.path().to_path_buf();
    config2.storage.performance.num_threads = 4; // Different config, but global pool already initialized

    let storage1 = StorageEngine::new(config1).unwrap();
    let storage2 = StorageEngine::new(config2).unwrap();

    // Both should report same thread pool (global pool is shared)
    let cpus1 = storage1.num_cpus();
    let cpus2 = storage2.num_cpus();

    assert_eq!(
        cpus1, cpus2,
        "All storage engines share the same global thread pool"
    );
}

// Parallel Query Execution Tests
#[test]
fn test_execute_queries_on_multiple_knowledge_graphs_concurrently() {
    let (storage, _temp) = create_test_storage();

    // Create 4 knowledge_graphs with different data
    for i in 1..=4 {
        let db_name = format!("db{}", i);
        storage.create_knowledge_graph(&db_name).unwrap();
        storage
            .insert_into(&db_name, "edge", vec![(i, i * 10)])
            .unwrap();
    }

    // Execute queries on all knowledge_graphs in parallel
    let queries = vec![
        ("db1", "result(X,Y) :- edge(X,Y)."),
        ("db2", "result(X,Y) :- edge(X,Y)."),
        ("db3", "result(X,Y) :- edge(X,Y)."),
        ("db4", "result(X,Y) :- edge(X,Y)."),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 4);

    // Verify each knowledge_graph returned its own data
    for (db_name, result) in &results {
        assert_eq!(result.len(), 1);
        let db_num = db_name.chars().last().unwrap().to_digit(10).unwrap() as i32;
        assert_eq!(result[0], (db_num, db_num * 10));
    }
}

#[test]
fn test_same_query_on_multiple_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge_graphs with increasing amounts of data
    for i in 1..=3 {
        let db_name = format!("db{}", i);
        storage.create_knowledge_graph(&db_name).unwrap();

        let edges: Vec<(i32, i32)> = (0..i).map(|j| (j, j + 1)).collect();
        storage.insert_into(&db_name, "edge", edges).unwrap();
    }

    // Execute same query on all knowledge_graphs in parallel
    let knowledge_graphs = vec!["db1", "db2", "db3"];
    let query = "result(X,Y) :- edge(X,Y).";

    let results = storage
        .execute_query_on_multiple_knowledge_graphs(knowledge_graphs, query)
        .unwrap();

    assert_eq!(results.len(), 3);
    // Results may come back in any order due to parallel execution
    // Use HashMap for order-independent comparison
    let results_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(results_map.get("db1").map(|v| v.len()), Some(1)); // db1 has 1 edge
    assert_eq!(results_map.get("db2").map(|v| v.len()), Some(2)); // db2 has 2 edges
    assert_eq!(results_map.get("db3").map(|v| v.len()), Some(3)); // db3 has 3 edges
}

#[test]
#[ignore] // Constraint syntax (X > 5, etc.) no longer supported - Constraint type removed
