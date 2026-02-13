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
        ("db1", "result(X,Y) <- edge(X,Y)"),
        ("db2", "result(X,Y) <- edge(X,Y)"),
        ("db3", "result(X,Y) <- edge(X,Y)"),
        ("db4", "result(X,Y) <- edge(X,Y)"),
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
    let query = "result(X,Y) <- edge(X,Y)";

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
fn test_multiple_queries_on_same_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage
        .insert_into(
            "test",
            "edge",
            vec![
                (1, 2),
                (2, 3),
                (3, 4),
                (4, 5),
                (5, 6),
                (6, 7),
                (7, 8),
                (8, 9),
                (9, 10),
            ],
        )
        .unwrap();

    // Execute multiple queries on the same knowledge_graph in parallel
    let queries = vec![
        "q1(X,Y) <- edge(X,Y)",               // All edges
        "q2(X,Y) <- edge(X,Y), X > 5",        // x > 5
        "q3(X,Y) <- edge(X,Y), X < 5",        // x < 5
        "q4(X,Y) <- edge(X,Y), X > 3, X < 7", // 3 < x < 7
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graph("test", queries)
        .unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].len(), 9); // All edges
    assert_eq!(results[1].len(), 4); // x > 5: 6,7,8,9
    assert_eq!(results[2].len(), 4); // x < 5: 1,2,3,4
    assert_eq!(results[3].len(), 3); // 3 < x < 7: 4,5,6
}

// KnowledgeGraph Isolation Tests (Concurrent Access)
#[test]
fn test_parallel_queries_maintain_knowledge_graph_isolation() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge_graphs with different data
    storage.create_knowledge_graph("db1").unwrap();
    storage
        .insert_into("db1", "edge", vec![(1, 2), (2, 3)])
        .unwrap();

    storage.create_knowledge_graph("db2").unwrap();
    storage
        .insert_into("db2", "edge", vec![(10, 20), (20, 30)])
        .unwrap();

    // Execute queries in parallel
    let queries = vec![
        ("db1", "result(X,Y) <- edge(X,Y)"),
        ("db2", "result(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    // Verify each knowledge_graph only sees its own data
    let db1_results = results.iter().find(|(db, _)| db == "db1").unwrap();
    let db2_results = results.iter().find(|(db, _)| db == "db2").unwrap();

    assert!(db1_results.1.contains(&(1, 2)));
    assert!(db1_results.1.contains(&(2, 3)));
    assert!(!db1_results.1.contains(&(10, 20))); // Should not see db2's data

    assert!(db2_results.1.contains(&(10, 20)));
    assert!(db2_results.1.contains(&(20, 30)));
    assert!(!db2_results.1.contains(&(1, 2))); // Should not see db1's data
}

#[test]
fn test_concurrent_queries_on_different_relations() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();
    storage
        .insert_into("test", "edge", vec![(1, 2), (2, 3)])
        .unwrap();
    storage
        .insert_into("test", "person", vec![(100, 200), (200, 300)])
        .unwrap();

    // Query different relations in parallel
    let queries = vec!["q1(X,Y) <- edge(X,Y)", "q2(X,Y) <- person(X,Y)"];

    let results = storage
        .execute_parallel_queries_on_knowledge_graph("test", queries)
        .unwrap();

    assert_eq!(results.len(), 2);
    // Both queries should return 2 results each
    assert_eq!(results[0].len(), 2);
    assert_eq!(results[1].len(), 2);

    // Results may come back in any order due to parallel execution
    // Find which result set contains edge data vs person data
    let (edge_results, person_results) = if results[0].contains(&(1, 2)) {
        (&results[0], &results[1])
    } else {
        (&results[1], &results[0])
    };

    // Verify edge results
    assert!(edge_results.contains(&(1, 2)));
    assert!(edge_results.contains(&(2, 3)));
    assert!(!edge_results.contains(&(100, 200))); // edge query shouldn't see person data

    // Verify person results
    assert!(person_results.contains(&(100, 200)));
    assert!(person_results.contains(&(200, 300)));
    assert!(!person_results.contains(&(1, 2))); // person query shouldn't see edge data
}

// Error Handling in Parallel Context
#[test]
fn test_parallel_queries_with_invalid_knowledge_graph() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("db1").unwrap();
    storage.insert_into("db1", "edge", vec![(1, 2)]).unwrap();

    // Mix valid and invalid knowledge_graphs
    let queries = vec![
        ("db1", "result(X,Y) <- edge(X,Y)"),
        ("nonexistent", "result(X,Y) <- edge(X,Y)"),
    ];

    let result = storage.execute_parallel_queries_on_knowledge_graphs(queries);

    // Should return error because one knowledge_graph doesn't exist
    assert!(result.is_err());
}

#[test]
fn test_parallel_queries_handle_empty_results() {
    let (storage, _temp) = create_test_storage();

    // Create knowledge_graphs with no data
    for i in 1..=3 {
        let db_name = format!("empty_db{}", i);
        storage.create_knowledge_graph(&db_name).unwrap();
    }

    let queries = vec![
        ("empty_db1", "result(X,Y) <- edge(X,Y)"),
        ("empty_db2", "result(X,Y) <- edge(X,Y)"),
        ("empty_db3", "result(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    // Should succeed but return empty results
    assert_eq!(results.len(), 3);
    for (_, result) in results {
        assert_eq!(result.len(), 0);
    }
}

// Performance and Scalability Tests
#[test]
fn test_parallel_execution_with_many_knowledge_graphs() {
    let (storage, _temp) = create_test_storage();

    // Create 10 knowledge_graphs
    let num_knowledge_graphs = 10;
    for i in 1..=num_knowledge_graphs {
        let db_name = format!("db{}", i);
        storage.create_knowledge_graph(&db_name).unwrap();
        storage
            .insert_into(&db_name, "data", vec![(i, i * 100)])
            .unwrap();
    }

    // Execute queries on all knowledge_graphs in parallel
    let queries: Vec<(&str, &str)> = (1..=num_knowledge_graphs)
        .map(|i| (format!("db{}", i), "result(X,Y) <- data(X,Y)"))
        .map(|(db, q)| (Box::leak(db.into_boxed_str()) as &str, q))
        .collect();

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), num_knowledge_graphs as usize);

    // Verify all results are correct
    for (db_name, result) in &results {
        assert_eq!(result.len(), 1);
        let db_num = db_name
            .chars()
            .skip(2)
            .collect::<String>()
            .parse::<i32>()
            .unwrap();
        assert_eq!(result[0], (db_num, db_num * 100));
    }
}

#[test]
#[ignore] // Constraint syntax (X > 5, etc.) no longer supported - Constraint type removed
fn test_parallel_execution_with_complex_queries() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test").unwrap();

    // Insert more data for complex queries
    let edges: Vec<(i32, i32)> = (0..20).map(|i| (i, i + 1)).collect();
    storage.insert_into("test", "edge", edges).unwrap();

    // Execute multiple complex queries in parallel
    let queries = vec![
        "q1(X,Y) <- edge(X,Y), X > 5",
        "q2(X,Y) <- edge(X,Y), X < 15",
        "q3(X,Y) <- edge(X,Y), X > 5, X < 15",
        "q4(X,Y) <- edge(X,Y), Y > 10",
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graph("test", queries)
        .unwrap();

    assert_eq!(results.len(), 4);
    assert!(results[0].len() > 0); // Should have results
    assert!(results[1].len() > 0);
    assert!(results[2].len() > 0);
    assert!(results[3].len() > 0);
}

// Thread Safety Tests
#[test]
fn test_parallel_queries_use_internal_thread_safety() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("shared_db").unwrap();
    storage
        .insert_into("shared_db", "edge", vec![(1, 2), (2, 3)])
        .unwrap();

    // Execute same query multiple times in parallel via the parallel API
    // This tests that the internal Arc<RwLock<KnowledgeGraph>> mechanism works
    let queries = vec![
        ("shared_db", "q1(X,Y) <- edge(X,Y)"),
        ("shared_db", "q2(X,Y) <- edge(X,Y)"),
        ("shared_db", "q3(X,Y) <- edge(X,Y)"),
        ("shared_db", "q4(X,Y) <- edge(X,Y)"),
    ];

    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    // Verify all queries got the same results (thread-safe access to shared knowledge_graph)
    assert_eq!(results.len(), 4);
    for (_, query_results) in &results {
        assert_eq!(query_results.len(), 2);
        assert!(query_results.contains(&(1, 2)));
        assert!(query_results.contains(&(2, 3)));
    }
}

#[test]
fn test_concurrent_queries_do_not_deadlock() {
    let (storage, _temp) = create_test_storage();

    // Create multiple knowledge_graphs
    for i in 1..=5 {
        let db_name = format!("db{}", i);
        storage.create_knowledge_graph(&db_name).unwrap();
        storage.insert_into(&db_name, "data", vec![(i, i)]).unwrap();
    }

    // Execute many parallel queries (more than thread pool size)
    let queries: Vec<(&str, &str)> = (1..=5)
        .flat_map(|i| {
            let db = format!("db{}", i);
            vec![
                (
                    Box::leak(db.clone().into_boxed_str()) as &str,
                    "q1(X,Y) <- data(X,Y)",
                ),
                (
                    Box::leak(db.into_boxed_str()) as &str,
                    "q2(X,Y) <- data(X,Y)",
                ),
            ]
        })
        .collect();

    // This should not deadlock
    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 10); // 5 knowledge_graphs × 2 queries
}

// Edge Cases
#[test]
fn test_parallel_query_with_empty_query_list() {
    let (storage, _temp) = create_test_storage();

    let queries: Vec<(&str, &str)> = vec![];
    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_parallel_query_with_single_query() {
    let (storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("solo").unwrap();
    storage.insert_into("solo", "edge", vec![(1, 2)]).unwrap();

    let queries = vec![("solo", "result(X,Y) <- edge(X,Y)")];
    let results = storage
        .execute_parallel_queries_on_knowledge_graphs(queries)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, vec![(1, 2)]);
}
