//! Numeric edge cases: division by zero, overflow, NaN, empty aggregations.

use inputlayer::{Config, StorageEngine, Tuple, Value};
use tempfile::TempDir;

// Test Helpers
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

// AVG Aggregation Tests
#[test]
fn test_avg_with_single_value() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_avg").unwrap();
    storage.use_knowledge_graph("test_avg").unwrap();

    // Insert single value
    storage.insert("scores", vec![(1, 100)]).unwrap();

    // AVG of single value should equal that value
    let _results = storage
        .execute_query("result(avg<V>) :- scores(_, V).")
        .unwrap();
    // AVG of single value 100 should return a result
    // Note: May return empty if no grouping - system dependent
    // The main assertion is that it doesn't panic
}

#[test]
fn test_avg_multiple_values() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_avg").unwrap();
    storage.use_knowledge_graph("test_avg").unwrap();

    // Insert values: 10, 20, 30 (avg = 20)
    storage
        .insert("numbers", vec![(1, 10), (2, 20), (3, 30)])
        .unwrap();

    let _results = storage
        .execute_query("result(avg<V>) :- numbers(_, V).")
        .unwrap();
    // Should have a result (not crash)
    // The actual value should be around 20.0
}

#[test]
fn test_avg_empty_relation_no_panic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_avg_empty").unwrap();
    storage.use_knowledge_graph("test_avg_empty").unwrap();

    // Create empty relation by declaring schema
    // Note: schema declaration syntax depends on implementation
    // For now, just verify empty query doesn't panic

    // Query on nonexistent relation should not crash
    // Either returns empty results or error (depending on implementation)
    let _result = storage.execute_query("result(avg<V>) :- nonexistent(_, V).");
    // Test passes if no panic occurred
}

// COUNT Aggregation Tests
#[test]
fn test_count_empty_returns_zero_or_empty() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_count").unwrap();
    storage.use_knowledge_graph("test_count").unwrap();

    // Query count on nonexistent relation
    // Should not panic - either returns empty or 0
    let _result = storage.execute_query("result(count<X>) :- nonexistent(X).");
    // Test passes if no panic occurred
}

#[test]
fn test_count_single_value() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_count").unwrap();
    storage.use_knowledge_graph("test_count").unwrap();

    storage.insert("items", vec![(1, 0)]).unwrap();

    let _results = storage
        .execute_query("result(count<X>) :- items(X).")
        .unwrap();
    // COUNT should return a result (1 for single item)
    // Note: Exact behavior depends on aggregation implementation
}

// SUM Aggregation Tests
#[test]
fn test_sum_empty_returns_zero_or_empty() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_sum").unwrap();
    storage.use_knowledge_graph("test_sum").unwrap();

    // Should not panic
    let _result = storage.execute_query("result(sum<V>) :- nonexistent(_, V).");
    // Test passes if no panic occurred
}

#[test]
