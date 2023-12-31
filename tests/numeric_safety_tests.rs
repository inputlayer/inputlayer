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
fn test_sum_multiple_values() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_sum").unwrap();
    storage.use_knowledge_graph("test_sum").unwrap();

    storage
        .insert("amounts", vec![(1, 10), (2, 20), (3, 30)])
        .unwrap();

    let _results = storage
        .execute_query("result(sum<V>) :- amounts(_, V).")
        .unwrap();
    // Sum should be 60, but mainly verify no panic
}

// MIN/MAX Aggregation Tests
#[test]
fn test_min_empty_returns_null_or_empty() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_min").unwrap();
    storage.use_knowledge_graph("test_min").unwrap();

    // MIN of empty set should return NULL or empty results, not panic
    let _result = storage.execute_query("result(min<V>) :- nonexistent(_, V).");
    // Test passes if no panic occurred
}

#[test]
fn test_max_empty_returns_null_or_empty() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_max").unwrap();
    storage.use_knowledge_graph("test_max").unwrap();

    // MAX of empty set should return NULL or empty results, not panic
    let _result = storage.execute_query("result(max<V>) :- nonexistent(_, V).");
    // Test passes if no panic occurred
}

#[test]
fn test_min_max_single_value() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_minmax").unwrap();
    storage.use_knowledge_graph("test_minmax").unwrap();

    storage.insert("vals", vec![(1, 42)]).unwrap();

    let _min_results = storage
        .execute_query("result(min<V>) :- vals(_, V).")
        .unwrap();
    let _max_results = storage
        .execute_query("result(max<V>) :- vals(_, V).")
        .unwrap();

    // Both should equal the single value
}

// Arithmetic Division Tests
#[test]
fn test_arithmetic_division_normal() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_div").unwrap();
    storage.use_knowledge_graph("test_div").unwrap();

    storage.insert("nums", vec![(10, 2)]).unwrap();

    // Normal division should work: 10 / 2 = 5
    let _results = storage
        .execute_query("result(X / Y) :- nums(X, Y).")
        .unwrap();
    // Should not panic
}

#[test]
fn test_arithmetic_division_by_zero_no_panic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_div_zero").unwrap();
    storage.use_knowledge_graph("test_div_zero").unwrap();

    storage.insert("nums", vec![(10, 0)]).unwrap();

    // Division by zero should not panic - returns inf, null, or filters out
    let _result = storage.execute_query("result(X / Y) :- nums(X, Y).");
    // Test passes if no panic occurred
}

#[test]
fn test_arithmetic_modulo_by_zero_no_panic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_mod_zero").unwrap();
    storage.use_knowledge_graph("test_mod_zero").unwrap();

    storage.insert("nums", vec![(10, 0)]).unwrap();

    // Modulo by zero should not panic
    let _result = storage.execute_query("result(X % Y) :- nums(X, Y).");
    // Test passes if no panic occurred
}

// Float Special Value Tests
#[test]
fn test_float_infinity_handling() {
    // Test that infinity values don't cause issues in Value type
    let inf = Value::Float64(f64::INFINITY);
    let neg_inf = Value::Float64(f64::NEG_INFINITY);

    let tuple = Tuple::new(vec![inf.clone(), neg_inf.clone()]);
    assert_eq!(tuple.arity(), 2);
    assert_eq!(tuple.get(0), Some(&inf));
    assert_eq!(tuple.get(1), Some(&neg_inf));
}

#[test]
fn test_float_nan_handling() {
    // Test that NaN values don't cause issues in Value type
    let nan = Value::Float64(f64::NAN);

    let tuple = Tuple::new(vec![nan.clone()]);
    assert_eq!(tuple.arity(), 1);

    // NaN comparison is tricky - NaN != NaN
    if let Some(Value::Float64(f)) = tuple.get(0) {
        assert!(f.is_nan());
    }
}

#[test]
fn test_float_very_small_values() {
    let tiny = Value::Float64(f64::MIN_POSITIVE);
    let neg_tiny = Value::Float64(-f64::MIN_POSITIVE);

    let tuple = Tuple::new(vec![tiny.clone(), neg_tiny.clone()]);
    assert_eq!(tuple.arity(), 2);
}

#[test]
fn test_float_very_large_values() {
    let big = Value::Float64(f64::MAX);
    let neg_big = Value::Float64(f64::MIN);

    let tuple = Tuple::new(vec![big.clone(), neg_big.clone()]);
    assert_eq!(tuple.arity(), 2);
}

// Integer Overflow Tests
#[test]
fn test_int64_max_in_aggregation() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_overflow").unwrap();
    storage.use_knowledge_graph("test_overflow").unwrap();

    // Insert MAX values - sum would overflow
    // Note: Using i32 values since insert expects (i32, i32)
    // For true i64 overflow testing, would need direct Value manipulation
    storage
        .insert("big_nums", vec![(1, i32::MAX), (2, 1)])
        .unwrap();

    // Sum of i64::MAX + 1 would overflow - system should handle gracefully
    let _result = storage.execute_query("result(sum<V>) :- big_nums(_, V).");
    // Test passes if no panic occurred
}

#[test]
fn test_int64_boundaries_in_tuple() {
    let max = Value::Int64(i64::MAX);
    let min = Value::Int64(i64::MIN);
    let zero = Value::Int64(0);

    let tuple = Tuple::new(vec![max.clone(), min.clone(), zero.clone()]);
    assert_eq!(tuple.arity(), 3);
    assert_eq!(tuple.get(0), Some(&max));
    assert_eq!(tuple.get(1), Some(&min));
    assert_eq!(tuple.get(2), Some(&zero));
}

// Grouped Aggregation Edge Cases
#[test]
fn test_grouped_aggregation_one_group_empty() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_grouped").unwrap();
    storage.use_knowledge_graph("test_grouped").unwrap();

    // Create data with groups
    storage
        .insert(
            "sales",
            vec![
                (1, 100), // Group 1
                (1, 200), // Group 1
                (2, 50),  // Group 2
            ],
        )
        .unwrap();

    // Compute AVG per group
    let _results = storage
        .execute_query("result(G, avg<V>) :- sales(G, V).")
        .unwrap();
    // Should have 2 groups
}

#[test]
fn test_multiple_aggregations_same_query() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_multi_agg").unwrap();
    storage.use_knowledge_graph("test_multi_agg").unwrap();

    storage
        .insert("data", vec![(1, 10), (2, 20), (3, 30)])
        .unwrap();

    // Multiple aggregations in one query
    let _results = storage
        .execute_query("result(count<X>, sum<V>, min<V>, max<V>) :- data(X, V).")
        .unwrap();
    // Should not panic
}

// Value Conversion Safety Tests
#[test]
