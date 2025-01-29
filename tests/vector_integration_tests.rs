//! Vector Integration Tests
//!
//! Comprehensive tests for vector operations including:
//! - Single scan + vector function (basic operations)
//! - Pairwise similarity (Cartesian product + vector functions)
//! - Edge cases (empty, single element, high dimensional)
//! - Rules with vector functions

use inputlayer::{Config, StorageEngine, Tuple, Value};
use std::sync::Arc;
use tempfile::TempDir;

/// Create a test storage engine with temp directory
fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.num_threads = 2;
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

/// Helper to create a vector value
fn vec_val(v: Vec<f32>) -> Value {
    Value::Vector(Arc::new(v))
}

/// Helper to create an (id, vector) tuple
fn id_vec_tuple(id: i64, v: Vec<f32>) -> Tuple {
    Tuple::new(vec![Value::Int64(id), vec_val(v)])
}

// Basic Vector Function Tests (Single Scan)
#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_euclidean_distance_basic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_vec").unwrap();
    storage.use_knowledge_graph("test_vec").unwrap();

    // Insert a single vector with known values
    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, vec![3.0, 4.0])])
        .unwrap();

    // Euclidean distance from origin should be 5.0 (3-4-5 triangle)
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- embedding(Id, V), D = euclidean(V, [0.0, 0.0]).",
        )
        .unwrap();

    assert!(
        !results.is_empty(),
        "Should have results for euclidean distance"
    );
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_cosine_similarity_basic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_cosine").unwrap();
    storage.use_knowledge_graph("test_cosine").unwrap();

    // Orthogonal vectors
    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, vec![1.0, 0.0, 0.0])])
        .unwrap();

    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- embedding(Id, V), D = cosine(V, [0.0, 1.0, 0.0]).",
        )
        .unwrap();

    assert!(
        !results.is_empty(),
        "Should have results for cosine similarity"
    );
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_dot_product_basic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_dot").unwrap();
    storage.use_knowledge_graph("test_dot").unwrap();

    // Dot product of [1,2,3] and [4,5,6] = 1*4 + 2*5 + 3*6 = 32
    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, vec![1.0, 2.0, 3.0])])
        .unwrap();

    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- embedding(Id, V), D = dot(V, [4.0, 5.0, 6.0]).",
        )
        .unwrap();

    assert!(!results.is_empty(), "Should have results for dot product");
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_manhattan_distance_basic() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_manhattan").unwrap();
    storage.use_knowledge_graph("test_manhattan").unwrap();

    // Manhattan distance: |1-0| + |2-0| + |3-0| = 6
    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, vec![1.0, 2.0, 3.0])])
        .unwrap();

    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- embedding(Id, V), D = manhattan(V, [0.0, 0.0, 0.0]).",
        )
        .unwrap();

    assert!(
        !results.is_empty(),
        "Should have results for manhattan distance"
    );
}

// Pairwise Similarity Tests (Cartesian Product)
#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
