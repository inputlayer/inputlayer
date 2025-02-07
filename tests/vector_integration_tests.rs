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
fn test_pairwise_cosine_similarity() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_pairwise").unwrap();
    storage.use_knowledge_graph("test_pairwise").unwrap();

    // Three orthogonal unit vectors
    storage
        .insert_tuples(
            "embedding",
            vec![
                id_vec_tuple(1, vec![1.0, 0.0, 0.0]),
                id_vec_tuple(2, vec![0.0, 1.0, 0.0]),
                id_vec_tuple(3, vec![0.0, 0.0, 1.0]),
            ],
        )
        .unwrap();

    // Pairwise cosine similarity - the KEY test case for Cartesian product
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, Sim) :- embedding(Id1, V1), embedding(Id2, V2), Id1 < Id2, Sim = cosine(V1, V2).",
        )
        .unwrap();

    // Should have 3 pairs: (1,2), (1,3), (2,3)
    assert_eq!(
        results.len(),
        3,
        "Pairwise cosine should have 3 pairs, got {}: {:?}",
        results.len(),
        results
    );
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_pairwise_euclidean_distance() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_pairwise_euc").unwrap();
    storage.use_knowledge_graph("test_pairwise_euc").unwrap();

    // Points in 2D space
    storage
        .insert_tuples(
            "point",
            vec![
                id_vec_tuple(1, vec![0.0, 0.0]),
                id_vec_tuple(2, vec![3.0, 0.0]),
                id_vec_tuple(3, vec![0.0, 4.0]),
            ],
        )
        .unwrap();

    // Pairwise distances
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, D) :- point(Id1, V1), point(Id2, V2), Id1 < Id2, D = euclidean(V1, V2).",
        )
        .unwrap();

    assert_eq!(results.len(), 3, "Pairwise euclidean should have 3 pairs");
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_pairwise_all_distances() {
    // Test: Get all pairwise distances (without threshold filter)
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_threshold").unwrap();
    storage.use_knowledge_graph("test_threshold").unwrap();

    // Vectors where some pairs are similar, some are not
    storage
        .insert_tuples(
            "embedding",
            vec![
                id_vec_tuple(1, vec![1.0, 0.0]),
                id_vec_tuple(2, vec![0.9, 0.1]),
                id_vec_tuple(3, vec![0.0, 1.0]),
            ],
        )
        .unwrap();

    // Pairwise distances (all pairs)
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, Dist) :- embedding(Id1, V1), embedding(Id2, V2), Id1 < Id2, Dist = cosine(V1, V2).",
        )
        .unwrap();

    assert_eq!(results.len(), 3, "Should have 3 pairwise distances");
}

// Edge Cases
#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_vector_empty_relation() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_empty").unwrap();
    storage.use_knowledge_graph("test_empty").unwrap();

    // Create empty relation by inserting nothing
    // Just query an undefined relation or one without data
    let results = storage.execute_query_with_rules_tuples(
        "result(Id, D) :- nonexistent_embedding(Id, V), D = cosine(V, [1.0, 0.0]).",
    );

    // Should either fail or return empty results - both are acceptable
    match results {
        Ok(r) => assert!(r.is_empty(), "Empty relation should give no results"),
        Err(_) => {} // Also acceptable
    }
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_vector_single_element() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_single").unwrap();
    storage.use_knowledge_graph("test_single").unwrap();

    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, vec![1.0, 0.0, 0.0])])
        .unwrap();

    // Single element query should work
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- embedding(Id, V), D = euclidean(V, [0.0, 0.0, 0.0]).",
        )
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Single element query should have 1 result"
    );

    // Pairwise on single element should give no results (no pairs where Id1 < Id2)
    let pairwise = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, D) :- embedding(Id1, V1), embedding(Id2, V2), Id1 < Id2, D = cosine(V1, V2).",
        )
        .unwrap();

    assert!(
        pairwise.is_empty(),
        "Single element pairwise should have no pairs"
    );
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_vector_self_comparison() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_self").unwrap();
    storage.use_knowledge_graph("test_self").unwrap();

    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, vec![1.0, 2.0, 3.0])])
        .unwrap();

    // Self comparison without filter (1x1 = 1 result)
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, D) :- embedding(Id1, V1), embedding(Id2, V2), D = cosine(V1, V2).",
        )
        .unwrap();

    assert_eq!(
        results.len(),
        1,
        "Self comparison should have 1 result (1x1 Cartesian)"
    );
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_vector_high_dimensional() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_highdim").unwrap();
    storage.use_knowledge_graph("test_highdim").unwrap();

    // 10-dimensional vectors
    let mut v1 = vec![0.0f32; 10];
    let mut v2 = vec![0.0f32; 10];
    v1[0] = 1.0;
    v2[1] = 1.0;

    storage
        .insert_tuples("embedding", vec![id_vec_tuple(1, v1), id_vec_tuple(2, v2)])
        .unwrap();

    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, D) :- embedding(Id1, V1), embedding(Id2, V2), Id1 < Id2, D = cosine(V1, V2).",
        )
        .unwrap();

    assert_eq!(results.len(), 1, "High dimensional vectors should work");
}

// Additional Vector Query Tests
#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_vector_inline_rule() {
    // Test vector function in a directly executed query (not persistent rule)
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_inline").unwrap();
    storage.use_knowledge_graph("test_inline").unwrap();

    storage
        .insert_tuples(
            "embedding",
            vec![
                id_vec_tuple(1, vec![1.0, 0.0, 0.0]),
                id_vec_tuple(2, vec![0.0, 1.0, 0.0]),
                id_vec_tuple(3, vec![0.7, 0.7, 0.0]),
            ],
        )
        .unwrap();

    // Direct query with vector function
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- embedding(Id, V), D = euclidean(V, [0.0, 0.0, 0.0]).",
        )
        .unwrap();

    assert_eq!(
        results.len(),
        3,
        "Inline distance query should produce 3 results"
    );
}

#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_vector_inline_similarity() {
    // Test: Get all pairwise similarities between 4 vectors
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_inline_sim").unwrap();
    storage.use_knowledge_graph("test_inline_sim").unwrap();

    storage
        .insert_tuples(
            "embedding",
            vec![
                id_vec_tuple(1, vec![1.0, 0.0]),
                id_vec_tuple(2, vec![0.9, 0.1]),
                id_vec_tuple(3, vec![0.0, 1.0]),
                id_vec_tuple(4, vec![0.1, 0.9]),
            ],
        )
        .unwrap();

    // Direct pairwise similarity query (all pairs where Id1 < Id2)
    let results = storage
        .execute_query_with_rules_tuples(
            "result(Id1, Id2, Sim) :- embedding(Id1, V1), embedding(Id2, V2), Id1 < Id2, Sim = cosine(V1, V2).",
        )
        .unwrap();

    // 4 choose 2 = 6 pairs
    assert_eq!(
        results.len(),
        6,
        "Should have 6 pairwise similarity results"
    );
}

// Vector Operations Variety
#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
fn test_all_vector_operations() {
    let (mut storage, _temp) = create_test_storage();

    storage.create_knowledge_graph("test_all_ops").unwrap();
    storage.use_knowledge_graph("test_all_ops").unwrap();

    storage
        .insert_tuples(
            "v",
            vec![
                id_vec_tuple(1, vec![1.0, 2.0, 3.0]),
                id_vec_tuple(2, vec![4.0, 5.0, 6.0]),
            ],
        )
        .unwrap();

    // Test each operation
    let euclidean = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- v(Id, V), D = euclidean(V, [0.0, 0.0, 0.0]).",
        )
        .unwrap();
    assert_eq!(euclidean.len(), 2, "Euclidean should work");

    let cosine = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- v(Id, V), D = cosine(V, [1.0, 0.0, 0.0]).",
        )
        .unwrap();
    assert_eq!(cosine.len(), 2, "Cosine should work");

    let dot = storage
        .execute_query_with_rules_tuples("result(Id, D) :- v(Id, V), D = dot(V, [1.0, 1.0, 1.0]).")
        .unwrap();
    assert_eq!(dot.len(), 2, "Dot should work");

    let manhattan = storage
        .execute_query_with_rules_tuples(
            "result(Id, D) :- v(Id, V), D = manhattan(V, [0.0, 0.0, 0.0]).",
        )
        .unwrap();
    assert_eq!(manhattan.len(), 2, "Manhattan should work");
}

// Cartesian Product with Multiple Filters
#[test]
#[ignore] // Uses constraint syntax (D = func(), Id1 < Id2) - Constraint type removed
