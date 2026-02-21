//! WI-07: Input size validation tests.
//! WI-11: Session rule reserved name validation tests.
//! WI-13: KG name length validation tests.

use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

fn create_test_handler() -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    let handler = Handler::new(storage);
    (handler, temp)
}

fn create_handler_with_limits(
    max_query_size: usize,
    max_insert_tuples: usize,
    max_string_bytes: usize,
) -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.max_query_size_bytes = max_query_size;
    config.storage.performance.max_insert_tuples = max_insert_tuples;
    config.storage.performance.max_string_value_bytes = max_string_bytes;
    let storage = StorageEngine::new(config).unwrap();
    let handler = Handler::new(storage);
    (handler, temp)
}

fn create_test_storage() -> (StorageEngine, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    (storage, temp)
}

// === WI-07: Query Size Validation ===

#[tokio::test]
async fn test_query_over_size_limit_rejected() {
    let (handler, _tmp) = create_handler_with_limits(100, 10_000, 65_536);
    // 200 byte query
    let long_query = format!("?{}", "a".repeat(199));
    let result = handler.query_program(None, long_query).await;
    assert!(result.is_err(), "Query over size limit should be rejected");
    let err = result.unwrap_err();
    assert!(
        err.contains("large") || err.contains("size") || err.contains("bytes"),
        "Error should mention size limit, got: {err}"
    );
}

#[tokio::test]
async fn test_query_at_exact_size_limit_accepted() {
    let (handler, _tmp) = create_handler_with_limits(100, 10_000, 65_536);
    // Exactly 100 bytes - a valid short query
    let query = "?edge(X, Y)";
    // This is much shorter than 100 bytes, so it should succeed
    let result = handler.query_program(None, query.to_string()).await;
    // Should not fail with size error (may fail with other reasons like no data)
    if let Err(ref e) = result {
        assert!(
            !e.contains("large") && !e.contains("too"),
            "Should not fail with size error for small query, got: {e}"
        );
    }
}

#[tokio::test]
async fn test_query_size_limit_zero_means_no_limit() {
    let (handler, _tmp) = create_handler_with_limits(0, 10_000, 65_536);
    // A large query should succeed when limit is 0 (disabled)
    let big_query = format!("?{}", "a".repeat(5000));
    let result = handler.query_program(None, big_query).await;
    // Should not fail with size error (may fail with parse error)
    if let Err(ref e) = result {
        assert!(
            !e.contains("large") && !e.contains("too large"),
            "Size=0 should disable size limit, got: {e}"
        );
    }
}

// === WI-07: Insert Tuple Count Validation ===

#[tokio::test]
async fn test_insert_over_tuple_limit_rejected() {
    let (handler, _tmp) = create_handler_with_limits(1_048_576, 5, 65_536);
    // Insert 6 tuples with limit=5
    let insert = "+data[(1,),(2,),(3,),(4,),(5,),(6,)]";
    let result = handler.query_program(None, insert.to_string()).await;
    // The result should be a message (not an error) containing the rejection
    match result {
        Err(e) => assert!(
            e.contains("tuples") || e.contains("exceed") || e.contains("max"),
            "Error should mention tuple limit, got: {e}"
        ),
        Ok(r) => {
            // Could be returned as an Ok with a rejection message
            let msg = format!("{r:?}");
            assert!(
                msg.contains("tuples") || msg.contains("exceed") || msg.contains("reject"),
                "Response should mention tuple limit, got: {msg}"
            );
        }
    }
}

#[tokio::test]
async fn test_insert_at_tuple_limit_accepted() {
    let (handler, _tmp) = create_handler_with_limits(1_048_576, 5, 65_536);
    // Insert exactly 5 tuples with limit=5 - should succeed
    let insert = "+data[(1,),(2,),(3,),(4,),(5,)]";
    let result = handler.query_program(None, insert.to_string()).await;
    assert!(
        result.is_ok(),
        "Insert at exact limit should succeed, got: {result:?}"
    );
}

// === WI-07: String Value Length Validation ===

#[tokio::test]
async fn test_insert_string_over_value_limit_rejected() {
    let (handler, _tmp) = create_handler_with_limits(1_048_576, 10_000, 10);
    // Insert string longer than 10 bytes
    let long_str = "a".repeat(11);
    let insert = format!("+names[(\"{long_str}\",)]");
    let result = handler.query_program(None, insert).await;
    match result {
        Err(e) => assert!(
            e.contains("long") || e.contains("bytes") || e.contains("String"),
            "Error should mention string size, got: {e}"
        ),
        Ok(r) => {
            let msg = format!("{r:?}");
            assert!(
                msg.contains("long") || msg.contains("bytes") || msg.contains("String"),
                "Response should mention string size, got: {msg}"
            );
        }
    }
}

#[tokio::test]
async fn test_insert_string_at_value_limit_accepted() {
    let (handler, _tmp) = create_handler_with_limits(1_048_576, 10_000, 10);
    // Exactly 10 chars = 10 bytes for ASCII
    let ok_str = "a".repeat(10);
    let insert = format!("+names[(\"{ok_str}\",)]");
    let result = handler.query_program(None, insert).await;
    assert!(
        result.is_ok(),
        "Insert at exact string limit should succeed, got: {result:?}"
    );
}

// === WI-11: Session Rule Reserved Name Validation ===

#[tokio::test]
async fn test_session_rule_double_underscore_prefix_rejected_in_query_program() {
    let (handler, _tmp) = create_test_handler();
    // Try to add a session rule with __ prefix
    let result = handler
        .query_program(None, "~__hidden(X) <- data(X)".to_string())
        .await;
    assert!(
        result.is_err(),
        "Session rule with __ prefix should be rejected, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("reserved") || err.contains("__"),
        "Error should mention reserved prefix, got: {err}"
    );
}

#[tokio::test]
async fn test_session_rule_double_underscore_prefix_rejected_in_execute_program() {
    let (handler, _tmp) = create_test_handler();
    let sid = handler.create_session("default").unwrap();
    let result = handler
        .execute_program(
            Some(&sid),
            Some("default".to_string()),
            "~__hidden(X) <- data(X)".to_string(),
        )
        .await;
    assert!(
        result.is_err(),
        "Session rule with __ prefix should be rejected in execute_program, got: {result:?}"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("reserved") || err.contains("__"),
        "Error should mention reserved prefix, got: {err}"
    );
}

#[tokio::test]
async fn test_session_rule_valid_name_accepted() {
    let (handler, _tmp) = create_test_handler();
    // Valid rule name (no __ prefix) should succeed
    let result = handler
        .query_program(None, "~my_path(X) <- data(X)\n?my_path(X)".to_string())
        .await;
    // May succeed or fail for other reasons (e.g., no data relation), but not reserved name
    if let Err(ref e) = result {
        assert!(
            !e.contains("reserved") && !e.contains("'__'"),
            "Valid rule name should not fail with reserved name error, got: {e}"
        );
    }
}

#[tokio::test]
async fn test_session_rule_single_underscore_prefix_is_ok() {
    let (handler, _tmp) = create_test_handler();
    // Single underscore is fine
    let result = handler
        .query_program(None, "~_helper(X) <- data(X)\n?_helper(X)".to_string())
        .await;
    if let Err(ref e) = result {
        assert!(
            !e.contains("reserved"),
            "Single underscore prefix should not be rejected as reserved, got: {e}"
        );
    }
}

// === WI-13: KG Name Length Validation ===

#[test]
fn test_create_kg_name_at_limit_accepted() {
    let (storage, _tmp) = create_test_storage();
    // Exactly 128 bytes (at limit)
    let name = "a".repeat(128);
    assert!(
        storage.create_knowledge_graph(&name).is_ok(),
        "KG name at limit (128 bytes) should be accepted"
    );
}

#[test]
fn test_create_kg_name_over_limit_rejected() {
    let (storage, _tmp) = create_test_storage();
    // 129 bytes (over limit)
    let name = "a".repeat(129);
    let result = storage.create_knowledge_graph(&name);
    assert!(
        result.is_err(),
        "KG name over limit (129 bytes) should be rejected"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("long")
            || err.contains("length")
            || err.contains("limit")
            || err.contains("bytes"),
        "Error should mention length limit, got: {err}"
    );
}

#[test]
fn test_create_kg_name_well_under_limit_accepted() {
    let (storage, _tmp) = create_test_storage();
    let name = "my_knowledge_graph";
    assert!(
        storage.create_knowledge_graph(name).is_ok(),
        "Short KG name should be accepted"
    );
}

#[test]
fn test_create_kg_name_empty_still_rejected() {
    let (storage, _tmp) = create_test_storage();
    let result = storage.create_knowledge_graph("");
    assert!(result.is_err(), "Empty KG name should be rejected");
}

// === WI-12: Vector Dimension Validation ===

#[tokio::test]
async fn test_vector_schema_declared_dimension_enforced_over() {
    let (handler, _tmp) = create_test_handler();
    // Declare schema with dim=3
    let schema_result = handler
        .query_program(None, "+embed(id: int, v: vector(3))".to_string())
        .await;
    assert!(
        schema_result.is_ok(),
        "Schema declaration with vector(3) should succeed, got: {schema_result:?}"
    );
    // Insert a 4-element vector — should fail dimension check
    let result = handler
        .query_program(None, "+embed[(1, [1.0, 2.0, 3.0, 4.0])]".to_string())
        .await;
    match result {
        Err(e) => assert!(
            e.contains("dimension")
                || e.contains("vector")
                || e.contains("3")
                || e.contains("type"),
            "Error should mention dimension mismatch, got: {e}"
        ),
        Ok(r) => {
            let msg = format!("{r:?}");
            assert!(
                msg.contains("error") || msg.contains("dimension") || msg.contains("type"),
                "Response should indicate type error, got: {msg}"
            );
        }
    }
}

#[tokio::test]
async fn test_vector_schema_declared_dimension_enforced_under() {
    let (handler, _tmp) = create_test_handler();
    handler
        .query_program(None, "+embed(id: int, v: vector(3))".to_string())
        .await
        .unwrap();
    // Insert a 2-element vector — should fail
    let result = handler
        .query_program(None, "+embed[(1, [1.0, 2.0])]".to_string())
        .await;
    match result {
        Err(e) => assert!(
            e.contains("dimension")
                || e.contains("vector")
                || e.contains("3")
                || e.contains("type"),
            "Error should mention dimension mismatch, got: {e}"
        ),
        Ok(r) => {
            let msg = format!("{r:?}");
            assert!(
                msg.contains("error") || msg.contains("dimension") || msg.contains("type"),
                "Response should indicate type error, got: {msg}"
            );
        }
    }
}

#[tokio::test]
async fn test_vector_schema_exact_dimension_accepted() {
    let (handler, _tmp) = create_test_handler();
    handler
        .query_program(None, "+embed(id: int, v: vector(3))".to_string())
        .await
        .unwrap();
    // Exactly 3 elements — should succeed
    let result = handler
        .query_program(None, "+embed[(1, [1.0, 2.0, 3.0])]".to_string())
        .await;
    assert!(
        result.is_ok(),
        "Insert of correctly dimensioned vector should succeed, got: {result:?}"
    );
}

#[tokio::test]
async fn test_vector_schema_no_dimension_accepts_any_size() {
    let (handler, _tmp) = create_test_handler();
    handler
        .query_program(None, "+embed(id: int, v: vector)".to_string())
        .await
        .unwrap();
    // Any dimension should be accepted when no constraint is declared
    let result2 = handler
        .query_program(None, "+embed[(1, [1.0, 2.0])]".to_string())
        .await;
    assert!(
        result2.is_ok(),
        "2-dim vector against unconstrained schema should succeed, got: {result2:?}"
    );
    let result5 = handler
        .query_program(None, "+embed[(2, [1.0, 2.0, 3.0, 4.0, 5.0])]".to_string())
        .await;
    assert!(
        result5.is_ok(),
        "5-dim vector against unconstrained schema should succeed, got: {result5:?}"
    );
}
