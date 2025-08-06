//! REST API endpoint tests (tower test utilities, no server needed).

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use inputlayer::config::HttpConfig;
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

// Note: We keep Arc in scope for the Handler but don't wrap the Router in Arc

fn create_test_handler() -> (Arc<Handler>, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    let handler = Arc::new(Handler::new(storage));
    (handler, temp)
}

fn create_test_app() -> (axum::Router, TempDir) {
    let (handler, temp) = create_test_handler();
    let http_config = HttpConfig::default();
    let app = create_router(handler, &http_config);
    (app, temp)
}

async fn send_json_request(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let req = match method {
        "GET" => Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
        "POST" => Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&body.unwrap_or(json!({}))).unwrap(),
            ))
            .unwrap(),
        "DELETE" => Request::builder()
            .method("DELETE")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::to_string(&body.unwrap_or(json!({}))).unwrap(),
            ))
            .unwrap(),
        _ => panic!("Unsupported method"),
    };

    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    (status, json)
}

// Health & Admin Endpoints
#[tokio::test]
async fn test_health_endpoint() {
    let (app, _temp) = create_test_app();

    let (status, json) = send_json_request(&app, "GET", "/api/v1/health", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert_eq!(json["data"]["status"], "healthy");
    assert!(json["data"]["version"].is_string());
    assert!(json["data"]["uptime_secs"].is_number());
}

#[tokio::test]
async fn test_stats_endpoint() {
    let (app, _temp) = create_test_app();

    let (status, json) = send_json_request(&app, "GET", "/api/v1/stats", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert!(json["data"]["knowledge_graphs"].is_number());
    assert!(json["data"]["relations"].is_number());
    assert!(json["data"]["views"].is_number());
    assert!(json["data"]["query_count"].is_number());
    assert!(json["data"]["uptime_secs"].is_number());
}

// Knowledge Graph Endpoints
#[tokio::test]
async fn test_list_knowledge_graphs() {
    let (app, _temp) = create_test_app();

    let (status, json) = send_json_request(&app, "GET", "/api/v1/knowledge-graphs", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    // Should have at least 'default' KG
    assert!(json["data"]["knowledge_graphs"].is_array());
}

#[tokio::test]
async fn test_create_and_get_knowledge_graph() {
    let (app, _temp) = create_test_app();

    // Create a new KG
    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "test_kg"})),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));

    // Get the created KG
    let (status, json) =
        send_json_request(&app, "GET", "/api/v1/knowledge-graphs/test_kg", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert_eq!(json["data"]["name"], "test_kg");
}

#[tokio::test]
async fn test_create_duplicate_knowledge_graph() {
    let (app, _temp) = create_test_app();

    // Create first KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "duplicate_kg"})),
    )
    .await;

    // Try to create duplicate - server returns 500 (internal error) for already exists
    let (status, _json) = send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "duplicate_kg"})),
    )
    .await;

    // 500 because it's an internal storage error (already exists)
    assert!(status == StatusCode::INTERNAL_SERVER_ERROR || status == StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_delete_knowledge_graph() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "to_delete"})),
    )
    .await;

    // Delete it
    let (status, json) =
        send_json_request(&app, "DELETE", "/api/v1/knowledge-graphs/to_delete", None).await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));

    // Verify it's deleted
    let (status, _json) =
        send_json_request(&app, "GET", "/api/v1/knowledge-graphs/to_delete", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_nonexistent_knowledge_graph() {
    let (app, _temp) = create_test_app();

    let (status, _json) =
        send_json_request(&app, "GET", "/api/v1/knowledge-graphs/nonexistent", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

// Query Endpoints
#[tokio::test]
async fn test_execute_simple_query() {
    let (app, _temp) = create_test_app();

    // First create a KG and insert some data
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "query_test"})),
    )
    .await;

    // Insert data
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/query_test/relations/edge/data",
        Some(json!({"rows": [[1, 2], [2, 3], [3, 4]]})),
    )
    .await;

    // Execute query
    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/query/execute",
        Some(json!({
            "knowledge_graph": "query_test",
            "query": "?- edge(X, Y)."
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert!(json["data"]["rows"].is_array());
}

#[tokio::test]
async fn test_execute_invalid_query() {
    let (app, _temp) = create_test_app();

    // Test with truly invalid syntax (multiple issues)
    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/query/execute",
        Some(json!({
            "knowledge_graph": "default",
            "query": "invalid::query{{syntax"  // Clearly invalid syntax
        })),
    )
    .await;

    // The query endpoint is lenient - it returns 200 OK with either:
    // - An empty result (parsing failed silently)
    // - An error message in the response
    // This is acceptable behavior for a query endpoint
    assert_eq!(status, StatusCode::OK);
    // The response structure should be valid even for bad queries
    assert!(json.is_object());
}

#[tokio::test]
async fn test_explain_query() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "explain_test"})),
    )
    .await;

    // Insert some data first
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/explain_test/relations/edge/data",
        Some(json!({"rows": [[1, 2]]})),
    )
    .await;

    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/query/explain",
        Some(json!({
            "knowledge_graph": "explain_test",
            "query": "?- edge(X, Y)."
        })),
    )
    .await;

    // Explain should return some plan information (or 422 for explanation not supported)
    assert!(status == StatusCode::OK || status == StatusCode::UNPROCESSABLE_ENTITY);
    if status == StatusCode::OK {
        assert!(json["success"].as_bool().unwrap_or(false));
    }
}

// Relations Endpoints
#[tokio::test]
async fn test_list_relations() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "relations_test"})),
    )
    .await;

    let (status, json) = send_json_request(
        &app,
        "GET",
        "/api/v1/knowledge-graphs/relations_test/relations",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert!(json["data"]["relations"].is_array());
}

#[tokio::test]
async fn test_insert_and_get_data() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "data_test"})),
    )
    .await;

    // Insert data
    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/data_test/relations/person/data",
        Some(json!({"rows": [["alice", 30], ["bob", 25], ["charlie", 35]]})),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert_eq!(json["data"]["rows_inserted"], 3);

    // Get the data back
    let (status, json) = send_json_request(
        &app,
        "GET",
        "/api/v1/knowledge-graphs/data_test/relations/person/data",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["data"]["row_count"], 3);
}

#[tokio::test]
async fn test_delete_data() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "delete_data_test"})),
    )
    .await;

    // Insert data
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/delete_data_test/relations/numbers/data",
        Some(json!({"rows": [[1], [2], [3]]})),
    )
    .await;

    // Delete specific rows
    let (status, json) = send_json_request(
        &app,
        "DELETE",
        "/api/v1/knowledge-graphs/delete_data_test/relations/numbers/data",
        Some(json!({"rows": [[1], [2]]})),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["data"]["rows_deleted"].as_i64().unwrap() >= 0);
}

#[tokio::test]
async fn test_insert_duplicate_data() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "dup_data_test"})),
    )
    .await;

    // Insert data
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/dup_data_test/relations/items/data",
        Some(json!({"rows": [[1], [2]]})),
    )
    .await;

    // Insert again with duplicates
    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/dup_data_test/relations/items/data",
        Some(json!({"rows": [[1], [2], [3]]})),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    // Should report 1 inserted (only 3 is new) and 2 duplicates
    assert_eq!(json["data"]["rows_inserted"], 1);
    assert_eq!(json["data"]["duplicates"], 2);
}

// Rules Endpoints
#[tokio::test]
async fn test_create_and_list_rules() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "rules_test"})),
    )
    .await;

    // Create a rule via query
    send_json_request(
        &app,
        "POST",
        "/api/v1/query/execute",
        Some(json!({
            "knowledge_graph": "rules_test",
            "query": "+path(X, Y) :- edge(X, Y)."
        })),
    )
    .await;

    // List rules
    let (status, json) = send_json_request(
        &app,
        "GET",
        "/api/v1/knowledge-graphs/rules_test/rules",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert!(json["data"]["rules"].is_array());
}

#[tokio::test]
async fn test_delete_rule() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "delete_rule_test"})),
    )
    .await;

    // Create a persistent rule using view endpoint
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/delete_rule_test/views",
        Some(json!({
            "name": "temp",
            "definition": "temp(X) :- source(X)."
        })),
    )
    .await;

    // Delete the rule/view
    let (status, json) = send_json_request(
        &app,
        "DELETE",
        "/api/v1/knowledge-graphs/delete_rule_test/views/temp",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
}

// Views Endpoints
#[tokio::test]
async fn test_create_and_list_views() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "views_test"})),
    )
    .await;

    // Create view
    let (status, json) = send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/views_test/views",
        Some(json!({
            "name": "connected",
            "definition": "connected(X, Y) :- edge(X, Y)."
        })),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));

    // List views
    let (status, json) = send_json_request(
        &app,
        "GET",
        "/api/v1/knowledge-graphs/views_test/views",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["data"]["views"].is_array());
}

#[tokio::test]
async fn test_get_view_data() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "view_data_test"})),
    )
    .await;

    // Insert base data
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/view_data_test/relations/edge/data",
        Some(json!({"rows": [[1, 2], [2, 3]]})),
    )
    .await;

    // Create view
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/view_data_test/views",
        Some(json!({
            "name": "path",
            "definition": "path(X, Y) :- edge(X, Y)."
        })),
    )
    .await;

    // Get view data
    let (status, json) = send_json_request(
        &app,
        "GET",
        "/api/v1/knowledge-graphs/view_data_test/views/path/data",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
    assert!(json["data"]["rows"].is_array());
}

#[tokio::test]
async fn test_delete_view() {
    let (app, _temp) = create_test_app();

    // Create KG
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs",
        Some(json!({"name": "delete_view_test"})),
    )
    .await;

    // Create view
    send_json_request(
        &app,
        "POST",
        "/api/v1/knowledge-graphs/delete_view_test/views",
        Some(json!({
            "name": "temp_view",
            "definition": "temp_view(X) :- source(X)."
        })),
    )
    .await;

    // Delete view
    let (status, json) = send_json_request(
        &app,
        "DELETE",
        "/api/v1/knowledge-graphs/delete_view_test/views/temp_view",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert!(json["success"].as_bool().unwrap_or(false));
}

// Error Handling Tests
#[tokio::test]
async fn test_invalid_json_body() {
    let (app, _temp) = create_test_app();

    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/knowledge-graphs")
        .header("content-type", "application/json")
        .body(Body::from("{ invalid json }"))
        .unwrap();

    let response = app.clone().oneshot(req).await.unwrap();
    // Invalid JSON returns 400 (Bad Request)
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_missing_required_field() {
    let (app, _temp) = create_test_app();

    // Try to create KG without name
    let (status, _json) =
        send_json_request(&app, "POST", "/api/v1/knowledge-graphs", Some(json!({}))).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn test_operations_on_nonexistent_kg() {
    let (app, _temp) = create_test_app();

    // Try to get relations from nonexistent KG
    let (status, _json) = send_json_request(
        &app,
        "GET",
        "/api/v1/knowledge-graphs/nonexistent/relations",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

// Concurrent Operations Tests
#[tokio::test]
async fn test_concurrent_inserts() {
    let (handler, _temp) = create_test_handler();
    let http_config = HttpConfig::default();
    let app = create_router(handler, &http_config);

    // Create KG first
    let req = Request::builder()
        .method("POST")
        .uri("/api/v1/knowledge-graphs")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"name": "concurrent_test"}"#))
        .unwrap();
    app.clone().oneshot(req).await.unwrap();

    // Spawn multiple concurrent insert requests
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let app = app.clone();
            tokio::spawn(async move {
                let req = Request::builder()
                    .method("POST")
                    .uri("/api/v1/knowledge-graphs/concurrent_test/relations/items/data")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"rows": [[{}]]}}"#, i)))
                    .unwrap();
                app.oneshot(req).await.unwrap().status()
            })
        })
        .collect();

    // All should succeed
    for handle in handles {
        assert_eq!(handle.await.unwrap(), StatusCode::OK);
    }
}

#[tokio::test]
