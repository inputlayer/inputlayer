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
