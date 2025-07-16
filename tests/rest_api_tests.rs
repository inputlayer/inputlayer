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
