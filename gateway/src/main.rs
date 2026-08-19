//! InputLayer Gateway service.
//!
//! The model gateway of the stack: an OpenAI-compatible endpoint that will
//! proxy completions to the model provider and verify every conversation
//! against the loaded ontology (issues #83/#84). This binary is the
//! activation skeleton: health/readiness plus honest 501 responses on the
//! API routes until the verify pipeline lands.
//!
//! A separate deployable next to the engine, with its own configuration:
//!   GATEWAY_HOST        bind address (default 127.0.0.1; 0.0.0.0 in Docker)
//!   GATEWAY_PORT        port (default 8081)
//!   INPUTLAYER_URL      engine base URL (default http://127.0.0.1:8080)
//!   INPUTLAYER_API_KEY  engine API key (WS access, used from #83 on)
//!   ANTHROPIC_API_KEY   model provider key (presence reported, never logged;
//!                       the engine process never sees it)

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    http: reqwest::Client,
    engine_url: String,
    has_model_key: bool,
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let host = env_or("GATEWAY_HOST", "127.0.0.1");
    let port = env_or("GATEWAY_PORT", "8081");
    let engine_url = env_or("INPUTLAYER_URL", "http://127.0.0.1:8080")
        .trim_end_matches('/')
        .to_string();
    let has_model_key = std::env::var("ANTHROPIC_API_KEY").is_ok_and(|v| !v.is_empty());

    let state = Arc::new(AppState {
        http: reqwest::Client::new(),
        engine_url,
        has_model_key,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/v1/verify", post(not_implemented))
        .route("/v1/chat/completions", post(not_implemented))
        .with_state(state);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("InputLayer Gateway listening on http://{addr}");
    println!("  /v1/verify and /v1/chat/completions respond 501 until #83/#84 land");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "inputlayer-gateway",
        "version": env!("CARGO_PKG_VERSION"),
        "model_key_configured": state.has_model_key,
    }))
}

/// Ready only when the engine is reachable: the gateway is useless without it.
async fn ready(State(state): State<Arc<AppState>>) -> (StatusCode, Json<Value>) {
    let engine_health = format!("{}/health", state.engine_url);
    let engine_ok = state
        .http
        .get(&engine_health)
        .send()
        .await
        .is_ok_and(|r| r.status().is_success());
    if engine_ok {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "engine unreachable", "engine": state.engine_url })),
        )
    }
}

async fn not_implemented() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": {
                "type": "not_implemented",
                "message": "The gateway is activated but this endpoint is not implemented yet. Verified Completions endpoints are tracked in https://github.com/inputlayer/inputlayer/issues/83 (POST /v1/verify) and /issues/84 (POST /v1/chat/completions)."
            }
        })),
    )
}
