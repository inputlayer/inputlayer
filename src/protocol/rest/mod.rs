//! HTTP API Module
//!
//! Provides the HTTP server with WebSocket endpoint, health/stats endpoints,
//! and AsyncAPI documentation. All data operations go through the WebSocket
//! `/ws` endpoint.

pub mod dto;
pub mod error;
pub mod handlers;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    response::{Html, IntoResponse},
    routing::get,
    Extension, Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};

use crate::config::HttpConfig;
use crate::protocol::Handler;

use self::handlers::{admin, ws};

/// Embedded AsyncAPI spec (from docs/spec/asyncapi.yaml)
const ASYNCAPI_YAML: &str = include_str!("../../../docs/spec/asyncapi.yaml");

/// Serve the raw AsyncAPI YAML spec
async fn asyncapi_yaml() -> impl IntoResponse {
    (
        [("content-type", "text/yaml; charset=utf-8")],
        ASYNCAPI_YAML,
    )
}

/// Serve a self-contained HTML page documenting the WebSocket API protocol
async fn asyncapi_docs() -> Html<&'static str> {
    Html(include_str!("asyncapi_docs.html"))
}

/// Creates the Axum router
pub fn create_router(handler: Arc<Handler>, config: &HttpConfig) -> Router {
    // Build CORS layer
    let cors = if config.cors_origins.is_empty() {
        // Development mode: allow all origins
        CorsLayer::permissive()
    } else {
        // Production mode: restrict to configured origins
        let origins: Vec<_> = config
            .cors_origins
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    // Main router with top-level health/metrics and WebSocket routes
    let mut app = Router::new()
        .route("/health", get(admin::health))
        .route("/metrics", get(admin::stats))
        .route("/ws", get(ws::global_websocket))
        .route("/sessions/:id/ws", get(ws::session_websocket))
        .route("/api/asyncapi.yaml", get(asyncapi_yaml))
        .route("/api/ws-docs", get(asyncapi_docs))
        .layer(Extension(handler))
        .layer(cors);

    // Serve GUI static files if enabled
    if config.gui.enabled {
        let static_dir = &config.gui.static_dir;
        let index_file = format!("{static_dir}/index.html");

        // Fallback to index.html for SPA routing
        app = app.fallback_service(ServeDir::new(static_dir).fallback(ServeFile::new(index_file)));
    }

    app
}

/// Starts the HTTP server
pub async fn start_http_server(
    handler: Arc<Handler>,
    config: &HttpConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = create_router(handler.clone(), config);

    // Spawn background session reaper (runs every 60 seconds)
    let reaper_handler = Arc::clone(&handler);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let reaped = reaper_handler.session_manager().reap_expired();
            if reaped > 0 {
                eprintln!("Session reaper: cleaned up {reaped} expired session(s)");
            }
        }
    });

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    println!("HTTP server listening on: http://{addr}");
    if config.gui.enabled {
        println!("GUI dashboard available at: http://{addr}/");
    }
    println!("WebSocket API docs at: http://{addr}/api/ws-docs");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
