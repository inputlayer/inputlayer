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
    let cors = if !config.cors_origins.is_empty() {
        // Explicit origins configured: restrict to those
        let origins: Vec<_> = config
            .cors_origins
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        Some(
            CorsLayer::new()
                .allow_origin(origins)
                .allow_methods(Any)
                .allow_headers(Any),
        )
    } else if config.cors_allow_all {
        // Explicit dev mode opt-in: allow all origins
        Some(CorsLayer::permissive())
    } else {
        // Default: same-origin only (no CORS layer = Axum denies cross-origin)
        None
    };

    // Main router with top-level health/metrics and WebSocket routes
    let mut app = Router::new()
        .route("/health", get(admin::health))
        .route("/metrics", get(admin::stats))
        .route("/ws", get(ws::global_websocket))
        .route("/sessions/:id/ws", get(ws::session_websocket))
        .route("/api/asyncapi.yaml", get(asyncapi_yaml))
        .route("/api/ws-docs", get(asyncapi_docs))
        .layer(Extension(handler));

    if let Some(cors) = cors {
        app = app.layer(cors);
    }

    // Serve GUI static files if enabled
    if config.gui.enabled {
        let static_dir = &config.gui.static_dir;
        let index_file = format!("{static_dir}/index.html");

        // Fallback to index.html for SPA routing
        app = app.fallback_service(ServeDir::new(static_dir).fallback(ServeFile::new(index_file)));
    }

    app
}

/// Starts the HTTP server with graceful shutdown support.
///
/// Listens for SIGINT (ctrl-c) and SIGTERM to trigger graceful shutdown.
/// On shutdown: stops accepting connections, cancels the session reaper,
/// and flushes WAL + metadata via `handler.shutdown()`.
pub async fn start_http_server(
    handler: Arc<Handler>,
    config: &HttpConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = create_router(handler.clone(), config);

    // Cancellation channel for the session reaper
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn background session reaper (runs every 60 seconds, stops on shutdown)
    let reaper_handler = Arc::clone(&handler);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let reaped = reaper_handler.session_manager().reap_expired();
                    if reaped > 0 {
                        eprintln!("Session reaper: cleaned up {reaped} expired session(s)");
                    }
                }
                _ = shutdown_rx.changed() => {
                    eprintln!("Session reaper: shutting down");
                    break;
                }
            }
        }
    });

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    println!("HTTP server listening on: http://{addr}");
    if config.gui.enabled {
        println!("GUI dashboard available at: http://{addr}/");
    }
    println!("WebSocket API docs at: http://{addr}/api/ws-docs");

    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Signal reaper to stop
    let _ = shutdown_tx.send(true);

    // Flush WAL and save metadata with a timeout.
    // If a long-running query holds the storage lock, we don't want to hang indefinitely.
    // The WAL is designed to replay on next startup, so skipping the final flush is safe.
    let shutdown_handler = handler.clone();
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(move || shutdown_handler.shutdown()),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("WARNING: Shutdown task panicked: {e}");
        }
        Err(_) => {
            eprintln!(
                "WARNING: Graceful shutdown timed out after 10s. \
                 WAL will be replayed on next startup."
            );
        }
    }

    Ok(())
}

/// Wait for a shutdown signal (SIGINT or SIGTERM).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => { eprintln!("\nReceived SIGINT, shutting down..."); }
            _ = sigterm.recv() => { eprintln!("Received SIGTERM, shutting down..."); }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to listen for ctrl-c");
        eprintln!("\nReceived SIGINT, shutting down...");
    }
}
