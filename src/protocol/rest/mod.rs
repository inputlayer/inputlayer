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
    body::Body,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Extension, Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::services::{ServeDir, ServeFile};

use tracing::{info, warn};

use crate::config::HttpConfig;
use crate::protocol::Handler;

use self::handlers::{admin, ws};

/// Middleware: Enforce maximum concurrent connections using a Semaphore.
/// Unlike an atomic counter, Semaphore provides atomic check-and-acquire,
/// eliminating the TOCTOU race that could allow over-limit connections.
async fn connection_limit_middleware(
    Extension(conn_semaphore): Extension<Option<Arc<tokio::sync::Semaphore>>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if let Some(ref sem) = conn_semaphore {
        let permit = match sem.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return (StatusCode::SERVICE_UNAVAILABLE, "Too many connections").into_response();
            }
        };
        let response = next.run(req).await;
        drop(permit);
        response
    } else {
        next.run(req).await
    }
}

/// Middleware: API key authentication.
/// Checks for `Authorization: Bearer <key>` header.
/// Skips auth for /health and /live endpoints (probes must work unauthenticated).
async fn auth_middleware(
    Extension(api_keys): Extension<ApiKeys>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Health/liveness probes are always public
    let path = req.uri().path();
    if path == "/health" || path == "/live" || path == "/ready" {
        return next.run(req).await;
    }

    let keys = &api_keys.0;
    if keys.is_empty() {
        // No API keys configured — auth not enforced
        return next.run(req).await;
    }

    // Check Authorization header
    if let Some(auth_header) = req.headers().get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if keys.iter().any(|k| k == token) {
                    return next.run(req).await;
                }
            }
        }
    }

    (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response()
}

#[derive(Clone)]
struct ApiKeys(Arc<Vec<String>>);

/// Newtype wrapper for WebSocket-specific connection semaphore.
/// Distinct from the general HTTP connection semaphore.
#[derive(Clone)]
pub struct WsSemaphore(pub Option<Arc<tokio::sync::Semaphore>>);

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
            .filter_map(|s| {
                let parsed = s.parse();
                if parsed.is_err() {
                    warn!(origin = %s, "Invalid CORS origin ignored");
                }
                parsed.ok()
            })
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
        .route("/live", get(admin::liveness))
        .route("/ready", get(admin::readiness))
        .route("/metrics", get(admin::stats))
        .route("/ws", get(ws::global_websocket))
        .route("/sessions/:id/ws", get(ws::session_websocket))
        .route("/api/asyncapi.yaml", get(asyncapi_yaml))
        .route("/api/ws-docs", get(asyncapi_docs))
        .layer(Extension(handler));

    // Apply authentication middleware (if enabled)
    // Note: Extension must be the OUTER layer (applied last) so the middleware can extract it.
    // In Axum, .layer(A).layer(B) means B wraps A, so B runs first.
    if config.auth.enabled && !config.auth.api_keys.is_empty() {
        let api_keys = ApiKeys(Arc::new(config.auth.api_keys.clone()));
        app = app
            .layer(middleware::from_fn(auth_middleware))
            .layer(Extension(api_keys.clone()));
    } else {
        app = app.layer(Extension(ApiKeys(Arc::new(Vec::new()))));
    }

    // Apply connection limit middleware using Semaphore for atomic check-and-acquire
    let conn_semaphore: Option<Arc<tokio::sync::Semaphore>> =
        if config.rate_limit.max_connections > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(
                config.rate_limit.max_connections,
            )))
        } else {
            None
        };
    app = app
        .layer(middleware::from_fn(connection_limit_middleware))
        .layer(Extension(conn_semaphore));

    // WebSocket-specific connection limit (separate from HTTP connection limit)
    let ws_semaphore: Option<Arc<tokio::sync::Semaphore>> =
        if config.rate_limit.max_ws_connections > 0 {
            Some(Arc::new(tokio::sync::Semaphore::new(
                config.rate_limit.max_ws_connections,
            )))
        } else {
            None
        };
    app = app.layer(Extension(WsSemaphore(ws_semaphore)));

    if let Some(cors) = cors {
        app = app.layer(cors);
    }

    // Enforce HTTP request body size limit (16 MB, matching WebSocket MAX_MESSAGE_SIZE)
    app = app.layer(RequestBodyLimitLayer::new(16 * 1024 * 1024));

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
                        info!(reaped, "session_reaper_cleanup");
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("session_reaper_shutdown");
                    break;
                }
            }
        }
    });

    // Warn if binding to a public interface without authentication
    let is_public = config.host == "0.0.0.0" || config.host == "::";
    if is_public && !config.auth.enabled {
        warn!(
            host = %config.host,
            "Server binding to a public interface WITHOUT authentication. \
             All endpoints are publicly accessible. \
             Set [http.auth] enabled = true for production."
        );
    }

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
    let timeout_secs = handler.config().http.shutdown_timeout_secs.max(1);
    let shutdown_handler = handler.clone();
    match tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio::task::spawn_blocking(move || shutdown_handler.shutdown()),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(error = %e, "Shutdown task panicked");
        }
        Err(_) => {
            warn!(
                timeout_secs,
                "Graceful shutdown timed out. WAL will be replayed on next startup."
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    fn make_handler() -> (Arc<Handler>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = crate::Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.data_dir = tmp.path().to_path_buf();
        config.http.gui.enabled = false;
        (Arc::new(Handler::from_config(config).unwrap()), tmp)
    }

    fn make_config_with_auth(keys: Vec<String>) -> HttpConfig {
        let mut config = HttpConfig::default();
        config.auth.enabled = true;
        config.auth.api_keys = keys;
        config.gui.enabled = false;
        config
    }

    // === Regression: Middleware Layer Ordering (root cause of 1116 E2E failures) ===
    // In Axum, .layer(A).layer(B) means B wraps A (B runs first).
    // Extension must be the OUTER layer so middleware can extract it.

    /// Regression: Router must not panic or 500 on /health with middleware enabled.
    /// This was the root cause of all E2E tests failing with 500 Internal Server Error.
    #[tokio::test]
    async fn test_router_health_with_middleware_does_not_500() {
        let (handler, _tmp) = make_handler();
        let config = HttpConfig::default();
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Health must return 200, not 500"
        );
    }

    /// Regression: Router with auth enabled must not 500 on /health (auth bypass).
    #[tokio::test]
    async fn test_router_health_bypasses_auth() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["secret123".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "/health must bypass auth");
    }

    /// Regression: /live bypasses auth (liveness probe must always work).
    #[tokio::test]
    async fn test_router_live_bypasses_auth() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["secret123".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder().uri("/live").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "/live must bypass auth");
    }

    /// Regression: /ready bypasses auth (readiness probe must always work).
    #[tokio::test]
    async fn test_router_ready_bypasses_auth() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["secret123".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/ready")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "/ready must bypass auth");
    }

    // === API Key Auth Middleware Tests ===

    /// Auth: Valid Bearer token is accepted.
    #[tokio::test]
    async fn test_auth_valid_key_accepted() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["my-secret-key".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", "Bearer my-secret-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Valid key should be accepted"
        );
    }

    /// Auth: Invalid Bearer token is rejected with 401.
    #[tokio::test]
    async fn test_auth_invalid_key_rejected() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["correct-key".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", "Bearer wrong-key")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Auth: Missing Authorization header is rejected with 401.
    #[tokio::test]
    async fn test_auth_missing_header_rejected() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["my-key".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Auth: Non-Bearer auth scheme is rejected.
    #[tokio::test]
    async fn test_auth_non_bearer_scheme_rejected() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["my-key".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", "Basic bXkta2V5")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Auth: No keys configured means auth is not enforced.
    #[tokio::test]
    async fn test_auth_disabled_when_no_keys() {
        let (handler, _tmp) = make_handler();
        let config = HttpConfig::default(); // no auth enabled
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "No keys = no auth enforced");
    }

    /// Auth: Multiple valid keys - any one should work.
    #[tokio::test]
    async fn test_auth_multiple_keys_any_valid() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["key-alpha".to_string(), "key-beta".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", "Bearer key-beta")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // === Connection Limit Middleware Tests ===

    /// Connection limit: limit=0 means unlimited.
    #[tokio::test]
    async fn test_connection_limit_zero_means_unlimited() {
        let (handler, _tmp) = make_handler();
        let mut config = HttpConfig::default();
        config.rate_limit.max_connections = 0;
        config.gui.enabled = false;
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Connection limit: Verify the middleware can extract MaxConnections extension.
    /// This is a regression for the layer ordering bug.
    #[tokio::test]
    async fn test_connection_limit_middleware_extracts_extension() {
        let (handler, _tmp) = make_handler();
        let mut config = HttpConfig::default();
        config.rate_limit.max_connections = 1000;
        config.gui.enabled = false;
        let app = create_router(handler, &config);

        // Should work fine with limit=1000 (not exceeded)
        let req = Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Middleware must successfully extract MaxConnections extension"
        );
    }

    /// Auth: 401 status is returned for unauthenticated requests to protected endpoints.
    #[tokio::test]
    async fn test_auth_rejection_returns_401_on_protected_endpoint() {
        let (handler, _tmp) = make_handler();
        let config = make_config_with_auth(vec!["secret".to_string()]);
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Unauthenticated request to /metrics must get 401"
        );
    }
}

/// Wait for a shutdown signal (SIGINT or SIGTERM).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => { info!("Received SIGINT, shutting down..."); }
            _ = sigterm.recv() => { info!("Received SIGTERM, shutting down..."); }
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to listen for ctrl-c");
        info!("Received SIGINT, shutting down...");
    }
}
