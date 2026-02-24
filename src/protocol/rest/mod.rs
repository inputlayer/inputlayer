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

/// Middleware: API key authentication via `_internal` KG.
/// Checks for `Authorization: Bearer <key>` header and validates against stored API keys.
/// Skips auth for /health, /live, /ready endpoints and WebSocket upgrades
/// (WS has its own auth flow).
async fn auth_middleware(
    Extension(handler): Extension<Arc<Handler>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Health/liveness probes are always public (both root and /v1/ prefixed)
    let path = req.uri().path();
    let effective_path = path.strip_prefix("/v1").unwrap_or(path);
    if effective_path == "/health" || effective_path == "/live" || effective_path == "/ready" {
        return next.run(req).await;
    }

    // WebSocket endpoints handle their own auth flow (Login/Authenticate messages)
    if effective_path == "/ws" || effective_path.starts_with("/sessions/") {
        return next.run(req).await;
    }

    // Check Authorization header
    if let Some(auth_header) = req.headers().get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                if handler.authenticate_api_key(token).is_ok() {
                    return next.run(req).await;
                }
            }
        }
    }

    (StatusCode::UNAUTHORIZED, "Invalid or missing API key").into_response()
}

/// Middleware: Add `X-API-Version` header to all responses (#25).
async fn api_version_middleware(req: Request<Body>, next: Next) -> Response {
    let mut response = next.run(req).await;
    response.headers_mut().insert(
        "x-api-version",
        axum::http::HeaderValue::from_static(concat!("v", env!("CARGO_PKG_VERSION"))),
    );
    response
}

/// Per-IP rate limiter state (#27).
/// Uses a simple sliding window: (window_start, request_count).
#[derive(Clone)]
pub struct IpRateLimiter {
    map: Arc<dashmap::DashMap<std::net::IpAddr, (std::time::Instant, u32)>>,
    max_rps: u32,
}

impl IpRateLimiter {
    fn new(max_rps: u32) -> Self {
        Self {
            map: Arc::new(dashmap::DashMap::new()),
            max_rps,
        }
    }

    /// Returns true if the request should be allowed.
    fn check(&self, ip: std::net::IpAddr) -> bool {
        if self.max_rps == 0 {
            return true;
        }
        let now = std::time::Instant::now();
        let mut entry = self.map.entry(ip).or_insert((now, 0));
        let (window_start, count) = entry.value_mut();
        if now.duration_since(*window_start).as_secs() >= 1 {
            // Reset window
            *window_start = now;
            *count = 1;
            true
        } else if *count < self.max_rps {
            *count += 1;
            true
        } else {
            false
        }
    }
}

/// Middleware: Per-IP rate limiting (#27).
async fn ip_rate_limit_middleware(
    Extension(limiter): Extension<IpRateLimiter>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if limiter.max_rps == 0 {
        return next.run(req).await;
    }

    // Extract client IP from X-Forwarded-For, X-Real-IP, or ConnectInfo
    let ip = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .and_then(|s| s.trim().parse::<std::net::IpAddr>().ok())
        .or_else(|| {
            req.headers()
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.trim().parse::<std::net::IpAddr>().ok())
        })
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

    if limiter.check(ip) {
        next.run(req).await
    } else {
        (StatusCode::TOO_MANY_REQUESTS, "Rate limit exceeded").into_response()
    }
}

/// Newtype wrapper for WebSocket-specific connection semaphore.
/// Distinct from the general HTTP connection semaphore.
#[derive(Clone)]
pub struct WsSemaphore(pub Option<Arc<tokio::sync::Semaphore>>);

/// Embedded AsyncAPI spec (from docs/spec/asyncapi.yaml)
const ASYNCAPI_YAML: &str = include_str!("../../../docs/spec/asyncapi.yaml");

/// Embedded OpenAPI spec (#30)
const OPENAPI_YAML: &str = include_str!("../../../docs/spec/openapi.yaml");

/// Serve the raw AsyncAPI YAML spec
async fn asyncapi_yaml() -> impl IntoResponse {
    (
        [("content-type", "text/yaml; charset=utf-8")],
        ASYNCAPI_YAML,
    )
}

/// Serve the OpenAPI YAML spec (#30)
async fn openapi_yaml() -> impl IntoResponse {
    ([("content-type", "text/yaml; charset=utf-8")], OPENAPI_YAML)
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

    // Versioned API routes (available at both / and /v1/ for backward compatibility)
    let api_routes = Router::new()
        .route("/health", get(admin::health))
        .route("/live", get(admin::liveness))
        .route("/ready", get(admin::readiness))
        .route("/metrics", get(admin::stats))
        .route("/metrics/prometheus", get(admin::prometheus_metrics))
        .route("/ws", get(ws::global_websocket))
        .route("/sessions/:id/ws", get(ws::session_websocket))
        .route("/api/asyncapi.yaml", get(asyncapi_yaml))
        .route("/api/openapi.yaml", get(openapi_yaml))
        .route("/api/ws-docs", get(asyncapi_docs));

    // Mount routes at root (backward compat) and /v1/ prefix (#25)
    let mut app = Router::new()
        .merge(api_routes.clone())
        .nest("/v1", api_routes);

    // Apply authentication middleware.
    // Auth is always required - API keys are validated against the _internal KG.
    // Health/live/ready endpoints and WebSocket paths bypass auth.
    // NOTE: Layer ordering matters! In Axum, .layer(A).layer(B) means B runs first.
    // Auth middleware needs Extension<Handler>, so Extension must be the OUTER layer.
    app = app
        .layer(middleware::from_fn(auth_middleware))
        .layer(Extension(handler));

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

    // Add X-API-Version header to all responses (#25)
    app = app.layer(middleware::from_fn(api_version_middleware));

    // Per-IP rate limiting (#27)
    let ip_limiter = IpRateLimiter::new(config.rate_limit.per_ip_max_rps);
    app = app
        .layer(middleware::from_fn(ip_rate_limit_middleware))
        .layer(Extension(ip_limiter));

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

    // Spawn background auto-compaction task (if enabled)
    let compact_interval = handler.config().storage.persist.auto_compact_interval_secs;
    let compact_threshold = handler.config().storage.persist.auto_compact_threshold;
    if compact_interval > 0 && compact_threshold > 0 {
        let compact_handler = Arc::clone(&handler);
        let mut compact_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(compact_interval));
            // Skip the first immediate tick
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let threshold = compact_threshold;
                        let h = Arc::clone(&compact_handler);
                        let result = tokio::task::spawn_blocking(move || {
                            let storage = h.get_storage();
                            storage.compact_if_needed(threshold)
                        })
                        .await;
                        match result {
                            Ok(Ok(count)) if count > 0 => {
                                info!(shards_compacted = count, "auto_compact_complete");
                            }
                            Ok(Err(e)) => {
                                warn!(error = %e, "auto_compact_error");
                            }
                            Err(e) => {
                                warn!(error = %e, "auto_compact_task_panicked");
                            }
                            _ => {} // count == 0, nothing to compact
                        }
                    }
                    _ = compact_shutdown.changed() => {
                        info!("auto_compact_shutdown");
                        break;
                    }
                }
            }
        });
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
        let handler = Arc::new(Handler::from_config(config).unwrap());
        handler.bootstrap_auth();
        (handler, tmp)
    }

    /// Create a handler with an API key for auth tests.
    /// Returns (handler, api_key, tmpdir).
    fn make_handler_with_api_key() -> (Arc<Handler>, String, tempfile::TempDir) {
        let (handler, tmp) = make_handler();
        let result = handler.handle_apikey_create("test-key", "admin").unwrap();
        let api_key = result.rows[0].values[1].as_str().unwrap().to_string();
        (handler, api_key, tmp)
    }

    fn make_default_config() -> HttpConfig {
        let mut config = HttpConfig::default();
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
        let config = make_default_config();
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

    /// Regression: Router with auth must not 500 on /health (auth bypass).
    #[tokio::test]
    async fn test_router_health_bypasses_auth() {
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
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
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
        let app = create_router(handler, &config);

        let req = Request::builder().uri("/live").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "/live must bypass auth");
    }

    /// Regression: /ready bypasses auth (readiness probe must always work).
    #[tokio::test]
    async fn test_router_ready_bypasses_auth() {
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/ready")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "/ready must bypass auth");
    }

    // === API Key Auth Middleware Tests ===

    /// Auth: Valid Bearer API key is accepted.
    #[tokio::test]
    async fn test_auth_valid_key_accepted() {
        let (handler, api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", format!("Bearer {api_key}"))
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
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
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
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
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
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", "Basic bXkta2V5")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// Auth: Multiple valid keys - any one should work.
    #[tokio::test]
    async fn test_auth_multiple_keys_any_valid() {
        let (handler, _key1, _tmp) = make_handler_with_api_key();
        // Create a second key
        let result = handler.handle_apikey_create("key-2", "admin").unwrap();
        let key2 = result.rows[0].values[1].as_str().unwrap().to_string();

        let config = make_default_config();
        let app = create_router(handler, &config);

        let req = Request::builder()
            .uri("/metrics")
            .header("authorization", format!("Bearer {key2}"))
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
        let mut config = make_default_config();
        config.rate_limit.max_connections = 0;
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
        let mut config = make_default_config();
        config.rate_limit.max_connections = 1000;
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
        let (handler, _api_key, _tmp) = make_handler_with_api_key();
        let config = make_default_config();
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
