//! Admin Handlers
//!
//! Health check and statistics endpoints.

use std::sync::Arc;

use axum::{http::StatusCode, Extension, Json};

use crate::protocol::rest::dto::{ApiResponse, HealthDto, SessionStatsDto, StatsDto};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// Health check endpoint.
///
/// Verifies the storage engine is accessible by attempting to acquire a read lock
/// within 1 second. Returns "degraded" with HTTP 503 if the lock cannot be acquired
/// (indicates a lock convoy or extremely long-running mutation).
pub async fn health(
    Extension(handler): Extension<Arc<Handler>>,
) -> (StatusCode, Json<ApiResponse<HealthDto>>) {
    // Try to acquire a read lock with a 1-second timeout.
    // Use spawn_blocking since even try_read_for can briefly block.
    let handler_clone = Arc::clone(&handler);
    let storage_ok = tokio::task::spawn_blocking(move || {
        handler_clone
            .try_get_storage(std::time::Duration::from_secs(1))
            .is_some()
    })
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Health check task panicked");
        false
    });

    let status = if storage_ok {
        "healthy".to_string()
    } else {
        "degraded".to_string()
    };

    let health = HealthDto {
        status,
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: handler.uptime_seconds(),
    };

    let http_status = if storage_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (http_status, Json(ApiResponse::success(health)))
}

/// Liveness probe: returns 200 if the process is alive.
///
/// Kubernetes liveness probes should hit this endpoint. It always returns 200
/// and does NOT check storage accessibility (to avoid false restarts).
pub async fn liveness() -> StatusCode {
    StatusCode::OK
}

/// Readiness probe: returns 200 if the server can handle requests.
///
/// Checks that the storage engine is accessible (read lock can be acquired).
/// Returns 503 if the server is not ready to handle requests.
pub async fn readiness(Extension(handler): Extension<Arc<Handler>>) -> StatusCode {
    let handler_clone = Arc::clone(&handler);
    let storage_ok = tokio::task::spawn_blocking(move || {
        handler_clone
            .try_get_storage(std::time::Duration::from_secs(1))
            .is_some()
    })
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(error = %e, "Readiness check task panicked");
        false
    });

    if storage_ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Server statistics endpoint
///
/// Uses `spawn_blocking` because acquiring the storage read lock can block
/// when a write lock is pending (parking_lot write-preferring policy).
/// Running this on a Tokio worker thread would risk starving the async runtime.
pub async fn stats(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<Json<ApiResponse<StatsDto>>, RestError> {
    let timeout_secs = handler.config().http.stats_timeout_secs.max(1);
    let stats = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio::task::spawn_blocking(move || {
            let storage = handler.get_storage();
            let kgs = storage.list_knowledge_graphs();
            let knowledge_graphs = kgs.len();

            // Count total relations and views across all KGs
            let mut total_relations = 0;
            let mut total_views = 0;

            // Estimate memory usage from tuple counts across all KGs.
            // Each tuple is approximately 64 bytes (Value enum + heap allocations).
            let mut total_tuples: u64 = 0;
            for kg_name in &kgs {
                if let Ok(relations) = storage.list_relations_in(kg_name) {
                    total_relations += relations.len();
                    for rel_name in &relations {
                        if let Ok(Some((_schema, count))) =
                            storage.get_relation_metadata_in(kg_name, rel_name)
                        {
                            total_tuples += count as u64;
                        }
                    }
                }
                if let Ok(rules) = storage.list_rules_in(kg_name) {
                    total_views += rules.len();
                }
            }
            let estimated_memory = total_tuples.saturating_mul(64);

            drop(storage);

            let session_stats = handler.session_stats();
            StatsDto {
                knowledge_graphs,
                relations: total_relations,
                views: total_views,
                memory_usage_bytes: estimated_memory,
                query_count: handler.total_queries(),
                uptime_secs: handler.uptime_seconds(),
                sessions: SessionStatsDto {
                    total: session_stats.total_sessions,
                    clean: session_stats.clean_sessions,
                    dirty: session_stats.dirty_sessions,
                    total_ephemeral_facts: session_stats.total_ephemeral_facts,
                    total_ephemeral_rules: session_stats.total_ephemeral_rules,
                },
            }
        }),
    )
    .await
    .map_err(|_| RestError::internal(format!("Stats computation timed out after {timeout_secs}s")))?
    .map_err(|e| {
        tracing::warn!(error = %e, "Stats computation failed");
        RestError::internal("Stats computation failed".to_string())
    })?;

    Ok(Json(ApiResponse::success(stats)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    fn make_handler() -> (Arc<Handler>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.data_dir = tmp.path().to_path_buf();
        (Arc::new(Handler::from_config(config).unwrap()), tmp)
    }

    #[tokio::test]
    async fn test_health_returns_healthy() {
        let (handler, _tmp) = make_handler();
        let (status, Json(resp)) = health(Extension(handler)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(resp.success);
        let data = resp.data.unwrap();
        assert_eq!(data.status, "healthy");
        assert!(!data.version.is_empty());
    }

    #[tokio::test]
    async fn test_health_uptime_is_reasonable() {
        let (handler, _tmp) = make_handler();
        let (_status, Json(resp)) = health(Extension(handler)).await;
        let data = resp.data.unwrap();
        assert!(data.uptime_secs < 5);
    }

    #[tokio::test]
    async fn test_stats_empty_server() {
        let (handler, _tmp) = make_handler();
        let result = stats(Extension(handler)).await.unwrap();
        let resp = result.0;
        assert!(resp.success);
        let data = resp.data.unwrap();
        assert_eq!(data.query_count, 0);
        assert_eq!(data.sessions.total, 0);
        assert_eq!(data.sessions.clean, 0);
        assert_eq!(data.sessions.dirty, 0);
    }

    #[tokio::test]
    async fn test_stats_after_insert() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(None, "+stuff[(1, 2)]".to_string())
            .await
            .unwrap();
        let result = stats(Extension(handler)).await.unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.query_count, 1);
        assert!(data.knowledge_graphs >= 1);
        assert!(data.relations >= 1);
    }

    #[tokio::test]
    async fn test_stats_memory_estimation() {
        let (handler, _tmp) = make_handler();
        // Insert some data
        handler
            .query_program(None, "+mem_test[(1, 2), (3, 4), (5, 6)]".to_string())
            .await
            .unwrap();
        let result = stats(Extension(handler)).await.unwrap();
        let data = result.0.data.unwrap();
        // 3 tuples * 64 bytes each = 192
        assert!(data.memory_usage_bytes > 0);
    }

    // === Regression tests for production readiness fixes ===

    /// P2-13: Verify health check returns "healthy" when storage is accessible.
    #[tokio::test]
    async fn test_health_returns_healthy_status_string() {
        let (handler, _tmp) = make_handler();
        let (status, Json(resp)) = health(Extension(handler)).await;
        assert_eq!(status, StatusCode::OK);
        let data = resp.data.unwrap();
        assert_eq!(data.status, "healthy");
    }

    /// P2-13: Verify try_get_storage works under normal conditions.
    #[tokio::test]
    async fn test_health_try_get_storage_succeeds() {
        let (handler, _tmp) = make_handler();
        let guard = handler.try_get_storage(std::time::Duration::from_millis(100));
        assert!(
            guard.is_some(),
            "try_get_storage should succeed under normal conditions"
        );
    }

    /// P1: Liveness probe always returns 200 (even under load).
    #[tokio::test]
    async fn test_liveness_always_200() {
        let status = liveness().await;
        assert_eq!(status, StatusCode::OK);
    }

    /// P1: Readiness probe returns 200 when storage is accessible.
    #[tokio::test]
    async fn test_readiness_returns_ok() {
        let (handler, _tmp) = make_handler();
        let status = readiness(Extension(handler)).await;
        assert_eq!(status, StatusCode::OK);
    }

    /// P2-13 regression: Health check returns degraded/503 when storage lock is contended.
    /// This verifies the health check doesn't hang when a write lock blocks readers.
    #[tokio::test]
    async fn test_health_returns_degraded_when_storage_locked() {
        let (handler, _tmp) = make_handler();

        // Hold a write lock from another thread for 3 seconds
        let h2 = Arc::clone(&handler);
        let lock_thread = std::thread::spawn(move || {
            let _guard = h2.get_storage_mut();
            std::thread::sleep(std::time::Duration::from_secs(3));
        });

        // Give the thread time to acquire the write lock
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Health check should return degraded (503), not hang
        let (status, Json(resp)) = health(Extension(handler)).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "Should return 503 when storage lock is contended"
        );
        let data = resp.data.unwrap();
        assert_eq!(
            data.status, "degraded",
            "Status should be 'degraded' when lock is contended"
        );

        lock_thread.join().unwrap();
    }

    /// Regression: Readiness probe returns 503 when storage lock is contended.
    /// Mirrors test_health_returns_degraded_when_storage_locked but for the /ready endpoint.
    #[tokio::test]
    async fn test_readiness_returns_503_when_storage_locked() {
        let (handler, _tmp) = make_handler();

        // Hold a write lock from another thread for 3 seconds
        let h2 = Arc::clone(&handler);
        let lock_thread = std::thread::spawn(move || {
            let _guard = h2.get_storage_mut();
            std::thread::sleep(std::time::Duration::from_secs(3));
        });

        // Give the thread time to acquire the write lock
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Readiness should return 503 when lock is contended
        let status = readiness(Extension(handler)).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "Readiness should return 503 when storage lock is contended"
        );

        lock_thread.join().unwrap();
    }
}
