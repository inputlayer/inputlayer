//! Admin Handlers
//!
//! Health check and statistics endpoints.

use std::sync::Arc;

use axum::{http::StatusCode, Extension, Json};

use crate::protocol::metrics::{resident_memory_bytes, Prometheus};
use crate::protocol::rest::dto::{ApiResponse, HealthDto, SessionStatsDto, StatsDto};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;
use crate::storage_engine::KgSummary;

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

/// Server statistics endpoint. Admin API keys only: the statistics cover
/// every knowledge graph.
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
            let totals = KgTotals::of(&storage.knowledge_graph_summaries());
            let replication = handler.replication_report(&storage);
            drop(storage);
            let KgTotals {
                knowledge_graphs,
                relations: total_relations,
                views: total_views,
                ..
            } = totals;

            let session_stats = handler.session_stats();
            StatsDto {
                knowledge_graphs,
                relations: total_relations,
                views: total_views,
                memory_usage_bytes: resident_memory_bytes().unwrap_or(0),
                query_count: handler.total_queries(),
                uptime_secs: handler.uptime_seconds(),
                sessions: SessionStatsDto {
                    total: session_stats.total_sessions,
                    clean: session_stats.clean_sessions,
                    dirty: session_stats.dirty_sessions,
                    total_ephemeral_facts: session_stats.total_ephemeral_facts,
                    total_ephemeral_rules: session_stats.total_ephemeral_rules,
                },
                replication,
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

/// Prometheus metrics endpoint (#12).
///
/// Exports server metrics in Prometheus text exposition format. Admin API
/// keys only: the metrics cover every knowledge graph.
pub async fn prometheus_metrics(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<
    (
        StatusCode,
        [(axum::http::HeaderName, axum::http::HeaderValue); 1],
        String,
    ),
    RestError,
> {
    let timeout_secs = handler.config().http.stats_timeout_secs.max(1);
    let body = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        tokio::task::spawn_blocking(move || prometheus_text(&handler)),
    )
    .await
    .map_err(|_| {
        RestError::internal(format!(
            "Metrics computation timed out after {timeout_secs}s"
        ))
    })?
    .map_err(|e| {
        tracing::warn!(error = %e, "Metrics computation failed");
        RestError::internal("Metrics computation failed".to_string())
    })?;

    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
        )],
        body,
    ))
}

/// The `/metrics/prometheus` body.
fn prometheus_text(handler: &Handler) -> String {
    let storage = handler.get_storage();
    let summaries = storage.knowledge_graph_summaries();
    let (kg_loads, kg_unloads) = storage.knowledge_graph_residency_counts();
    let persist = storage.persist_stats();
    let replication = handler.replication_report(&storage);
    drop(storage);
    let totals = KgTotals::of(&summaries);
    let session_stats = handler.session_stats();
    let sessions_by_kg = handler.session_manager().sessions_by_knowledge_graph();
    let subscriptions = handler.subscription_metrics();
    let rate_limit = &handler.config().http.rate_limit;
    let (query_memory_held, query_memory_budget) = handler.query_memory_usage();

    let mut out = Prometheus::new();
    out.single(
        "inputlayer_uptime_seconds",
        "gauge",
        "Server uptime in seconds.",
        handler.uptime_seconds(),
    );
    out.single(
        "inputlayer_queries_total",
        "counter",
        "Total queries executed.",
        handler.total_queries(),
    );

    // Knowledge graphs, totals and per KG
    out.single(
        "inputlayer_knowledge_graphs",
        "gauge",
        "Number of knowledge graphs.",
        totals.knowledge_graphs,
    );
    out.single(
        "inputlayer_knowledge_graphs_loaded",
        "gauge",
        "Knowledge graphs held in memory.",
        totals.loaded_knowledge_graphs,
    );
    out.single(
        "inputlayer_knowledge_graph_loads_total",
        "counter",
        "Knowledge graphs loaded from disk on use.",
        kg_loads,
    );
    out.single(
        "inputlayer_knowledge_graph_unloads_total",
        "counter",
        "Knowledge graphs unloaded from memory.",
        kg_unloads,
    );
    out.single(
        "inputlayer_relations_total",
        "gauge",
        "Total base relations.",
        totals.relations,
    );
    out.single(
        "inputlayer_views_total",
        "gauge",
        "Total derived views (rules).",
        totals.views,
    );
    out.single(
        "inputlayer_tuples_total",
        "gauge",
        "Total stored tuples.",
        totals.tuples,
    );
    let per_kg = |value: fn(&KgSummary) -> usize| {
        summaries
            .iter()
            .map(move |(kg, summary)| (vec![("kg", kg.clone())], value(summary)))
    };
    out.family(
        "inputlayer_kg_relations",
        "gauge",
        "Base relations holding facts, per knowledge graph.",
        per_kg(|s| s.relations),
    );
    out.family(
        "inputlayer_kg_tuples",
        "gauge",
        "Stored tuples, per knowledge graph.",
        per_kg(|s| s.tuples),
    );
    out.family(
        "inputlayer_kg_views",
        "gauge",
        "Derived views (rules), per knowledge graph.",
        per_kg(|s| s.rules),
    );
    out.family(
        "inputlayer_kg_loaded",
        "gauge",
        "1 while the knowledge graph is held in memory, else 0.",
        per_kg(|s| usize::from(s.loaded)),
    );
    let mut sessions_by_kg: Vec<_> = sessions_by_kg.into_iter().collect();
    sessions_by_kg.sort_unstable();
    out.family(
        "inputlayer_kg_sessions",
        "gauge",
        "Open sessions, per knowledge graph they are bound to.",
        sessions_by_kg
            .into_iter()
            .map(|(kg, count)| (vec![("kg", kg)], count)),
    );

    // Memory
    if let Some(rss) = resident_memory_bytes() {
        out.single(
            "inputlayer_memory_usage_bytes",
            "gauge",
            "Resident set size of the server process in bytes.",
            rss,
        );
    }
    if let Some(limit) = crate::execution::memory::container_memory_limit() {
        out.single(
            "inputlayer_memory_limit_bytes",
            "gauge",
            "The memory limit of the server's cgroup (container or systemd unit).",
            limit,
        );
    }
    out.single(
        "inputlayer_query_memory_bytes",
        "gauge",
        "Bytes held by the computations of the requests in flight.",
        query_memory_held.max(0),
    );
    out.single(
        "inputlayer_query_memory_budget_bytes",
        "gauge",
        "Most bytes the computations in flight may hold together (storage.performance.max_total_query_memory_bytes; 0: no limit).",
        query_memory_budget,
    );
    out.single(
        "inputlayer_compute_permits",
        "gauge",
        "Queries that can compute at once; more wait for a permit.",
        handler.compute_permits(),
    );
    out.single(
        "inputlayer_compute_permits_in_use",
        "gauge",
        "Compute permits held by queries running now.",
        handler.compute_permits_in_use(),
    );

    // Sessions
    out.single(
        "inputlayer_sessions_total",
        "gauge",
        "Active sessions.",
        session_stats.total_sessions,
    );
    out.single(
        "inputlayer_sessions_clean",
        "gauge",
        "Sessions with no ephemeral facts or rules.",
        session_stats.clean_sessions,
    );
    out.single(
        "inputlayer_sessions_dirty",
        "gauge",
        "Sessions holding ephemeral facts or rules.",
        session_stats.dirty_sessions,
    );
    out.single(
        "inputlayer_ephemeral_facts",
        "gauge",
        "Total ephemeral facts across sessions.",
        session_stats.total_ephemeral_facts,
    );
    out.single(
        "inputlayer_ephemeral_rules",
        "gauge",
        "Total ephemeral rules across sessions.",
        session_stats.total_ephemeral_rules,
    );

    // Subscriptions
    out.single(
        "inputlayer_subscriptions_active",
        "gauge",
        "Subscriptions registered across all connections.",
        subscriptions.active(),
    );
    out.single(
        "inputlayer_subscription_views",
        "gauge",
        "Shared standing-query views being evaluated.",
        subscriptions.views(),
    );
    out.single(
        "inputlayer_subscription_evaluations_total",
        "counter",
        "Standing-query evaluations (initial snapshots and re-evaluations), one per shared view.",
        subscriptions.evaluations(),
    );
    out.single(
        "inputlayer_subscription_shared_evaluations_total",
        "counter",
        "Evaluations of lifted queries, each serving every view of its family.",
        subscriptions.shared_evaluations(),
    );

    // Write-ahead log and flushes
    out.single(
        "inputlayer_wal_size_bytes",
        "gauge",
        "Bytes in the write-ahead log.",
        persist.wal_bytes,
    );
    out.single(
        "inputlayer_wal_size_limit_bytes",
        "gauge",
        "WAL size that makes the next commit flush every shard (storage.persist.max_wal_size_bytes; 0: no limit).",
        persist.wal_limit_bytes,
    );
    out.single(
        "inputlayer_persist_dirty_shards",
        "gauge",
        "Shards holding committed updates not yet flushed to a batch file.",
        persist.dirty_shards,
    );
    out.single(
        "inputlayer_persist_buffered_updates",
        "gauge",
        "Committed updates not yet flushed to a batch file (they are in the WAL).",
        persist.buffered_updates,
    );
    out.single(
        "inputlayer_persist_oldest_unflushed_seconds",
        "gauge",
        "How long the longest-waiting dirty shard has held unflushed updates (0: none).",
        persist
            .oldest_unflushed
            .map_or(0.0, |age| age.as_secs_f64()),
    );
    out.single(
        "inputlayer_persist_flushes_total",
        "counter",
        "Shard buffers flushed to batch files.",
        persist.flushes,
    );
    out.single(
        "inputlayer_persist_flush_failures_total",
        "counter",
        "Shard flushes that failed (the updates stay in the WAL and are retried).",
        persist.flush_failures,
    );
    out.single(
        "inputlayer_store_read_only",
        "gauge",
        "1 once the store refuses writes until restart after a failed WAL write or fsync, else 0.",
        u8::from(persist.read_only),
    );

    // Connections, requests and rejections
    out.single(
        "inputlayer_http_requests_in_flight_limit",
        "gauge",
        "rate_limit.max_connections (0: no limit).",
        rate_limit.max_connections,
    );
    out.single(
        "inputlayer_ws_connections_limit",
        "gauge",
        "rate_limit.max_ws_connections (0: no limit).",
        rate_limit.max_ws_connections,
    );
    handler.server_metrics().format_prometheus(&mut out);

    out.raw(&crate::execution::view_counters().format_prometheus());
    out.raw(&handler.timing_histograms().format_prometheus());
    if let Some(replication) = &replication {
        out.raw(&replication.format_prometheus());
    }
    out.finish()
}

/// Totals over every knowledge graph, from their summaries: stats must not
/// load knowledge graphs that are not in memory.
#[derive(Debug, Clone, Copy, Default)]
struct KgTotals {
    knowledge_graphs: usize,
    loaded_knowledge_graphs: usize,
    relations: usize,
    views: usize,
    tuples: u64,
}

impl KgTotals {
    fn of(summaries: &[(String, KgSummary)]) -> Self {
        summaries
            .iter()
            .fold(Self::default(), |totals, (_, summary)| Self {
                knowledge_graphs: totals.knowledge_graphs + 1,
                loaded_knowledge_graphs: totals.loaded_knowledge_graphs
                    + usize::from(summary.loaded),
                relations: totals.relations + summary.relations,
                views: totals.views + summary.rules,
                tuples: totals.tuples + summary.tuples as u64,
            })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
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

    /// Memory is the process's resident set, not an estimate from tuples.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_stats_memory_is_resident_set() {
        let (handler, _tmp) = make_handler();
        let result = stats(Extension(handler)).await.unwrap();
        let data = result.0.data.unwrap();
        assert!(
            data.memory_usage_bytes > 1 << 20,
            "{}",
            data.memory_usage_bytes
        );
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

    /// The storage write lock, held by another thread until this is dropped.
    struct HeldWriteLock {
        release: Option<std::sync::mpsc::Sender<()>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for HeldWriteLock {
        fn drop(&mut self) {
            // Closing the channel wakes the thread, also when the test fails.
            drop(self.release.take());
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// Take the storage write lock on another thread; returns once it is held.
    fn hold_storage_write_lock(handler: &Arc<Handler>) -> HeldWriteLock {
        let locked = Arc::new(std::sync::Barrier::new(2));
        let (release, released) = std::sync::mpsc::channel::<()>();
        let thread = {
            let handler = Arc::clone(handler);
            let locked = Arc::clone(&locked);
            std::thread::spawn(move || {
                let _guard = handler.get_storage_mut();
                locked.wait();
                let _ = released.recv();
            })
        };
        locked.wait();
        HeldWriteLock {
            release: Some(release),
            thread: Some(thread),
        }
    }

    /// P2-13 regression: Health check returns degraded/503 when storage lock is contended.
    /// This verifies the health check doesn't hang when a write lock blocks readers.
    #[tokio::test]
    async fn test_health_returns_degraded_when_storage_locked() {
        let (handler, _tmp) = make_handler();

        let _held = hold_storage_write_lock(&handler);

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
    }

    /// Regression: Readiness probe returns 503 when storage lock is contended.
    /// Mirrors test_health_returns_degraded_when_storage_locked but for the /ready endpoint.
    #[tokio::test]
    async fn test_readiness_returns_503_when_storage_locked() {
        let (handler, _tmp) = make_handler();

        let _held = hold_storage_write_lock(&handler);

        // Readiness should return 503 when lock is contended
        let status = readiness(Extension(handler)).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "Readiness should return 503 when storage lock is contended"
        );
    }

    #[tokio::test]
    async fn test_prometheus_metrics_format() {
        let (handler, _tmp) = make_handler();
        let (status, _headers, body) = prometheus_metrics(Extension(handler)).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("# HELP inputlayer_uptime_seconds"));
        assert!(body.contains("# TYPE inputlayer_uptime_seconds gauge"));
        assert!(body.contains("inputlayer_queries_total 0"));
        assert!(body.contains("inputlayer_knowledge_graphs"));
        assert!(body.contains("inputlayer_sessions_total 0"));
        for counter in [
            "inputlayer_view_reads_total",
            "inputlayer_rule_evaluations_total",
            "inputlayer_view_maintenance_us_total",
        ] {
            assert!(
                body.contains(&format!("# TYPE {counter} counter\n{counter} ")),
                "{counter} is exported"
            );
        }
    }

    /// Every sample belongs to a family declared once with HELP and TYPE.
    #[tokio::test]
    async fn test_prometheus_exposition_is_well_formed() {
        let (handler, _tmp) = make_handler();
        let (_, _, body) = prometheus_metrics(Extension(handler)).await.unwrap();
        let mut declared = std::collections::HashSet::new();
        let mut kinds = std::collections::HashMap::new();
        for line in body.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest.split(' ').next().unwrap();
                assert!(declared.insert(name.to_string()), "{name} declared twice");
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').unwrap();
                kinds.insert(name.to_string(), kind.to_string());
            } else {
                let (series, value) = line.rsplit_once(' ').unwrap();
                assert!(value.parse::<f64>().is_ok(), "{line}");
                let name = series.split('{').next().unwrap();
                let family = ["_bucket", "_sum", "_count"]
                    .iter()
                    .find_map(|suffix| {
                        name.strip_suffix(suffix)
                            .filter(|base| kinds.get(*base).is_some_and(|k| k == "histogram"))
                    })
                    .unwrap_or(name);
                assert!(declared.contains(family), "undeclared: {line}");
            }
        }
    }

    /// The blind spots of #300: subscriptions, WAL and flushes, the store's
    /// write state, per-KG counts, memory, compute and connections.
    #[tokio::test]
    async fn test_prometheus_metrics_cover_operations() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(None, "+ops_test[(1, 2), (3, 4)]".to_string())
            .await
            .unwrap();
        let (_, _, body) = prometheus_metrics(Extension(handler)).await.unwrap();
        for line in [
            "inputlayer_kg_tuples{kg=\"default\"} 2",
            "inputlayer_kg_relations{kg=\"default\"} 1",
            "inputlayer_kg_loaded{kg=\"default\"} 1",
            "inputlayer_subscriptions_active 0",
            "inputlayer_subscription_views 0",
            "inputlayer_persist_dirty_shards 1",
            "inputlayer_persist_buffered_updates 2",
            "inputlayer_persist_flush_failures_total 0",
            "inputlayer_store_read_only 0",
            "inputlayer_compute_permits_in_use 0",
            "inputlayer_ws_connections 0",
            "inputlayer_rejections_total{reason=\"ws_connection_limit\"} 0",
            "inputlayer_auth_failures_total{method=\"password\"} 0",
        ] {
            assert!(body.lines().any(|l| l == line), "missing {line}:\n{body}");
        }
        for family in [
            "inputlayer_wal_size_bytes ",
            "inputlayer_persist_oldest_unflushed_seconds ",
            "inputlayer_query_memory_bytes ",
            "inputlayer_compute_permits ",
            "inputlayer_http_requests_in_flight ",
            "inputlayer_ws_connections_limit 1024",
        ] {
            assert!(
                body.lines().any(|l| l.starts_with(family)),
                "missing {family}:\n{body}"
            );
        }
        #[cfg(target_os = "linux")]
        assert!(body
            .lines()
            .any(|l| l.starts_with("inputlayer_memory_usage_bytes ")));
    }

    #[tokio::test]
    async fn test_prometheus_metrics_after_query() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(None, "+prom_test[(1, 2)]".to_string())
            .await
            .unwrap();
        let (status, _headers, body) = prometheus_metrics(Extension(handler)).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("inputlayer_queries_total 1"));
        assert!(body.contains("inputlayer_tuples_total"));
    }
}
