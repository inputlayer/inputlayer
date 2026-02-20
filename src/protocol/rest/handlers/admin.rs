//! Admin Handlers
//!
//! Health check and statistics endpoints.

use std::sync::Arc;

use axum::{Extension, Json};

use crate::protocol::rest::dto::{ApiResponse, HealthDto, SessionStatsDto, StatsDto};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// Health check endpoint
pub async fn health(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<Json<ApiResponse<HealthDto>>, RestError> {
    let health = HealthDto {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: handler.uptime_seconds(),
    };

    Ok(Json(ApiResponse::success(health)))
}

/// Server statistics endpoint
pub async fn stats(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<Json<ApiResponse<StatsDto>>, RestError> {
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
    let estimated_memory = total_tuples * 64;

    drop(storage);

    let session_stats = handler.session_stats();
    let stats = StatsDto {
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
    };

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
        let result = health(Extension(handler)).await.unwrap();
        let resp = result.0;
        assert!(resp.success);
        let data = resp.data.unwrap();
        assert_eq!(data.status, "healthy");
        assert!(!data.version.is_empty());
    }

    #[tokio::test]
    async fn test_health_uptime_is_reasonable() {
        let (handler, _tmp) = make_handler();
        let result = health(Extension(handler)).await.unwrap();
        let data = result.0.data.unwrap();
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
}
