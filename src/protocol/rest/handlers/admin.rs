//! Admin Handlers
//!
//! Health check and statistics endpoints.

use std::sync::Arc;

use axum::{Extension, Json};

use crate::protocol::rest::dto::{ApiResponse, HealthDto, StatsDto};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    tag = "admin",
    responses(
        (status = 200, description = "Server is healthy", body = ApiResponse<HealthDto>),
    )
)]
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
#[utoipa::path(
    get,
    path = "/stats",
    tag = "admin",
    responses(
        (status = 200, description = "Server statistics", body = ApiResponse<StatsDto>),
    )
)]
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

    let stats = StatsDto {
        knowledge_graphs,
        relations: total_relations,
        views: total_views,
        memory_usage_bytes: estimated_memory,
        query_count: handler.total_queries(),
        uptime_secs: handler.uptime_seconds(),
    };

    Ok(Json(ApiResponse::success(stats)))
}
