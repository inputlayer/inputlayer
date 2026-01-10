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
    let knowledge_graphs = storage.list_knowledge_graphs().len();
    drop(storage);

    let stats = StatsDto {
        knowledge_graphs,
        relations: 0,
        views: 0,
        memory_usage_bytes: 0,
        query_count: handler.total_queries(),
        uptime_secs: handler.uptime_seconds(),
    };

    Ok(Json(ApiResponse::success(stats)))
}
