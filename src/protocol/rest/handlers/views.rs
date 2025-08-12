//! Views Handlers
//!
//! Endpoints for view management operations.

use std::sync::Arc;

use axum::{
    extract::{Path, Query},
    Extension, Json,
};

use super::wire_value_to_json;
use crate::protocol::rest::dto::{
    ApiResponse, CreateViewRequest, RelationDataDto, RelationDataQuery, ViewDto, ViewListDto,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// List all views in a knowledge graph
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/views",
    tag = "views",
    params(
        ("kg" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "List of views", body = ApiResponse<ViewListDto>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn list_views(
    Extension(handler): Extension<Arc<Handler>>,
    Path(kg): Path<String>,
) -> Result<Json<ApiResponse<ViewListDto>>, RestError> {
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    let rule_names = storage
        .list_rules_in(&kg)
        .map_err(|e| RestError::internal(format!("Failed to list rules: {e}")))?;

    let views: Vec<ViewDto> = rule_names
        .into_iter()
        .map(|name| {
            let description = storage
                .describe_rule_in(&kg, &name)
                .ok()
                .flatten()
                .unwrap_or_default();
            let arity = storage
                .rule_arity_in(&kg, &name)
                .ok()
                .flatten()
                .unwrap_or(0);
            ViewDto {
                name,
                definition: description,
                arity,
                columns: vec![],
                dependencies: vec![],
            }
        })
        .collect();

    Ok(Json(ApiResponse::success(ViewListDto { views })))
}

/// Get view details
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/views/{name}",
    tag = "views",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "View name")
    ),
    responses(
        (status = 200, description = "View details", body = ApiResponse<ViewDto>),
        (status = 404, description = "View not found"),
        (status = 500, description = "Internal server error"),
    )
)]
