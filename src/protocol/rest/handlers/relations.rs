//! Relations Handlers
//!
//! Endpoints for relation data access.

use std::sync::Arc;

use axum::{
    extract::{Path, Query},
    Extension, Json,
};

use super::wire_value_to_json;
use crate::protocol::rest::dto::{
    ApiResponse, RelationDataDto, RelationDataQuery, RelationDto, RelationListDto,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// List all relations in a knowledge graph
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/relations",
    tag = "relations",
    params(
        ("kg" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "List of relations", body = ApiResponse<RelationListDto>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn list_relations(
    Extension(handler): Extension<Arc<Handler>>,
    Path(kg): Path<String>,
) -> Result<Json<ApiResponse<RelationListDto>>, RestError> {
    let storage = handler.get_storage();

    let relations_meta = storage
        .list_relations_with_metadata(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    let relations: Vec<RelationDto> = relations_meta
        .into_iter()
        .map(|(name, schema, tuple_count)| RelationDto {
            name,
            arity: schema.len(),
            tuple_count,
            columns: schema,
            is_view: false,
        })
        .collect();

    Ok(Json(ApiResponse::success(RelationListDto { relations })))
}

/// Get relation details
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/relations/{name}",
    tag = "relations",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Relation name")
    ),
    responses(
        (status = 200, description = "Relation details", body = ApiResponse<RelationDto>),
        (status = 404, description = "Relation not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_relation(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<RelationDto>>, RestError> {
    let storage = handler.get_storage();

    let (schema, tuple_count) = storage
        .get_relation_metadata_in(&kg, &name)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?
        .ok_or_else(|| RestError::not_found(format!("Relation '{name}' not found")))?;

    let relation = RelationDto {
        name,
        arity: schema.len(),
        tuple_count,
        columns: schema,
        is_view: false,
    };

    Ok(Json(ApiResponse::success(relation)))
}

/// Generate variable names for a given arity (A, B, C, ..., Z, A1, B1, ...)
