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
fn generate_variables(arity: usize) -> Vec<String> {
    (0..arity)
        .map(|i| {
            let letter = (b'A' + (i % 26) as u8) as char;
            let suffix = i / 26;
            if suffix == 0 {
                letter.to_string()
            } else {
                format!("{letter}{suffix}")
            }
        })
        .collect()
}

/// Get relation data with pagination
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/relations/{name}/data",
    tag = "relations",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Relation name"),
        ("offset" = Option<usize>, Query, description = "Offset for pagination"),
        ("limit" = Option<usize>, Query, description = "Limit for pagination")
    ),
    responses(
        (status = 200, description = "Relation data", body = ApiResponse<RelationDataDto>),
        (status = 404, description = "Relation not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_relation_data(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
    Query(query_params): Query<RelationDataQuery>,
) -> Result<Json<ApiResponse<RelationDataDto>>, RestError> {
    // First get the relation metadata to determine arity (in a block to drop the guard before await)
    let arity = {
        let storage = handler.get_storage();
        let (schema, _tuple_count) = storage
            .get_relation_metadata_in(&kg, &name)
            .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?
            .ok_or_else(|| RestError::not_found(format!("Relation '{name}' not found")))?;
        schema.len()
    };

    if arity == 0 {
        // Empty relation - return empty result
        return Ok(Json(ApiResponse::success(RelationDataDto {
            name,
            columns: vec![],
            rows: vec![],
            row_count: 0,
            total_count: 0,
            offset: query_params.offset,
            limit: query_params.limit,
        })));
    }

    // Generate query with correct arity
    let vars = generate_variables(arity);
    let query = format!("?- {}({}).", name, vars.join(", "));

    let result = handler
        .query_program(Some(kg.clone()), query)
        .await
        .map_err(|e| RestError::internal(format!("Query failed: {e:?}")))?;

    let total_count = result.rows.len();
    let offset = query_params.offset.unwrap_or(0);
    let limit = query_params.limit.unwrap_or(1000);

    let columns: Vec<String> = result.schema.iter().map(|c| c.name.clone()).collect();

    let rows: Vec<Vec<serde_json::Value>> = result
        .rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
        .collect();

    let row_count = rows.len();

    let data = RelationDataDto {
        name,
        columns,
        rows,
        row_count,
        total_count,
        offset: query_params.offset,
        limit: query_params.limit,
    };

    Ok(Json(ApiResponse::success(data)))
}
