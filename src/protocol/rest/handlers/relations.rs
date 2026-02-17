//! Relations Handlers
//!
//! Endpoints for relation data access.

use std::sync::Arc;

use axum::{
    extract::{Path, Query},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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
    let query = format!("?{}({})", name, vars.join(", "));

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

/// Query parameters for prefix-based relation clearing
#[derive(Debug, Deserialize)]
pub struct ClearPrefixQuery {
    pub prefix: String,
}

/// Result of prefix-based relation clearing
#[derive(Debug, Serialize, ToSchema)]
pub struct ClearByPrefixResult {
    /// Relations that were cleared, with the number of facts deleted from each
    pub cleared: Vec<ClearedRelation>,
    /// Total number of facts deleted
    pub total_deleted: usize,
}

/// A single relation that was cleared
#[derive(Debug, Serialize, ToSchema)]
pub struct ClearedRelation {
    pub name: String,
    pub deleted: usize,
}

/// Clear all facts from relations matching a prefix
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{kg}/relations",
    tag = "relations",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("prefix" = String, Query, description = "Prefix to match relation names against")
    ),
    responses(
        (status = 200, description = "Relations cleared", body = ApiResponse<ClearByPrefixResult>),
        (status = 400, description = "Invalid request (empty prefix)"),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn clear_relations_by_prefix(
    Extension(handler): Extension<Arc<Handler>>,
    Path(kg): Path<String>,
    Query(params): Query<ClearPrefixQuery>,
) -> Result<Json<ApiResponse<ClearByPrefixResult>>, RestError> {
    if params.prefix.is_empty() {
        return Err(RestError::bad_request("Prefix cannot be empty".to_string()));
    }

    let results = handler
        .clear_relations_by_prefix_in(&kg, &params.prefix)
        .map_err(|e| RestError::internal(format!("Failed to clear relations by prefix: {e}")))?;

    let total_deleted: usize = results.iter().map(|(_, c)| c).sum();
    let cleared: Vec<ClearedRelation> = results
        .into_iter()
        .map(|(name, deleted)| ClearedRelation { name, deleted })
        .collect();

    Ok(Json(ApiResponse::success(ClearByPrefixResult {
        cleared,
        total_deleted,
    })))
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

    #[test]
    fn test_generate_variables_small() {
        let vars = generate_variables(3);
        assert_eq!(vars, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_generate_variables_zero() {
        let vars = generate_variables(0);
        assert!(vars.is_empty());
    }

    #[test]
    fn test_generate_variables_26() {
        let vars = generate_variables(26);
        assert_eq!(vars[0], "A");
        assert_eq!(vars[25], "Z");
    }

    #[test]
    fn test_generate_variables_over_26() {
        let vars = generate_variables(28);
        assert_eq!(vars[26], "A1");
        assert_eq!(vars[27], "B1");
    }

    #[tokio::test]
    async fn test_list_relations_empty() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("list_rel_kg")
            .unwrap();
        let result = list_relations(Extension(handler), Path("list_rel_kg".to_string()))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(data.relations.is_empty());
    }

    #[tokio::test]
    async fn test_list_relations_with_data() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("list_rel_d_kg".to_string()),
                "+alpha[(1, 2)]\n+beta[(3,)]".to_string(),
            )
            .await
            .unwrap();
        let result = list_relations(Extension(handler), Path("list_rel_d_kg".to_string()))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.relations.len(), 2);
    }

    #[tokio::test]
    async fn test_get_relation() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("get_rel_d_kg".to_string()),
                "+edge[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .unwrap();
        let result = get_relation(
            Extension(handler),
            Path(("get_rel_d_kg".to_string(), "edge".to_string())),
        )
        .await
        .unwrap();
        let rel = result.0.data.unwrap();
        assert_eq!(rel.name, "edge");
        assert_eq!(rel.arity, 2);
        assert_eq!(rel.tuple_count, 2);
        assert!(!rel.is_view);
    }

    #[tokio::test]
    async fn test_get_relation_not_found() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("get_rel_nf_kg")
            .unwrap();
        let result = get_relation(
            Extension(handler),
            Path(("get_rel_nf_kg".to_string(), "missing".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_relation_data() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("rel_data_kg".to_string()),
                "+points[(1,), (2,), (3,)]".to_string(),
            )
            .await
            .unwrap();
        let result = get_relation_data(
            Extension(handler),
            Path(("rel_data_kg".to_string(), "points".to_string())),
            Query(RelationDataQuery {
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.name, "points");
        assert_eq!(data.total_count, 3);
        assert_eq!(data.row_count, 3);
    }

    #[tokio::test]
    async fn test_get_relation_data_with_pagination() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("rel_page_kg".to_string()),
                "+nums[(1,), (2,), (3,), (4,), (5,)]".to_string(),
            )
            .await
            .unwrap();
        let result = get_relation_data(
            Extension(handler),
            Path(("rel_page_kg".to_string(), "nums".to_string())),
            Query(RelationDataQuery {
                offset: Some(1),
                limit: Some(2),
            }),
        )
        .await
        .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.total_count, 5);
        assert_eq!(data.row_count, 2);
        assert_eq!(data.offset, Some(1));
        assert_eq!(data.limit, Some(2));
    }

    #[tokio::test]
    async fn test_clear_relations_by_prefix() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("clear_pfx_kg".to_string()),
                "+env_a[(1,), (2,)]\n+env_b[(3,)]\n+other[(4,)]".to_string(),
            )
            .await
            .unwrap();
        let result = clear_relations_by_prefix(
            Extension(handler),
            Path("clear_pfx_kg".to_string()),
            Query(ClearPrefixQuery {
                prefix: "env_".to_string(),
            }),
        )
        .await
        .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.total_deleted, 3);
        assert_eq!(data.cleared.len(), 2);
        assert!(data
            .cleared
            .iter()
            .any(|c| c.name == "env_a" && c.deleted == 2));
        assert!(data
            .cleared
            .iter()
            .any(|c| c.name == "env_b" && c.deleted == 1));
    }

    #[tokio::test]
    async fn test_clear_relations_by_prefix_no_match() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("clear_nomatch_kg".to_string()),
                "+alpha[(1,)]".to_string(),
            )
            .await
            .unwrap();
        let result = clear_relations_by_prefix(
            Extension(handler),
            Path("clear_nomatch_kg".to_string()),
            Query(ClearPrefixQuery {
                prefix: "zzz_".to_string(),
            }),
        )
        .await
        .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.total_deleted, 0);
        assert!(data.cleared.is_empty());
    }

    #[tokio::test]
    async fn test_clear_relations_preserves_other() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("clear_preserve_kg".to_string()),
                "+env_data[(1,), (2,)]\n+keep_data[(10,)]".to_string(),
            )
            .await
            .unwrap();
        // Clear env_ prefix
        let _ = clear_relations_by_prefix(
            Extension(handler.clone()),
            Path("clear_preserve_kg".to_string()),
            Query(ClearPrefixQuery {
                prefix: "env_".to_string(),
            }),
        )
        .await
        .unwrap();
        // Verify other relation still has data
        let result = get_relation(
            Extension(handler),
            Path(("clear_preserve_kg".to_string(), "keep_data".to_string())),
        )
        .await
        .unwrap();
        let rel = result.0.data.unwrap();
        assert_eq!(rel.tuple_count, 1);
    }

    #[tokio::test]
    async fn test_clear_relations_empty_prefix_rejected() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("clear_empty_pfx_kg")
            .unwrap();
        let result = clear_relations_by_prefix(
            Extension(handler),
            Path("clear_empty_pfx_kg".to_string()),
            Query(ClearPrefixQuery {
                prefix: String::new(),
            }),
        )
        .await;
        assert!(result.is_err());
    }
}
