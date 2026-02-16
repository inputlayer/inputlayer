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
pub async fn get_view(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<ViewDto>>, RestError> {
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    let description = storage
        .describe_rule_in(&kg, &name)
        .map_err(|e| RestError::internal(format!("Failed to get view: {e}")))?
        .ok_or_else(|| RestError::not_found(format!("View '{name}' not found")))?;

    let arity = storage
        .rule_arity_in(&kg, &name)
        .ok()
        .flatten()
        .unwrap_or(0);

    let view = ViewDto {
        name,
        definition: description,
        arity,
        columns: vec![],
        dependencies: vec![],
    };

    Ok(Json(ApiResponse::success(view)))
}

/// Get view data with pagination
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/views/{name}/data",
    tag = "views",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "View name"),
        ("offset" = Option<usize>, Query, description = "Offset for pagination"),
        ("limit" = Option<usize>, Query, description = "Limit for pagination")
    ),
    responses(
        (status = 200, description = "View data", body = ApiResponse<RelationDataDto>),
        (status = 404, description = "View not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_view_data(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
    Query(query_params): Query<RelationDataQuery>,
) -> Result<Json<ApiResponse<RelationDataDto>>, RestError> {
    // Get the arity of the view to build the correct query
    let arity = {
        let storage = handler.get_storage();
        storage
            .ensure_knowledge_graph(&kg)
            .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

        storage
            .rule_arity_in(&kg, &name)
            .map_err(|e| RestError::internal(format!("Failed to get view arity: {e}")))?
            .ok_or_else(|| RestError::not_found(format!("View '{name}' not found")))?
    };

    // Build query with correct number of variables (A, B, C, ...)
    let vars: Vec<String> = (0..arity)
        .map(|i| ((b'A' + i as u8) as char).to_string())
        .collect();
    let query = format!("?{}({})", name, vars.join(", "));

    let result = handler
        .query_program(Some(kg.clone()), query)
        .await
        .map_err(|e| RestError::not_found(format!("View '{name}' not found: {e:?}")))?;

    let total_count = result.rows.len();
    let offset = query_params.offset.unwrap_or(0);
    let limit = query_params.limit.unwrap_or(1000);

    // Get columns from schema
    let columns: Vec<String> = result.schema.iter().map(|c| c.name.clone()).collect();

    // Apply pagination
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

/// Create a new view
#[utoipa::path(
    post,
    path = "/knowledge-graphs/{kg}/views",
    tag = "views",
    params(
        ("kg" = String, Path, description = "Knowledge graph name")
    ),
    request_body = CreateViewRequest,
    responses(
        (status = 200, description = "View created", body = ApiResponse<ViewDto>),
        (status = 400, description = "Invalid view definition"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn create_view(
    Extension(handler): Extension<Arc<Handler>>,
    Path(kg): Path<String>,
    Json(request): Json<CreateViewRequest>,
) -> Result<Json<ApiResponse<ViewDto>>, RestError> {
    // Register the view by executing the rule as a query
    let rule_text = format!("+{}", request.definition);
    handler
        .query_program(Some(kg.clone()), rule_text)
        .await
        .map_err(|e| RestError::bad_request(format!("{e:?}")))?;

    // Get the arity of the newly created view
    let arity = {
        let storage = handler.get_storage();
        storage
            .rule_arity_in(&kg, &request.name)
            .ok()
            .flatten()
            .unwrap_or(0)
    };

    // Return the created view
    let view = ViewDto {
        name: request.name,
        definition: request.definition,
        arity,
        columns: vec![],
        dependencies: vec![],
    };

    Ok(Json(ApiResponse::success(view)))
}

/// Delete a view
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{kg}/views/{name}",
    tag = "views",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "View name")
    ),
    responses(
        (status = 200, description = "View deleted", body = ApiResponse<()>),
        (status = 404, description = "View not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_view(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<()>>, RestError> {
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    storage
        .drop_rule_in(&kg, &name)
        .map_err(|e| RestError::not_found(format!("View '{name}' not found: {e}")))?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::rest::dto::CreateViewRequest;
    use crate::Config;

    fn make_handler() -> Arc<Handler> {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.data_dir = temp_dir.into_path();
        Arc::new(Handler::from_config(config).unwrap())
    }

    #[tokio::test]
    async fn test_list_views_empty() {
        let handler = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("views_empty_kg")
            .unwrap();
        let result = list_views(Extension(handler), Path("views_empty_kg".to_string()))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(data.views.is_empty());
    }

    #[tokio::test]
    async fn test_create_and_list_view() {
        let handler = make_handler();
        handler
            .query_program(
                Some("views_create_kg".to_string()),
                "+base[(1,), (2,)]".to_string(),
            )
            .await
            .unwrap();
        let request = CreateViewRequest {
            name: "doubled".to_string(),
            definition: "doubled(X, Y) <- base(X), Y = X * 2".to_string(),
        };
        let created = create_view(
            Extension(handler.clone()),
            Path("views_create_kg".to_string()),
            Json(request),
        )
        .await
        .unwrap();
        let view = created.0.data.unwrap();
        assert_eq!(view.name, "doubled");
        assert_eq!(view.arity, 2);

        let list = list_views(Extension(handler), Path("views_create_kg".to_string()))
            .await
            .unwrap();
        let data = list.0.data.unwrap();
        assert!(data.views.iter().any(|v| v.name == "doubled"));
    }

    #[tokio::test]
    async fn test_get_view() {
        let handler = make_handler();
        handler
            .query_program(
                Some("views_get_kg".to_string()),
                "+src[(1,)]\n+my_view(X) <- src(X)".to_string(),
            )
            .await
            .unwrap();
        let result = get_view(
            Extension(handler),
            Path(("views_get_kg".to_string(), "my_view".to_string())),
        )
        .await
        .unwrap();
        let view = result.0.data.unwrap();
        assert_eq!(view.name, "my_view");
        assert_eq!(view.arity, 1);
    }

    #[tokio::test]
    async fn test_get_view_not_found() {
        let handler = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("views_nf_kg")
            .unwrap();
        let result = get_view(
            Extension(handler),
            Path(("views_nf_kg".to_string(), "ghost".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_view_data() {
        let handler = make_handler();
        handler
            .query_program(
                Some("views_data_kg".to_string()),
                "+numbers[(1,), (2,), (3,)]\n+tripled(X, Y) <- numbers(X), Y = X * 3".to_string(),
            )
            .await
            .unwrap();
        let result = get_view_data(
            Extension(handler),
            Path(("views_data_kg".to_string(), "tripled".to_string())),
            Query(RelationDataQuery {
                offset: None,
                limit: None,
            }),
        )
        .await
        .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.total_count, 3);
        assert_eq!(data.row_count, 3);
    }

    #[tokio::test]
    async fn test_delete_view() {
        let handler = make_handler();
        handler
            .query_program(
                Some("views_del_kg".to_string()),
                "+src[(1,)]\n+del_view(X) <- src(X)".to_string(),
            )
            .await
            .unwrap();
        let result = delete_view(
            Extension(handler),
            Path(("views_del_kg".to_string(), "del_view".to_string())),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_view_not_found() {
        let handler = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("views_delnf_kg")
            .unwrap();
        let result = delete_view(
            Extension(handler),
            Path(("views_delnf_kg".to_string(), "missing".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_view_invalid_definition() {
        let handler = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("views_inv_kg")
            .unwrap();
        let request = CreateViewRequest {
            name: "bad_view".to_string(),
            definition: "totally invalid!!!".to_string(),
        };
        let result = create_view(
            Extension(handler),
            Path("views_inv_kg".to_string()),
            Json(request),
        )
        .await;
        assert!(result.is_err());
    }
}
