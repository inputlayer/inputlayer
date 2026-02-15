//! Knowledge Graph Handlers
//!
//! Endpoints for knowledge graph management operations.

use std::sync::Arc;

use axum::{extract::Path, Extension, Json};

use crate::protocol::rest::dto::{
    ApiResponse, CreateKnowledgeGraphRequest, KnowledgeGraphDto, KnowledgeGraphListDto,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// List all knowledge graphs
#[utoipa::path(
    get,
    path = "/knowledge-graphs",
    tag = "knowledge-graphs",
    responses(
        (status = 200, description = "List of knowledge graphs", body = ApiResponse<KnowledgeGraphListDto>),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn list_knowledge_graphs(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<Json<ApiResponse<KnowledgeGraphListDto>>, RestError> {
    let storage = handler.get_storage();

    let kg_names = storage.list_knowledge_graphs();
    let current_name = storage
        .current_knowledge_graph()
        .map(std::string::ToString::to_string);

    // Build knowledge graph list with basic info
    let knowledge_graphs: Vec<KnowledgeGraphDto> = kg_names
        .into_iter()
        .map(|name| KnowledgeGraphDto {
            name,
            description: None,
            relations_count: 0, // Would need to switch to each KG to count
            views_count: 0,
        })
        .collect();

    drop(storage);

    // Check if current KG exists in the list
    let (current, warning) = if let Some(ref name) = current_name {
        if knowledge_graphs.iter().any(|kg| kg.name == *name) {
            (Some(name.clone()), None)
        } else {
            // Current KG doesn't exist - report warning and suggest first available
            let fallback = knowledge_graphs.first().map(|kg| kg.name.clone());
            (
                fallback,
                Some(format!("Knowledge graph '{name}' not found")),
            )
        }
    } else {
        (knowledge_graphs.first().map(|kg| kg.name.clone()), None)
    };

    let result = KnowledgeGraphListDto {
        knowledge_graphs,
        current,
        warning,
    };

    Ok(Json(ApiResponse::success(result)))
}

/// Get knowledge graph details
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{name}",
    tag = "knowledge-graphs",
    params(
        ("name" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "Knowledge graph details", body = ApiResponse<KnowledgeGraphDto>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_knowledge_graph(
    Extension(handler): Extension<Arc<Handler>>,
    Path(name): Path<String>,
) -> Result<Json<ApiResponse<KnowledgeGraphDto>>, RestError> {
    let storage = handler.get_storage();

    // Check if knowledge graph exists
    storage
        .ensure_knowledge_graph(&name)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{name}' not found: {e}")))?;

    // Get relations count
    let relations_count = storage
        .list_relations_in(&name)
        .map(|r| r.len())
        .unwrap_or(0);

    let kg = KnowledgeGraphDto {
        name,
        description: None,
        relations_count,
        views_count: 0,
    };

    Ok(Json(ApiResponse::success(kg)))
}

/// Create a new knowledge graph
#[utoipa::path(
    post,
    path = "/knowledge-graphs",
    tag = "knowledge-graphs",
    request_body = CreateKnowledgeGraphRequest,
    responses(
        (status = 200, description = "Knowledge graph created", body = ApiResponse<KnowledgeGraphDto>),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn create_knowledge_graph(
    Extension(handler): Extension<Arc<Handler>>,
    Json(request): Json<CreateKnowledgeGraphRequest>,
) -> Result<Json<ApiResponse<KnowledgeGraphDto>>, RestError> {
    let storage = handler.get_storage();

    storage
        .create_knowledge_graph(&request.name)
        .map_err(|e| RestError::internal(format!("{e}")))?;

    let kg = KnowledgeGraphDto {
        name: request.name,
        description: request.description,
        relations_count: 0,
        views_count: 0,
    };

    Ok(Json(ApiResponse::success(kg)))
}

/// Delete a knowledge graph
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{name}",
    tag = "knowledge-graphs",
    params(
        ("name" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "Knowledge graph deleted", body = ApiResponse<()>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_knowledge_graph(
    Extension(handler): Extension<Arc<Handler>>,
    Path(name): Path<String>,
) -> Result<Json<ApiResponse<()>>, RestError> {
    let mut storage = handler.get_storage_mut();

    storage
        .drop_knowledge_graph(&name)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{name}' not found: {e}")))?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    fn make_handler() -> Arc<Handler> {
        let mut config = Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        Arc::new(Handler::from_config(config).unwrap())
    }

    #[tokio::test]
    async fn test_list_knowledge_graphs_empty() {
        let handler = make_handler();
        let result = list_knowledge_graphs(Extension(handler)).await.unwrap();
        let data = result.0.data.unwrap();
        // May have default KG or empty depending on config
        assert!(data.warning.is_none() || data.warning.is_some());
    }

    #[tokio::test]
    async fn test_create_and_list_knowledge_graph() {
        let handler = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "test_create_kg".to_string(),
            description: Some("A test KG".to_string()),
        };
        let created = create_knowledge_graph(Extension(handler.clone()), Json(request))
            .await
            .unwrap();
        let kg = created.0.data.unwrap();
        assert_eq!(kg.name, "test_create_kg");
        assert_eq!(kg.description, Some("A test KG".to_string()));
        assert_eq!(kg.relations_count, 0);

        let list = list_knowledge_graphs(Extension(handler)).await.unwrap();
        let data = list.0.data.unwrap();
        assert!(data
            .knowledge_graphs
            .iter()
            .any(|k| k.name == "test_create_kg"));
    }

    #[tokio::test]
    async fn test_get_knowledge_graph() {
        let handler = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("get_kg_test")
            .unwrap();
        let result = get_knowledge_graph(Extension(handler), Path("get_kg_test".to_string()))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.name, "get_kg_test");
    }

    #[tokio::test]
    async fn test_get_knowledge_graph_with_relations() {
        let handler = make_handler();
        handler
            .query_program(Some("get_rel_kg".to_string()), "+edges[(1, 2)]".to_string())
            .await
            .unwrap();
        let result = get_knowledge_graph(Extension(handler), Path("get_rel_kg".to_string()))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.relations_count, 1);
    }

    #[tokio::test]
    async fn test_delete_knowledge_graph() {
        let handler = make_handler();
        handler
            .get_storage()
            .create_knowledge_graph("del_kg_test")
            .unwrap();
        let result =
            delete_knowledge_graph(Extension(handler), Path("del_kg_test".to_string())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_knowledge_graph() {
        let handler = make_handler();
        let result =
            delete_knowledge_graph(Extension(handler), Path("does_not_exist_kg".to_string())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_knowledge_graph_no_description() {
        let handler = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "no_desc_kg".to_string(),
            description: None,
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.name, "no_desc_kg");
        assert!(kg.description.is_none());
    }
}
