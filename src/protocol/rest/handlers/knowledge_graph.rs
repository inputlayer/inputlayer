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
    let current_name = storage.current_knowledge_graph().map(|s| s.to_string());

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
                Some(format!("Knowledge graph '{}' not found", name)),
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
    let mut storage = handler.get_storage_mut();

    // Check if knowledge graph exists by trying to switch to it
    storage.use_knowledge_graph(&name).map_err(|e| {
        RestError::not_found(format!("Knowledge graph '{}' not found: {}", name, e))
    })?;

    // Get relations count
    let relations_count = storage.list_relations().map(|r| r.len()).unwrap_or(0);

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
    let mut storage = handler.get_storage_mut();

    storage
        .create_knowledge_graph(&request.name)
        .map_err(|e| RestError::internal(format!("{}", e)))?;

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

    storage.drop_knowledge_graph(&name).map_err(|e| {
        RestError::not_found(format!("Knowledge graph '{}' not found: {}", name, e))
    })?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
    }))
}
