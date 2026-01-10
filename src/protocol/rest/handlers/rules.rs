//! Rules Handlers
//!
//! Endpoints for persistent rule (policy) management.

use std::sync::Arc;

use axum::{extract::Path, Extension, Json};
use serde::Serialize;
use utoipa::ToSchema;

use crate::protocol::rest::dto::ApiResponse;
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

/// Rule information
#[derive(Debug, Serialize, ToSchema)]
pub struct RuleDto {
    /// Rule name (relation name)
    pub name: String,
    /// Number of clauses defining this rule
    pub clause_count: usize,
    /// Human-readable description of the rule
    pub description: String,
}

/// List of rules
#[derive(Debug, Serialize, ToSchema)]
pub struct RuleListDto {
    pub rules: Vec<RuleDto>,
}

/// List all rules in a knowledge graph
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/rules",
    tag = "rules",
    params(
        ("kg" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "List of rules", body = ApiResponse<RuleListDto>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn list_rules(
    Extension(handler): Extension<Arc<Handler>>,
    Path(kg): Path<String>,
) -> Result<Json<ApiResponse<RuleListDto>>, RestError> {
    let mut storage = handler.get_storage_mut();

    // Switch to target knowledge graph
    storage
        .use_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{}' not found: {}", kg, e)))?;

    let rule_names = storage
        .list_rules()
        .map_err(|e| RestError::internal(format!("Failed to list rules: {}", e)))?;
    let mut rules = Vec::new();

    for name in rule_names {
        let clause_count = storage.rule_count(&name).ok().flatten().unwrap_or(0);
        let description = storage
            .describe_rule(&name)
            .ok()
            .flatten()
            .unwrap_or_default();
        rules.push(RuleDto {
            name,
            clause_count,
            description,
        });
    }

    Ok(Json(ApiResponse::success(RuleListDto { rules })))
}

/// Get rule details
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{kg}/rules/{name}",
    tag = "rules",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Rule name")
    ),
    responses(
        (status = 200, description = "Rule details", body = ApiResponse<RuleDto>),
        (status = 404, description = "Rule not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_rule(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<RuleDto>>, RestError> {
    let mut storage = handler.get_storage_mut();

    // Switch to target knowledge graph
    storage
        .use_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{}' not found: {}", kg, e)))?;

    // Check if rule exists by trying to describe it
    let description = storage
        .describe_rule(&name)
        .map_err(|e| RestError::internal(format!("Failed to get rule: {}", e)))?
        .ok_or_else(|| RestError::not_found(format!("Rule '{}' not found", name)))?;

    let clause_count = storage.rule_count(&name).ok().flatten().unwrap_or(0);

    let rule = RuleDto {
        name,
        clause_count,
        description,
    };

    Ok(Json(ApiResponse::success(rule)))
}

/// Delete a rule
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{kg}/rules/{name}",
    tag = "rules",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Rule name")
    ),
    responses(
        (status = 200, description = "Rule deleted", body = ApiResponse<()>),
        (status = 404, description = "Rule not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_rule(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
) -> Result<Json<ApiResponse<()>>, RestError> {
    let mut storage = handler.get_storage_mut();

    // Switch to target knowledge graph
    storage
        .use_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{}' not found: {}", kg, e)))?;

    storage
        .drop_rule(&name)
        .map_err(|e| RestError::not_found(format!("Rule '{}': {}", name, e)))?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
    }))
}
