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
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    let rule_names = storage
        .list_rules_in(&kg)
        .map_err(|e| RestError::internal(format!("Failed to list rules: {e}")))?;
    let mut rules = Vec::new();

    for name in rule_names {
        let clause_count = storage
            .rule_count_in(&kg, &name)
            .ok()
            .flatten()
            .unwrap_or(0);
        let description = storage
            .describe_rule_in(&kg, &name)
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
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    // Check if rule exists by trying to describe it
    let description = storage
        .describe_rule_in(&kg, &name)
        .map_err(|e| RestError::internal(format!("Failed to get rule: {e}")))?
        .ok_or_else(|| RestError::not_found(format!("Rule '{name}' not found")))?;

    let clause_count = storage
        .rule_count_in(&kg, &name)
        .ok()
        .flatten()
        .unwrap_or(0);

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
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    storage
        .drop_rule_in(&kg, &name)
        .map_err(|e| RestError::not_found(format!("Rule '{name}': {e}")))?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
    }))
}

/// Delete result
#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteClauseResult {
    /// True if the entire rule was deleted (last clause removed)
    pub rule_deleted: bool,
    /// Message describing what happened
    pub message: String,
}

/// Delete a specific clause from a rule
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{kg}/rules/{name}/{index}",
    tag = "rules",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Rule name"),
        ("index" = usize, Path, description = "Clause index (1-based)")
    ),
    responses(
        (status = 200, description = "Clause removed", body = ApiResponse<DeleteClauseResult>),
        (status = 404, description = "Rule or clause not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_rule_clause(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name, index)): Path<(String, String, usize)>,
) -> Result<Json<ApiResponse<DeleteClauseResult>>, RestError> {
    let storage = handler.get_storage();

    // Validate index (1-based from user)
    if index == 0 {
        return Err(RestError::bad_request(
            "Index must be 1 or greater (1-based indexing)".to_string(),
        ));
    }
    let zero_based_index = index - 1;

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    let rule_deleted = storage
        .remove_rule_clause_in(&kg, &name, zero_based_index)
        .map_err(|e| RestError::not_found(format!("{e}")))?;

    let message = if rule_deleted {
        format!(
            "Clause {index} removed from rule '{name}'. Rule completely deleted (no clauses remaining)."
        )
    } else {
        format!("Clause {index} removed from rule '{name}'.")
    };

    Ok(Json(ApiResponse::success(DeleteClauseResult {
        rule_deleted,
        message,
    })))
}
