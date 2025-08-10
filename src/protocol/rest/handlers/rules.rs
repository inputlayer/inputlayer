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
        // FIXME: extract to named variable
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
