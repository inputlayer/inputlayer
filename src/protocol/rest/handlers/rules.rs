//! Rules Handlers
//!
//! Endpoints for persistent rule (policy) management.

use std::sync::Arc;

use axum::extract::{Path, Query};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
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

/// Query parameters for prefix-based rule deletion
#[derive(Debug, Deserialize)]
pub struct PrefixQuery {
    pub prefix: String,
}

/// Result of prefix-based rule deletion
#[derive(Debug, Serialize, ToSchema)]
pub struct DropByPrefixResult {
    /// Names of dropped rules
    pub dropped: Vec<String>,
    /// Number of rules dropped
    pub count: usize,
}

/// Delete all rules matching a prefix
pub async fn delete_rules_by_prefix(
    Extension(handler): Extension<Arc<Handler>>,
    Path(kg): Path<String>,
    Query(params): Query<PrefixQuery>,
) -> Result<Json<ApiResponse<DropByPrefixResult>>, RestError> {
    let storage = handler.get_storage();

    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    let dropped = storage
        .drop_rules_by_prefix_in(&kg, &params.prefix)
        .map_err(|e| RestError::internal(format!("Failed to drop rules by prefix: {e}")))?;

    let count = dropped.len();
    Ok(Json(ApiResponse::success(DropByPrefixResult {
        dropped,
        count,
    })))
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

    #[tokio::test]
    async fn test_list_rules_empty() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("rules_empty_kg")
            .unwrap();
        let result = list_rules(Extension(handler), Path("rules_empty_kg".to_string()))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(data.rules.is_empty());
    }

    #[tokio::test]
    async fn test_list_rules_with_rule() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("rules_list_kg".to_string()),
                "+base[(1,), (2,)]\n+doubled(X, Y) <- base(X), Y = X * 2".to_string(),
            )
            .await
            .unwrap();
        let result = list_rules(Extension(handler), Path("rules_list_kg".to_string()))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(data.rules.iter().any(|r| r.name == "doubled"));
    }

    #[tokio::test]
    async fn test_get_rule() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("rules_get_kg".to_string()),
                "+base[(1,)]\n+my_rule(X, Y) <- base(X), Y = X + 1".to_string(),
            )
            .await
            .unwrap();
        let result = get_rule(
            Extension(handler),
            Path(("rules_get_kg".to_string(), "my_rule".to_string())),
        )
        .await
        .unwrap();
        let rule = result.0.data.unwrap();
        assert_eq!(rule.name, "my_rule");
        assert!(rule.clause_count >= 1);
    }

    #[tokio::test]
    async fn test_get_rule_not_found() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("rules_nf_kg")
            .unwrap();
        let result = get_rule(
            Extension(handler),
            Path(("rules_nf_kg".to_string(), "nonexistent".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_rule() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("rules_del_kg".to_string()),
                "+base[(1,)]\n+to_delete(X) <- base(X)".to_string(),
            )
            .await
            .unwrap();
        let result = delete_rule(
            Extension(handler),
            Path(("rules_del_kg".to_string(), "to_delete".to_string())),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_rule_not_found() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("rules_delnf_kg")
            .unwrap();
        let result = delete_rule(
            Extension(handler),
            Path(("rules_delnf_kg".to_string(), "ghost".to_string())),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_rule_clause_zero_index() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("rules_cl0_kg")
            .unwrap();
        let result = delete_rule_clause(
            Extension(handler),
            Path(("rules_cl0_kg".to_string(), "rule".to_string(), 0)),
        )
        .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_rule_dto_serialize() {
        let rule = RuleDto {
            name: "path".to_string(),
            clause_count: 2,
            description: "path(X,Y) <- edge(X,Y)".to_string(),
        };
        let json = serde_json::to_string(&rule).unwrap();
        assert!(json.contains("\"name\":\"path\""));
        assert!(json.contains("\"clause_count\":2"));
    }

    #[test]
    fn test_delete_clause_result_serialize() {
        let result = DeleteClauseResult {
            rule_deleted: true,
            message: "Rule deleted".to_string(),
        };
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"rule_deleted\":true"));
    }
}
