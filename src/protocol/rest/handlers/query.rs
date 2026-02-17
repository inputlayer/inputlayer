//! Query Handlers
//!
//! Endpoints for query execution and explanation.

use std::sync::Arc;
use std::time::Instant;

use axum::{Extension, Json};

use super::wire_value_to_json;
use crate::protocol::rest::dto::{
    ApiResponse, ExplainRequest, ExplainResponse, QueryRequest, QueryResponse, QueryStatus,
    ValidationError,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;
use crate::statement;

/// Execute a Datalog query
#[utoipa::path(
    post,
    path = "/query/execute",
    tag = "queries",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query executed", body = ApiResponse<QueryResponse>),
        (status = 400, description = "Invalid query"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn execute_query(
    Extension(handler): Extension<Arc<Handler>>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<ApiResponse<QueryResponse>>, RestError> {
    let start = Instant::now();

    // When validate_first is set, pre-parse all statements before executing any.
    if request.validate_first {
        let errors = validate_program(&request.query);
        if !errors.is_empty() {
            let execution_time_ms = start.elapsed().as_millis() as u64;
            let query_response = QueryResponse {
                query: request.query,
                status: QueryStatus::Error,
                columns: vec![],
                rows: vec![],
                row_count: 0,
                total_count: 0,
                truncated: false,
                execution_time_ms,
                error: Some(format!(
                    "Validation failed: {} statement(s) have errors",
                    errors.len()
                )),
                validation_errors: Some(errors),
            };
            return Ok(Json(ApiResponse::success(query_response)));
        }
    }

    let result = handler
        .query_program(Some(request.knowledge_graph.clone()), request.query.clone())
        .await;
    let execution_time_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(response) => {
            // Convert rows to JSON values
            let rows: Vec<Vec<serde_json::Value>> = response
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
                .collect();

            let row_count = rows.len();

            // Get column names from schema
            let columns: Vec<String> = response.schema.iter().map(|c| c.name.clone()).collect();

            let query_response = QueryResponse {
                query: request.query,
                status: QueryStatus::Success,
                columns,
                rows,
                row_count,
                total_count: response.total_count,
                truncated: response.truncated,
                execution_time_ms,
                error: None,
                validation_errors: None,
            };

            Ok(Json(ApiResponse::success(query_response)))
        }
        Err(e) => {
            let query_response = QueryResponse {
                query: request.query,
                status: QueryStatus::Error,
                columns: vec![],
                rows: vec![],
                row_count: 0,
                total_count: 0,
                truncated: false,
                execution_time_ms,
                error: Some(e),
                validation_errors: None,
            };

            Ok(Json(ApiResponse::success(query_response)))
        }
    }
}

/// Pre-validate all statements in a program without executing them.
/// Returns a list of validation errors (empty if all statements parse OK).
fn validate_program(program: &str) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    let mut stmt_index = 0;

    for (line_num_0, line) in program.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with('%') {
            continue;
        }
        if statement::parse_statement(trimmed).is_err() {
            errors.push(ValidationError {
                statement_index: stmt_index,
                line: line_num_0 + 1,
                error: format!("Failed to parse: {trimmed}"),
            });
        }
        stmt_index += 1;
    }

    errors
}

/// Explain a query plan
#[utoipa::path(
    post,
    path = "/query/explain",
    tag = "queries",
    request_body = ExplainRequest,
    responses(
        (status = 200, description = "Query plan explanation", body = ApiResponse<ExplainResponse>),
        (status = 400, description = "Invalid query"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn explain_query(
    Extension(handler): Extension<Arc<Handler>>,
    Json(request): Json<ExplainRequest>,
) -> Result<Json<ApiResponse<ExplainResponse>>, RestError> {
    // Transform ?query syntax into a rule using the same logic as execute endpoint.
    // This correctly handles any arity, not just 2-variable queries.
    let query = crate::protocol::handler::transform_query_shorthand(&request.query)
        .map_or_else(|_| request.query.clone(), |t| t.query);

    match handler.explain_query(Some(request.knowledge_graph.clone()), query) {
        Ok((plan, optimizations)) => {
            let response = ExplainResponse {
                query: request.query,
                plan,
                optimizations,
            };
            Ok(Json(ApiResponse::success(response)))
        }
        Err(e) => {
            // Return error as part of the plan, not as HTTP error
            let response = ExplainResponse {
                query: request.query,
                plan: format!("Error generating query plan: {e}"),
                optimizations: vec![],
            };
            Ok(Json(ApiResponse::success(response)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::rest::dto::QueryRequest;
    use crate::Config;

    fn make_handler() -> (Arc<Handler>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.data_dir = tmp.path().to_path_buf();
        (Arc::new(Handler::from_config(config).unwrap()), tmp)
    }

    #[tokio::test]
    async fn test_execute_query_insert() {
        let (handler, _tmp) = make_handler();
        let request = QueryRequest {
            query: "+data[(1, 2), (3, 4)]".to_string(),
            knowledge_graph: "query_ins_kg".to_string(),
            timeout_ms: 30000,
            validate_first: false,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let resp = result.0;
        assert!(resp.success);
        let data = resp.data.unwrap();
        assert_eq!(data.status, QueryStatus::Success);
    }

    #[tokio::test]
    async fn test_execute_query_select() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("sel_test_kg".to_string()),
                "+items[(1,), (2,), (3,)]".to_string(),
            )
            .await
            .unwrap();
        let request = QueryRequest {
            query: "?items(X)".to_string(),
            knowledge_graph: "sel_test_kg".to_string(),
            timeout_ms: 30000,
            validate_first: false,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.status, QueryStatus::Success);
        assert_eq!(data.row_count, 3);
        assert_eq!(data.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_execute_query_error_returns_success_with_error_status() {
        let (handler, _tmp) = make_handler();
        let request = QueryRequest {
            query: "?nonexistent(X, Y, Z, invalid!!!".to_string(),
            knowledge_graph: "err_test_kg".to_string(),
            timeout_ms: 30000,
            validate_first: false,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.status, QueryStatus::Error);
        assert!(data.error.is_some());
    }

    #[tokio::test]
    async fn test_explain_query_basic() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("explain_kg")
            .unwrap();
        let request = ExplainRequest {
            query: "__q__(X, Y) <- edge(X, Y)".to_string(),
            knowledge_graph: "explain_kg".to_string(),
        };
        let result = explain_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(!data.plan.is_empty());
        assert!(!data.optimizations.is_empty());
    }

    #[tokio::test]
    async fn test_explain_query_shorthand() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("explain_sh_kg")
            .unwrap();
        let request = ExplainRequest {
            query: "?edge(X, Y)".to_string(),
            knowledge_graph: "explain_sh_kg".to_string(),
        };
        let result = explain_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(!data.plan.is_empty());
    }

    #[tokio::test]
    async fn test_explain_query_error_in_plan() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("explain_e_kg")
            .unwrap();
        let request = ExplainRequest {
            query: "totally invalid!!!".to_string(),
            knowledge_graph: "explain_e_kg".to_string(),
        };
        let result = explain_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert!(data.plan.contains("Error"));
    }

    // ── validate_first tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_validate_first_valid_program() {
        let (handler, _tmp) = make_handler();
        let program = "+vfdata[(1, 2), (3, 4)]\n?vfdata(X, Y)";
        let request = QueryRequest {
            query: program.to_string(),
            knowledge_graph: "vf_valid_kg".to_string(),
            timeout_ms: 30000,
            validate_first: true,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.status, QueryStatus::Success);
        assert!(data.validation_errors.is_none());
        assert!(data.error.is_none());
        assert_eq!(data.row_count, 2);
    }

    #[tokio::test]
    async fn test_validate_first_invalid_statement() {
        let (handler, _tmp) = make_handler();
        // First statement is valid, second is invalid syntax
        let program = "+vfdata2[(1, 2)]\nthis is totally invalid!!!";
        let request = QueryRequest {
            query: program.to_string(),
            knowledge_graph: "vf_invalid_kg".to_string(),
            timeout_ms: 30000,
            validate_first: true,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.status, QueryStatus::Error);
        assert!(data.error.is_some());
        let errors = data.validation_errors.unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].statement_index, 1);
        assert_eq!(errors[0].line, 2);
        // No execution should have happened — rows empty
        assert_eq!(data.row_count, 0);
        assert!(data.rows.is_empty());
    }

    #[tokio::test]
    async fn test_validate_first_multiple_errors() {
        let (handler, _tmp) = make_handler();
        // Two invalid statements among a valid one
        let program = "bad statement one!!!\n+ok[(1,)]\nanother bad!!!";
        let request = QueryRequest {
            query: program.to_string(),
            knowledge_graph: "vf_multi_err_kg".to_string(),
            timeout_ms: 30000,
            validate_first: true,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.status, QueryStatus::Error);
        let errors = data.validation_errors.unwrap();
        assert_eq!(errors.len(), 2);
        // First bad statement: index 0, line 1
        assert_eq!(errors[0].statement_index, 0);
        assert_eq!(errors[0].line, 1);
        // Second bad statement: index 2, line 3
        assert_eq!(errors[1].statement_index, 2);
        assert_eq!(errors[1].line, 3);
    }

    #[tokio::test]
    async fn test_validate_first_with_comments() {
        let (handler, _tmp) = make_handler();
        // Mix of comments, empty lines, and valid statements
        let program =
            "// this is a comment\n\n% another comment\n+cfdata[(10, 20)]\n\n?cfdata(X, Y)";
        let request = QueryRequest {
            query: program.to_string(),
            knowledge_graph: "vf_comments_kg".to_string(),
            timeout_ms: 30000,
            validate_first: true,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.status, QueryStatus::Success);
        assert!(data.validation_errors.is_none());
        assert!(data.error.is_none());
        assert_eq!(data.row_count, 1);
    }

    #[tokio::test]
    async fn test_validate_first_false_default() {
        let (handler, _tmp) = make_handler();
        // With validate_first=false, the invalid statement is attempted at runtime
        let request = QueryRequest {
            query: "this is totally invalid!!!".to_string(),
            knowledge_graph: "vf_false_kg".to_string(),
            timeout_ms: 30000,
            validate_first: false,
        };
        let result = execute_query(Extension(handler), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        // Runtime execution should produce an error (not validation_errors)
        assert_eq!(data.status, QueryStatus::Error);
        assert!(data.error.is_some());
        assert!(data.validation_errors.is_none());
    }
}
