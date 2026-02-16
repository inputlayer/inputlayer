//! Query Handlers
//!
//! Endpoints for query execution and explanation.

use std::sync::Arc;
use std::time::Instant;

use axum::{Extension, Json};

use super::wire_value_to_json;
use crate::protocol::rest::dto::{
    ApiResponse, ExplainRequest, ExplainResponse, QueryRequest, QueryResponse, QueryStatus,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

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
                execution_time_ms,
                error: None,
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
                execution_time_ms,
                error: Some(e),
            };

            Ok(Json(ApiResponse::success(query_response)))
        }
    }
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
        .unwrap_or_else(|_| request.query.clone());

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
}
