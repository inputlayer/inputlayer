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
    // Transform ?query syntax into a rule (same as execute endpoint)
    let query = if request.query.trim().starts_with('?')
        && request
            .query
            .trim()
            .chars()
            .nth(1)
            .is_some_and(char::is_alphabetic)
    {
        let q = request.query.trim()[1..].trim();
        format!("__explain__(X, Y) <- {q}")
    } else {
        request.query.clone()
    };

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
