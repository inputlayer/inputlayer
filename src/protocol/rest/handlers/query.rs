//! Query Handlers
//!
//! Endpoints for query execution and explanation.

use std::sync::Arc;
use std::time::Instant;

use axum::{Extension, Json};

use crate::protocol::rest::dto::{
    ApiResponse, ExplainRequest, ExplainResponse, QueryRequest, QueryResponse, QueryStatus,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::wire::WireValue;
use crate::protocol::Handler;

/// Convert WireValue to serde_json::Value
fn wire_value_to_json(value: WireValue) -> serde_json::Value {
    match value {
        WireValue::Null => serde_json::Value::Null,
        WireValue::Int32(i) => serde_json::Value::Number(i.into()),
        WireValue::Int64(i) => serde_json::json!(i),
        WireValue::Float64(f) => serde_json::json!(f),
        WireValue::String(s) => serde_json::Value::String(s),
        WireValue::Bool(b) => serde_json::Value::Bool(b),
        WireValue::Timestamp(t) => serde_json::json!(t),
        WireValue::Vector(v) => serde_json::json!(v),
        WireValue::VectorInt8(v) => serde_json::json!(v),
        WireValue::Bytes(b) => serde_json::json!(b),
    }
}

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
    Extension(_handler): Extension<Arc<Handler>>,
    Json(request): Json<ExplainRequest>,
) -> Result<Json<ApiResponse<ExplainResponse>>, RestError> {
    // Simple explain - just return the query text with a placeholder plan
    let plan = format!(
        "Query Plan for: {}\nKnowledge Graph: {}\n\n(Query plan generation not yet implemented)",
        request.query, request.knowledge_graph
    );

    let response = ExplainResponse {
        query: request.query,
        plan,
        optimizations: vec![],
    };

    Ok(Json(ApiResponse::success(response)))
}
