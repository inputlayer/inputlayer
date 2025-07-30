//! Data Handlers
//!
//! Endpoints for data manipulation (insert, delete).

use std::sync::Arc;

use axum::{extract::Path, Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::protocol::rest::dto::ApiResponse;
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;
use crate::value::{Tuple, Value};

/// Convert a JSON value to a storage Value
fn json_to_value(json: &serde_json::Value) -> Option<Value> {
    match json {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(Value::Int64(i64::from(*b))),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::Int64(i))
            } else {
                n.as_f64().map(Value::Float64)
            }
        }
        serde_json::Value::String(s) => Some(Value::string(s)),
        serde_json::Value::Array(arr) => {
            // Check if it's a vector of numbers (for vector values)
            let floats: Option<Vec<f32>> =
                arr.iter().map(|v| v.as_f64().map(|f| f as f32)).collect();
            floats.map(Value::vector)
        }
        serde_json::Value::Object(_) => None, // Objects not supported
    }
}

/// Insert data request
#[derive(Debug, Deserialize, ToSchema)]
pub struct InsertDataRequest {
    /// Rows to insert, each row is an array of values
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Insert data response
#[derive(Debug, Serialize, ToSchema)]
pub struct InsertDataResponse {
    pub rows_inserted: usize,
    pub duplicates: usize,
}

/// Delete data request
#[derive(Debug, Deserialize, ToSchema)]
pub struct DeleteDataRequest {
    /// Rows to delete, each row is an array of values
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Delete data response
#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteDataResponse {
    pub rows_deleted: usize,
}

/// Insert data into a relation
#[utoipa::path(
    post,
    path = "/knowledge-graphs/{kg}/relations/{name}/data",
    tag = "data",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Relation name")
    ),
    request_body = InsertDataRequest,
    responses(
        (status = 200, description = "Data inserted", body = ApiResponse<InsertDataResponse>),
        (status = 400, description = "Invalid request"),
        (status = 404, description = "Relation not found"),
        (status = 500, description = "Internal server error"),
    )
)]
