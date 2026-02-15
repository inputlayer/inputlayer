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
pub async fn insert_data(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
    Json(request): Json<InsertDataRequest>,
) -> Result<Json<ApiResponse<InsertDataResponse>>, RestError> {
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    // Convert JSON rows to Tuples with proper type handling
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut skipped = 0;

    for row in &request.rows {
        if row.is_empty() {
            skipped += 1;
            continue;
        }

        // Convert all values in the row
        let values: Option<Vec<Value>> = row.iter().map(json_to_value).collect();

        match values {
            Some(vals) => tuples.push(Tuple::new(vals)),
            None => {
                // Skip rows with unsupported values (null, objects)
                skipped += 1;
            }
        }
    }

    // Validate against schema if one exists (per-KG isolation ensures correct validation)
    storage
        .validate_tuples_in(&kg, &name, &tuples)
        .map_err(|e| RestError::bad_request(format!("Schema validation failed: {e}")))?;

    // Use insert_tuples_into which supports arbitrary arity and validates consistency
    let (inserted, duplicates) = storage
        .insert_tuples_into(&kg, &name, tuples)
        .map_err(|e| RestError::bad_request(format!("Insert failed: {e}")))?;

    // Notify WebSocket subscribers of persistent data change
    if inserted > 0 {
        handler.notify_persistent_update(&kg, &name, "insert", inserted);
    }

    let response = InsertDataResponse {
        rows_inserted: inserted,
        duplicates: duplicates + skipped,
    };

    Ok(Json(ApiResponse::success(response)))
}

/// Delete data from a relation
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{kg}/relations/{name}/data",
    tag = "data",
    params(
        ("kg" = String, Path, description = "Knowledge graph name"),
        ("name" = String, Path, description = "Relation name")
    ),
    request_body = DeleteDataRequest,
    responses(
        (status = 200, description = "Data deleted", body = ApiResponse<DeleteDataResponse>),
        (status = 400, description = "Invalid request"),
        (status = 404, description = "Relation not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_data(
    Extension(handler): Extension<Arc<Handler>>,
    Path((kg, name)): Path<(String, String)>,
    Json(request): Json<DeleteDataRequest>,
) -> Result<Json<ApiResponse<DeleteDataResponse>>, RestError> {
    let storage = handler.get_storage();

    // Ensure target knowledge graph exists
    storage
        .ensure_knowledge_graph(&kg)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{kg}' not found: {e}")))?;

    // Convert JSON rows to Tuples with proper type handling
    let mut tuples: Vec<Tuple> = Vec::new();
    for row in &request.rows {
        if row.is_empty() {
            continue;
        }
        // Convert all values in the row
        let values: Option<Vec<Value>> = row.iter().map(json_to_value).collect();
        if let Some(vals) = values {
            tuples.push(Tuple::new(vals));
        }
    }

    let deleted_count = storage
        .delete_tuples_from(&kg, &name, tuples)
        .map_err(|e| RestError::internal(format!("Delete failed: {e}")))?;

    // Notify WebSocket subscribers of persistent data change
    if deleted_count > 0 {
        handler.notify_persistent_update(&kg, &name, "delete", deleted_count);
    }

    let response = DeleteDataResponse {
        rows_deleted: deleted_count,
    };

    Ok(Json(ApiResponse::success(response)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_value_integer() {
        let val = json_to_value(&serde_json::json!(42));
        assert_eq!(val, Some(Value::Int64(42)));
    }

    #[test]
    fn test_json_to_value_negative() {
        let val = json_to_value(&serde_json::json!(-100));
        assert_eq!(val, Some(Value::Int64(-100)));
    }

    #[test]
    fn test_json_to_value_float() {
        let val = json_to_value(&serde_json::json!(3.14));
        assert_eq!(val, Some(Value::Float64(3.14)));
    }

    #[test]
    fn test_json_to_value_string() {
        let val = json_to_value(&serde_json::json!("hello"));
        assert_eq!(val, Some(Value::string("hello")));
    }

    #[test]
    fn test_json_to_value_bool_true() {
        let val = json_to_value(&serde_json::json!(true));
        assert_eq!(val, Some(Value::Int64(1)));
    }

    #[test]
    fn test_json_to_value_bool_false() {
        let val = json_to_value(&serde_json::json!(false));
        assert_eq!(val, Some(Value::Int64(0)));
    }

    #[test]
    fn test_json_to_value_null() {
        let val = json_to_value(&serde_json::json!(null));
        assert_eq!(val, None);
    }

    #[test]
    fn test_json_to_value_object() {
        let val = json_to_value(&serde_json::json!({"key": "val"}));
        assert_eq!(val, None);
    }

    #[test]
    fn test_json_to_value_array_floats() {
        let val = json_to_value(&serde_json::json!([1.0, 2.0, 3.0]));
        assert_eq!(val, Some(Value::vector(vec![1.0, 2.0, 3.0])));
    }

    #[test]
    fn test_json_to_value_array_mixed() {
        let val = json_to_value(&serde_json::json!([1.0, "bad"]));
        assert_eq!(val, None);
    }

    #[test]
    fn test_json_to_value_empty_array() {
        let val = json_to_value(&serde_json::json!([]));
        assert_eq!(val, Some(Value::vector(vec![])));
    }

    #[test]
    fn test_json_to_value_large_int() {
        let val = json_to_value(&serde_json::json!(i64::MAX));
        assert_eq!(val, Some(Value::Int64(i64::MAX)));
    }

    #[test]
    fn test_insert_data_request_deserialize() {
        let json = r#"{"rows": [[1, 2], [3, 4]]}"#;
        let req: InsertDataRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.rows.len(), 2);
    }

    #[test]
    fn test_delete_data_request_deserialize() {
        let json = r#"{"rows": [[1, 2]]}"#;
        let req: DeleteDataRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.rows.len(), 1);
    }

    #[test]
    fn test_insert_data_response_serialize() {
        let resp = InsertDataResponse {
            rows_inserted: 5,
            duplicates: 2,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"rows_inserted\":5"));
        assert!(json.contains("\"duplicates\":2"));
    }

    #[test]
    fn test_delete_data_response_serialize() {
        let resp = DeleteDataResponse { rows_deleted: 3 };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"rows_deleted\":3"));
    }
}
