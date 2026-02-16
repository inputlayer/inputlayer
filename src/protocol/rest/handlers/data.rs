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
use crate::value::Tuple;

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
    /// Rows that were skipped due to conversion errors or being empty
    #[serde(skip_serializing_if = "is_zero")]
    pub skipped: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
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
    /// Rows that were skipped due to conversion errors or being empty
    #[serde(skip_serializing_if = "is_zero")]
    pub skipped: usize,
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

        match row
            .iter()
            .map(super::json_to_value)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(vals) => tuples.push(Tuple::new(vals)),
            Err(_) => {
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
        duplicates,
        skipped,
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
    let mut skipped = 0;
    for row in &request.rows {
        if row.is_empty() {
            skipped += 1;
            continue;
        }
        match row
            .iter()
            .map(super::json_to_value)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(vals) => tuples.push(Tuple::new(vals)),
            Err(_) => {
                skipped += 1;
            }
        }
    }

    let deleted_count = storage
        .delete_tuples_from(&kg, &name, tuples)
        .map_err(|e| RestError::bad_request(format!("Delete failed: {e}")))?;

    // Notify WebSocket subscribers of persistent data change
    if deleted_count > 0 {
        handler.notify_persistent_update(&kg, &name, "delete", deleted_count);
    }

    let response = DeleteDataResponse {
        rows_deleted: deleted_count,
        skipped,
    };

    Ok(Json(ApiResponse::success(response)))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            skipped: 0,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"rows_inserted\":5"));
        assert!(json.contains("\"duplicates\":2"));
        // skipped=0 should be omitted via skip_serializing_if
        assert!(!json.contains("\"skipped\""));
    }

    #[test]
    fn test_insert_data_response_serialize_with_skipped() {
        let resp = InsertDataResponse {
            rows_inserted: 3,
            duplicates: 1,
            skipped: 2,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"rows_inserted\":3"));
        assert!(json.contains("\"duplicates\":1"));
        assert!(json.contains("\"skipped\":2"));
    }

    #[test]
    fn test_delete_data_response_serialize() {
        let resp = DeleteDataResponse {
            rows_deleted: 3,
            skipped: 0,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"rows_deleted\":3"));
        // skipped=0 should be omitted
        assert!(!json.contains("\"skipped\""));
    }

    #[test]
    fn test_delete_data_response_serialize_with_skipped() {
        let resp = DeleteDataResponse {
            rows_deleted: 2,
            skipped: 1,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"rows_deleted\":2"));
        assert!(json.contains("\"skipped\":1"));
    }
}
