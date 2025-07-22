//! REST API Data Transfer Objects
//!
//! Defines request/response types for the REST API endpoints.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// JSON response: { success, data?, error? }
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorDto>,
}

