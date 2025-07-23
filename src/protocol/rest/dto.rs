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

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> ApiResponse<()> {
        ApiResponse {
            success: false,
            data: None,
            error: Some(ApiErrorDto {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

/// Error details in API response
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorDto {
    pub code: String,
    pub message: String,
}

// Knowledge Graph DTOs
/// Knowledge Graph information
#[derive(Debug, Serialize, ToSchema)]
pub struct KnowledgeGraphDto {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub relations_count: usize,
    pub views_count: usize,
}


/// List of knowledge graphs
#[derive(Debug, Serialize, ToSchema)]
pub struct KnowledgeGraphListDto {
    pub knowledge_graphs: Vec<KnowledgeGraphDto>,
    /// Currently selected knowledge graph (may not exist if invalid)
    pub current: Option<String>,
    /// Warning message if current KG was not found
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Create knowledge graph request
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateKnowledgeGraphRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

// Query DTOs
/// Query execution request
#[derive(Debug, Deserialize, ToSchema)]
pub struct QueryRequest {
    /// The Datalog query to execute
    pub query: String,
    /// Knowledge graph to execute against
    pub knowledge_graph: String,
    /// Optional timeout in milliseconds
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

