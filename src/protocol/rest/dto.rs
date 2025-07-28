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

fn default_timeout() -> u64 {
    30000
}

/// Query execution response
#[derive(Debug, Serialize, ToSchema)]
pub struct QueryResponse {
    pub query: String,
    pub status: QueryStatus,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Query execution status
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Success,
    Error,
}

/// Query explanation request
#[derive(Debug, Deserialize, ToSchema)]
pub struct ExplainRequest {
    pub query: String,
    pub knowledge_graph: String,
}

/// Query explanation response
#[derive(Debug, Serialize, ToSchema)]
pub struct ExplainResponse {
    pub query: String,
    pub plan: String,
    pub optimizations: Vec<String>,
}

// Relation DTOs
/// Relation information
#[derive(Debug, Serialize, ToSchema)]
pub struct RelationDto {
    pub name: String,
    pub arity: usize,
    pub tuple_count: usize,
    pub columns: Vec<String>,
    pub is_view: bool,
}

/// Relation list response
#[derive(Debug, Serialize, ToSchema)]
pub struct RelationListDto {
    pub relations: Vec<RelationDto>,
}

/// Relation data response
#[derive(Debug, Serialize, ToSchema)]
pub struct RelationDataDto {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub total_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Query parameters for relation data
#[derive(Debug, Deserialize, ToSchema)]
pub struct RelationDataQuery {
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

// View DTOs
/// View information
#[derive(Debug, Serialize, ToSchema)]
pub struct ViewDto {
    pub name: String,
    pub definition: String,
    pub arity: usize,
    pub columns: Vec<String>,
    pub dependencies: Vec<String>,
}

/// View list response
#[derive(Debug, Serialize, ToSchema)]
pub struct ViewListDto {
    pub views: Vec<ViewDto>,
}

/// Create view request
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateViewRequest {
    pub name: String,
    pub definition: String,
}

// Admin DTOs
/// Health check response
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthDto {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
}

/// Server statistics response
#[derive(Debug, Serialize, ToSchema)]
pub struct StatsDto {
    pub knowledge_graphs: usize,
    pub relations: usize,
    pub views: usize,
    pub memory_usage_bytes: u64,
    pub query_count: u64,
    pub uptime_secs: u64,
}
