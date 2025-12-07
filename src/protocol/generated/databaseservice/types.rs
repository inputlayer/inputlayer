//! Type definitions for the service.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListViewsRequest {
    pub database: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropViewResponse {
    pub success: bool,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropDatabaseRequest {
    pub name: String,
    pub confirm: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatabasesRequest {}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseOptions {
    pub schemas: std::collections::HashMap<String, Vec<ColumnDef>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeViewResponse {
    pub description: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,
    pub rules_count: usize,
    pub created_at: String,
    pub description: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropDatabaseResponse {
    pub success: bool,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterViewRequest {
    pub database: Option<String>,
    pub name: String,
    pub rule: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterViewResponse {
    pub success: bool,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeViewRequest {
    pub database: Option<String>,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
    pub options: Option<DatabaseOptions>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListViewsResponse {
    pub views: Vec<ViewInfo>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseError {
    NotFound { name: String },
    AlreadyExists { name: String },
    CannotDropDefault { name: String },
    CannotDropCurrent { name: String },
    Internal { message: String },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfoRequest {
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub created_at: i64,
    pub relations_count: usize,
    pub total_tuples: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseResponse {
    pub success: bool,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatabasesResponse {
    pub databases: Vec<DatabaseInfo>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfoResponse {
    pub info: DatabaseInfo,
    pub relations: Vec<RelationInfo>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationInfo {
    pub name: String,
    pub schema: Vec<ColumnDef>,
    pub tuple_count: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropViewRequest {
    pub database: Option<String>,
    pub name: String,
}
