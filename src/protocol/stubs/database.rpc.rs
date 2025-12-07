//! Database Service Definition
//!
//! RPC service definition for database management operations.
//!
//! Generate code with:
//! ```bash
//! rpcnet-gen --input src/protocol/stubs/database.rpc.rs --output src/protocol/generated
//! ```

use serde::{Deserialize, Serialize};

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
    pub options: Option<DatabaseOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseOptions {
    pub schemas: std::collections::HashMap<String, Vec<ColumnDef>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropDatabaseRequest {
    pub name: String,
    pub confirm: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropDatabaseResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatabasesRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatabasesResponse {
    pub databases: Vec<DatabaseInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub created_at: i64,
    pub relations_count: usize,
    pub total_tuples: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfoRequest {
    pub name: String,
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

// ============================================================================
// View Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewInfo {
    pub name: String,
    pub rules_count: usize,
    pub created_at: String,
    pub description: Option<String>,
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
pub struct DropViewRequest {
    pub database: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropViewResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListViewsRequest {
    pub database: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListViewsResponse {
    pub views: Vec<ViewInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeViewRequest {
    pub database: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DescribeViewResponse {
    pub description: String,
}

// ============================================================================
// Error Type
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DatabaseError {
    NotFound { name: String },
    AlreadyExists { name: String },
    CannotDropDefault { name: String },
    CannotDropCurrent { name: String },
    Internal { message: String },
}

// ============================================================================
// Service Definition
// ============================================================================

#[rpcnet::service]
pub trait DatabaseService {
    /// Create a new database.
    async fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, DatabaseError>;

    /// Drop an existing database.
    async fn drop_database(
        &self,
        request: DropDatabaseRequest,
    ) -> Result<DropDatabaseResponse, DatabaseError>;

    /// List all databases.
    async fn list_databases(
        &self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, DatabaseError>;

    /// Get detailed information about a database.
    async fn database_info(
        &self,
        request: DatabaseInfoRequest,
    ) -> Result<DatabaseInfoResponse, DatabaseError>;

    /// Register a persistent view.
    async fn register_view(
        &self,
        request: RegisterViewRequest,
    ) -> Result<RegisterViewResponse, DatabaseError>;

    /// Drop a view.
    async fn drop_view(
        &self,
        request: DropViewRequest,
    ) -> Result<DropViewResponse, DatabaseError>;

    /// List all views in a database.
    async fn list_views(
        &self,
        request: ListViewsRequest,
    ) -> Result<ListViewsResponse, DatabaseError>;

    /// Describe a view.
    async fn describe_view(
        &self,
        request: DescribeViewRequest,
    ) -> Result<DescribeViewResponse, DatabaseError>;
}
