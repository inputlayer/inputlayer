//! Admin Service Definition
//!
//! RPC service definition for server administration operations.
//!
//! Generate code with:
//! ```bash
//! rpcnet-gen --input src/protocol/stubs/admin.rpc.rs --output src/protocol/generated
//! ```

use serde::{Deserialize, Serialize};

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub uptime_seconds: u64,
    pub memory_used_bytes: u64,
    pub active_queries: u32,
    pub databases_loaded: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded { reason: String },
    Unhealthy { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsRequest {
    pub database: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsResponse {
    pub total_queries: u64,
    pub total_inserts: u64,
    pub cache_hit_rate: f64,
    pub avg_query_time_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownRequest {
    pub graceful: bool,
    pub timeout_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownResponse {
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRequest {
    pub database: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupResponse {
    pub success: bool,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearCachesRequest {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClearCachesResponse {
    pub caches_cleared: Vec<String>,
}

// ============================================================================
// Error Type
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminError {
    NotAuthorized { reason: String },
    BackupFailed { reason: String },
    ShutdownFailed { reason: String },
    Internal { message: String },
}

// ============================================================================
// Service Definition
// ============================================================================

#[rpcnet::service]
pub trait AdminService {
    /// Check server health status.
    async fn health(&self, request: HealthRequest) -> Result<HealthResponse, AdminError>;

    /// Get server statistics.
    async fn stats(&self, request: StatsRequest) -> Result<StatsResponse, AdminError>;

    /// Initiate server shutdown.
    async fn shutdown(&self, request: ShutdownRequest) -> Result<ShutdownResponse, AdminError>;

    /// Create a backup of a database.
    async fn backup(&self, request: BackupRequest) -> Result<BackupResponse, AdminError>;

    /// Clear server caches.
    async fn clear_caches(
        &self,
        request: ClearCachesRequest,
    ) -> Result<ClearCachesResponse, AdminError>;
}
