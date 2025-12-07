//! Query Service Definition
//!
//! RPC service definition for Datalog query execution.
//!
//! Generate code with:
//! ```bash
//! rpcnet-gen --input src/protocol/stubs/query.rpc.rs --output src/protocol/generated
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Wire Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Timestamp(i64),
    Vector(Vec<f32>),
    VectorInt8(Vec<i8>),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireTuple {
    pub values: Vec<WireValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    /// Target database (None = current default).
    pub database: Option<String>,
    /// Datalog program text.
    pub program: String,
    /// Optional query parameters.
    pub params: Option<HashMap<String, WireValue>>,
    /// Max results to return (default: unlimited).
    pub limit: Option<usize>,
    /// Timeout in milliseconds (default: 30000).
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Result tuples.
    pub rows: Vec<WireTuple>,
    /// Schema of result columns.
    pub schema: Vec<ColumnDef>,
    /// Execution statistics.
    pub stats: QueryStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub execution_time_ms: u64,
    pub rows_scanned: u64,
    pub rows_returned: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResultBatch {
    /// Batch of result tuples.
    pub rows: Vec<WireTuple>,
    /// Sequence number for ordering.
    pub batch_number: u32,
    /// True if this is the last batch.
    pub is_final: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainRequest {
    pub database: Option<String>,
    pub program: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResponse {
    /// Human-readable query plan.
    pub plan_text: String,
}

// ============================================================================
// Error Type
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryError {
    ParseError {
        message: String,
        line: Option<u32>,
        column: Option<u32>,
    },
    ExecutionError {
        message: String,
    },
    Timeout {
        timeout_ms: u64,
    },
    DatabaseNotFound {
        name: String,
    },
    Internal {
        message: String,
    },
}

// ============================================================================
// Service Definition
// ============================================================================

#[rpcnet::service]
pub trait QueryService {
    /// Execute a Datalog query and return all results.
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, QueryError>;

    /// Execute a streaming query (server-streaming RPC).
    #[rpcnet::server_streaming]
    async fn query_stream(&self, request: QueryRequest) -> Result<QueryResultBatch, QueryError>;

    /// Explain the query execution plan without running the query.
    async fn explain(&self, request: ExplainRequest) -> Result<ExplainResponse, QueryError>;
}
