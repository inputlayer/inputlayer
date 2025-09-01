//! `InputLayer` Client Binary - HTTP-based Datalog Client
//!
//! Interactive client for `InputLayer` that connects to the server via HTTP REST API.
//!
//! ## Usage
//!
//! ```bash
//! # Connect to local server
//! cargo run --bin inputlayer-client
//!
//! # Connect to remote server
//! cargo run --bin inputlayer-client -- --server http://192.168.1.100:8080
//!
//! # Execute a Datalog script
//! cargo run --bin inputlayer-client -- --script examples/datalog/basic/same_component.dl
//! ```

use inputlayer::ast::Term;
use inputlayer::statement::{parse_statement, MetaCommand, Statement};

use reqwest::Client;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::watch;

// DTO Types (matching REST API)
// These DTOs must have all fields present for JSON deserialization to work
// correctly, even if not all fields are explicitly accessed in the code.
// The `#[allow(dead_code)]` suppresses warnings for fields that exist only
// for completeness of the REST API contract.

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    data: Option<T>,
    error: Option<ApiError>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApiError {
    code: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime_secs: u64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct KnowledgeGraphInfo {
    name: String,
    #[serde(default)]
    description: Option<String>,
    relations_count: usize,
    views_count: usize,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct KnowledgeGraphListResponse {
    knowledge_graphs: Vec<KnowledgeGraphInfo>,
    current: String,
}

#[derive(Debug, Serialize)]
struct CreateKnowledgeGraphRequest {
    name: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RelationInfo {
    name: String,
    arity: usize,
    tuple_count: usize,
}

#[derive(Debug, Deserialize)]
struct RelationListResponse {
    relations: Vec<RelationInfo>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RelationDataResponse {
    name: String,
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
    total_count: usize,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ViewInfo {
    name: String,
    definition: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ViewListResponse {
    views: Vec<ViewInfo>,
}

#[derive(Debug, Serialize)]
struct QueryRequest {
    query: String,
    knowledge_graph: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout_ms: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ColumnDef {
    name: String,
    data_type: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct WireTuple {
    values: Vec<WireValue>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum WireValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Array(Vec<serde_json::Value>),
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct QueryResponse {
    query: String,
    status: String,
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
    execution_time_ms: u64,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct InsertDataRequest {
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct InsertDataResponse {
    rows_inserted: usize,
    duplicates: usize,
}

#[derive(Debug, Serialize)]
struct DeleteDataRequest {
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct DeleteDataResponse {
    rows_deleted: usize,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct RuleDto {
    name: String,
    clause_count: usize,
    description: String,
}

#[derive(Debug, Deserialize)]
struct RuleListDto {
    rules: Vec<RuleDto>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct StatsResponse {
    knowledge_graphs: usize,
    relations: usize,
    views: usize,
    memory_usage_bytes: u64,
    query_count: u64,
    uptime_secs: u64,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct CreateViewRequest {
    name: String,
    definition: String,
}

// Client State
/// Heartbeat configuration
const HEARTBEAT_INTERVAL_SECS: u64 = 5;
const HEARTBEAT_TIMEOUT_SECS: u64 = 3;
const HEARTBEAT_MAX_FAILURES: u32 = 2;

/// Command-line arguments
struct Args {
    script: Option<String>,
    repl: bool,
    server: String,
}

/// HTTP Client state
struct HttpClient {
    client: Client,
    base_url: String,
}

impl HttpClient {
    fn new(base_url: &str) -> Self {
        HttpClient {
            client: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| Client::new()),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn api_url(&self, path: &str) -> String {
        format!("{}/api/v1{}", self.base_url, path)
    }
}

