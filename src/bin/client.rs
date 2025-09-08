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

struct ReplState {
    http: HttpClient,
    current_kg: Option<String>,
    /// Session-scoped transient rules (cleared on exit or knowledge graph switch)
    session_rules: Vec<inputlayer::ast::Rule>,
    /// Session-scoped transient facts (cleared on exit or knowledge graph switch)
    /// These are NOT persisted - only used in queries during this session
    session_facts: Vec<inputlayer::ast::Rule>,
    /// Receiver for server disconnect signal from heartbeat task
    disconnect_rx: watch::Receiver<bool>,
}

impl ReplState {
    fn prompt(&self) -> String {
        let has_session_data = !self.session_rules.is_empty() || !self.session_facts.is_empty();
        let session_indicator = if has_session_data { "*" } else { "" };
        match &self.current_kg {
            Some(db) => format!("{db}{session_indicator}> "),
            None => "inputlayer> ".to_string(),
        }
    }

    /// Check if the server is still connected
    fn is_server_alive(&self) -> bool {
        !*self.disconnect_rx.borrow()
    }
}

fn parse_args() -> Args {
    let args: Vec<String> = env::args().collect();
    let mut result = Args {
        script: None,
        repl: false,
        server: "http://127.0.0.1:8080".to_string(),
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--script" | "-s" => {
                if i + 1 < args.len() {
                    result.script = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: --script requires a file path");
                    std::process::exit(1);
                }
            }
            "--repl" | "-r" => {
                result.repl = true;
                i += 1;
            }
            "--server" => {
                if i + 1 < args.len() {
                    result.server.clone_from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --server requires a URL");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            arg if arg.to_ascii_lowercase().ends_with(".dl") => {
                result.script = Some(arg.to_string());
                i += 1;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
    }

    result
}

fn print_usage() {
    println!("InputLayer Datalog Client (HTTP)");
    println!();
    println!("USAGE:");
    println!("  inputlayer-client [OPTIONS] [SCRIPT.dl]");
    println!();
    println!("OPTIONS:");
    println!("  -s, --script <FILE>   Execute a Datalog script file");
    println!("  -r, --repl            Open REPL after script execution");
    println!("      --server <URL>    Server URL (default: http://127.0.0.1:8080)");
    println!("  -h, --help            Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("  inputlayer-client                              # Connect to local server");
    println!("  inputlayer-client --server http://10.0.0.5:8080   # Connect to remote server");
    println!("  inputlayer-client script.dl                    # Execute script");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    println!("Connecting to server at {}...", args.server);

    let http = HttpClient::new(&args.server);

    // Check server health
    let health_url = http.api_url("/health");
    let health_resp = http
        .client
        .get(&health_url)
        .send()
        .await
        .map_err(|e| format!("Failed to connect to server: {e}"))?;

    if !health_resp.status().is_success() {
        return Err(format!("Server returned error: {}", health_resp.status()).into());
    }

    let health: ApiResponse<HealthResponse> = health_resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse health response: {e}"))?;

    let health_data = health.data.ok_or("No health data returned")?;
    println!("Connected!");
    println!();
    println!("Server status: {}", health_data.status);

    // Get knowledge graph list and select first one if available
    let db_list_url = http.api_url("/knowledge-graphs");
    let db_resp = http
        .client
        .get(&db_list_url)
        .send()
        .await
        .map_err(|e| format!("Failed to list knowledge graphs: {e}"))?;

    let db_list: ApiResponse<KnowledgeGraphListResponse> = db_resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse knowledge graph list: {e}"))?;

    let current_kg = db_list
        .data
        .and_then(|d| d.knowledge_graphs.first().map(|kg| kg.name.clone()));

    if let Some(ref db) = current_kg {
        println!("Current knowledge graph: {db}");
    }
    println!();

    // Create heartbeat channel and spawn background task
    let (disconnect_tx, disconnect_rx) = watch::channel(false);
    let server_url = args.server.clone();
    tokio::spawn(async move {
        heartbeat_task(server_url, disconnect_tx).await;
    });

    let mut state = ReplState {
        http,
        current_kg,
        session_rules: Vec::new(),
        session_facts: Vec::new(),
        disconnect_rx,
    };

    // If a script is provided, execute it
    if let Some(script_path) = &args.script {
        match execute_script(&mut state, script_path).await {
            Ok(()) => {
                if !args.repl {
                    return Ok(());
                }
                println!();
                println!("Script completed. Entering REPL...");
                println!();
            }
            Err(e) => {
                println!("Script error: {e}");
                if !args.repl {
                    std::process::exit(1);
                }
            }
        }
    } else {
        println!("Type .help for syntax reference.");
        println!();
    }

    // Run REPL
    run_repl(&mut state).await
}

fn execute_script<'a>(
    state: &'a mut ReplState,
    path: &'a str,
) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
    Box::pin(async move {
        let content =
            fs::read_to_string(path).map_err(|e| format!("Failed to read script '{path}': {e}"))?;

        // Strip block comments first
        let content = strip_block_comments(&content);

        let mut accumulated = String::new();

        for line in content.lines() {
            // Check for server disconnect during script execution
            if !state.is_server_alive() {
                return Err("Server connection lost".to_string());
            }

            let line = line.trim();
            // Skip empty lines and line comments (% Prolog style, // C-style)
            if line.is_empty() || line.starts_with('%') || line.starts_with("//") {
                continue;
            }

            // Strip inline % comments
            let line = strip_inline_comment(line);
            if line.is_empty() {
                continue;
            }

            accumulated.push_str(line);
            accumulated.push(' ');

            if is_complete_statement(&accumulated) {
                println!("> {}", accumulated.trim());
                match parse_statement(&accumulated) {
                    Ok(stmt) => {
                        handle_statement(state, stmt).await?;
                    }
                    Err(e) => {
                        return Err(format!("Parse error: {e}"));
                    }
                }
                accumulated.clear();
            }
        }

        Ok(())
    })
}

/// Strip block comments (/* ... */) from source text
/// Respects string literals - doesn't strip comments inside strings
