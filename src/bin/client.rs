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
//! cargo run --bin inputlayer-client -- --script examples/datalog/basic/same_component.idl
//! ```

use inputlayer::ast::Term;
use inputlayer::statement::{parse_statement, MetaCommand, Statement};

use reqwest::Client;
use rustyline::error::ReadlineError;
use rustyline::Editor;
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
    #[serde(default)]
    provenance: Option<String>,
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

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct InsertDataResponse {
    rows_inserted: usize,
    duplicates: usize,
    #[serde(default)]
    skipped: usize,
}

#[derive(Debug, Serialize)]
struct DeleteDataRequest {
    rows: Vec<Vec<serde_json::Value>>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct DeleteDataResponse {
    rows_deleted: usize,
    #[serde(default)]
    skipped: usize,
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

/// Display configuration for query results
struct DisplayConfig {
    max_rows: usize,      // 0 = unlimited; default 50 for REPL, 0 for --script
    max_col_width: usize, // default 40
    show_timing: bool,    // show execution time; false for --script
}

impl Default for DisplayConfig {
    fn default() -> Self {
        DisplayConfig {
            max_rows: 50,
            max_col_width: 40,
            show_timing: true,
        }
    }
}

/// Command-line arguments
struct Args {
    script: Option<String>,
    repl: bool,
    server: String,
    display_limit: Option<usize>,
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
    /// Display configuration for query results
    display_config: DisplayConfig,
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
        display_limit: None,
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
            "--limit" | "-l" => {
                if i + 1 < args.len() {
                    result.display_limit = Some(args[i + 1].parse().unwrap_or_else(|_| {
                        eprintln!("Error: --limit requires a number");
                        std::process::exit(1);
                    }));
                    i += 2;
                } else {
                    eprintln!("Error: --limit requires a number");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            arg if arg.to_ascii_lowercase().ends_with(".idl")
                || arg.to_ascii_lowercase().ends_with(".dl") =>
            {
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
    println!("  inputlayer-client [OPTIONS] [SCRIPT.idl]");
    println!();
    println!("OPTIONS:");
    println!("  -s, --script <FILE>   Execute a Datalog script file");
    println!("  -r, --repl            Open REPL after script execution");
    println!("      --server <URL>    Server URL (default: http://127.0.0.1:8080)");
    println!(
        "  -l, --limit <N>       Max rows to display (0 = unlimited, default: 50 REPL, 0 script)"
    );
    println!("  -h, --help            Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("  inputlayer-client                              # Connect to local server");
    println!("  inputlayer-client --server http://10.0.0.5:8080   # Connect to remote server");
    println!("  inputlayer-client script.idl                   # Execute script");
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

    // Display config: script mode defaults to unlimited + no timing, REPL defaults to 50 + timing
    let is_script = args.script.is_some();
    let display_config = DisplayConfig {
        max_rows: args.display_limit.unwrap_or(if is_script { 0 } else { 50 }),
        max_col_width: 40,
        show_timing: !is_script,
    };

    let mut state = ReplState {
        http,
        current_kg,
        session_rules: Vec::new(),
        session_facts: Vec::new(),
        disconnect_rx,
        display_config,
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
                    let _ = std::io::Write::flush(&mut std::io::stdout());
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
            // Skip empty lines and line comments
            if line.is_empty() || line.starts_with("//") {
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
fn strip_block_comments(source: &str) -> String {
    let mut result = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut depth = 0;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        // Track string literals - don't strip comments inside strings
        if c == '"' && depth == 0 {
            in_string = !in_string;
            result.push(c);
        } else if in_string {
            // Inside a string, copy everything as-is
            result.push(c);
        } else if c == '/' && chars.peek() == Some(&'*') {
            chars.next();
            depth += 1;
        } else if c == '*' && chars.peek() == Some(&'/') && depth > 0 {
            chars.next();
            depth -= 1;
            if depth == 0 {
                result.push(' ');
            }
        } else if depth == 0 {
            result.push(c);
        }
    }

    result
}

/// Strip inline comments (`//`) from a line, respecting string literals.
fn strip_inline_comment(line: &str) -> &str {
    let mut in_string = false;
    let bytes = line.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == b'"' {
            in_string = !in_string;
        } else if !in_string && bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            return line[..i].trim_end();
        }
    }
    line
}

fn is_complete_statement(line: &str) -> bool {
    let stripped = line.trim();
    if stripped.is_empty() {
        return false;
    }
    // Meta commands are always complete
    if stripped.starts_with('.') {
        return true;
    }
    // Track delimiter balance: (), []
    // Note: angle brackets are NOT tracked because `<-` arrows
    // would create false imbalance (the `<` is not a delimiter).
    let mut paren: i32 = 0;
    let mut bracket: i32 = 0;
    let mut in_string = false;
    for c in stripped.chars() {
        match c {
            '"' => in_string = !in_string,
            '(' if !in_string => paren += 1,
            ')' if !in_string => paren -= 1,
            '[' if !in_string => bracket += 1,
            ']' if !in_string => bracket -= 1,
            _ => {}
        }
    }
    paren <= 0 && bracket <= 0
}

async fn run_repl(state: &mut ReplState) -> Result<(), Box<dyn std::error::Error>> {
    let mut rl = Editor::new()?;
    rl.set_helper(Some(inputlayer::syntax::highlight::DatalogHelper::new()));

    let history_path = get_history_path();
    if history_path.exists() {
        let _ = rl.load_history(&history_path);
    }

    loop {
        // Check if server is still connected
        if !state.is_server_alive() {
            eprintln!();
            eprintln!("Server connection lost. Exiting...");
            let _ = rl.save_history(&history_path);
            std::process::exit(1);
        }

        let prompt = state.prompt();
        match rl.readline(&prompt) {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line);

                match parse_statement(line) {
                    Ok(stmt) => {
                        if let Err(e) = handle_statement(state, stmt).await {
                            println!("Error: {e}");
                        }
                    }
                    Err(e) => {
                        println!("Parse error: {e}");
                        println!("Type .help for syntax reference.");
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                println!("Error: {err:?}");
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

fn get_history_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        let config_dir = home.join(".inputlayer");
        let _ = std::fs::create_dir_all(&config_dir);
        config_dir.join("history")
    } else {
        PathBuf::from(".inputlayer_history")
    }
}

async fn handle_statement(state: &mut ReplState, stmt: Statement) -> Result<(), String> {
    match stmt {
        Statement::Meta(cmd) => handle_meta_command(state, cmd).await,
        Statement::Insert(op) => handle_insert(state, op).await,
        Statement::Delete(op) => handle_delete(state, op).await,
        Statement::Query(goal) => handle_query(state, goal).await,
        Statement::SessionRule(rule) => handle_session_rule(state, rule).await,
        Statement::PersistentRule(rule) => handle_persistent_rule(state, rule).await,
        Statement::Fact(rule) => handle_fact(state, rule).await,
        Statement::DeleteRelationOrRule(name) => handle_delete_relation(state, name).await,
        Statement::SchemaDecl(decl) => handle_schema_decl(state, decl).await,
        Statement::TypeDecl(decl) => {
            println!("Type '{}' declared (local only).", decl.name);
            Ok(())
        }
        Statement::Update(update) => handle_update(state, update).await,
    }
}

async fn handle_meta_command(state: &mut ReplState, cmd: MetaCommand) -> Result<(), String> {
    match cmd {
        MetaCommand::KgShow => {
            if let Some(ref kg) = state.current_kg {
                println!("Current knowledge graph: {kg}");
            } else {
                println!("No knowledge graph selected.");
            }
        }

        MetaCommand::KgList => {
            let url = state.http.api_url("/knowledge-graphs");
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            let result: ApiResponse<KnowledgeGraphListResponse> =
                resp.json().await.map_err(|e| format!("{e}"))?;

            let knowledge_graphs = result.data.map(|d| d.knowledge_graphs).unwrap_or_default();
            if knowledge_graphs.is_empty() {
                println!("No knowledge graphs found.");
            } else {
                println!("Knowledge Graphs:");
                for kg in knowledge_graphs {
                    let marker = if state.current_kg.as_ref() == Some(&kg.name) {
                        " *"
                    } else {
                        ""
                    };
                    println!("  {}{}", kg.name, marker);
                }
            }
        }

        MetaCommand::KgCreate(name) => {
            let url = state.http.api_url("/knowledge-graphs");
            let req = CreateKnowledgeGraphRequest { name: name.clone() };
            let resp = state
                .http
                .client
                .post(&url)
                .json(&req)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                let body = resp.text().await.unwrap_or_default();
                if let Ok(error) = serde_json::from_str::<ApiResponse<()>>(&body) {
                    return Err(error
                        .error
                        .map_or("Create failed".to_string(), |e| e.message));
                }
                return Err(format!("Create failed: {body}"));
            }

            println!("Knowledge graph '{name}' created.");
            state.current_kg = Some(name.clone());
            println!("Switched to knowledge graph: {name}");
        }

        MetaCommand::KgUse(name) => {
            // Verify knowledge graph exists
            let url = state.http.api_url(&format!("/knowledge-graphs/{name}"));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                return Err(format!("Knowledge graph '{name}' not found"));
            }

            state.current_kg = Some(name.clone());
            let rules_count = state.session_rules.len();
            let facts_count = state.session_facts.len();
            state.session_rules.clear();
            state.session_facts.clear();
            println!("Switched to knowledge graph: {name}");
            if rules_count > 0 || facts_count > 0 {
                println!("(Cleared {rules_count} session rule(s), {facts_count} session fact(s))");
            }
        }

        MetaCommand::KgDrop(name) => {
            if state.current_kg.as_ref() == Some(&name) {
                return Err(
                    "Cannot drop current knowledge graph. Switch to another first.".to_string(),
                );
            }
            let url = state.http.api_url(&format!("/knowledge-graphs/{name}"));
            let resp = state
                .http
                .client
                .delete(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                return Err(format!("Failed to drop knowledge graph '{name}'"));
            }
            println!("Knowledge graph '{name}' dropped.");
        }

        MetaCommand::RelList => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state
                .http
                .api_url(&format!("/knowledge-graphs/{db}/relations"));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            let result: ApiResponse<RelationListResponse> =
                resp.json().await.map_err(|e| format!("{e}"))?;

            let mut relations = result.data.map(|d| d.relations).unwrap_or_default();
            // Sort relations alphabetically for deterministic output
            relations.sort_by(|a, b| a.name.cmp(&b.name));
            if relations.is_empty() {
                println!("No relations in current knowledge graph.");
            } else {
                println!("Relations:");
                for rel in relations {
                    println!("  {} (arity: {})", rel.name, rel.arity);
                }
            }
        }

        MetaCommand::RelDescribe(name) => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state.http.api_url(&format!(
                "/knowledge-graphs/{db}/relations/{name}/data?limit=10"
            ));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                return Err(format!("Relation '{name}' not found"));
            }

            let result: ApiResponse<RelationDataResponse> =
                resp.json().await.map_err(|e| format!("{e}"))?;

            let data = result.data.ok_or("No data returned")?;
            display_relation_data(&data, &name);
        }

        MetaCommand::RuleList => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state.http.api_url(&format!("/knowledge-graphs/{db}/rules"));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            let result: ApiResponse<RuleListDto> = resp.json().await.map_err(|e| format!("{e}"))?;

            let rules = result.data.map(|d| d.rules).unwrap_or_default();
            if rules.is_empty() {
                println!("No rules defined.");
            } else {
                println!("Rules:");
                for rule in rules {
                    println!("  {} ({} clause(s))", rule.name, rule.clause_count);
                }
            }
        }

        MetaCommand::RuleQuery(name) => {
            // Query the rule to show its data
            let query = format!("?{name}(X, Y)");
            let result = execute_query(state, query).await?;
            println!("Rule: {name}");
            display_query_result(&result, &state.display_config);
        }

        MetaCommand::RuleDrop(name) => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state
                .http
                .api_url(&format!("/knowledge-graphs/{db}/rules/{name}"));
            let resp = state
                .http
                .client
                .delete(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                return Err(format!("Rule '{name}' not found"));
            }
            println!("Rule '{name}' dropped.");
        }

        MetaCommand::SessionList => {
            let has_facts = !state.session_facts.is_empty();
            let has_rules = !state.session_rules.is_empty();

            if !has_facts && !has_rules {
                println!("No session data defined.");
            } else {
                if has_facts {
                    println!("Session facts ({}):", state.session_facts.len());
                    for (i, fact) in state.session_facts.iter().enumerate() {
                        println!("  {}. {}", i + 1, format_rule(fact));
                    }
                }
                if has_rules {
                    println!("Session rules ({}):", state.session_rules.len());
                    for (i, rule) in state.session_rules.iter().enumerate() {
                        println!("  {}. {}", i + 1, format_rule(rule));
                    }
                }
            }
        }

        MetaCommand::SessionClear => {
            let facts_count = state.session_facts.len();
            let rules_count = state.session_rules.len();
            state.session_facts.clear();
            state.session_rules.clear();
            println!("Cleared {facts_count} session fact(s), {rules_count} session rule(s).");
        }

        MetaCommand::SessionDrop(index) => {
            if index >= state.session_rules.len() {
                return Err(format!("Rule index {} out of bounds.", index + 1));
            }
            let removed = state.session_rules.remove(index);
            println!("Removed rule {}: {}", index + 1, format_rule(&removed));
        }

        MetaCommand::SessionDropName(name) => {
            let before = state.session_rules.len();
            state.session_rules.retain(|r| r.head.relation != name);
            let removed = before - state.session_rules.len();
            if removed == 0 {
                return Err(format!("No session rules found for relation '{name}'."));
            }
            println!("Dropped {removed} session rule(s) for '{name}'.");
        }

        MetaCommand::Status => {
            let health_url = state.http.api_url("/health");
            let health_resp = state
                .http
                .client
                .get(&health_url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;
            let health: ApiResponse<HealthResponse> =
                health_resp.json().await.map_err(|e| format!("{e}"))?;
            let health_data = health.data.ok_or("No health data")?;

            let stats_url = state.http.api_url("/stats");
            let stats_resp = state
                .http
                .client
                .get(&stats_url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;
            let stats: ApiResponse<StatsResponse> =
                stats_resp.json().await.map_err(|e| format!("{e}"))?;
            let stats_data = stats.data.unwrap_or(StatsResponse {
                knowledge_graphs: 0,
                relations: 0,
                views: 0,
                memory_usage_bytes: 0,
                query_count: 0,
                uptime_secs: 0,
            });

            println!("Server Status");
            println!("  Health: {}", health_data.status);
            println!("  Version: {}", health_data.version);
            println!("  Uptime: {} seconds", health_data.uptime_secs);
            println!("  Total queries: {}", stats_data.query_count);
        }

        MetaCommand::Help => print_help(),

        MetaCommand::Quit => {
            println!("Goodbye!");
            std::process::exit(0);
        }

        MetaCommand::Compact => {
            println!("Compaction command not available over HTTP.");
        }

        MetaCommand::RuleShowDef(_) | MetaCommand::RuleEdit { .. } | MetaCommand::RuleClear(_) => {
            println!("This command is available in embedded mode. HTTP support is planned for a future release.");
        }

        MetaCommand::RuleRemove { name, index } => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            // Server expects 1-based index, but we already converted to 0-based in parsing
            // So convert back to 1-based for the API
            let one_based_index = index + 1;
            let url = state.http.api_url(&format!(
                "/knowledge-graphs/{db}/rules/{name}/{one_based_index}"
            ));
            let resp = state
                .http
                .client
                .delete(&url)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                let body: serde_json::Value = resp.json().await.unwrap_or_default();
                let err_msg = body
                    .get("error")
                    .and_then(|e| {
                        // Try as object with message field first, then as string
                        e.get("message")
                            .and_then(|m| m.as_str())
                            .or_else(|| e.as_str())
                    })
                    .unwrap_or("Unknown error");
                return Err(err_msg.to_string());
            }
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            if let Some(data) = body.get("data") {
                if let Some(msg) = data.get("message").and_then(|m| m.as_str()) {
                    println!("{msg}");
                }
            } else {
                println!("Clause {one_based_index} removed from rule '{name}'.");
            }
        }

        MetaCommand::Load { path, .. } => {
            println!("Loading file: {path}");
            execute_script(state, &path).await?;
            println!("File loaded.");
        }

        MetaCommand::IndexList => {
            println!("Index commands are available in embedded mode. HTTP support is planned for a future release.");
        }

        MetaCommand::IndexCreate(opts) => {
            println!(
                "Would create index '{}' on {}.{} (type: {}, metric: {})",
                opts.name,
                opts.relation,
                opts.column,
                opts.index_type,
                opts.metric.as_deref().unwrap_or("default")
            );
            println!("Index creation is available in embedded mode. HTTP support is planned for a future release.");
        }

        MetaCommand::IndexDrop(name) => {
            println!("Would drop index '{name}'");
            println!("Index management is available in embedded mode. HTTP support is planned for a future release.");
        }

        MetaCommand::IndexStats(name) => {
            println!("Would show stats for index '{name}'");
            println!("Index management is available in embedded mode. HTTP support is planned for a future release.");
        }

        MetaCommand::IndexRebuild(name) => {
            println!("Would rebuild index '{name}'");
            println!("Index management is available in embedded mode. HTTP support is planned for a future release.");
        }
    }

    Ok(())
}

async fn execute_query(state: &ReplState, query: String) -> Result<QueryResponse, String> {
    let knowledge_graph = state
        .current_kg
        .clone()
        .ok_or("No knowledge graph selected")?;
    let url = state.http.api_url("/query/execute");
    let req = QueryRequest {
        query,
        knowledge_graph,
        timeout_ms: Some(30000),
    };

    let resp = state
        .http
        .client
        .post(&url)
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;

    if !resp.status().is_success() {
        let error_text = resp.text().await.unwrap_or_default();
        return Err(format!("Query failed: {error_text}"));
    }

    let result: ApiResponse<QueryResponse> = resp.json().await.map_err(|e| format!("{e}"))?;

    // First check for API-level error
    let query_response = result.data.ok_or_else(|| {
        result
            .error
            .map_or("Unknown error".to_string(), |e| e.message)
    })?;

    // Then check for query-level error (e.g., stratification errors)
    if let Some(error) = &query_response.error {
        return Err(error.clone());
    }

    Ok(query_response)
}

async fn handle_insert(
    state: &mut ReplState,
    op: inputlayer::statement::InsertOp,
) -> Result<(), String> {
    let db = state
        .current_kg
        .as_ref()
        .ok_or("No knowledge graph selected")?;
    let url = state.http.api_url(&format!(
        "/knowledge-graphs/{}/relations/{}/data",
        db, op.relation
    ));

    let rows: Vec<Vec<serde_json::Value>> = op
        .tuples
        .iter()
        .map(|tuple| tuple.iter().map(term_to_json).collect())
        .collect();

    let req = InsertDataRequest { rows };
    let resp = state
        .http
        .client
        .post(&url)
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;

    if !resp.status().is_success() {
        let error_text = resp.text().await.unwrap_or_default();
        return Err(extract_error_message(&error_text));
    }

    let result: ApiResponse<InsertDataResponse> = resp.json().await.map_err(|e| format!("{e}"))?;

    let data = result.data.ok_or("No response data")?;
    if data.rows_inserted == 0 && data.duplicates > 0 {
        println!("No facts inserted ({} duplicate skipped).", data.duplicates);
    } else {
        println!(
            "Inserted {} fact(s) into '{}'.",
            data.rows_inserted, op.relation
        );
    }
    Ok(())
}

async fn handle_delete(
    state: &mut ReplState,
    op: inputlayer::statement::DeleteOp,
) -> Result<(), String> {
    use inputlayer::statement::DeletePattern;

    match op.pattern {
        DeletePattern::SingleTuple(terms) => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state.http.api_url(&format!(
                "/knowledge-graphs/{}/relations/{}/data",
                db, op.relation
            ));

            let row: Vec<serde_json::Value> = terms.iter().map(term_to_json).collect();
            let req = DeleteDataRequest { rows: vec![row] };

            let resp = state
                .http
                .client
                .delete(&url)
                .json(&req)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                let error_text = resp.text().await.unwrap_or_default();
                return Err(format!("Delete failed: {error_text}"));
            }

            let result: ApiResponse<DeleteDataResponse> =
                resp.json().await.map_err(|e| format!("{e}"))?;

            let data = result.data.ok_or("No response data")?;
            println!(
                "Deleted {} facts from '{}'.",
                data.rows_deleted, op.relation
            );
        }
        DeletePattern::BulkTuples(tuples) => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state.http.api_url(&format!(
                "/knowledge-graphs/{}/relations/{}/data",
                db, op.relation
            ));

            let rows: Vec<Vec<serde_json::Value>> = tuples
                .iter()
                .map(|t| t.iter().map(term_to_json).collect())
                .collect();
            let req = DeleteDataRequest { rows };

            let resp = state
                .http
                .client
                .delete(&url)
                .json(&req)
                .send()
                .await
                .map_err(|e| format!("{e}"))?;

            if !resp.status().is_success() {
                let error_text = resp.text().await.unwrap_or_default();
                return Err(format!("Delete failed: {error_text}"));
            }

            let result: ApiResponse<DeleteDataResponse> =
                resp.json().await.map_err(|e| format!("{e}"))?;

            let data = result.data.ok_or("No response data")?;
            println!(
                "Deleted {} facts from '{}'.",
                data.rows_deleted, op.relation
            );
        }
        DeletePattern::Conditional { head_args, body } => {
            // Format head arguments
            let head_str: String = head_args
                .iter()
                .map(format_term)
                .collect::<Vec<_>>()
                .join(", ");

            // Format body predicates
            let body_str: String = body
                .iter()
                .map(format_body_pred)
                .collect::<Vec<_>>()
                .join(", ");

            // Build the conditional delete statement
            let delete_stmt = format!("-{}({}) <- {}", op.relation, head_str, body_str);

            // Send through query API
            let response = execute_query(state, delete_stmt).await?;

            // Print the result messages (rows contain message strings)
            for row in &response.rows {
                if let Some(msg) = row.first() {
                    if let Some(s) = msg.as_str() {
                        println!("{s}");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_query(
    state: &mut ReplState,
    goal: inputlayer::statement::QueryGoal,
) -> Result<(), String> {
    // Build query program with session facts and rules
    let mut program = String::new();

    // Add session facts (transient, not persisted)
    for fact in &state.session_facts {
        program.push_str(&format_rule(fact));
        program.push('\n');
    }

    // Add session rules
    for rule in &state.session_rules {
        program.push_str(&format_rule(rule));
        program.push('\n');
    }

    // Add the query
    let query_args: Vec<String> = goal.goal.args.iter().map(format_term).collect();
    let mut body_parts: Vec<String> =
        vec![format!("{}({})", goal.goal.relation, query_args.join(", "))];

    // Add additional body predicates (for complex queries like ?- foo(X), bar(Y).)
    for pred in &goal.body {
        body_parts.push(format_body_pred(pred));
    }

    program.push_str(&format!("?{}", body_parts.join(", ")));

    let response = execute_query(state, program).await?;
    display_query_result(&response, &state.display_config);
    Ok(())
}

async fn handle_session_rule(
    state: &mut ReplState,
    rule: inputlayer::ast::Rule,
) -> Result<(), String> {
    let head_relation = rule.head.relation.clone();
    let head_arity = rule.head.effective_arity();

    // Validate aggregation/arity compatibility with existing session rules
    inputlayer::rule_catalog::validate_session_rule_compatibility(&state.session_rules, &rule)?;

    state.session_rules.push(rule);

    // Generate variables matching the rule's effective output arity
    let var_names = ["X", "Y", "Z", "W", "V", "U", "T", "S", "R", "Q"];
    let args: Vec<Term> = (0..head_arity)
        .map(|i| {
            let name = if i < var_names.len() {
                var_names[i].to_string()
            } else {
                format!("V{i}")
            };
            Term::Variable(name)
        })
        .collect();

    // Execute query to show results
    let goal = inputlayer::statement::QueryGoal {
        goal: inputlayer::ast::Atom {
            relation: head_relation.clone(),
            args,
        },
        body: vec![],
    };

    handle_query(state, goal).await?;
    println!("(session: {} rule(s))", state.session_rules.len());
    Ok(())
}

async fn handle_persistent_rule(
    state: &mut ReplState,
    rule: inputlayer::ast::Rule,
) -> Result<(), String> {
    // Send the rule as a query - the server will register it
    let rule_text = format!("+{}", format_rule(&rule));
    let _ = execute_query(state, rule_text).await?;
    println!("Rule '{}' registered.", rule.head.relation);
    Ok(())
}

async fn handle_fact(state: &mut ReplState, rule: inputlayer::ast::Rule) -> Result<(), String> {
    // Session facts are NOT persisted - they are only available for queries during this session
    // Use +relation(args). syntax to persist facts permanently

    // Validate that all terms are ground values (constants, not variables)
    validate_fact(&rule)?;

    let relation = rule.head.relation.clone();
    state.session_facts.push(rule);
    println!("Session fact added for '{relation}'. (Use +{relation}(...) to persist)");
    Ok(())
}

async fn handle_delete_relation(state: &mut ReplState, name: String) -> Result<(), String> {
    let db = state
        .current_kg
        .as_ref()
        .ok_or("No knowledge graph selected")?;

    // Try dropping as a rule first
    let url = state
        .http
        .api_url(&format!("/knowledge-graphs/{db}/rules/{name}"));
    let resp = state
        .http
        .client
        .delete(&url)
        .send()
        .await
        .map_err(|e| format!("{e}"))?;

    if resp.status().is_success() {
        println!("Rule '{name}' deleted.");
        return Ok(());
    }

    Err(format!(
        "'{name}' is not a rule. Use conditional delete to remove facts."
    ))
}

async fn handle_schema_decl(
    state: &mut ReplState,
    decl: inputlayer::statement::SchemaDecl,
) -> Result<(), String> {
    // Send schema declaration as a query
    let prefix = if decl.persistent { "+" } else { "" };
    let cols: Vec<String> = decl
        .columns
        .iter()
        .map(|col| format!("{}: {}", col.name, col.col_type))
        .collect();
    let schema_text = format!("{}{}({}).", prefix, decl.name, cols.join(", "));

    let _ = execute_query(state, schema_text).await?;

    if decl.persistent {
        println!("Schema '{}' declared (persistent).", decl.name);
    } else {
        println!("Schema '{}' declared (session).", decl.name);
    }
    Ok(())
}

async fn handle_update(
    state: &mut ReplState,
    update: inputlayer::statement::UpdateOp,
) -> Result<(), String> {
    // Build update as query text
    let mut update_text = String::new();

    for (i, target) in update.deletes.iter().enumerate() {
        if i > 0 {
            update_text.push_str(", ");
        }
        let args: Vec<String> = target.args.iter().map(format_term).collect();
        update_text.push_str(&format!("-{}({})", target.relation, args.join(", ")));
    }

    for target in &update.inserts {
        if !update_text.is_empty() {
            update_text.push_str(", ");
        }
        let args: Vec<String> = target.args.iter().map(format_term).collect();
        update_text.push_str(&format!("+{}({})", target.relation, args.join(", ")));
    }

    update_text.push_str(" <- ");

    let mut condition_parts = Vec::new();
    for pred in &update.body {
        condition_parts.push(format_body_pred(pred));
    }
    update_text.push_str(&condition_parts.join(", "));

    let _ = execute_query(state, update_text).await?;
    println!("Update executed.");
    Ok(())
}

/// Extract error message from JSON API error response
fn extract_error_message(body: &str) -> String {
    // Try to parse as JSON error response
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(error) = json.get("error") {
            if let Some(message) = error.get("message") {
                if let Some(msg) = message.as_str() {
                    return msg.to_string();
                }
            }
        }
    }
    // Fall back to raw body
    body.to_string()
}

/// Validate that a term is a ground value (suitable for facts).
/// Returns an error message if the term is not a valid constant.
fn validate_fact_term(term: &Term) -> Result<(), String> {
    match term {
        Term::Constant(_)
        | Term::FloatConstant(_)
        | Term::StringConstant(_)
        | Term::BoolConstant(_)
        | Term::VectorLiteral(_) => Ok(()),
        Term::Variable(v) => Err(format!(
            "Cannot use variable '{v}' in a fact - use constants only (wrap in quotes for strings)"
        )),
        Term::Placeholder => {
            Err("Cannot use placeholder '_' in a fact - use constants only".to_string())
        }
        Term::Arithmetic(_) => {
            Err("Cannot use arithmetic expression in a fact - use constants only".to_string())
        }
        Term::Aggregate(_, _) => {
            Err("Cannot use aggregate in a fact - use constants only".to_string())
        }
        Term::FunctionCall(_, _) => {
            Err("Cannot use function call in a fact - use constants only".to_string())
        }
        Term::FieldAccess(_, _) => {
            Err("Cannot use field access in a fact - use constants only".to_string())
        }
        Term::RecordPattern(_) => {
            Err("Cannot use record pattern in a fact - use constants only".to_string())
        }
    }
}

/// Validate that all terms in a fact (rule with empty body) are ground values.
fn validate_fact(rule: &inputlayer::ast::Rule) -> Result<(), String> {
    for (i, term) in rule.head.args.iter().enumerate() {
        validate_fact_term(term).map_err(|e| format!("Argument {}: {}", i + 1, e))?;
    }
    Ok(())
}

fn term_to_json(term: &Term) -> serde_json::Value {
    match term {
        Term::Constant(n) => serde_json::Value::Number((*n).into()),
        Term::FloatConstant(f) => serde_json::json!(*f),
        Term::StringConstant(s) => serde_json::Value::String(s.clone()),
        Term::BoolConstant(b) => serde_json::Value::Bool(*b),
        Term::VectorLiteral(v) => {
            serde_json::Value::Array(v.iter().map(|x| serde_json::json!(*x)).collect())
        }
        Term::Variable(_) => serde_json::Value::Null,
        Term::Placeholder => serde_json::Value::Null,
        Term::Arithmetic(_) => serde_json::Value::Null,
        Term::Aggregate(_, _) => serde_json::Value::Null,
        Term::FunctionCall(_, _) => serde_json::Value::Null,
        Term::FieldAccess(_, _) => serde_json::Value::Null,
        Term::RecordPattern(_) => serde_json::Value::Null,
    }
}

// ── Table formatting ──────────────────────────────────────────────

/// Format a JSON value for display in a table cell.
fn format_cell_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Number(n) => {
            let s = n.to_string();
            s.replace("e+", "e")
        }
        serde_json::Value::String(s) => format!("\"{s}\""),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_cell_value).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => format!("{obj:?}"),
    }
}

/// Truncate a string to `max` characters, appending `…` if truncated.
fn truncate_str(s: &str, max: usize) -> String {
    if max == 0 || s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{truncated}\u{2026}")
    }
}

/// Detect whether a column contains only numeric values (for right-alignment).
fn is_numeric_column(rows: &[Vec<serde_json::Value>], col_idx: usize) -> bool {
    rows.iter()
        .all(|row| row.get(col_idx).is_some_and(serde_json::Value::is_number))
}

/// Compute the display width for each column, capped at `max_width`.
fn compute_column_widths(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
    max_width: usize,
) -> Vec<usize> {
    let mut widths: Vec<usize> = columns.iter().map(std::string::String::len).collect();
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                let cell = format_cell_value(val);
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
    }
    if max_width > 0 {
        for w in &mut widths {
            if *w > max_width {
                *w = max_width;
            }
        }
    }
    widths
}

/// Display a query result as a formatted table.
fn display_query_result(response: &QueryResponse, config: &DisplayConfig) {
    if response.rows.is_empty() {
        println!("No results.");
        return;
    }

    let total_rows = response.rows.len();
    let display_rows = if config.max_rows > 0 && total_rows > config.max_rows {
        config.max_rows
    } else {
        total_rows
    };

    let visible_rows = &response.rows[..display_rows];
    let widths = compute_column_widths(&response.columns, visible_rows, config.max_col_width);
    let numeric: Vec<bool> = (0..response.columns.len())
        .map(|i| is_numeric_column(visible_rows, i))
        .collect();

    // Top border
    let top: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("\u{252c}");
    println!("\u{250c}{top}\u{2510}");

    // Header
    let header: String = widths
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let name = response
                .columns
                .get(i)
                .map_or("", std::string::String::as_str);
            format!(" {name:<w$} ")
        })
        .collect::<Vec<_>>()
        .join("\u{2502}");
    println!("\u{2502}{header}\u{2502}");

    // Header separator
    let sep: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("\u{253c}");
    println!("\u{251c}{sep}\u{2524}");

    // Data rows
    for row in visible_rows {
        let cells: String = widths
            .iter()
            .enumerate()
            .map(|(i, w)| {
                let raw = row.get(i).map(format_cell_value).unwrap_or_default();
                let cell = truncate_str(&raw, config.max_col_width);
                if numeric[i] {
                    format!(" {cell:>w$} ")
                } else {
                    format!(" {cell:<w$} ")
                }
            })
            .collect::<Vec<_>>()
            .join("\u{2502}");
        println!("\u{2502}{cells}\u{2502}");
    }

    // Bottom border
    let bottom: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("\u{2534}");
    println!("\u{2514}{bottom}\u{2518}");

    // Row count + truncation notice
    if display_rows < total_rows {
        if config.show_timing {
            println!(
                "{display_rows} of {total_rows} rows ({}ms). Use --limit 0 for all rows.",
                response.execution_time_ms
            );
        } else {
            println!("{display_rows} of {total_rows} rows. Use --limit 0 for all rows.");
        }
    } else if config.show_timing {
        println!("{total_rows} rows ({}ms)", response.execution_time_ms);
    } else {
        println!("{total_rows} rows");
    }
}

/// Display relation data (from `.rel name`) as a table.
fn display_relation_data(data: &RelationDataResponse, name: &str) {
    println!("Relation: {name}");
    if data.rows.is_empty() {
        println!("  No data.");
        return;
    }

    let widths = compute_column_widths(&data.columns, &data.rows, 40);
    let numeric: Vec<bool> = (0..data.columns.len())
        .map(|i| is_numeric_column(&data.rows, i))
        .collect();

    // Top border
    let top: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("\u{252c}");
    println!("\u{250c}{top}\u{2510}");

    // Header
    let header: String = widths
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let name = data.columns.get(i).map_or("", std::string::String::as_str);
            format!(" {name:<w$} ")
        })
        .collect::<Vec<_>>()
        .join("\u{2502}");
    println!("\u{2502}{header}\u{2502}");

    // Header separator
    let sep: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("\u{253c}");
    println!("\u{251c}{sep}\u{2524}");

    // Data rows
    for row in &data.rows {
        let cells: String = widths
            .iter()
            .enumerate()
            .map(|(i, w)| {
                let raw = row.get(i).map(format_cell_value).unwrap_or_default();
                let cell = truncate_str(&raw, 40);
                if numeric[i] {
                    format!(" {cell:>w$} ")
                } else {
                    format!(" {cell:<w$} ")
                }
            })
            .collect::<Vec<_>>()
            .join("\u{2502}");
        println!("\u{2502}{cells}\u{2502}");
    }

    // Bottom border
    let bottom: String = widths
        .iter()
        .map(|w| "\u{2500}".repeat(w + 2))
        .collect::<Vec<_>>()
        .join("\u{2534}");
    println!("\u{2514}{bottom}\u{2518}");

    println!("{} of {} total rows", data.rows.len(), data.total_count);
}

/// Format a rule as Datalog text (uses Rule's Display impl)
fn format_rule(rule: &inputlayer::ast::Rule) -> String {
    rule.to_string()
}

/// Format a body predicate as Datalog text (uses BodyPredicate's Display impl)
#[allow(dead_code)]
fn format_body_pred(pred: &inputlayer::ast::BodyPredicate) -> String {
    pred.to_string()
}

/// Format a term as Datalog text (uses Term's Display impl)
#[allow(dead_code)]
fn format_term(term: &Term) -> String {
    term.to_string()
}

/// Background heartbeat task that monitors server health
/// Sends disconnect signal if server becomes unresponsive
async fn heartbeat_task(base_url: String, disconnect_tx: watch::Sender<bool>) {
    let client = Client::builder()
        .timeout(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS))
        .build()
        .unwrap_or_else(|_| Client::new());

    let health_url = format!("{base_url}/api/v1/health");
    let mut consecutive_failures: u32 = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(HEARTBEAT_INTERVAL_SECS)).await;

        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                consecutive_failures = 0;
            }
            Ok(_) | Err(_) => {
                consecutive_failures += 1;
                if consecutive_failures >= HEARTBEAT_MAX_FAILURES {
                    // Server is unresponsive, signal disconnect
                    let _ = disconnect_tx.send(true);
                    break;
                }
            }
        }
    }
}

fn print_help() {
    println!("InputLayer Client (HTTP)");
    println!("================================");
    println!();
    println!("Meta Commands:");
    println!("  .kg                  Show current knowledge graph");
    println!("  .kg list             List all knowledge graphs");
    println!("  .kg create <name>    Create knowledge graph");
    println!("  .kg use <name>       Switch to knowledge graph");
    println!("  .kg drop <name>      Drop knowledge graph");
    println!("  .rel                 List relations");
    println!("  .rel <name>          Describe relation");
    println!("  .rule                List rules");
    println!("  .rule <name>         Query rule");
    println!("  .rule drop <name>    Drop all clauses of a rule");
    println!("  .rule remove <name> <n>  Remove clause n from rule (1-based)");
    println!("  .session             List session rules");
    println!("  .session clear       Clear all session rules");
    println!("  .session drop <n|name>  Drop session rule by index or relation name");
    println!("  .status              Server status");
    println!("  .help                Show this help");
    println!("  .quit                Exit");
    println!();
    println!("Data Manipulation:");
    println!("  edge(1, 2)           Insert fact");
    println!("  -edge(1, 2)          Delete fact");
    println!();
    println!("Rules:");
    println!("  +path(X, Y) <- edge(X, Y)    Persistent rule");
    println!("  foo(X, Y) <- bar(X, Y)       Session rule");
    println!();
    println!("Queries:");
    println!("  ?path(1, X)          Query");
    println!();
}

// Unit Tests
#[cfg(test)]
mod tests {
    use super::*;
    use inputlayer::ast::{Atom, Rule, Term};

    /// Helper to create a fact (rule with empty body)
    fn make_fact(relation: &str, args: Vec<Term>) -> Rule {
        Rule {
            head: Atom {
                relation: relation.to_string(),
                args,
            },
            body: vec![],
        }
    }

    // Happy Path Tests
    #[test]
    fn test_validate_fact_with_integer_constant() {
        let fact = make_fact(
            "person",
            vec![
                Term::StringConstant("alice".to_string()),
                Term::Constant(30),
            ],
        );
        assert!(validate_fact(&fact).is_ok());
    }

    #[test]
    fn test_validate_fact_with_string_constant() {
        let fact = make_fact("name", vec![Term::StringConstant("alice".to_string())]);
        assert!(validate_fact(&fact).is_ok());
    }

    #[test]
    fn test_validate_fact_with_float_constant() {
        let fact = make_fact("price", vec![Term::FloatConstant(19.99)]);
        assert!(validate_fact(&fact).is_ok());
    }

    #[test]
    fn test_validate_fact_with_vector_literal() {
        let fact = make_fact("embedding", vec![Term::VectorLiteral(vec![1.0, 2.0, 3.0])]);
        assert!(validate_fact(&fact).is_ok());
    }

    #[test]
    fn test_validate_fact_with_multiple_constants() {
        let fact = make_fact(
            "person",
            vec![
                Term::StringConstant("alice".to_string()),
                Term::Constant(30),
                Term::FloatConstant(1.75),
            ],
        );
        assert!(validate_fact(&fact).is_ok());
    }

    #[test]
    fn test_validate_fact_with_empty_args() {
        let fact = make_fact("empty", vec![]);
        assert!(validate_fact(&fact).is_ok());
    }

    // Error Path Tests - Variables
    #[test]
    fn test_reject_fact_with_variable() {
        // This is the bug case: person(Ruben, 2).
        // "Ruben" starts with uppercase, so it's parsed as a variable
        let fact = make_fact(
            "person",
            vec![Term::Variable("Ruben".to_string()), Term::Constant(2)],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("variable 'Ruben'"),
            "Expected error about variable, got: {}",
            err
        );
        assert!(
            err.contains("Argument 1"),
            "Expected argument number, got: {}",
            err
        );
    }

    #[test]
    fn test_reject_fact_with_variable_in_second_position() {
        let fact = make_fact(
            "person",
            vec![
                Term::StringConstant("alice".to_string()),
                Term::Variable("Age".to_string()),
            ],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains("variable 'Age'"),
            "Expected error about variable, got: {}",
            err
        );
        assert!(
            err.contains("Argument 2"),
            "Expected argument number 2, got: {}",
            err
        );
    }

    #[test]
    fn test_reject_fact_with_anonymous_variable() {
        let fact = make_fact(
            "person",
            vec![Term::Variable("_".to_string()), Term::Constant(30)],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("variable '_'"));
    }

    // Error Path Tests - Placeholders
    #[test]
    fn test_reject_fact_with_placeholder() {
        let fact = make_fact(
            "person",
            vec![Term::StringConstant("alice".to_string()), Term::Placeholder],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("placeholder '_'"));
    }

    // Error Path Tests - Expressions
    #[test]
    fn test_reject_fact_with_arithmetic() {
        use inputlayer::ast::{ArithExpr, ArithOp};
        let fact = make_fact(
            "result",
            vec![Term::Arithmetic(ArithExpr::Binary {
                op: ArithOp::Add,
                left: Box::new(ArithExpr::Constant(1)),
                right: Box::new(ArithExpr::Constant(2)),
            })],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("arithmetic expression"));
    }

    #[test]
    fn test_reject_fact_with_aggregate() {
        use inputlayer::ast::AggregateFunc;
        let fact = make_fact(
            "result",
            vec![Term::Aggregate(AggregateFunc::Count, "x".to_string())],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("aggregate"));
    }

    #[test]
    fn test_reject_fact_with_function_call() {
        use inputlayer::ast::BuiltinFunc;
        let fact = make_fact(
            "result",
            vec![Term::FunctionCall(BuiltinFunc::TimeNow, vec![])],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("function call"));
    }

    #[test]
    fn test_reject_fact_with_field_access() {
        let fact = make_fact(
            "result",
            vec![Term::FieldAccess(
                Box::new(Term::Variable("X".to_string())),
                "field".to_string(),
            )],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("field access"));
    }

    #[test]
    fn test_reject_fact_with_record_pattern() {
        let fact = make_fact(
            "result",
            vec![Term::RecordPattern(vec![(
                "id".to_string(),
                Term::Variable("X".to_string()),
            )])],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("record pattern"));
    }

    // Edge Cases
    #[test]
    fn test_reject_mixed_valid_and_invalid() {
        // First arg valid, second arg invalid
        let fact = make_fact(
            "mixed",
            vec![
                Term::Constant(42),
                Term::Variable("X".to_string()),
                Term::StringConstant("valid".to_string()),
            ],
        );
        let result = validate_fact(&fact);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Argument 2"));
    }

    #[test]
    fn test_lowercase_string_is_accepted() {
        // Lowercase unquoted identifier becomes StringConstant, which is valid
        // This tests that "sam" (lowercase) is accepted as a string constant
        let fact = make_fact(
            "person",
            vec![Term::StringConstant("sam".to_string()), Term::Constant(1)],
        );
        assert!(validate_fact(&fact).is_ok());
    }

    #[test]
    fn test_error_message_is_helpful() {
        let fact = make_fact("person", vec![Term::Variable("Name".to_string())]);
        let result = validate_fact(&fact);
        let err = result.unwrap_err();
        // Error should suggest using quotes
        assert!(
            err.contains("wrap in quotes for strings"),
            "Expected helpful suggestion, got: {}",
            err
        );
    }

    // is_complete_statement tests
    #[test]
    fn test_complete_statement_simple_fact() {
        assert!(is_complete_statement("+edge(1, 2)"));
    }

    #[test]
    fn test_complete_statement_rule_with_arrow() {
        // The `<-` arrow must NOT cause the statement to be incomplete
        assert!(is_complete_statement("path(X, Y) <- edge(X, Y)"));
    }

    #[test]
    fn test_complete_statement_rule_with_comparison() {
        assert!(is_complete_statement("path(X, Y) <- edge(X, Y), X > 1"));
    }

    #[test]
    fn test_complete_statement_meta_command() {
        assert!(is_complete_statement(".session clear"));
        assert!(is_complete_statement(".kg list"));
    }

    #[test]
    fn test_complete_statement_empty() {
        assert!(!is_complete_statement(""));
        assert!(!is_complete_statement("   "));
    }

    #[test]
    fn test_complete_statement_unbalanced_parens() {
        assert!(!is_complete_statement("edge(1, 2"));
    }

    #[test]
    fn test_complete_statement_balanced_brackets() {
        assert!(is_complete_statement("+edge[(1,2), (3,4)]"));
    }

    #[test]
    fn test_complete_statement_aggregate_with_angle_brackets() {
        // Aggregates use angle brackets but should still be complete
        assert!(is_complete_statement(
            "top2(top_k<2, X, Y:desc>) <- score(X, Y)"
        ));
    }

    #[test]
    fn test_complete_statement_query() {
        assert!(is_complete_statement("?path(1, X)"));
    }

    #[test]
    fn test_complete_statement_session_rule_no_plus() {
        assert!(is_complete_statement("mortal(X) <- human(X)"));
    }
}
