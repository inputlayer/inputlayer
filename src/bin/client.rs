//! InputLayer Client Binary - HTTP-based Datalog Client
//!
//! Interactive client for InputLayer that connects to the server via HTTP REST API.
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

// ============================================================================
// DTO Types (matching REST API)
// ============================================================================
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

// ============================================================================
// Client State
// ============================================================================

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
            Some(db) => format!("{}{}> ", db, session_indicator),
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
                    result.server = args[i + 1].clone();
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
            arg if arg.ends_with(".dl") => {
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
        .map_err(|e| format!("Failed to connect to server: {}", e))?;

    if !health_resp.status().is_success() {
        return Err(format!("Server returned error: {}", health_resp.status()).into());
    }

    let health: ApiResponse<HealthResponse> = health_resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse health response: {}", e))?;

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
        .map_err(|e| format!("Failed to list knowledge graphs: {}", e))?;

    let db_list: ApiResponse<KnowledgeGraphListResponse> = db_resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse knowledge graph list: {}", e))?;

    let current_kg = db_list
        .data
        .and_then(|d| d.knowledge_graphs.first().map(|kg| kg.name.clone()));

    if let Some(ref db) = current_kg {
        println!("Current knowledge graph: {}", db);
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
                println!("Script error: {}", e);
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
        let content = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read script '{}': {}", path, e))?;

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
                        if let Err(e) = handle_statement(state, stmt).await {
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(format!("Parse error: {}", e));
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

/// Strip inline comments (% Prolog-style or // C-style) from a line
fn strip_inline_comment(line: &str) -> &str {
    let mut in_string = false;
    let chars: Vec<char> = line.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        if *c == '"' {
            in_string = !in_string;
        } else if !in_string {
            // Check for % comment (Prolog style)
            if *c == '%' {
                let byte_pos = line.char_indices().nth(i).map(|(pos, _)| pos).unwrap_or(line.len());
                return line[..byte_pos].trim_end();
            }
            // Check for // comment (C-style)
            if *c == '/' && i + 1 < chars.len() && chars[i + 1] == '/' {
                let byte_pos = line.char_indices().nth(i).map(|(pos, _)| pos).unwrap_or(line.len());
                return line[..byte_pos].trim_end();
            }
        }
    }
    line
}

fn is_complete_statement(line: &str) -> bool {
    let stripped = line.trim();
    if stripped.is_empty() {
        return false;
    }
    if stripped.starts_with('.') {
        return true;
    }
    stripped.ends_with('.')
}

async fn run_repl(state: &mut ReplState) -> Result<(), Box<dyn std::error::Error>> {
    let mut rl = DefaultEditor::new()?;

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
                            println!("Error: {}", e);
                        }
                    }
                    Err(e) => {
                        println!("Parse error: {}", e);
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
                println!("Error: {:?}", err);
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
                println!("Current knowledge graph: {}", kg);
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
                .map_err(|e| format!("{}", e))?;

            let result: ApiResponse<KnowledgeGraphListResponse> =
                resp.json().await.map_err(|e| format!("{}", e))?;

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
                .map_err(|e| format!("{}", e))?;

            if !resp.status().is_success() {
                let body = resp.text().await.unwrap_or_default();
                if let Ok(error) = serde_json::from_str::<ApiResponse<()>>(&body) {
                    return Err(error
                        .error
                        .map(|e| e.message)
                        .unwrap_or("Create failed".to_string()));
                }
                return Err(format!("Create failed: {}", body));
            }

            println!("Knowledge graph '{}' created.", name);
            state.current_kg = Some(name.clone());
            println!("Switched to knowledge graph: {}", name);
        }

        MetaCommand::KgUse(name) => {
            // Verify knowledge graph exists
            let url = state.http.api_url(&format!("/knowledge-graphs/{}", name));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

            if !resp.status().is_success() {
                return Err(format!("Knowledge graph '{}' not found", name));
            }

            state.current_kg = Some(name.clone());
            let rules_count = state.session_rules.len();
            let facts_count = state.session_facts.len();
            state.session_rules.clear();
            state.session_facts.clear();
            println!("Switched to knowledge graph: {}", name);
            if rules_count > 0 || facts_count > 0 {
                println!(
                    "(Cleared {} session rule(s), {} session fact(s))",
                    rules_count, facts_count
                );
            }
        }

        MetaCommand::KgDrop(name) => {
            if state.current_kg.as_ref() == Some(&name) {
                return Err(
                    "Cannot drop current knowledge graph. Switch to another first.".to_string(),
                );
            }
            let url = state.http.api_url(&format!("/knowledge-graphs/{}", name));
            let resp = state
                .http
                .client
                .delete(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

            if !resp.status().is_success() {
                return Err(format!("Failed to drop knowledge graph '{}'", name));
            }
            println!("Knowledge graph '{}' dropped.", name);
        }

        MetaCommand::RelList => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state
                .http
                .api_url(&format!("/knowledge-graphs/{}/relations", db));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

            let result: ApiResponse<RelationListResponse> =
                resp.json().await.map_err(|e| format!("{}", e))?;

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
                "/knowledge-graphs/{}/relations/{}/data?limit=10",
                db, name
            ));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

            if !resp.status().is_success() {
                return Err(format!("Relation '{}' not found", name));
            }

            let result: ApiResponse<RelationDataResponse> =
                resp.json().await.map_err(|e| format!("{}", e))?;

            let data = result.data.ok_or("No data returned")?;
            println!("Relation: {}", name);
            println!("  Columns: {:?}", data.columns);
            println!("  Total rows: {}", data.total_count);
            if !data.rows.is_empty() {
                println!("  Preview (first {}):", data.rows.len());
                for row in &data.rows {
                    let vals: Vec<String> = row.iter().map(format_json_value).collect();
                    println!("    ({})", vals.join(", "));
                }
            }
        }

        MetaCommand::RuleList => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state
                .http
                .api_url(&format!("/knowledge-graphs/{}/rules", db));
            let resp = state
                .http
                .client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

            let result: ApiResponse<RuleListDto> =
                resp.json().await.map_err(|e| format!("{}", e))?;

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
            let query = format!("?- {}(X, Y).", name);
            let result = execute_query(state, query).await?;
            println!("Rule: {}", name);
            println!("{} rows:", result.rows.len());
            for row in &result.rows {
                let vals: Vec<String> = row.iter().map(format_json_value).collect();
                println!("  ({})", vals.join(", "));
            }
        }

        MetaCommand::RuleDrop(name) => {
            let db = state
                .current_kg
                .as_ref()
                .ok_or("No knowledge graph selected")?;
            let url = state
                .http
                .api_url(&format!("/knowledge-graphs/{}/rules/{}", db, name));
            let resp = state
                .http
                .client
                .delete(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

            if !resp.status().is_success() {
                return Err(format!("Rule '{}' not found", name));
            }
            println!("Rule '{}' dropped.", name);
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
            println!(
                "Cleared {} session fact(s), {} session rule(s).",
                facts_count, rules_count
            );
        }

        MetaCommand::SessionDrop(index) => {
            if index >= state.session_rules.len() {
                return Err(format!("Rule index {} out of bounds.", index + 1));
            }
            let removed = state.session_rules.remove(index);
            println!("Removed rule {}: {}", index + 1, format_rule(&removed));
        }

        MetaCommand::Status => {
            let health_url = state.http.api_url("/health");
            let health_resp = state
                .http
                .client
                .get(&health_url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;
            let health: ApiResponse<HealthResponse> =
                health_resp.json().await.map_err(|e| format!("{}", e))?;
            let health_data = health.data.ok_or("No health data")?;

            let stats_url = state.http.api_url("/stats");
            let stats_resp = state
                .http
                .client
                .get(&stats_url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;
            let stats: ApiResponse<StatsResponse> =
                stats_resp.json().await.map_err(|e| format!("{}", e))?;
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
            println!("This command is not yet implemented over HTTP.");
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
                "/knowledge-graphs/{}/rules/{}/{}",
                db, name, one_based_index
            ));
            let resp = state
                .http
                .client
                .delete(&url)
                .send()
                .await
                .map_err(|e| format!("{}", e))?;

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
                    println!("{}", msg);
                }
            } else {
                println!("Clause {} removed from rule '{}'.", one_based_index, name);
            }
        }

        MetaCommand::Load { path, .. } => {
            println!("Loading file: {}", path);
            execute_script(state, &path).await?;
            println!("File loaded.");
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
        .map_err(|e| format!("{}", e))?;

    if !resp.status().is_success() {
        let error_text = resp.text().await.unwrap_or_default();
        return Err(format!("Query failed: {}", error_text));
    }

    let result: ApiResponse<QueryResponse> = resp.json().await.map_err(|e| format!("{}", e))?;

    // First check for API-level error
    let query_response = result.data.ok_or_else(|| {
        result
            .error
            .map(|e| e.message)
            .unwrap_or("Unknown error".to_string())
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
        .map_err(|e| format!("{}", e))?;

    if !resp.status().is_success() {
        let error_text = resp.text().await.unwrap_or_default();
        return Err(extract_error_message(&error_text));
    }

    let result: ApiResponse<InsertDataResponse> =
        resp.json().await.map_err(|e| format!("{}", e))?;

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
                .map_err(|e| format!("{}", e))?;

            if !resp.status().is_success() {
                let error_text = resp.text().await.unwrap_or_default();
                return Err(format!("Delete failed: {}", error_text));
            }

            let result: ApiResponse<DeleteDataResponse> =
                resp.json().await.map_err(|e| format!("{}", e))?;

            let data = result.data.ok_or("No response data")?;
            println!(
                "Deleted {} facts from '{}'.",
                data.rows_deleted, op.relation
            );
        }
        DeletePattern::Conditional { head_args, body } => {
            // Format head arguments
            let head_str: String = head_args.iter().map(format_term).collect::<Vec<_>>().join(", ");

            // Format body predicates
            let body_str: String = body.iter().map(format_body_pred).collect::<Vec<_>>().join(", ");

            // Build the conditional delete statement
            let delete_stmt = format!("-{}({}) :- {}.", op.relation, head_str, body_str);

            // Send through query API
            let response = execute_query(state, delete_stmt).await?;

            // Print the result messages (rows contain message strings)
            for row in &response.rows {
                if let Some(msg) = row.first() {
                    if let Some(s) = msg.as_str() {
                        println!("{}", s);
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
    let mut body_parts: Vec<String> = vec![format!(
        "{}({})",
        goal.goal.relation,
        query_args.join(", ")
    )];

    // Add additional body predicates (for complex queries like ?- foo(X), bar(Y).)
    for pred in &goal.body {
        body_parts.push(format_body_pred(pred));
    }

    program.push_str(&format!("?- {}.", body_parts.join(", ")));

    let response = execute_query(state, program).await?;

    if response.rows.is_empty() {
        println!("No results.");
    } else {
        println!("{} rows:", response.rows.len());
        for row in &response.rows {
            let vals: Vec<String> = row.iter().map(format_json_value).collect();
            println!("  ({})", vals.join(", "));
        }
    }

    Ok(())
}

async fn handle_session_rule(
    state: &mut ReplState,
    rule: inputlayer::ast::Rule,
) -> Result<(), String> {
    let head_relation = rule.head.relation.clone();
    let head_arity = rule.head.args.len();
    state.session_rules.push(rule);

    // Generate variables matching the rule's actual head arity
    let var_names = ["X", "Y", "Z", "W", "V", "U", "T", "S", "R", "Q"];
    let args: Vec<Term> = (0..head_arity)
        .map(|i| {
            let name = if i < var_names.len() {
                var_names[i].to_string()
            } else {
                format!("V{}", i)
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
    let relation = rule.head.relation.clone();
    state.session_facts.push(rule);
    println!(
        "Session fact added for '{}'. (Use +{}(...) to persist)",
        relation, relation
    );
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
        .api_url(&format!("/knowledge-graphs/{}/rules/{}", db, name));
    let resp = state
        .http
        .client
        .delete(&url)
        .send()
        .await
        .map_err(|e| format!("{}", e))?;

    if resp.status().is_success() {
        println!("Rule '{}' deleted.", name);
        return Ok(());
    }

    Err(format!(
        "'{}' is not a rule. Use conditional delete to remove facts.",
        name
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

    update_text.push_str(" :- ");

    let mut condition_parts = Vec::new();
    for pred in &update.body {
        condition_parts.push(format_body_pred(pred));
    }
    update_text.push_str(&condition_parts.join(", "));
    update_text.push('.');

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

fn term_to_json(term: &Term) -> serde_json::Value {
    match term {
        Term::Constant(n) => serde_json::Value::Number((*n).into()),
        Term::FloatConstant(f) => serde_json::json!(*f),
        Term::StringConstant(s) => serde_json::Value::String(s.clone()),
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

fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("\"{}\"", s),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(arr) => format!("{:?}", arr),
        serde_json::Value::Object(obj) => format!("{:?}", obj),
    }
}

// TODO: Use this function when implementing rich output formatting for query results.
// Reserved for formatted display of WireValue types in query results. Currently
// format_json_value is used instead for JSON-based responses. This function will
// be useful when implementing native wire protocol display or export features.
#[allow(dead_code)]
fn format_wire_value(value: &WireValue) -> String {
    match value {
        WireValue::Null => "null".to_string(),
        WireValue::Int(n) => n.to_string(),
        WireValue::Float(f) => f.to_string(),
        WireValue::String(s) => format!("\"{}\"", s),
        WireValue::Bool(b) => b.to_string(),
        WireValue::Array(v) => format!("{:?}", v),
    }
}

fn format_rule(rule: &inputlayer::ast::Rule) -> String {
    let head = format_atom(&rule.head);

    if rule.body.is_empty() {
        return format!("{}.", head);
    }

    let mut parts = Vec::new();
    for pred in &rule.body {
        parts.push(format_body_pred(pred));
    }

    format!("{} :- {}.", head, parts.join(", "))
}

fn format_atom(atom: &inputlayer::ast::Atom) -> String {
    let args: Vec<String> = atom.args.iter().map(format_term).collect();
    format!("{}({})", atom.relation, args.join(", "))
}

fn format_term(term: &Term) -> String {
    match term {
        Term::Variable(name) => name.clone(),
        Term::Constant(val) => val.to_string(),
        Term::StringConstant(s) => format!("\"{}\"", s),
        Term::FloatConstant(f) => f.to_string(),
        Term::Placeholder => "_".to_string(),
        Term::Arithmetic(expr) => format_arith_expr(expr),
        Term::Aggregate(func, var) => {
            let func_name = match func {
                inputlayer::ast::AggregateFunc::Count => "count",
                inputlayer::ast::AggregateFunc::Sum => "sum",
                inputlayer::ast::AggregateFunc::Min => "min",
                inputlayer::ast::AggregateFunc::Max => "max",
                inputlayer::ast::AggregateFunc::Avg => "avg",
                inputlayer::ast::AggregateFunc::TopK { k, order_var, descending } => {
                    let dir = if *descending { ", desc" } else { "" };
                    return format!("top_k<{}, {}{}>", k, order_var, dir);
                }
                inputlayer::ast::AggregateFunc::TopKThreshold { k, order_var, threshold, descending } => {
                    let dir = if *descending { ", desc" } else { "" };
                    return format!("top_k_threshold<{}, {}, {}{}>", k, order_var, threshold, dir);
                }
                inputlayer::ast::AggregateFunc::WithinRadius { distance_var, max_distance } => {
                    return format!("within_radius<{}, {}>", distance_var, max_distance);
                }
            };
            format!("{}<{}>", func_name, var)
        }
        Term::VectorLiteral(values) => {
            let vals: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            format!("[{}]", vals.join(", "))
        }
        Term::FunctionCall(func, args) => {
            let args_str: Vec<String> = args.iter().map(format_term).collect();
            let func_name = match func {
                inputlayer::ast::BuiltinFunc::Euclidean => "euclidean",
                inputlayer::ast::BuiltinFunc::Cosine => "cosine",
                inputlayer::ast::BuiltinFunc::DotProduct => "dot",
                inputlayer::ast::BuiltinFunc::Manhattan => "manhattan",
                inputlayer::ast::BuiltinFunc::LshBucket => "lsh_bucket",
                inputlayer::ast::BuiltinFunc::VecNormalize => "normalize",
                inputlayer::ast::BuiltinFunc::VecDim => "vec_dim",
                inputlayer::ast::BuiltinFunc::VecAdd => "vec_add",
                inputlayer::ast::BuiltinFunc::VecScale => "vec_scale",
                inputlayer::ast::BuiltinFunc::TimeNow => "time_now",
                inputlayer::ast::BuiltinFunc::TimeDiff => "time_diff",
                inputlayer::ast::BuiltinFunc::TimeAdd => "time_add",
                inputlayer::ast::BuiltinFunc::TimeSub => "time_sub",
                inputlayer::ast::BuiltinFunc::TimeDecay => "time_decay",
                inputlayer::ast::BuiltinFunc::TimeDecayLinear => "time_decay_linear",
                inputlayer::ast::BuiltinFunc::TimeBefore => "time_before",
                inputlayer::ast::BuiltinFunc::TimeAfter => "time_after",
                inputlayer::ast::BuiltinFunc::TimeBetween => "time_between",
                inputlayer::ast::BuiltinFunc::WithinLast => "within_last",
                inputlayer::ast::BuiltinFunc::IntervalsOverlap => "intervals_overlap",
                inputlayer::ast::BuiltinFunc::IntervalContains => "interval_contains",
                inputlayer::ast::BuiltinFunc::IntervalDuration => "interval_duration",
                inputlayer::ast::BuiltinFunc::PointInInterval => "point_in_interval",
                // Int8 quantization functions
                inputlayer::ast::BuiltinFunc::QuantizeLinear => "quantize_linear",
                inputlayer::ast::BuiltinFunc::QuantizeSymmetric => "quantize_symmetric",
                inputlayer::ast::BuiltinFunc::Dequantize => "dequantize",
                inputlayer::ast::BuiltinFunc::DequantizeScaled => "dequantize_scaled",
                // Int8 distance functions
                inputlayer::ast::BuiltinFunc::EuclideanInt8 => "euclidean_int8",
                inputlayer::ast::BuiltinFunc::CosineInt8 => "cosine_int8",
                inputlayer::ast::BuiltinFunc::DotProductInt8 => "dot_int8",
                inputlayer::ast::BuiltinFunc::ManhattanInt8 => "manhattan_int8",
                // Multi-probe LSH functions
                inputlayer::ast::BuiltinFunc::LshProbes => "lsh_probes",
                inputlayer::ast::BuiltinFunc::LshMultiProbe => "lsh_multi_probe",
                // Math utility functions
                inputlayer::ast::BuiltinFunc::AbsInt64 => "abs_int64",
                inputlayer::ast::BuiltinFunc::AbsFloat64 => "abs_float64",
            };
            format!("{}({})", func_name, args_str.join(", "))
        }
        Term::FieldAccess(term, field) => {
            format!("{}.{}", format_term(term), field)
        }
        Term::RecordPattern(fields) => {
            let fields_str: Vec<String> = fields
                .iter()
                .map(|(name, term)| format!("{}: {}", name, format_term(term)))
                .collect();
            format!("{{ {} }}", fields_str.join(", "))
        }
    }
}

fn format_arith_expr(expr: &inputlayer::ast::ArithExpr) -> String {
    match expr {
        inputlayer::ast::ArithExpr::Variable(name) => name.clone(),
        inputlayer::ast::ArithExpr::Constant(val) => val.to_string(),
        inputlayer::ast::ArithExpr::Binary { op, left, right } => {
            format!(
                "{}{}{}",
                format_arith_expr(left),
                op.as_str(),
                format_arith_expr(right)
            )
        }
    }
}

fn format_body_pred(pred: &inputlayer::ast::BodyPredicate) -> String {
    match pred {
        inputlayer::ast::BodyPredicate::Positive(atom) => format_atom(atom),
        inputlayer::ast::BodyPredicate::Negated(atom) => format!("!{}", format_atom(atom)),
        inputlayer::ast::BodyPredicate::Comparison(left, op, right) => {
            let op_str = match op {
                inputlayer::ast::ComparisonOp::Equal => "=",
                inputlayer::ast::ComparisonOp::NotEqual => "!=",
                inputlayer::ast::ComparisonOp::LessThan => "<",
                inputlayer::ast::ComparisonOp::LessOrEqual => "<=",
                inputlayer::ast::ComparisonOp::GreaterThan => ">",
                inputlayer::ast::ComparisonOp::GreaterOrEqual => ">=",
            };
            format!("{} {} {}", format_term(left), op_str, format_term(right))
        }
    }
}

/// Background heartbeat task that monitors server health
/// Sends disconnect signal if server becomes unresponsive
async fn heartbeat_task(base_url: String, disconnect_tx: watch::Sender<bool>) {
    let client = Client::builder()
        .timeout(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS))
        .build()
        .unwrap_or_else(|_| Client::new());

    let health_url = format!("{}/api/v1/health", base_url);
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
    println!("InputLayer Datalog Client (HTTP)");
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
    println!("  .session clear       Clear session rules");
    println!("  .status              Server status");
    println!("  .help                Show this help");
    println!("  .quit                Exit");
    println!();
    println!("Data Manipulation:");
    println!("  edge(1, 2).          Insert fact");
    println!("  -edge(1, 2).         Delete fact");
    println!();
    println!("Rules:");
    println!("  +path(X, Y) :- edge(X, Y).   Persistent rule");
    println!("  foo(X, Y) :- bar(X, Y).      Session rule");
    println!();
    println!("Queries:");
    println!("  ?- path(1, X).       Query");
    println!();
}
