//! `InputLayer` Client Binary - WebSocket-based Datalog Client
//!
//! Interactive client for `InputLayer` that connects to the server via WebSocket.
//! All commands (queries, inserts, meta commands) are sent as raw text through
//! a single WebSocket connection with auto-managed session lifecycle.
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

use inputlayer::statement::{parse_statement, MetaCommand, Statement};

use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use tokio_tungstenite::tungstenite;

// ── Health check DTOs (HTTP, one-time on startup) ────────────────

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
    #[allow(dead_code)]
    version: String,
    #[allow(dead_code)]
    uptime_secs: u64,
}

// ── WebSocket protocol types (matching server GlobalWs*) ────────

/// Client → Server message
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsRequest {
    Execute {
        program: String,
    },
    #[allow(dead_code)]
    Ping,
}

/// Server → Client message
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsResponse {
    Connected {
        #[allow(dead_code)]
        session_id: u64,
        knowledge_graph: String,
    },
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
        #[allow(dead_code)]
        row_count: usize,
        #[allow(dead_code)]
        total_count: usize,
        #[allow(dead_code)]
        truncated: bool,
        execution_time_ms: u64,
        #[serde(default)]
        #[allow(dead_code)]
        row_provenance: Vec<String>,
        #[serde(default)]
        switched_kg: Option<String>,
    },
    Error {
        message: String,
    },
    Pong,
    #[allow(dead_code)]
    Notification {
        event: String,
        knowledge_graph: String,
        relation: String,
        operation: String,
        count: usize,
    },
}

// ── Internal channel message for WS background reader ───────────

enum WsMessage {
    Response(WsResponse),
    Closed,
    Error(String),
}

// ── WebSocket client ────────────────────────────────────────────

type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tungstenite::Message,
>;

struct WsClient {
    sender: WsSink,
    msg_rx: tokio::sync::mpsc::UnboundedReceiver<WsMessage>,
}

impl WsClient {
    /// Connect to the WebSocket endpoint, wait for the Connected message,
    /// and spawn a background reader task.
    async fn connect(ws_url: &str) -> Result<(Self, WsResponse), String> {
        let (ws_stream, _) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio_tungstenite::connect_async(ws_url),
        )
        .await
        .map_err(|_| "WebSocket connection timeout (10s)".to_string())?
        .map_err(|e| format!("WebSocket connection failed: {e}"))?;

        let (sender, mut receiver) = ws_stream.split();

        // Wait for the Connected message before spawning background reader
        let connected = loop {
            match receiver.next().await {
                Some(Ok(tungstenite::Message::Text(text))) => {
                    let resp: WsResponse = serde_json::from_str(&text)
                        .map_err(|e| format!("Failed to parse Connected message: {e}"))?;
                    break resp;
                }
                Some(Ok(_)) => continue, // skip non-text frames
                Some(Err(e)) => return Err(format!("WebSocket error: {e}")),
                None => return Err("Connection closed before Connected message".to_string()),
            }
        };

        // Spawn background task to read from WS and forward to channel
        let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(msg_result) = receiver.next().await {
                match msg_result {
                    Ok(tungstenite::Message::Text(text)) => {
                        match serde_json::from_str::<WsResponse>(&text) {
                            Ok(resp) => {
                                if msg_tx.send(WsMessage::Response(resp)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                if msg_tx
                                    .send(WsMessage::Error(format!("Parse error: {e}")))
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(tungstenite::Message::Close(_)) => {
                        let _ = msg_tx.send(WsMessage::Closed);
                        break;
                    }
                    Ok(_) => continue, // skip binary, ping, pong
                    Err(e) => {
                        let _ = msg_tx.send(WsMessage::Error(format!("WebSocket error: {e}")));
                        break;
                    }
                }
            }
            // Ensure closed is signaled when task ends
            let _ = msg_tx.send(WsMessage::Closed);
        });

        Ok((Self { sender, msg_rx }, connected))
    }

    /// Send an execute message to the server.
    async fn send_execute(&mut self, program: &str) -> Result<(), String> {
        let req = WsRequest::Execute {
            program: program.to_string(),
        };
        let text = serde_json::to_string(&req).map_err(|e| format!("Serialize error: {e}"))?;
        self.sender
            .send(tungstenite::Message::Text(text))
            .await
            .map_err(|e| format!("Send failed: {e}"))
    }

    /// Receive the next non-notification response. Notifications are silently skipped.
    /// Times out after 120 seconds to prevent hanging under server load.
    async fn recv_response(&mut self) -> Result<WsResponse, String> {
        let timeout = std::time::Duration::from_secs(120);
        loop {
            match tokio::time::timeout(timeout, self.msg_rx.recv()).await {
                Ok(Some(WsMessage::Response(resp))) => {
                    if matches!(&resp, WsResponse::Notification { .. }) {
                        // Skip notifications — they'll be displayed by the REPL idle loop
                        continue;
                    }
                    return Ok(resp);
                }
                Ok(Some(WsMessage::Error(e))) => return Err(e),
                Ok(Some(WsMessage::Closed) | None) => {
                    return Err("WebSocket connection closed".to_string())
                }
                Err(_) => return Err("Response timeout (120s)".to_string()),
            }
        }
    }

    /// Send a close frame to the server for clean disconnect.
    async fn close(&mut self) {
        let _ = self.sender.send(tungstenite::Message::Close(None)).await;
    }
}

// ── Display configuration ───────────────────────────────────────

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

// ── REPL state ──────────────────────────────────────────────────

struct ReplState {
    ws: WsClient,
    current_kg: Option<String>,
    display_config: DisplayConfig,
    /// Local tracking of session rule count for display purposes
    session_rule_count: usize,
    /// Local tracking of session fact count for display purposes
    session_fact_count: usize,
}

impl ReplState {
    fn prompt(&self) -> String {
        let has_session = self.session_rule_count > 0 || self.session_fact_count > 0;
        let indicator = if has_session { "*" } else { "" };
        match &self.current_kg {
            Some(db) => format!("{db}{indicator}> "),
            None => "inputlayer> ".to_string(),
        }
    }
}

// ── CLI arguments ───────────────────────────────────────────────

struct Args {
    script: Option<String>,
    repl: bool,
    server: String,
    display_limit: Option<usize>,
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
    println!("InputLayer Datalog Client (WebSocket)");
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

// ── Main ────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();

    println!("Connecting to server at {}...", args.server);

    // HTTP health check (one-time, fast feedback if server is down)
    let http_base = args.server.trim_end_matches('/');
    let health_url = format!("{http_base}/health");
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| Client::new());

    // Retry health check up to 3 times (server may be slow to respond under load)
    let mut health: Option<ApiResponse<HealthResponse>> = None;
    for attempt in 0..3 {
        match http_client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<ApiResponse<HealthResponse>>().await {
                    Ok(h) => {
                        health = Some(h);
                        break;
                    }
                    Err(e) if attempt < 2 => {
                        eprintln!("Health parse retry ({attempt}): {e}");
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                    Err(e) => return Err(format!("Failed to parse health response: {e}").into()),
                }
            }
            Ok(resp) => {
                return Err(format!("Server returned error: {}", resp.status()).into());
            }
            Err(e) if attempt < 2 => {
                eprintln!("Connection retry ({attempt}): {e}");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(format!("Failed to connect to server: {e}").into()),
        }
    }
    let health = health.ok_or("Failed to connect after retries")?;

    let health_data = health.data.ok_or("No health data returned")?;
    println!("Connected!");
    println!();
    println!("Server status: {}", health_data.status);

    // Connect via WebSocket (retry up to 3 times under load)
    let ws_url = http_to_ws_url(http_base);
    let mut ws_result = None;
    for attempt in 0..3 {
        match WsClient::connect(&ws_url).await {
            Ok(pair) => {
                ws_result = Some(pair);
                break;
            }
            Err(e) if attempt < 2 => {
                eprintln!("WebSocket retry ({attempt}): {e}");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => return Err(format!("WebSocket connection failed: {e}").into()),
        }
    }
    let (ws_client, connected) = ws_result.ok_or("WebSocket connection failed after retries")?;

    let current_kg = match &connected {
        WsResponse::Connected {
            knowledge_graph, ..
        } => {
            println!("Current knowledge graph: {knowledge_graph}");
            Some(knowledge_graph.clone())
        }
        _ => None,
    };
    println!();

    // Display config: script mode defaults to unlimited + no timing, REPL defaults to 50 + timing
    let is_script = args.script.is_some();
    let display_config = DisplayConfig {
        max_rows: args.display_limit.unwrap_or(if is_script { 0 } else { 50 }),
        max_col_width: 40,
        show_timing: !is_script,
    };

    let mut state = ReplState {
        ws: ws_client,
        current_kg,
        display_config,
        session_rule_count: 0,
        session_fact_count: 0,
    };

    // If a script is provided, execute it
    if let Some(script_path) = &args.script {
        match execute_script(&mut state, script_path).await {
            Ok(()) => {
                if !args.repl {
                    state.ws.close().await;
                    return Ok(());
                }
                println!();
                println!("Script completed. Entering REPL...");
                println!();
            }
            Err(e) => {
                println!("Script error: {e}");
                if !args.repl {
                    state.ws.close().await;
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

/// Convert an HTTP URL to a WebSocket URL for the /ws endpoint.
fn http_to_ws_url(http_url: &str) -> String {
    let base = http_url.trim_end_matches('/');
    let ws_base = if base.starts_with("https://") {
        base.replacen("https://", "wss://", 1)
    } else {
        base.replacen("http://", "ws://", 1)
    };
    format!("{ws_base}/api/v1/ws")
}

// ── Script execution ────────────────────────────────────────────

/// Check if an error message indicates a fatal connection loss
/// (as opposed to a recoverable server-side error like "Unknown relation").
/// Check if an error indicates a true connection loss (not just a slow response).
/// Response timeouts are NOT connection errors — the connection may still be alive,
/// just slow under load. Only closed/failed/reset connections abort the script.
fn is_connection_error(msg: &str) -> bool {
    msg.contains("WebSocket connection closed")
        || msg.contains("WebSocket connection timeout")
        || msg.contains("WebSocket connection failed")
        || msg.contains("connection reset")
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

            // If this line starts a meta command (.) and we have incomplete
            // accumulated content (unbalanced delimiters), flush the accumulated
            // content as a parse error first. This ensures cleanup commands
            // (.kg use, .kg drop) are never swallowed by a preceding syntax error.
            if line.starts_with('.') && !accumulated.trim().is_empty() {
                let trimmed = accumulated.trim().to_string();
                println!("> {trimmed}");
                println!("Parse error: unterminated statement (unbalanced delimiters)");
                accumulated.clear();
            }

            accumulated.push_str(line);
            accumulated.push(' ');

            if is_complete_statement(&accumulated) {
                let trimmed = accumulated.trim().to_string();
                println!("> {trimmed}");
                match parse_statement(&trimmed) {
                    Ok(stmt) => {
                        if let Err(e) = handle_statement(state, &trimmed, stmt).await {
                            if is_connection_error(&e) {
                                return Err(e);
                            }
                            // Non-fatal error: print and continue so cleanup runs
                            println!("{e}");
                        }
                    }
                    Err(e) => {
                        // Parse error: print and continue so cleanup runs
                        println!("Parse error: {e}");
                    }
                }
                accumulated.clear();
            }
        }

        // Flush any remaining accumulated content at end of file
        if !accumulated.trim().is_empty() {
            let trimmed = accumulated.trim().to_string();
            println!("> {trimmed}");
            println!("Parse error: unterminated statement (unbalanced delimiters)");
        }

        Ok(())
    })
}

// ── REPL ────────────────────────────────────────────────────────

async fn run_repl(state: &mut ReplState) -> Result<(), Box<dyn std::error::Error>> {
    let history_path = get_history_path();
    let initial_prompt = state.prompt();

    // Channel for readline results (thread -> async)
    let (line_tx, mut line_rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<String, ReadlineError>>();
    // Channel for prompt updates (async -> thread)
    let (prompt_tx, prompt_rx) = std::sync::mpsc::channel::<String>();

    // Spawn a dedicated thread for blocking readline
    let history_clone = history_path.clone();
    std::thread::spawn(move || {
        let mut rl = match Editor::new() {
            Ok(rl) => rl,
            Err(_) => return,
        };
        rl.set_helper(Some(inputlayer::syntax::highlight::DatalogHelper::new()));
        if history_clone.exists() {
            let _ = rl.load_history(&history_clone);
        }

        let mut current_prompt = initial_prompt;
        loop {
            match rl.readline(&current_prompt) {
                Ok(line) => {
                    let _ = rl.add_history_entry(&line);
                    if line_tx.send(Ok(line)).is_err() {
                        break;
                    }
                    match prompt_rx.recv() {
                        Ok(p) => current_prompt = p,
                        Err(_) => break,
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    if line_tx.send(Err(ReadlineError::Interrupted)).is_err() {
                        break;
                    }
                    match prompt_rx.recv() {
                        Ok(p) => current_prompt = p,
                        Err(_) => break,
                    }
                }
                Err(e) => {
                    let _ = line_tx.send(Err(e));
                    break;
                }
            }
        }
        let _ = rl.save_history(&history_clone);
    });

    // Async loop: race readline input against WS disconnect
    loop {
        tokio::select! {
            result = line_rx.recv() => {
                match result {
                    Some(Ok(line)) => {
                        let line = line.trim().to_string();
                        if line.is_empty() {
                            let _ = prompt_tx.send(state.prompt());
                            continue;
                        }

                        match parse_statement(&line) {
                            Ok(stmt) => {
                                if let Err(e) = handle_statement(state, &line, stmt).await {
                                    println!("Error: {e}");
                                }
                            }
                            Err(e) => {
                                println!("Parse error: {e}");
                                println!("Type .help for syntax reference.");
                            }
                        }
                        let _ = prompt_tx.send(state.prompt());
                    }
                    Some(Err(ReadlineError::Interrupted)) => {
                        println!("^C");
                        let _ = prompt_tx.send(state.prompt());
                    }
                    Some(Err(ReadlineError::Eof)) => {
                        println!("Goodbye!");
                        break;
                    }
                    Some(Err(err)) => {
                        println!("Error: {err:?}");
                        break;
                    }
                    None => break, // readline thread exited
                }
            }
            // Watch for WS disconnection or notifications while idle
            msg = state.ws.msg_rx.recv() => {
                match msg {
                    Some(WsMessage::Response(WsResponse::Notification {
                        relation, operation, count, ..
                    })) => {
                        eprintln!("[notification] {operation} {count} in {relation}");
                    }
                    Some(WsMessage::Closed) | None => {
                        eprintln!();
                        eprintln!("Server connection lost. Exiting...");
                        std::process::exit(1);
                    }
                    Some(WsMessage::Error(e)) => {
                        eprintln!("WebSocket error: {e}");
                    }
                    _ => {}
                }
            }
        }
    }

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

// ── Statement handling ──────────────────────────────────────────

async fn handle_statement(
    state: &mut ReplState,
    raw_text: &str,
    stmt: Statement,
) -> Result<(), String> {
    match stmt {
        // Client-only commands — never sent to server
        Statement::Meta(MetaCommand::Help) => {
            print_help();
            Ok(())
        }
        Statement::Meta(MetaCommand::Quit) => {
            println!("Goodbye!");
            std::process::exit(0);
        }
        Statement::Meta(MetaCommand::Load { path, .. }) => {
            println!("Loading file: {path}");
            execute_script(state, &path).await?;
            println!("File loaded.");
            Ok(())
        }

        // Session rules: send to server, then auto-query to show results
        Statement::SessionRule(ref rule) => {
            let head_relation = rule.head.relation.clone();
            let head_arity = rule.head.effective_arity();

            // Send the rule to the server (silently — don't display "Session rule added")
            state.ws.send_execute(raw_text).await?;
            let resp = state.ws.recv_response().await?;
            if let WsResponse::Error { message } = resp {
                return Err(message);
            }
            // Handle switched_kg if present
            if let WsResponse::Result {
                switched_kg: Some(ref kg),
                ..
            } = resp
            {
                state.current_kg = Some(kg.clone());
            }

            // Build and execute a follow-up query on the head relation
            let var_names = ["X", "Y", "Z", "W", "V", "U", "T", "S", "R", "Q"];
            let args: Vec<String> = (0..head_arity)
                .map(|i| {
                    if i < var_names.len() {
                        var_names[i].to_string()
                    } else {
                        format!("V{i}")
                    }
                })
                .collect();
            let query = format!("?{}({})", head_relation, args.join(", "));
            ws_execute_and_display(state, &query).await?;

            state.session_rule_count += 1;
            println!("(session: {} rule(s))", state.session_rule_count);
            Ok(())
        }

        // Session facts: send to server, track count locally
        Statement::Fact(_) => {
            ws_execute_and_display(state, raw_text).await?;
            state.session_fact_count += 1;
            Ok(())
        }

        // Session-clearing meta commands: send to server, reset local counts
        Statement::Meta(MetaCommand::SessionClear) => {
            ws_execute_and_display(state, raw_text).await?;
            state.session_rule_count = 0;
            state.session_fact_count = 0;
            Ok(())
        }

        // KG switching: send to server, clear session, reset session counts
        Statement::Meta(MetaCommand::KgUse(_) | MetaCommand::KgCreate(_)) => {
            ws_execute_and_display(state, raw_text).await?;
            // Clear session state on KG switch and report what was cleared
            if state.session_rule_count > 0 || state.session_fact_count > 0 {
                println!(
                    "(Cleared {} session rule(s), {} session fact(s))",
                    state.session_rule_count, state.session_fact_count
                );
                // Tell the server to clear the session too
                let _ = state.ws.send_execute(".session clear").await;
                let _ = state.ws.recv_response().await;
            }
            state.session_rule_count = 0;
            state.session_fact_count = 0;
            Ok(())
        }

        // Relation describe: add header and custom footer to match expected format
        Statement::Meta(MetaCommand::RelDescribe(ref name)) => {
            handle_rel_describe(state, raw_text, name).await
        }

        // Everything else — send raw text to server via WebSocket
        _ => ws_execute_and_display(state, raw_text).await,
    }
}

/// Handle `.rel <name>` — prints header, table, and custom footer.
async fn handle_rel_describe(
    state: &mut ReplState,
    raw_text: &str,
    name: &str,
) -> Result<(), String> {
    state.ws.send_execute(raw_text).await?;
    let resp = state.ws.recv_response().await?;

    match resp {
        WsResponse::Result {
            columns,
            rows,
            total_count,
            switched_kg,
            ..
        } => {
            if let Some(ref new_kg) = switched_kg {
                state.current_kg = Some(new_kg.clone());
            }

            // Check for message-style response (error or "not found")
            if columns.len() == 1 && columns[0] == "message" {
                for row in &rows {
                    if let Some(msg) = row.first().and_then(|v| v.as_str()) {
                        println!("{msg}");
                    }
                }
                return Ok(());
            }

            println!("Relation: {name}");
            if rows.is_empty() {
                println!("  No data.");
            } else {
                // Display table without timing
                display_relation_table(&columns, &rows, &state.display_config);
                println!("{} of {} total rows", rows.len(), total_count);
            }
            Ok(())
        }
        WsResponse::Error { message } => Err(message),
        _ => Err("Unexpected response from server".to_string()),
    }
}

/// Send a raw text program to the server and display the result.
async fn ws_execute_and_display(state: &mut ReplState, program: &str) -> Result<(), String> {
    state.ws.send_execute(program).await?;
    let resp = state.ws.recv_response().await?;

    match resp {
        WsResponse::Result {
            columns,
            rows,
            execution_time_ms,
            switched_kg,
            ..
        } => {
            // Handle KG switch signal from server
            if let Some(ref new_kg) = switched_kg {
                state.current_kg = Some(new_kg.clone());
            }

            // Display based on response type
            if columns.len() == 1 && columns[0] == "message" {
                // Message-style output (meta commands, inserts, deletes, etc.)
                for row in &rows {
                    if let Some(msg) = row.first().and_then(|v| v.as_str()) {
                        println!("{msg}");
                    }
                }
            } else if rows.is_empty() {
                println!("No results.");
            } else {
                // Table display (query results)
                display_table_result(&columns, &rows, execution_time_ms, &state.display_config);
            }
            Ok(())
        }
        WsResponse::Error { message } => Err(message),
        _ => Err("Unexpected response from server".to_string()),
    }
}

// ── Table formatting ────────────────────────────────────────────

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

/// Truncate a string to `max` characters, appending `...` if truncated.
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
fn display_table_result(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
    execution_time_ms: u64,
    config: &DisplayConfig,
) {
    let total_rows = rows.len();
    let display_rows = if config.max_rows > 0 && total_rows > config.max_rows {
        config.max_rows
    } else {
        total_rows
    };

    let visible_rows = &rows[..display_rows];
    let widths = compute_column_widths(columns, visible_rows, config.max_col_width);
    let numeric: Vec<bool> = (0..columns.len())
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
            let name = columns.get(i).map_or("", std::string::String::as_str);
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
                "{display_rows} of {total_rows} rows ({execution_time_ms}ms). Use --limit 0 for all rows."
            );
        } else {
            println!("{display_rows} of {total_rows} rows. Use --limit 0 for all rows.");
        }
    } else if config.show_timing {
        println!("{total_rows} rows ({execution_time_ms}ms)");
    } else {
        println!("{total_rows} rows");
    }
}

/// Display a relation data table (for `.rel <name>`) — no footer, caller handles it.
fn display_relation_table(
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
    config: &DisplayConfig,
) {
    let widths = compute_column_widths(columns, rows, config.max_col_width);
    let numeric: Vec<bool> = (0..columns.len())
        .map(|i| is_numeric_column(rows, i))
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
            let name = columns.get(i).map_or("", std::string::String::as_str);
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
    for row in rows {
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
}

// ── Text processing utilities ───────────────────────────────────

/// Strip block comments (/* ... */) from source text.
/// Respects string literals - doesn't strip comments inside strings.
fn strip_block_comments(source: &str) -> String {
    let mut result = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut depth = 0;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        if c == '"' && depth == 0 {
            in_string = !in_string;
            result.push(c);
        } else if in_string {
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

// ── Help text ───────────────────────────────────────────────────

fn print_help() {
    println!("InputLayer Client (WebSocket)");
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
    println!("  .rule drop prefix <p> Drop all rules matching prefix");
    println!("  .rule remove <name> <n>  Remove clause n from rule (1-based)");
    println!("  .session             List session rules");
    println!("  .session clear       Clear all session rules");
    println!("  .session drop <n|name>  Drop session rule by index or relation name");
    println!("  .clear prefix <p>    Clear all facts from relations with prefix");
    println!("  .explain <query>     Show query plan without executing");
    println!("  .status              Server status");
    println!("  .help                Show this help");
    println!("  .quit                Exit");
    println!();
    println!("Data Manipulation:");
    println!("  +edge(1, 2)          Insert fact (persistent)");
    println!("  -edge(1, 2)          Delete fact");
    println!("  edge(1, 2)           Session fact (transient)");
    println!();
    println!("Rules:");
    println!("  +path(X, Y) <- edge(X, Y)    Persistent rule");
    println!("  foo(X, Y) <- bar(X, Y)       Session rule (transient)");
    println!();
    println!("Queries:");
    println!("  ?path(1, X)          Query");
    println!();
}

// ── Unit Tests ──────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // is_complete_statement tests
    #[test]
    fn test_complete_statement_simple_fact() {
        assert!(is_complete_statement("+edge(1, 2)"));
    }

    #[test]
    fn test_complete_statement_rule_with_arrow() {
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

    // WsRequest serialization tests
    #[test]
    fn test_ws_request_execute_serialize() {
        let req = WsRequest::Execute {
            program: "+edge(1,2).".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains(r#""type":"execute""#));
        assert!(json.contains(r#""program":"+edge(1,2).""#));
    }

    #[test]
    fn test_ws_request_ping_serialize() {
        let req = WsRequest::Ping;
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains(r#""type":"ping""#));
    }

    // WsResponse deserialization tests
    #[test]
    fn test_ws_response_connected_deserialize() {
        let json = r#"{"type":"connected","session_id":42,"knowledge_graph":"default"}"#;
        let resp: WsResponse = serde_json::from_str(json).unwrap();
        assert!(matches!(
            resp,
            WsResponse::Connected {
                session_id: 42,
                knowledge_graph,
            } if knowledge_graph == "default"
        ));
    }

    #[test]
    fn test_ws_response_result_deserialize() {
        let json = r#"{"type":"result","columns":["col0"],"rows":[[1]],"row_count":1,"total_count":1,"truncated":false,"execution_time_ms":5}"#;
        let resp: WsResponse = serde_json::from_str(json).unwrap();
        assert!(matches!(resp, WsResponse::Result { row_count: 1, .. }));
    }

    #[test]
    fn test_ws_response_result_with_switched_kg() {
        let json = r#"{"type":"result","columns":["message"],"rows":[["Switched."]],"row_count":1,"total_count":1,"truncated":false,"execution_time_ms":0,"switched_kg":"test"}"#;
        let resp: WsResponse = serde_json::from_str(json).unwrap();
        match resp {
            WsResponse::Result { switched_kg, .. } => {
                assert_eq!(switched_kg.as_deref(), Some("test"));
            }
            _ => panic!("Expected Result"),
        }
    }

    #[test]
    fn test_ws_response_error_deserialize() {
        let json = r#"{"type":"error","message":"bad query"}"#;
        let resp: WsResponse = serde_json::from_str(json).unwrap();
        assert!(matches!(resp, WsResponse::Error { message } if message == "bad query"));
    }

    #[test]
    fn test_ws_response_pong_deserialize() {
        let json = r#"{"type":"pong"}"#;
        let resp: WsResponse = serde_json::from_str(json).unwrap();
        assert!(matches!(resp, WsResponse::Pong));
    }

    // URL conversion test
    #[test]
    fn test_http_to_ws_url() {
        assert_eq!(
            http_to_ws_url("http://127.0.0.1:8080"),
            "ws://127.0.0.1:8080/api/v1/ws"
        );
        assert_eq!(
            http_to_ws_url("https://example.com:443"),
            "wss://example.com:443/api/v1/ws"
        );
        assert_eq!(
            http_to_ws_url("http://10.0.0.5:8080/"),
            "ws://10.0.0.5:8080/api/v1/ws"
        );
    }

    // strip_block_comments tests
    #[test]
    fn test_strip_block_comments() {
        assert_eq!(strip_block_comments("a /* b */ c"), "a   c");
        assert_eq!(strip_block_comments("a /* b /* c */ d */ e"), "a   e");
        assert_eq!(
            strip_block_comments(r#"a "/* not a comment */" b"#),
            r#"a "/* not a comment */" b"#
        );
    }

    // strip_inline_comment tests
    #[test]
    fn test_strip_inline_comment() {
        assert_eq!(strip_inline_comment("foo // bar"), "foo");
        assert_eq!(strip_inline_comment(r#""a // b" // c"#), r#""a // b""#);
        assert_eq!(strip_inline_comment("no comment"), "no comment");
    }
}
