//! WebSocket Handler
//!
//! Provides session-scoped WebSocket connections for real-time bidirectional
//! communication. Supports queries, ephemeral data operations, and push
//! notifications when persistent data changes.
//!
//! ## Protocol
//!
//! Messages are JSON objects with a `type` field:
//!
//! **Client → Server:**
//! - `{"type": "query", "query": "?edge(X,Y)"}`
//! - `{"type": "insert_facts", "relation": "r", "tuples": [[1,2]]}`
//! - `{"type": "retract_facts", "relation": "r", "tuples": [[1,2]]}`
//! - `{"type": "add_rule", "rule": "path(X,Y) <- edge(X,Y)"}`
//! - `{"type": "ping"}`
//!
//! **Server → Client:**
//! - `{"type": "result", "rows": [...], "columns": [...], "metadata": {...}}`
//! - `{"type": "error", "message": "..."}`
//! - `{"type": "ack", "message": "..."}`
//! - `{"type": "pong"}`
//! - `{"type": "notification", "event": "persistent_update", "relation": "..."}`

use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, Query, WebSocketUpgrade,
    },
    response::IntoResponse,
    Extension,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn, Instrument};

use super::wire_value_to_json;
use crate::protocol::handler::{PersistentNotification, ValidationError, VALIDATION_ERROR_PREFIX};
use crate::protocol::rest::dto::SessionQueryMetadataDto;
use crate::protocol::rest::error::RestError;
use crate::protocol::rest::WsSemaphore;
use crate::protocol::Handler;
use crate::protocol::MAX_MESSAGE_SIZE;

/// Threshold in bytes: results whose single-message JSON exceeds this are
/// streamed as `result_start` / `result_chunk` / `result_end` messages.
/// Below this threshold, the classic single `result` message is used.
const STREAMING_THRESHOLD: usize = 1024 * 1024; // 1 MB

/// Maximum number of rows per `result_chunk` message.
const STREAMING_CHUNK_ROWS: usize = 500;

/// Incoming WebSocket message from client
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsRequest {
    Query {
        query: String,
    },
    InsertFacts {
        relation: String,
        tuples: Vec<Vec<serde_json::Value>>,
    },
    RetractFacts {
        relation: String,
        tuples: Vec<Vec<serde_json::Value>>,
    },
    AddRule {
        rule: String,
    },
    Ping,
}

/// Outgoing WebSocket message to client
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsResponse {
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
        row_count: usize,
        execution_time_ms: u64,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        row_provenance: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<SessionQueryMetadataDto>,
    },
    Error {
        message: String,
    },
    Ack {
        message: String,
    },
    Pong,
}

/// Upgrade to a session-scoped WebSocket connection for real-time bidirectional communication.
///
/// ## WebSocket Protocol
///
/// All messages are JSON objects with a `type` field. The connection is scoped to a single
/// session and inherits its knowledge graph binding and ephemeral data.
///
/// ### Client → Server Messages
///
/// **Query** - Execute a Datalog query in the session context:
/// ```json
/// {"type": "query", "query": "?edge(X, Y)"}
/// ```
///
/// **Insert Ephemeral Facts** - Add session-scoped facts:
/// ```json
/// {"type": "insert_facts", "relation": "edge", "tuples": [[1, 2], [3, 4]]}
/// ```
///
/// **Retract Ephemeral Facts** - Remove session-scoped facts:
/// ```json
/// {"type": "retract_facts", "relation": "edge", "tuples": [[1, 2]]}
/// ```
///
/// **Add Ephemeral Rule** - Add a session-scoped rule (exactly one rule per message):
/// ```json
/// {"type": "add_rule", "rule": "path(X, Y) <- edge(X, Y)"}
/// ```
///
/// **Ping** - Keep-alive:
/// ```json
/// {"type": "ping"}
/// ```
///
/// ### Server → Client Messages
///
/// **Result** - Query results with per-row provenance tracking:
/// ```json
/// {"type": "result", "columns": ["x", "y"], "rows": [[1, 2]], "row_count": 1,
///  "execution_time_ms": 5, "row_provenance": ["persistent"],
///  "metadata": {"has_ephemeral": false, "ephemeral_sources": [], "warnings": []}}
/// ```
///
/// **Ack** - Acknowledgement for insert/retract/add_rule operations:
/// ```json
/// {"type": "ack", "message": "Inserted 2 fact(s) into 'edge'"}
/// ```
///
/// **Error** - Error response:
/// ```json
/// {"type": "error", "message": "Invalid query syntax"}
/// ```
///
/// **Pong** - Response to ping:
/// ```json
/// {"type": "pong"}
/// ```
///
/// **Notification** - Push notification when persistent data changes in the session's KG:
/// ```json
/// {"type": "notification", "event": "persistent_update", "knowledge_graph": "default",
///  "relation": "edge", "operation": "insert", "count": 5}
/// ```
///
/// ### Backpressure
///
/// If the client falls behind on reading notifications, missed notifications are reported
/// via an error message: `{"type": "error", "message": "Missed N notification(s) due to backpressure"}`.
///
/// ### Connection Lifecycle
///
/// The WebSocket connection closes when:
/// - The client sends a close frame
/// - The underlying session is closed (server sends an error message before closing)
/// - The notification broadcast channel is shut down
/// Deprecated: Use the global `/ws` endpoint instead, which auto-manages session lifecycle.
pub async fn session_websocket(
    Extension(handler): Extension<Arc<Handler>>,
    Extension(ws_sem): Extension<WsSemaphore>,
    Path(id): Path<String>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, RestError> {
    // Verify session exists before upgrading
    if !handler.session_manager().has_session(&id) {
        return Err(RestError::not_found(format!("Session {id} not found")));
    }

    // Enforce WebSocket connection limit
    let ws_permit = if let Some(ref sem) = ws_sem.0 {
        match sem.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                return Err(RestError::service_unavailable(
                    "Too many WebSocket connections".to_string(),
                ));
            }
        }
    } else {
        None
    };

    Ok(ws
        .max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| {
            // Move permit into the async block so it's held for connection lifetime
            let permit = ws_permit;
            async move {
                handle_ws_connection(socket, handler, id).await;
                drop(permit);
            }
        }))
}

/// Handle a WebSocket connection for a specific session.
/// Uses `tokio::select!` to concurrently process client messages and push notifications.
async fn handle_ws_connection(socket: WebSocket, handler: Arc<Handler>, session_id: String) {
    let (mut sender, mut receiver) = socket.split();

    // Re-validate session exists after upgrade (it may have closed during the
    // HTTP→WebSocket upgrade handshake)
    if !handler.session_manager().has_session(&session_id) {
        let err_msg = WsResponse::Error {
            message: format!("Session {session_id} closed during upgrade"),
        };
        if let Ok(json) = serde_json::to_string(&err_msg) {
            let _ = sender.send(Message::Text(json)).await;
        }
        let _ = sender.close().await;
        return;
    }

    let mut notify_rx = handler.subscribe_notifications();

    // Idle timeout (matches global WS handler behavior)
    let idle_ms = handler.config().http.ws_idle_timeout_ms;
    let idle_duration = if idle_ms > 0 {
        Some(std::time::Duration::from_millis(idle_ms))
    } else {
        None
    };
    let mut last_activity = std::time::Instant::now();

    // Cumulative notification lag - disconnect if subscriber falls too far behind
    let max_lag = handler.config().http.rate_limit.notification_buffer_size as u64;
    let mut total_lagged: u64 = 0;

    // Per-connection message rate limiting
    let max_msgs_per_sec = handler.config().http.rate_limit.ws_max_messages_per_sec;
    let mut rate_window_start = std::time::Instant::now();
    let mut rate_window_count: u32 = 0;

    // Connection lifetime limit
    let connection_start = std::time::Instant::now();
    let max_lifetime_secs = handler.config().http.rate_limit.ws_max_lifetime_secs;
    let max_lifetime = if max_lifetime_secs > 0 {
        Some(std::time::Duration::from_secs(max_lifetime_secs))
    } else {
        None
    };

    // Server-initiated heartbeat: send ping every 30s to detect dead connections
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat_interval.tick().await; // consume the immediate first tick

    loop {
        // Check connection lifetime
        if let Some(max_lt) = max_lifetime {
            if connection_start.elapsed() >= max_lt {
                info!(session_id = %session_id, max_lifetime_secs, "ws_max_lifetime_exceeded");
                let err_msg = WsResponse::Error {
                    message: format!("Connection lifetime exceeded ({max_lifetime_secs}s)"),
                };
                if let Ok(json) = serde_json::to_string(&err_msg) {
                    let _ = sender.send(Message::Text(json)).await;
                }
                break;
            }
        }

        // Compute remaining idle time for this iteration
        let idle_sleep: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            match idle_duration {
                Some(dur) => {
                    let elapsed = last_activity.elapsed();
                    if elapsed >= dur {
                        Box::pin(std::future::ready(()))
                    } else {
                        Box::pin(tokio::time::sleep(dur.saturating_sub(elapsed)))
                    }
                }
                None => Box::pin(std::future::pending()),
            };

        tokio::select! {
            // Idle timeout
            () = idle_sleep => {
                if idle_duration.is_some() {
                    info!(session_id = %session_id, idle_ms, "ws_idle_timeout");
                    let err_msg = WsResponse::Error {
                        message: "Idle timeout".to_string(),
                    };
                    if let Ok(json) = serde_json::to_string(&err_msg) {
                        let _ = sender.send(Message::Text(json)).await;
                    }
                    break;
                }
            }
            // Client message
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_activity = std::time::Instant::now();

                        // Per-connection message rate limiting
                        if max_msgs_per_sec > 0 {
                            let now = std::time::Instant::now();
                            if now.duration_since(rate_window_start) >= std::time::Duration::from_secs(1) {
                                rate_window_start = now;
                                rate_window_count = 0;
                            }
                            rate_window_count += 1;
                            if rate_window_count > max_msgs_per_sec {
                                let err_msg = WsResponse::Error {
                                    message: format!("Rate limit exceeded ({max_msgs_per_sec} msgs/sec)"),
                                };
                                if let Ok(json) = serde_json::to_string(&err_msg) {
                                    let _ = sender.send(Message::Text(json)).await;
                                }
                                continue;
                            }
                        }

                        let response = process_ws_message(&handler, &session_id, &text).await;
                        let json = match serde_json::to_string(&response) {
                            Ok(j) => j,
                            Err(e) => {
                                tracing::error!(error = %e, "Failed to serialize WsResponse");
                                let err = WsResponse::Error {
                                    message: "Internal server error".to_string(),
                                };
                                serde_json::to_string(&err).unwrap_or_else(|_| {
                                    r#"{"type":"error","message":"Internal serialization error"}"#.to_string()
                                })
                            }
                        };
                        // Guard against oversized WS frames
                        let json = if json.len() > MAX_MESSAGE_SIZE {
                            warn!(session_id = %session_id, size = json.len(), max = MAX_MESSAGE_SIZE, "ws_result_too_large");
                            let err = WsResponse::Error {
                                message: format!("Result too large ({} bytes, max {})", json.len(), MAX_MESSAGE_SIZE),
                            };
                            serde_json::to_string(&err).unwrap_or_else(|_| {
                                r#"{"type":"error","message":"Result too large"}"#.to_string()
                            })
                        } else {
                            json
                        };
                        if sender.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break, // Protocol error, close connection
                    _ => {} // Ignore binary, ping/pong handled by axum
                }
            }
            // Server-initiated heartbeat ping
            _ = heartbeat_interval.tick() => {
                if sender.send(Message::Ping(Vec::new())).await.is_err() {
                    break; // Connection dead
                }
            }
            // Push notification from persistent data changes
            notification = notify_rx.recv() => {
                match notification {
                    Ok(ref notif) => {
                        // Get current session KG (may have changed via switch_kg)
                        let session_kg = match handler
                            .session_manager()
                            .with_session(&session_id, |s| s.knowledge_graph.clone())
                        {
                            Ok(kg) => kg,
                            Err(_) => {
                                // Session was closed - notify client before disconnecting
                                let err_msg = WsResponse::Error {
                                    message: "Session closed".to_string(),
                                };
                                if let Ok(json) = serde_json::to_string(&err_msg) {
                                    let _ = sender.send(Message::Text(json)).await;
                                }

                                break;
                            }
                        };
                        // Extract the KG name from any notification variant
                        let notif_kg = match notif {
                            PersistentNotification::PersistentUpdate { knowledge_graph, .. } => knowledge_graph,
                            PersistentNotification::RuleChange { knowledge_graph, .. } => knowledge_graph,
                            PersistentNotification::KgChange { knowledge_graph, .. } => knowledge_graph,
                            PersistentNotification::SchemaChange { knowledge_graph, .. } => knowledge_graph,
                        };
                        // Only forward notifications for this session's knowledge graph
                        // (KgChange notifications are always forwarded - they affect the KG list)
                        let is_kg_change = matches!(notif, PersistentNotification::KgChange { .. });
                        if *notif_kg == session_kg || is_kg_change {
                            if let Ok(json) = serde_json::to_string(&notif) {
                                if sender.send(Message::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        total_lagged += count;
                        if total_lagged > max_lag {
                            warn!(session_id = %session_id, total_lagged, max_lag, "ws_slow_subscriber_disconnected");
                            let err = WsResponse::Error {
                                message: format!("Disconnected: missed {total_lagged} total notification(s)"),
                            };
                            if let Ok(json) = serde_json::to_string(&err) {
                                let _ = sender.send(Message::Text(json)).await;
                            }
                            break;
                        }
                        let warn = WsResponse::Error {
                            message: format!("Missed {count} notification(s) due to backpressure"),
                        };
                        if let Ok(json) = serde_json::to_string(&warn) {
                            if sender.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Notification channel closed - server shutting down
                        let shutdown_msg = WsResponse::Error {
                            message: "Server shutting down".to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&shutdown_msg) {
                            let _ = sender.send(Message::Text(json)).await;
                        }
                        break;
                    }
                }
            }
        }
    }

    // Send close frame before cleanup (prevents "connection reset without handshake" warnings)
    let _ = sender.send(Message::Close(None)).await;

    // Guarantee session cleanup on WS disconnect (WI-03)
    let stats = handler.session_stats();
    info!(session_id = %session_id, active_sessions = stats.total_sessions, "ws_legacy_session_disconnecting");
    if let Err(e) = handler.close_session(&session_id) {
        tracing::warn!(session_id = %session_id, error = %e, "session_cleanup_failed");
    }
}

/// Process a single WebSocket message and return a response
async fn process_ws_message(handler: &Arc<Handler>, session_id: &str, text: &str) -> WsResponse {
    let request: WsRequest = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "Invalid WsRequest message");
            return WsResponse::Error {
                message: "Invalid message format".to_string(),
            };
        }
    };

    match request {
        WsRequest::Query { query } => handle_ws_query(handler, session_id, query).await,
        WsRequest::InsertFacts { relation, tuples } => {
            handle_ws_insert_facts(handler, session_id, &relation, tuples)
        }
        WsRequest::RetractFacts { relation, tuples } => {
            handle_ws_retract_facts(handler, session_id, &relation, tuples)
        }
        WsRequest::AddRule { rule } => handle_ws_add_rule(handler, session_id, &rule),
        WsRequest::Ping => WsResponse::Pong,
    }
}

async fn handle_ws_query(handler: &Arc<Handler>, session_id: &str, query: String) -> WsResponse {
    let start = std::time::Instant::now();
    match handler
        .query_program_with_session(&session_id.to_string(), query)
        .await
    {
        Ok(response) => {
            let row_provenance: Vec<String> = response
                .rows
                .iter()
                .map(|row| {
                    row.provenance
                        .as_ref()
                        .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
                })
                .collect();

            let rows: Vec<Vec<serde_json::Value>> = response
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
                .collect();

            let columns: Vec<String> = response.schema.iter().map(|c| c.name.clone()).collect();
            let row_count = rows.len();

            let metadata = response.metadata.map(|m| SessionQueryMetadataDto {
                has_ephemeral: m.has_ephemeral,
                ephemeral_sources: m.ephemeral_sources,
                warnings: m.warnings,
            });

            WsResponse::Result {
                columns,
                rows,
                row_count,
                execution_time_ms: start.elapsed().as_millis() as u64,
                row_provenance,
                metadata,
            }
        }
        Err(e) => WsResponse::Error { message: e },
    }
}

fn handle_ws_insert_facts(
    handler: &Arc<Handler>,
    session_id: &str,
    relation: &str,
    tuples: Vec<Vec<serde_json::Value>>,
) -> WsResponse {
    let max_str = handler.config().storage.performance.max_string_value_bytes;
    let parsed = match super::json_tuples_to_tuples_with_limits(&tuples, max_str, 65_536) {
        Ok(t) => t,
        Err(e) => return WsResponse::Error { message: e },
    };
    match handler.session_insert_ephemeral(&session_id.to_string(), relation, parsed) {
        Ok(inserted) => WsResponse::Ack {
            message: format!("Inserted {inserted} fact(s) into '{relation}'"),
        },
        Err(e) => WsResponse::Error { message: e },
    }
}

fn handle_ws_retract_facts(
    handler: &Arc<Handler>,
    session_id: &str,
    relation: &str,
    tuples: Vec<Vec<serde_json::Value>>,
) -> WsResponse {
    let max_str = handler.config().storage.performance.max_string_value_bytes;
    let parsed = match super::json_tuples_to_tuples_with_limits(&tuples, max_str, 65_536) {
        Ok(t) => t,
        Err(e) => return WsResponse::Error { message: e },
    };
    match handler.session_retract_ephemeral(&session_id.to_string(), relation, parsed) {
        Ok(retracted) => WsResponse::Ack {
            message: format!("Retracted {retracted} fact(s) from '{relation}'"),
        },
        Err(e) => WsResponse::Error { message: e },
    }
}

fn handle_ws_add_rule(handler: &Arc<Handler>, session_id: &str, rule_text: &str) -> WsResponse {
    // Enforce max_query_size_bytes on rule text to prevent DoS via huge rules
    let max_bytes = handler.config().storage.performance.max_query_size_bytes;
    if max_bytes > 0 && rule_text.len() > max_bytes {
        return WsResponse::Error {
            message: format!(
                "Rule text too large: {} bytes (max {})",
                rule_text.len(),
                max_bytes
            ),
        };
    }

    let program = match crate::parser::parse_program(rule_text) {
        Ok(p) => p,
        Err(e) => {
            return WsResponse::Error {
                message: format!("Invalid rule syntax: {e}"),
            };
        }
    };

    if program.rules.is_empty() {
        return WsResponse::Error {
            message: "No rule found in input".to_string(),
        };
    }
    if program.rules.len() > 1 {
        return WsResponse::Error {
            message: format!(
                "Expected exactly one rule, got {}. Add rules one at a time.",
                program.rules.len()
            ),
        };
    }
    let rule = match program.rules.into_iter().next() {
        Some(r) => r,
        None => {
            return WsResponse::Error {
                message: "Internal error: parsed rule not found".to_string(),
            };
        }
    };

    let head = rule.head.relation.clone();
    match handler.session_add_rule(&session_id.to_string(), rule, rule_text.to_string()) {
        Ok(()) => WsResponse::Ack {
            message: format!("Rule added for '{head}'"),
        },
        Err(e) => WsResponse::Error { message: e },
    }
}

// =============================================================================
// Global WebSocket Endpoint (/ws)
//
// Auto-session lifecycle: connect → server creates session → sends Connected →
// all commands via Execute message → disconnect closes session.
// =============================================================================

/// Query parameters for the global WebSocket connection
#[derive(Debug, Deserialize)]
pub struct WsConnectParams {
    /// Knowledge graph to bind to (defaults to "default")
    #[serde(default = "default_kg")]
    pub kg: String,
    /// Last notification sequence number seen by the client.
    /// If provided, the server replays buffered notifications with seq > last_seq on connect (#39).
    #[serde(default)]
    pub last_seq: Option<u64>,
}

fn default_kg() -> String {
    "default".to_string()
}

/// Incoming message for the global WebSocket protocol
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GlobalWsRequest {
    /// Authenticate with username and password
    Login { username: String, password: String },
    /// Authenticate with an API key
    Authenticate { api_key: String },
    /// Execute any Datalog statement or meta command as raw text
    Execute { program: String },
    /// Keep-alive ping
    Ping,
}

/// Outgoing message for the global WebSocket protocol
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GlobalWsResponse {
    /// Sent after successful authentication
    Authenticated {
        session_id: String,
        knowledge_graph: String,
        version: String,
        role: String,
    },
    /// Authentication failed
    AuthError { message: String },
    /// Query/command result (single message for small results)
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
        row_count: usize,
        total_count: usize,
        truncated: bool,
        execution_time_ms: u64,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        row_provenance: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<SessionQueryMetadataDto>,
        #[serde(skip_serializing_if = "Option::is_none")]
        switched_kg: Option<String>,
    },
    /// Streaming: header sent before row chunks (large results)
    ResultStart {
        columns: Vec<String>,
        total_count: usize,
        truncated: bool,
        execution_time_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<SessionQueryMetadataDto>,
        #[serde(skip_serializing_if = "Option::is_none")]
        switched_kg: Option<String>,
    },
    /// Streaming: a batch of rows
    ResultChunk {
        rows: Vec<Vec<serde_json::Value>>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        row_provenance: Vec<String>,
        chunk_index: usize,
    },
    /// Streaming: final message after all chunks
    ResultEnd {
        row_count: usize,
        chunk_count: usize,
    },
    /// Error response
    Error {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        validation_errors: Option<Vec<ValidationError>>,
    },
    /// Pong response to keep-alive ping
    Pong,
}

/// Global WebSocket endpoint with auto-session lifecycle.
///
/// Connect to `/ws?kg=<name>` to auto-create a session bound to the given
/// knowledge graph (defaults to "default"). The server sends a `Connected`
/// message with the session ID. On disconnect, the session is automatically
/// closed.
///
/// ## Client → Server Messages
///
/// **Execute** - Send any Datalog statement or meta command as raw text:
/// ```json
/// {"type": "execute", "program": "+edge(1,2)."}
/// {"type": "execute", "program": "?edge(X,Y)"}
/// {"type": "execute", "program": ".kg list"}
/// {"type": "execute", "program": ".rule list"}
/// ```
///
/// **Ping** - Keep-alive:
/// ```json
/// {"type": "ping"}
/// ```
///
/// ## Server → Client Messages
///
/// **Connected** - Sent on connection:
/// ```json
/// {"type": "connected", "session_id": 42, "knowledge_graph": "default"}
/// ```
///
/// **Result** - Command/query results:
/// ```json
/// {"type": "result", "columns": ["col0", "col1"], "rows": [[1, 2]], "row_count": 1,
///  "total_count": 1, "truncated": false, "execution_time_ms": 5}
/// ```
///
/// **Error** - Error:
/// ```json
/// {"type": "error", "message": "..."}
/// ```
///
/// **Pong** - Response to ping:
/// ```json
/// {"type": "pong"}
/// ```
///
/// **Notification** - Push notification for persistent data changes:
/// ```json
/// {"type": "notification", "event": "persistent_update", ...}
/// ```
pub async fn global_websocket(
    Extension(handler): Extension<Arc<Handler>>,
    Extension(ws_sem): Extension<WsSemaphore>,
    ws: WebSocketUpgrade,
    Query(params): Query<WsConnectParams>,
) -> Result<impl IntoResponse, RestError> {
    // Enforce WebSocket connection limit
    let ws_permit = if let Some(ref sem) = ws_sem.0 {
        match sem.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                return Err(RestError::service_unavailable(
                    "Too many WebSocket connections".to_string(),
                ));
            }
        }
    } else {
        None
    };

    Ok(ws
        .max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| {
            let permit = ws_permit;
            async move {
                handle_global_ws_connection(socket, handler, params.kg, params.last_seq).await;
                drop(permit);
            }
        }))
}

/// Handle a global WebSocket connection with auth loop + message loop.
async fn handle_global_ws_connection(
    socket: WebSocket,
    handler: Arc<Handler>,
    kg: String,
    last_seq: Option<u64>,
) {
    use crate::auth::AuthIdentity;

    let (mut sender, mut receiver) = socket.split();

    info!(kg = %kg, "ws_connection_start");

    // ── Auth Loop: wait for Login or Authenticate ────────────────────────
    let auth_timeout = std::time::Duration::from_secs(30);
    let auth_deadline = tokio::time::Instant::now() + auth_timeout;
    let auth_identity: AuthIdentity;

    loop {
        let msg = tokio::select! {
            () = tokio::time::sleep_until(auth_deadline) => {
                let err = GlobalWsResponse::AuthError {
                    message: "Authentication timeout (30s)".to_string(),
                };
                if let Ok(json) = serde_json::to_string(&err) {
                    let _ = sender.send(Message::Text(json)).await;
                }
                let _ = sender.send(Message::Close(None)).await;
                return;
            }
            msg = receiver.next() => msg,
        };

        match msg {
            Some(Ok(Message::Text(text))) => {
                let request: GlobalWsRequest = match serde_json::from_str(&text) {
                    Ok(r) => r,
                    Err(_) => {
                        let err = GlobalWsResponse::AuthError {
                            message: "Invalid message format. Send login or authenticate."
                                .to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&err) {
                            let _ = sender.send(Message::Text(json)).await;
                        }
                        continue;
                    }
                };

                match request {
                    GlobalWsRequest::Login { username, password } => {
                        match handler.authenticate_user(&username, &password) {
                            Ok(identity) => {
                                auth_identity = identity;
                                break;
                            }
                            Err(e) => {
                                warn!(username = %username, "ws_login_failed");
                                let err = GlobalWsResponse::AuthError { message: e };
                                if let Ok(json) = serde_json::to_string(&err) {
                                    let _ = sender.send(Message::Text(json)).await;
                                }
                                continue; // Allow retry
                            }
                        }
                    }
                    GlobalWsRequest::Authenticate { api_key } => {
                        match handler.authenticate_api_key(&api_key) {
                            Ok(identity) => {
                                auth_identity = identity;
                                break;
                            }
                            Err(e) => {
                                warn!("ws_apikey_auth_failed");
                                let err = GlobalWsResponse::AuthError { message: e };
                                if let Ok(json) = serde_json::to_string(&err) {
                                    let _ = sender.send(Message::Text(json)).await;
                                }
                                continue; // Allow retry
                            }
                        }
                    }
                    GlobalWsRequest::Execute { .. } | GlobalWsRequest::Ping => {
                        let err = GlobalWsResponse::AuthError {
                            message: "Authentication required. Send login or authenticate first."
                                .to_string(),
                        };
                        if let Ok(json) = serde_json::to_string(&err) {
                            let _ = sender.send(Message::Text(json)).await;
                        }
                        continue;
                    }
                }
            }
            Some(Ok(Message::Close(_))) | None => {
                let _ = sender.send(Message::Close(None)).await;
                return;
            }
            Some(Err(e)) => {
                warn!(error = %e, "ws_auth_protocol_error");
                return;
            }
            _ => continue,
        }
    }

    // ── Authenticated: create session ────────────────────────────────────
    info!(
        kg = %kg,
        username = %auth_identity.username,
        role = %auth_identity.role,
        "ws_authenticated"
    );

    let session_id = match handler.create_session(&kg) {
        Ok(id) => {
            let stats = handler.session_stats();
            info!(session_id = %id, kg = %kg, active_sessions = stats.total_sessions, "ws_session_created");
            id
        }
        Err(e) => {
            warn!(kg = %kg, error = %e, "ws_session_create_failed");
            let err_msg = GlobalWsResponse::Error {
                message: "Failed to create session".to_string(),
                validation_errors: None,
            };
            if let Ok(json) = serde_json::to_string(&err_msg) {
                let _ = sender.send(Message::Text(json)).await;
            }
            let _ = sender.close().await;
            return;
        }
    };

    // Send Authenticated message
    let authenticated = GlobalWsResponse::Authenticated {
        session_id: session_id.clone(),
        knowledge_graph: kg,
        version: env!("CARGO_PKG_VERSION").to_string(),
        role: auth_identity.role.to_string(),
    };
    if let Ok(json) = serde_json::to_string(&authenticated) {
        if sender.send(Message::Text(json)).await.is_err() {
            if let Err(e) = handler.close_session(&session_id) {
                tracing::warn!(session_id = %session_id, error = %e, "session_cleanup_failed");
            }
            let _ = sender.send(Message::Close(None)).await;
            return;
        }
    }

    let mut notify_rx = handler.subscribe_notifications();
    let mut request_seq: u64 = 0;

    // Replay missed notifications on reconnect (#39)
    if let Some(since_seq) = last_seq {
        let missed = handler.get_notifications_since(since_seq);
        if !missed.is_empty() {
            let session_kg = handler
                .session_manager()
                .with_session(&session_id, |s| s.knowledge_graph.clone())
                .unwrap_or_default();
            debug!(session_id = %session_id, missed_count = missed.len(), since_seq, "ws_replaying_missed_notifications");
            for notif in &missed {
                // Apply same KG filtering as live notifications
                let notif_kg = match notif {
                    PersistentNotification::PersistentUpdate {
                        knowledge_graph, ..
                    } => knowledge_graph,
                    PersistentNotification::RuleChange {
                        knowledge_graph, ..
                    } => knowledge_graph,
                    PersistentNotification::KgChange {
                        knowledge_graph, ..
                    } => knowledge_graph,
                    PersistentNotification::SchemaChange {
                        knowledge_graph, ..
                    } => knowledge_graph,
                };
                let is_kg_change = matches!(notif, PersistentNotification::KgChange { .. });
                if *notif_kg == session_kg || is_kg_change {
                    if let Ok(json) = serde_json::to_string(notif) {
                        if sender.send(Message::Text(json)).await.is_err() {
                            if let Err(e) = handler.close_session(&session_id) {
                                tracing::warn!(session_id = %session_id, error = %e, "session_cleanup_failed");
                            }
                            return;
                        }
                    }
                }
            }
        }
    }

    // Cumulative notification lag - disconnect if subscriber falls too far behind
    let max_lag = handler.config().http.rate_limit.notification_buffer_size as u64;
    let mut total_lagged: u64 = 0;

    // Idle timeout configuration (WI-02)
    let idle_ms = handler.config().http.ws_idle_timeout_ms;
    let idle_duration = if idle_ms > 0 {
        Some(std::time::Duration::from_millis(idle_ms))
    } else {
        None
    };
    let mut last_activity = std::time::Instant::now();

    // Connection lifetime limit
    let connection_start = std::time::Instant::now();
    let max_lifetime_secs = handler.config().http.rate_limit.ws_max_lifetime_secs;
    let max_lifetime = if max_lifetime_secs > 0 {
        Some(std::time::Duration::from_secs(max_lifetime_secs))
    } else {
        None
    };

    // Per-connection message rate limiting
    let max_msgs_per_sec = handler.config().http.rate_limit.ws_max_messages_per_sec;
    let mut rate_window_start = std::time::Instant::now();
    let mut rate_window_count: u32 = 0;

    // Server-initiated heartbeat: send ping every 30s to detect dead connections
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat_interval.tick().await; // consume the immediate first tick

    loop {
        // Compute remaining idle time for this iteration
        let idle_sleep: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
            match idle_duration {
                Some(dur) => {
                    let elapsed = last_activity.elapsed();
                    if elapsed >= dur {
                        // Already exceeded idle timeout
                        Box::pin(std::future::ready(()))
                    } else {
                        Box::pin(tokio::time::sleep(dur.saturating_sub(elapsed)))
                    }
                }
                None => Box::pin(std::future::pending()),
            };

        // Check connection lifetime
        if let Some(max_lt) = max_lifetime {
            if connection_start.elapsed() >= max_lt {
                info!(session_id = %session_id, max_lifetime_secs, "ws_max_lifetime_exceeded");
                let err_msg = GlobalWsResponse::Error {
                    message: format!("Connection lifetime exceeded ({max_lifetime_secs}s)"),
                    validation_errors: None,
                };
                if let Ok(json) = serde_json::to_string(&err_msg) {
                    let _ = sender.send(Message::Text(json)).await;
                }
                break;
            }
        }

        tokio::select! {
            // Idle timeout
            () = idle_sleep => {
                if idle_duration.is_some() {
                    info!(session_id = %session_id, idle_ms, "ws_idle_timeout");
                    let err_msg = GlobalWsResponse::Error {
                        message: "Idle timeout".to_string(),
                        validation_errors: None,
                    };
                    if let Ok(json) = serde_json::to_string(&err_msg) {
                        let _ = sender.send(Message::Text(json)).await;
                    }
                    break;
                }
            }
            // Client message
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_activity = std::time::Instant::now();
                        request_seq = request_seq.saturating_add(1);

                        // Per-connection message rate limiting
                        if max_msgs_per_sec > 0 {
                            let now = std::time::Instant::now();
                            if now.duration_since(rate_window_start) >= std::time::Duration::from_secs(1) {
                                rate_window_start = now;
                                rate_window_count = 0;
                            }
                            rate_window_count += 1;
                            if rate_window_count > max_msgs_per_sec {
                                let err_msg = GlobalWsResponse::Error {
                                    message: format!("Rate limit exceeded ({max_msgs_per_sec} msgs/sec)"),
                                    validation_errors: None,
                                };
                                if let Ok(json) = serde_json::to_string(&err_msg) {
                                    let _ = sender.send(Message::Text(json)).await;
                                }
                                continue;
                            }
                        }
                        let span = tracing::info_span!(
                            "ws_request",
                            session_id = %session_id,
                            request_id = request_seq,
                            msg_bytes = text.len()
                        );
                        let send_ok = process_and_send_global_ws_message(
                            &handler, &session_id, &text, &auth_identity, &mut sender,
                        )
                        .instrument(span)
                        .await;
                        if !send_ok {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        debug!(session_id = %session_id, "ws_close_frame_received");
                        break;
                    }
                    None => {
                        debug!(session_id = %session_id, "ws_stream_ended");
                        break;
                    }
                    Some(Err(e)) => {
                        warn!(session_id = %session_id, error = %e, "ws_protocol_error");
                        break;
                    }
                    _ => {}
                }
            }
            // Server-initiated heartbeat ping
            _ = heartbeat_interval.tick() => {
                if sender.send(Message::Ping(Vec::new())).await.is_err() {
                    break; // Connection dead
                }
            }
            // Push notification
            notification = notify_rx.recv() => {
                match notification {
                    Ok(ref notif) => {
                        let session_kg = match handler
                            .session_manager()
                            .with_session(&session_id, |s| s.knowledge_graph.clone())
                        {
                            Ok(kg) => kg,
                            Err(_) => break,
                        };
                        let notif_kg = match notif {
                            PersistentNotification::PersistentUpdate { knowledge_graph, .. } => knowledge_graph,
                            PersistentNotification::RuleChange { knowledge_graph, .. } => knowledge_graph,
                            PersistentNotification::KgChange { knowledge_graph, .. } => knowledge_graph,
                            PersistentNotification::SchemaChange { knowledge_graph, .. } => knowledge_graph,
                        };
                        let is_kg_change = matches!(notif, PersistentNotification::KgChange { .. });
                        if *notif_kg == session_kg || is_kg_change {
                            if let Ok(json) = serde_json::to_string(&notif) {
                                if sender.send(Message::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        total_lagged += count;
                        if total_lagged > max_lag {
                            warn!(session_id = %session_id, total_lagged, max_lag, "ws_slow_subscriber_disconnected");
                            let err = GlobalWsResponse::Error {
                                message: format!("Disconnected: missed {total_lagged} total notification(s)"),
                                validation_errors: None,
                            };
                            if let Ok(json) = serde_json::to_string(&err) {
                                let _ = sender.send(Message::Text(json)).await;
                            }
                            break;
                        }
                        let warn = GlobalWsResponse::Error {
                            message: format!("Missed {count} notification(s) due to backpressure"),
                            validation_errors: None,
                        };
                        if let Ok(json) = serde_json::to_string(&warn) {
                            if sender.send(Message::Text(json)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Notification channel closed - server shutting down
                        let shutdown_msg = GlobalWsResponse::Error {
                            message: "Server shutting down".to_string(),
                            validation_errors: None,
                        };
                        if let Ok(json) = serde_json::to_string(&shutdown_msg) {
                            let _ = sender.send(Message::Text(json)).await;
                        }
                        break;
                    }
                }
            }
        }
    }

    // Send close frame before cleanup (prevents "connection reset without handshake" warnings)
    let _ = sender.send(Message::Close(None)).await;

    // Auto-close session on disconnect
    let stats = handler.session_stats();
    info!(session_id = %session_id, active_sessions = stats.total_sessions, "ws_session_disconnecting");
    if let Err(e) = handler.close_session(&session_id) {
        tracing::warn!(session_id = %session_id, error = %e, "session_cleanup_failed");
    }
}

/// Helper: serialize a `GlobalWsResponse` and send it. Returns `false` if the
/// send fails (connection dead).
async fn send_global_response(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    response: &GlobalWsResponse,
    session_id: &str,
) -> bool {
    let json = match serde_json::to_string(response) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "Failed to serialize GlobalWsResponse");
            let err = GlobalWsResponse::Error {
                message: "Internal server error".to_string(),
                validation_errors: None,
            };
            serde_json::to_string(&err).unwrap_or_else(|_| {
                r#"{"type":"error","message":"Internal serialization error"}"#.to_string()
            })
        }
    };
    // Guard against oversized WS frames (shouldn't happen for streamed chunks,
    // but protects against non-streamed single messages)
    let json = if json.len() > MAX_MESSAGE_SIZE {
        warn!(session_id = %session_id, size = json.len(), max = MAX_MESSAGE_SIZE, "ws_result_too_large");
        let err = GlobalWsResponse::Error {
            message: format!(
                "Result too large ({} bytes, max {})",
                json.len(),
                MAX_MESSAGE_SIZE
            ),
            validation_errors: None,
        };
        serde_json::to_string(&err)
            .unwrap_or_else(|_| r#"{"type":"error","message":"Result too large"}"#.to_string())
    } else {
        json
    };
    sender.send(Message::Text(json)).await.is_ok()
}

/// Process a single global WebSocket message and send the response(s).
/// Returns `true` if the connection is still alive, `false` if it should close.
///
/// For Execute messages, this may stream multiple messages (result_start /
/// result_chunk / result_end) when the result is large. Non-execute messages
/// always send a single response.
async fn process_and_send_global_ws_message(
    handler: &Arc<Handler>,
    session_id: &str,
    text: &str,
    auth: &crate::auth::AuthIdentity,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    let request: GlobalWsRequest = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "Invalid GlobalWsRequest message");
            return send_global_response(
                sender,
                &GlobalWsResponse::Error {
                    message: "Invalid message format".to_string(),
                    validation_errors: None,
                },
                session_id,
            )
            .await;
        }
    };

    match request {
        GlobalWsRequest::Execute { program } => {
            send_global_execute(handler, session_id, program, auth, sender).await
        }
        GlobalWsRequest::Ping => {
            send_global_response(sender, &GlobalWsResponse::Pong, session_id).await
        }
        // Login/Authenticate after already authenticated is a no-op
        GlobalWsRequest::Login { .. } | GlobalWsRequest::Authenticate { .. } => {
            send_global_response(
                sender,
                &GlobalWsResponse::Error {
                    message: "Already authenticated".to_string(),
                    validation_errors: None,
                },
                session_id,
            )
            .await
        }
    }
}

/// Handle an Execute message on the global WebSocket.
///
/// For small results (< STREAMING_THRESHOLD bytes when serialized), sends a
/// single `result` message. For large results, streams the data as:
/// 1. `result_start` - schema, metadata, totals
/// 2. `result_chunk` (×N) - batches of up to STREAMING_CHUNK_ROWS rows
/// 3. `result_end` - row_count + chunk_count summary
///
/// Returns `true` if connection still alive, `false` to close.
async fn send_global_execute(
    handler: &Arc<Handler>,
    session_id: &str,
    program: String,
    auth: &crate::auth::AuthIdentity,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    let start = std::time::Instant::now();
    let program_len = program.len();
    let program_preview = program.lines().next().unwrap_or("").trim();
    info!(
        session_id,
        program_len,
        program_preview = %program_preview,
        "ws_execute_start"
    );
    let sid = session_id.to_string();
    let result = handler
        .execute_program(Some(&sid), None, program.clone(), Some(auth))
        .await;
    let elapsed = start.elapsed();
    let slow_query_ms = handler.config().storage.performance.slow_query_log_ms;
    if slow_query_ms > 0 && elapsed.as_millis() as u64 >= slow_query_ms {
        warn!(
            session_id,
            elapsed_ms = elapsed.as_millis() as u64,
            threshold_ms = slow_query_ms,
            program_preview = %&program[..program.len().min(80)],
            "ws_slow_execute"
        );
    }
    info!(
        session_id,
        program_len,
        elapsed_ms = elapsed.as_millis() as u64,
        ok = result.is_ok(),
        "ws_execute_end"
    );

    match result {
        Ok(response) => {
            let row_provenance: Vec<String> = response
                .rows
                .iter()
                .map(|row| {
                    row.provenance
                        .as_ref()
                        .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
                })
                .collect();

            let rows: Vec<Vec<serde_json::Value>> = response
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
                .collect();

            let columns: Vec<String> = response.schema.iter().map(|c| c.name.clone()).collect();
            let row_count = rows.len();

            let metadata = response.metadata.map(|m| SessionQueryMetadataDto {
                has_ephemeral: m.has_ephemeral,
                ephemeral_sources: m.ephemeral_sources,
                warnings: m.warnings,
            });

            // Build the single-message response to check its size
            let single_response = GlobalWsResponse::Result {
                columns: columns.clone(),
                rows: rows.clone(),
                row_count,
                total_count: response.total_count,
                truncated: response.truncated,
                execution_time_ms: response.execution_time_ms,
                row_provenance: row_provenance.clone(),
                metadata: metadata.clone(),
                switched_kg: response.switched_kg.clone(),
            };

            // Check serialized size to decide: single message vs streaming
            let single_json = match serde_json::to_string(&single_response) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to serialize result");
                    return send_global_response(
                        sender,
                        &GlobalWsResponse::Error {
                            message: "Internal server error".to_string(),
                            validation_errors: None,
                        },
                        session_id,
                    )
                    .await;
                }
            };

            if single_json.len() <= STREAMING_THRESHOLD {
                // Small result: send as single message (backward compatible)
                if single_json.len() > MAX_MESSAGE_SIZE {
                    warn!(session_id = %session_id, size = single_json.len(), max = MAX_MESSAGE_SIZE, "ws_result_too_large");
                    return send_global_response(
                        sender,
                        &GlobalWsResponse::Error {
                            message: format!(
                                "Result too large ({} bytes, max {})",
                                single_json.len(),
                                MAX_MESSAGE_SIZE
                            ),
                            validation_errors: None,
                        },
                        session_id,
                    )
                    .await;
                }
                sender.send(Message::Text(single_json)).await.is_ok()
            } else {
                // Large result: stream as chunks
                info!(
                    session_id,
                    row_count,
                    json_size = single_json.len(),
                    "ws_streaming_result"
                );
                drop(single_json); // free memory

                // 1. Send result_start header
                let start_msg = GlobalWsResponse::ResultStart {
                    columns,
                    total_count: response.total_count,
                    truncated: response.truncated,
                    execution_time_ms: response.execution_time_ms,
                    metadata,
                    switched_kg: response.switched_kg,
                };
                if !send_global_response(sender, &start_msg, session_id).await {
                    return false;
                }

                // 2. Send row chunks
                let mut chunk_index: usize = 0;
                let mut row_iter = rows.into_iter();
                let mut prov_iter = row_provenance.into_iter();
                loop {
                    let chunk_rows: Vec<Vec<serde_json::Value>> =
                        row_iter.by_ref().take(STREAMING_CHUNK_ROWS).collect();
                    if chunk_rows.is_empty() {
                        break;
                    }
                    let chunk_prov: Vec<String> =
                        prov_iter.by_ref().take(chunk_rows.len()).collect();
                    let chunk_msg = GlobalWsResponse::ResultChunk {
                        rows: chunk_rows,
                        row_provenance: chunk_prov,
                        chunk_index,
                    };
                    if !send_global_response(sender, &chunk_msg, session_id).await {
                        return false;
                    }
                    chunk_index += 1;
                }

                // 3. Send result_end
                let end_msg = GlobalWsResponse::ResultEnd {
                    row_count,
                    chunk_count: chunk_index,
                };
                send_global_response(sender, &end_msg, session_id).await
            }
        }
        Err(e) => {
            // Check for structured validation errors
            let response = if let Some(json_str) = e.strip_prefix(VALIDATION_ERROR_PREFIX) {
                if let Ok(errors) = serde_json::from_str::<Vec<ValidationError>>(json_str) {
                    let count = errors.len();
                    GlobalWsResponse::Error {
                        message: format!("Program has {count} parse error(s)"),
                        validation_errors: Some(errors),
                    }
                } else {
                    GlobalWsResponse::Error {
                        message: e,
                        validation_errors: None,
                    }
                }
            } else {
                GlobalWsResponse::Error {
                    message: e,
                    validation_errors: None,
                }
            };
            send_global_response(sender, &response, session_id).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_request_query_deserialize() {
        let json = r#"{"type": "query", "query": "?edge(X,Y)"}"#;
        let req: WsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, WsRequest::Query { query } if query == "?edge(X,Y)"));
    }

    #[test]
    fn test_ws_request_insert_facts_deserialize() {
        let json = r#"{"type": "insert_facts", "relation": "edge", "tuples": [[1,2],[3,4]]}"#;
        let req: WsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, WsRequest::InsertFacts { relation, tuples }
            if relation == "edge" && tuples.len() == 2));
    }

    #[test]
    fn test_ws_request_ping_deserialize() {
        let json = r#"{"type": "ping"}"#;
        let req: WsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, WsRequest::Ping));
    }

    #[test]
    fn test_ws_response_result_serialize() {
        let resp = WsResponse::Result {
            columns: vec!["x".to_string()],
            rows: vec![vec![serde_json::json!(1)]],
            row_count: 1,
            execution_time_ms: 5,
            row_provenance: vec!["persistent".to_string()],
            metadata: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result\""));
        assert!(json.contains("\"row_count\":1"));
    }

    #[test]
    fn test_ws_response_error_serialize() {
        let resp = WsResponse::Error {
            message: "bad query".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("bad query"));
    }

    #[test]
    fn test_ws_response_pong_serialize() {
        let resp = WsResponse::Pong;
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"type":"pong"}"#);
    }

    #[test]
    fn test_ws_response_ack_serialize() {
        let resp = WsResponse::Ack {
            message: "done".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"ack\""));
    }

    #[test]
    fn test_ws_request_add_rule_deserialize() {
        let json = r#"{"type": "add_rule", "rule": "path(X,Y) <- edge(X,Y)"}"#;
        let req: WsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, WsRequest::AddRule { rule } if rule.contains("path")));
    }

    #[test]
    fn test_ws_request_retract_facts_deserialize() {
        let json = r#"{"type": "retract_facts", "relation": "edge", "tuples": [[1,2]]}"#;
        let req: WsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, WsRequest::RetractFacts { relation, .. } if relation == "edge"));
    }

    #[test]
    fn test_persistent_notification_serialize() {
        let notif = PersistentNotification::PersistentUpdate {
            knowledge_graph: "kg1".to_string(),
            relation: "users".to_string(),
            operation: "insert".to_string(),
            count: 3,
            timestamp_ms: 1700000000000,
            session_id: Some("sess-123".to_string()),
            seq: 1,
        };
        let json = serde_json::to_string(&notif).unwrap();
        assert!(json.contains("\"type\":\"persistent_update\""));
        assert!(json.contains("\"relation\":\"users\""));
    }

    // === Global WebSocket protocol tests ===

    #[test]
    fn test_global_ws_request_execute_deserialize() {
        let json = r#"{"type": "execute", "program": "+edge(1,2)."}"#;
        let req: GlobalWsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, GlobalWsRequest::Execute { program } if program == "+edge(1,2)."));
    }

    #[test]
    fn test_global_ws_request_ping_deserialize() {
        let json = r#"{"type": "ping"}"#;
        let req: GlobalWsRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req, GlobalWsRequest::Ping));
    }

    #[test]
    fn test_global_ws_response_authenticated_serialize() {
        let resp = GlobalWsResponse::Authenticated {
            session_id: "42".to_string(),
            knowledge_graph: "default".to_string(),
            version: "0.1.0".to_string(),
            role: "admin".to_string(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"authenticated\""));
        assert!(json.contains("\"session_id\":\"42\""));
        assert!(json.contains("\"knowledge_graph\":\"default\""));
        assert!(json.contains("\"role\":\"admin\""));
    }

    #[test]
    fn test_global_ws_response_result_serialize() {
        let resp = GlobalWsResponse::Result {
            columns: vec!["col0".to_string()],
            rows: vec![vec![serde_json::json!(1)]],
            row_count: 1,
            total_count: 1,
            truncated: false,
            execution_time_ms: 5,
            row_provenance: vec![],
            metadata: None,
            switched_kg: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result\""));
        assert!(json.contains("\"total_count\":1"));
        assert!(json.contains("\"truncated\":false"));
        // switched_kg should be omitted when None
        assert!(!json.contains("switched_kg"));
    }

    #[test]
    fn test_global_ws_response_result_with_kg_switch() {
        let resp = GlobalWsResponse::Result {
            columns: vec!["message".to_string()],
            rows: vec![],
            row_count: 0,
            total_count: 0,
            truncated: false,
            execution_time_ms: 1,
            row_provenance: vec![],
            metadata: None,
            switched_kg: Some("new_kg".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"switched_kg\":\"new_kg\""));
    }

    #[test]
    fn test_global_ws_response_error_serialize() {
        let resp = GlobalWsResponse::Error {
            message: "test error".to_string(),
            validation_errors: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("test error"));
    }

    #[test]
    fn test_global_ws_response_pong_serialize() {
        let resp = GlobalWsResponse::Pong;
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"type":"pong"}"#);
    }

    #[test]
    fn test_ws_connect_params_default() {
        let params: WsConnectParams = serde_json::from_str("{}").unwrap();
        assert_eq!(params.kg, "default");
    }

    #[test]
    fn test_ws_connect_params_custom_kg() {
        let params: WsConnectParams = serde_json::from_str(r#"{"kg": "my_graph"}"#).unwrap();
        assert_eq!(params.kg, "my_graph");
    }

    #[test]
    fn test_ws_connect_params_last_seq() {
        let params: WsConnectParams =
            serde_json::from_str(r#"{"kg": "test", "last_seq": 42}"#).unwrap();
        assert_eq!(params.kg, "test");
        assert_eq!(params.last_seq, Some(42));
    }

    #[test]
    fn test_ws_connect_params_last_seq_omitted() {
        let params: WsConnectParams = serde_json::from_str(r#"{"kg": "test"}"#).unwrap();
        assert_eq!(params.last_seq, None);
    }

    #[test]
    fn test_global_ws_response_error_with_validation_errors() {
        let resp = GlobalWsResponse::Error {
            message: "Program has 1 parse error(s)".to_string(),
            validation_errors: Some(vec![ValidationError {
                line: 2,
                statement_index: 1,
                error: "Expected relation name".to_string(),
            }]),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"validation_errors\""));
        assert!(json.contains("\"line\":2"));
        assert!(json.contains("\"statement_index\":1"));
    }

    #[test]
    fn test_global_ws_response_error_without_validation_errors() {
        let resp = GlobalWsResponse::Error {
            message: "some error".to_string(),
            validation_errors: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(!json.contains("validation_errors"));
    }

    // === Streaming result protocol tests ===

    #[test]
    fn test_global_ws_response_result_start_serialize() {
        let resp = GlobalWsResponse::ResultStart {
            columns: vec!["x".to_string(), "y".to_string()],
            total_count: 10_000,
            truncated: false,
            execution_time_ms: 42,
            metadata: None,
            switched_kg: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_start\""));
        assert!(json.contains("\"total_count\":10000"));
        assert!(json.contains("\"columns\":[\"x\",\"y\"]"));
        // Optional fields omitted when None
        assert!(!json.contains("metadata"));
        assert!(!json.contains("switched_kg"));
    }

    #[test]
    fn test_global_ws_response_result_chunk_serialize() {
        let resp = GlobalWsResponse::ResultChunk {
            rows: vec![
                vec![serde_json::json!(1), serde_json::json!("a")],
                vec![serde_json::json!(2), serde_json::json!("b")],
            ],
            row_provenance: vec!["persistent".to_string(), "ephemeral".to_string()],
            chunk_index: 3,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_chunk\""));
        assert!(json.contains("\"chunk_index\":3"));
        assert!(json.contains("\"row_provenance\""));
    }

    #[test]
    fn test_global_ws_response_result_chunk_empty_provenance() {
        let resp = GlobalWsResponse::ResultChunk {
            rows: vec![vec![serde_json::json!(1)]],
            row_provenance: vec![],
            chunk_index: 0,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_chunk\""));
        // Empty provenance should be omitted
        assert!(!json.contains("row_provenance"));
    }

    #[test]
    fn test_global_ws_response_result_end_serialize() {
        let resp = GlobalWsResponse::ResultEnd {
            row_count: 5000,
            chunk_count: 10,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_end\""));
        assert!(json.contains("\"row_count\":5000"));
        assert!(json.contains("\"chunk_count\":10"));
    }

    #[test]
    fn test_streaming_threshold_constants() {
        // Verify exact values (prevents accidental changes)
        assert_eq!(STREAMING_THRESHOLD, 1024 * 1024); // 1 MB
        assert_eq!(STREAMING_CHUNK_ROWS, 500);
        // Sanity: streaming threshold must be well below max message size
        let ratio = MAX_MESSAGE_SIZE / STREAMING_THRESHOLD;
        assert!(
            ratio >= 2,
            "threshold should be at most half of max message size, got ratio={ratio}"
        );
    }

    #[test]
    fn test_global_ws_response_result_start_with_metadata() {
        let resp = GlobalWsResponse::ResultStart {
            columns: vec!["col0".to_string()],
            total_count: 100,
            truncated: true,
            execution_time_ms: 10,
            metadata: Some(SessionQueryMetadataDto {
                has_ephemeral: true,
                ephemeral_sources: vec!["edge".to_string()],
                warnings: vec![],
            }),
            switched_kg: Some("new_kg".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_start\""));
        assert!(json.contains("\"truncated\":true"));
        assert!(json.contains("\"metadata\""));
        assert!(json.contains("\"switched_kg\":\"new_kg\""));
    }
}
