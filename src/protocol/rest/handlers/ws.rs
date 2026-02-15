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
        Path, WebSocketUpgrade,
    },
    response::IntoResponse,
    Extension,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};

use super::wire_value_to_json;
use crate::protocol::handler::PersistentNotification;
use crate::protocol::rest::dto::SessionQueryMetadataDto;
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;

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
    /// Push notification when persistent data changes
    Notification {
        event: String,
        knowledge_graph: String,
        relation: String,
        operation: String,
        count: usize,
    },
}

/// WebSocket upgrade endpoint for session-scoped connections
#[utoipa::path(
    get,
    path = "/sessions/{id}/ws",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    responses(
        (status = 101, description = "WebSocket connection established"),
        (status = 404, description = "Session not found"),
    )
)]
pub async fn session_websocket(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, RestError> {
    // Verify session exists before upgrading
    if !handler.session_manager().has_session(id) {
        return Err(RestError::not_found(format!("Session {id} not found")));
    }

    Ok(ws.on_upgrade(move |socket| handle_ws_connection(socket, handler, id)))
}

/// Handle a WebSocket connection for a specific session.
/// Uses `tokio::select!` to concurrently process client messages and push notifications.
async fn handle_ws_connection(socket: WebSocket, handler: Arc<Handler>, session_id: u64) {
    let (mut sender, mut receiver) = socket.split();
    let mut notify_rx = handler.subscribe_notifications();

    // Get the session's knowledge graph for filtering notifications
    let session_kg = handler
        .session_manager()
        .with_session(session_id, |s| s.knowledge_graph.clone())
        .unwrap_or_default();

    loop {
        tokio::select! {
            // Client message
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let response = process_ws_message(&handler, session_id, &text).await;
                        let json = match serde_json::to_string(&response) {
                            Ok(j) => j,
                            Err(e) => {
                                let err = WsResponse::Error {
                                    message: format!("Serialization error: {e}"),
                                };
                                serde_json::to_string(&err).unwrap_or_default()
                            }
                        };
                        if sender.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {} // Ignore binary, ping/pong handled by axum
                }
            }
            // Push notification from persistent data changes
            notification = notify_rx.recv() => {
                match notification {
                    Ok(PersistentNotification::PersistentUpdate { knowledge_graph, relation, operation, count }) => {
                        // Only forward notifications for this session's knowledge graph
                        if knowledge_graph == session_kg {
                            let ws_msg = WsResponse::Notification {
                                event: "persistent_update".to_string(),
                                knowledge_graph,
                                relation,
                                operation,
                                count,
                            };
                            if let Ok(json) = serde_json::to_string(&ws_msg) {
                                if sender.send(Message::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Missed some notifications — continue
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }
}

/// Process a single WebSocket message and return a response
async fn process_ws_message(handler: &Arc<Handler>, session_id: u64, text: &str) -> WsResponse {
    let request: WsRequest = match serde_json::from_str(text) {
        Ok(r) => r,
        Err(e) => {
            return WsResponse::Error {
                message: format!("Invalid message: {e}"),
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

async fn handle_ws_query(handler: &Arc<Handler>, session_id: u64, query: String) -> WsResponse {
    let start = std::time::Instant::now();
    match handler.query_program_with_session(session_id, query).await {
        Ok(response) => {
            let row_provenance: Vec<String> = response
                .rows
                .iter()
                .filter_map(|row| {
                    row.provenance.as_ref().map(|p| match p {
                        crate::session::Provenance::Persistent => "persistent".to_string(),
                        crate::session::Provenance::Ephemeral => "ephemeral".to_string(),
                        crate::session::Provenance::Mixed => "mixed".to_string(),
                    })
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
    session_id: u64,
    relation: &str,
    tuples: Vec<Vec<serde_json::Value>>,
) -> WsResponse {
    let parsed = match super::sessions::json_tuples_to_tuples(&tuples) {
        Ok(t) => t,
        Err(e) => return WsResponse::Error { message: e },
    };
    let count = parsed.len();
    match handler.session_insert_ephemeral(session_id, relation, parsed) {
        Ok(()) => WsResponse::Ack {
            message: format!("Inserted {count} fact(s) into '{relation}'"),
        },
        Err(e) => WsResponse::Error { message: e },
    }
}

fn handle_ws_retract_facts(
    handler: &Arc<Handler>,
    session_id: u64,
    relation: &str,
    tuples: Vec<Vec<serde_json::Value>>,
) -> WsResponse {
    let parsed = match super::sessions::json_tuples_to_tuples(&tuples) {
        Ok(t) => t,
        Err(e) => return WsResponse::Error { message: e },
    };
    match handler.session_retract_ephemeral(session_id, relation, parsed) {
        Ok(retracted) => WsResponse::Ack {
            message: format!("Retracted {retracted} fact(s) from '{relation}'"),
        },
        Err(e) => WsResponse::Error { message: e },
    }
}

fn handle_ws_add_rule(handler: &Arc<Handler>, session_id: u64, rule_text: &str) -> WsResponse {
    let program = match crate::parser::parse_program(rule_text) {
        Ok(p) => p,
        Err(e) => {
            return WsResponse::Error {
                message: format!("Invalid rule syntax: {e}"),
            };
        }
    };

    let rule = match program.rules.into_iter().next() {
        Some(r) => r,
        None => {
            return WsResponse::Error {
                message: "No rule found in input".to_string(),
            };
        }
    };

    let head = rule.head.relation.clone();
    match handler.session_add_rule(session_id, rule, rule_text.to_string()) {
        Ok(()) => WsResponse::Ack {
            message: format!("Rule added for '{head}'"),
        },
        Err(e) => WsResponse::Error { message: e },
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
    fn test_ws_response_notification_serialize() {
        let resp = WsResponse::Notification {
            event: "persistent_update".to_string(),
            knowledge_graph: "test_kg".to_string(),
            relation: "edge".to_string(),
            operation: "insert".to_string(),
            count: 5,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"notification\""));
        assert!(json.contains("\"event\":\"persistent_update\""));
        assert!(json.contains("\"relation\":\"edge\""));
        assert!(json.contains("\"count\":5"));
    }

    #[test]
    fn test_persistent_notification_serialize() {
        let notif = PersistentNotification::PersistentUpdate {
            knowledge_graph: "kg1".to_string(),
            relation: "users".to_string(),
            operation: "insert".to_string(),
            count: 3,
        };
        let json = serde_json::to_string(&notif).unwrap();
        assert!(json.contains("\"type\":\"persistent_update\""));
        assert!(json.contains("\"relation\":\"users\""));
    }
}
