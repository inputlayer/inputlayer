//! WebSocket Handler
//!
//! The global `/ws` endpoint: authenticate, then send `execute` messages
//! carrying IQL. Each connection owns an auto-managed session and receives
//! push notifications for its knowledge graph. Standing queries
//! (`.subscribe` / `.unsubscribe`) push `subscription_delta` and
//! `subscription_error`; see [`crate::protocol::subscription`].
//!
//! A connection is bound to the [`Principal`] it authenticated as. Revoking
//! that credential closes the connection, and every outbound data frame is
//! fenced by it (see the `outbound` module).
//!
//! Each connection is one loop that owns the socket's read half, the single
//! writer, its subscriptions and its request `pipeline`. Requests run as
//! pipeline tasks (see the `request` module), so a long query never holds up
//! the connection's pushes; the pipeline releases replies in request order
//! and keeps writes, KG switches and session changes in order.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, WebSocketUpgrade,
    },
    response::IntoResponse,
    Extension,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn, Instrument};

mod outbound;
mod pipeline;
mod request;

use futures_util::FutureExt;
use outbound::Outbound;
use pipeline::{Access, Released, RequestPipeline, Startable};
use request::{Job, Reply};

use crate::auth::{Principal, Role, INTERNAL_KG};
use crate::protocol::handler::{PersistentNotification, ValidationError};
use crate::protocol::rest::dto::SessionQueryMetadataDto;
use crate::protocol::rest::error::RestError;
use crate::protocol::rest::{ClientIp, PreAuthSlots, WsSemaphore};
use crate::protocol::subscription::{ConnectionSubscriptions, Push};
use crate::protocol::wire::{ErrorCode, StatementError};
use crate::protocol::Handler;
use crate::protocol::MAX_MESSAGE_SIZE;

/// Threshold in bytes: results whose single-message JSON exceeds this are
/// streamed as `result_start` / `result_chunk` / `result_end` messages.
/// Below this threshold, the classic single `result` message is used.
const STREAMING_THRESHOLD: usize = 1024 * 1024; // 1 MB

/// Maximum number of rows per `result_chunk` message.
const STREAMING_CHUNK_ROWS: usize = 500;

/// Failed authentications allowed per connection before it is closed.
const MAX_AUTH_FAILURES: u32 = 3;

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
    /// Execute any IQL statement or meta command as raw text
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
        #[serde(skip_serializing_if = "Option::is_none")]
        proof_trees: Option<Vec<crate::provenance::proof_tree::ProofTree>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timing_breakdown: Option<crate::execution::TimingBreakdown>,
        /// Failed statements, empty when every statement succeeded.
        errors: Vec<StatementError>,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        proof_trees: Option<Vec<crate::provenance::proof_tree::ProofTree>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timing_breakdown: Option<crate::execution::TimingBreakdown>,
        errors: Vec<StatementError>,
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
        /// Why the statement failed, when known.
        #[serde(skip_serializing_if = "Option::is_none")]
        code: Option<ErrorCode>,
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
/// **Execute** - Send any IQL statement or meta command as raw text:
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
///  "total_count": 1, "truncated": false, "execution_time_ms": 5, "errors": []}
/// ```
/// `errors` lists the failed statements of a multi-statement program
/// (`{"index", "code", "message"}`).
///
/// **Error** - Error, with `code` when a one-statement program failed:
/// ```json
/// {"type": "error", "message": "...", "code": "not_found"}
/// ```
///
/// **Pong** - Response to ping:
/// ```json
/// {"type": "pong"}
/// ```
///
/// **Notification** - Push notification for persistent data changes:
/// ```json
/// {"type": "persistent_update", "knowledge_graph": "default", "relation": "edge",
///  "operation": "insert", "count": 5, "timestamp_ms": 1700000000000, "seq": 42}
/// ```
///
/// **Standing queries** - `{"type": "execute", "program": ".subscribe <id> ?<query>"}`
/// replies with a normal `result` (the initial snapshot); afterwards the server
/// pushes changes to the result:
/// ```json
/// {"type": "subscription_delta", "subscription": "<id>", "knowledge_graph": "default",
///  "seq": 1, "columns": ["X"], "inserted": [[3]], "retracted": []}
/// {"type": "subscription_error", "subscription": "<id>", "message": "..."}
/// ```
/// `.unsubscribe <id>` removes one; disconnecting or switching KG removes all.
pub async fn global_websocket(
    Extension(handler): Extension<Arc<Handler>>,
    Extension(ws_sem): Extension<WsSemaphore>,
    Extension(preauth_slots): Extension<PreAuthSlots>,
    client_ip: Option<Extension<ClientIp>>,
    ws: WebSocketUpgrade,
    Query(params): Query<WsConnectParams>,
) -> Result<impl IntoResponse, RestError> {
    let peer = client_ip.map_or(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        |Extension(ClientIp(ip))| ip,
    );
    let Some(preauth_slot) = preauth_slots.try_acquire(peer) else {
        warn!(%peer, "ws_preauth_limit_exceeded");
        return Err(RestError::too_many_requests(
            "Too many unauthenticated WebSocket connections".to_string(),
        ));
    };

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
                handle_global_ws_connection(
                    socket,
                    handler,
                    params.kg,
                    params.last_seq,
                    peer,
                    preauth_slot,
                )
                .await;
                drop(permit);
            }
        }))
}

/// Per-connection message rate limit over one-second windows.
struct MessageRate {
    max_per_sec: u32,
    window_start: std::time::Instant,
    count: u32,
}

impl MessageRate {
    fn new(max_per_sec: u32) -> Self {
        Self {
            max_per_sec,
            window_start: std::time::Instant::now(),
            count: 0,
        }
    }

    /// Count one message; `false` once the window is over the limit.
    fn allow(&mut self) -> bool {
        if self.max_per_sec == 0 {
            return true;
        }
        let now = std::time::Instant::now();
        if now.duration_since(self.window_start) >= std::time::Duration::from_secs(1) {
            self.window_start = now;
            self.count = 0;
        }
        self.count += 1;
        self.count <= self.max_per_sec
    }
}

/// Send `auth_error` with `message`; `false` if the connection is dead.
async fn send_auth_error(sender: &mut Outbound, message: String) -> bool {
    send_global_response(sender, &GlobalWsResponse::AuthError { message }).await
}

/// Tell the client its credential was revoked, if it was. The only data frame
/// sent past the credential fence.
async fn notify_if_revoked(sender: &mut Outbound, principal: &Principal) {
    if !principal.is_revoked() {
        return;
    }
    let notice = GlobalWsResponse::Error {
        message: "Credential revoked; reconnect with valid credentials".to_string(),
        validation_errors: None,
        code: None,
    };
    if let Ok(json) = serde_json::to_string(&notice) {
        sender.send_revocation_notice(json).await;
    }
}

/// Whether a connection bound to `session_kg` may see `notification`: changes
/// to its own KG, plus KG creation and drop for admins. Changes to the
/// internal KG are never visible.
fn notification_visible(
    notification: &PersistentNotification,
    session_kg: &str,
    principal: &Principal,
) -> bool {
    let kg = notification.knowledge_graph();
    if kg == INTERNAL_KG {
        return false;
    }
    kg == session_kg
        || (matches!(notification, PersistentNotification::KgChange { .. })
            && principal.role() == Ok(Role::Admin))
}

/// Handle a global WebSocket connection with auth loop + message loop.
async fn handle_global_ws_connection(
    socket: WebSocket,
    handler: Arc<Handler>,
    kg: String,
    last_seq: Option<u64>,
    peer: IpAddr,
    preauth_slot: crate::protocol::rest::PreAuthSlot,
) {
    let (sink, mut receiver) = socket.split();
    let mut sender = Outbound::new(sink);

    info!(kg = %kg, "ws_connection_start");

    // Per-connection message rate limiting, auth phase included
    let max_msgs_per_sec = handler.config().http.rate_limit.ws_max_messages_per_sec;
    let mut rate = MessageRate::new(max_msgs_per_sec);

    // ── Auth Loop: wait for Login or Authenticate ────────────────────────
    let auth_timeout = std::time::Duration::from_millis(handler.config().http.ws_auth_timeout_ms);
    let auth_deadline = tokio::time::Instant::now() + auth_timeout;
    let timeout_message = format!("Authentication timeout ({}s)", auth_timeout.as_secs_f32());
    let mut auth_failures = 0;
    let principal: Principal;

    loop {
        let msg = tokio::select! {
            () = tokio::time::sleep_until(auth_deadline) => {
                send_auth_error(&mut sender, timeout_message).await;
                let _ = sender.send(Message::Close(None)).await;
                return;
            }
            msg = receiver.next() => msg,
        };

        let text = match msg {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Close(_))) | None => {
                let _ = sender.send(Message::Close(None)).await;
                return;
            }
            Some(Err(e)) => {
                warn!(error = %e, "ws_auth_protocol_error");
                return;
            }
            _ => continue,
        };

        if !rate.allow() {
            warn!(%peer, "ws_auth_rate_limited");
            send_auth_error(
                &mut sender,
                format!("Rate limit exceeded ({max_msgs_per_sec} msgs/sec)"),
            )
            .await;
            let _ = sender.send(Message::Close(None)).await;
            return;
        }

        let outcome = match serde_json::from_str::<GlobalWsRequest>(&text) {
            Ok(GlobalWsRequest::Login { username, password }) => {
                let login = handler.login(&username, &password, peer);
                match tokio::time::timeout_at(auth_deadline, login).await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        send_auth_error(&mut sender, timeout_message).await;
                        let _ = sender.send(Message::Close(None)).await;
                        return;
                    }
                }
            }
            Ok(GlobalWsRequest::Authenticate { api_key }) => handler
                .authenticate_api_key(&api_key)
                .inspect_err(|_| warn!(%peer, "ws_apikey_auth_failed")),
            Ok(GlobalWsRequest::Execute { .. } | GlobalWsRequest::Ping) => {
                send_auth_error(
                    &mut sender,
                    "Authentication required. Send login or authenticate first.".to_string(),
                )
                .await;
                continue;
            }
            Err(_) => {
                send_auth_error(
                    &mut sender,
                    "Invalid message format. Send login or authenticate.".to_string(),
                )
                .await;
                continue;
            }
        };

        match outcome {
            Ok(authenticated) => {
                principal = authenticated;
                break;
            }
            Err(message) => {
                auth_failures += 1;
                send_auth_error(&mut sender, message).await;
                if auth_failures >= MAX_AUTH_FAILURES {
                    warn!(%peer, auth_failures, "ws_auth_failures_exceeded");
                    let _ = sender.send(Message::Close(None)).await;
                    return;
                }
            }
        }
    }
    drop(preauth_slot);
    sender.bind(principal.clone());

    // ── Authenticated: create session ────────────────────────────────────
    let Ok(auth_identity) = principal.identity() else {
        send_auth_error(&mut sender, "Invalid credentials".to_string()).await;
        sender.close().await;
        return;
    };
    info!(
        kg = %kg,
        username = %auth_identity.username,
        role = %auth_identity.role,
        credential = %principal.credential(),
        "ws_authenticated"
    );

    let session_id = match handler.create_session_with_auth(&kg, &principal) {
        Ok(id) => {
            let stats = handler.session_stats();
            info!(kg = %kg, active_sessions = stats.total_sessions, "ws_session_created");
            debug!(session_id = %id, "ws_session_id");
            id
        }
        Err(e) => {
            warn!(kg = %kg, error = %e, "ws_session_create_failed");
            let err_msg = GlobalWsResponse::Error {
                message: e.clone(),
                validation_errors: None,
                code: None,
            };
            if let Ok(json) = serde_json::to_string(&err_msg) {
                let _ = sender.send(Message::Text(json)).await;
            }
            sender.close().await;
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
                tracing::warn!(error = %e, "session_cleanup_failed");
            }
            let _ = sender.send(Message::Close(None)).await;
            return;
        }
    }

    let mut notify_rx = handler.subscribe_notifications();
    let mut request_seq: u64 = 0;
    let mut subscriptions =
        ConnectionSubscriptions::new(Arc::clone(&handler), Some(principal.clone()));
    let mut requests: Requests =
        RequestPipeline::new(handler.config().http.rate_limit.ws_max_in_flight_requests);

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
                if !notification_visible(notif, &session_kg, &principal) {
                    continue;
                }
                if let Ok(json) = serde_json::to_string(notif) {
                    if sender.send(Message::Text(json)).await.is_err() {
                        if let Err(e) = handler.close_session(&session_id) {
                            tracing::warn!(error = %e, "session_cleanup_failed");
                        }
                        notify_if_revoked(&mut sender, &principal).await;
                        return;
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
    // One timer, moved on activity: re-arming it every loop turn would cost a
    // timer registration per request.
    let idle_timer = tokio::time::sleep(idle_duration.unwrap_or_default());
    tokio::pin!(idle_timer);
    let touch = |timer: std::pin::Pin<&mut tokio::time::Sleep>| {
        if let Some(duration) = idle_duration {
            timer.reset(tokio::time::Instant::now() + duration);
        }
    };

    // Connection lifetime limit
    let connection_start = std::time::Instant::now();
    let max_lifetime_secs = handler.config().http.rate_limit.ws_max_lifetime_secs;
    let max_lifetime = if max_lifetime_secs > 0 {
        Some(std::time::Duration::from_secs(max_lifetime_secs))
    } else {
        None
    };

    // Fires once the connection's credential is revoked.
    let mut revocation = principal.revocation();

    // Server-initiated heartbeat: send ping every 30s to detect dead connections
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat_interval.tick().await; // consume the immediate first tick

    loop {
        // Check connection lifetime
        if let Some(max_lt) = max_lifetime {
            if connection_start.elapsed() >= max_lt {
                info!(max_lifetime_secs, "ws_max_lifetime_exceeded");
                let err_msg = GlobalWsResponse::Error {
                    message: format!("Connection lifetime exceeded ({max_lifetime_secs}s)"),
                    validation_errors: None,
                    code: None,
                };
                if let Ok(json) = serde_json::to_string(&err_msg) {
                    let _ = sender.send(Message::Text(json)).await;
                }
                break;
            }
        }

        tokio::select! {
            // Credential revoked: stop everything this connection was doing
            () = &mut revocation => {
                info!(credential = %principal.credential(), "ws_credential_revoked");
                break;
            }
            // Idle timeout; a connection with requests in progress is not idle
            () = &mut idle_timer, if idle_duration.is_some() && requests.is_idle() => {
                info!(idle_ms, "ws_idle_timeout");
                let err_msg = GlobalWsResponse::Error {
                    message: "Idle timeout".to_string(),
                    validation_errors: None,
                    code: None,
                };
                if let Ok(json) = serde_json::to_string(&err_msg) {
                    let _ = sender.send(Message::Text(json)).await;
                }
                break;
            }
            // Client message, read only while the pipeline has room
            msg = receiver.next(), if requests.has_capacity() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        touch(idle_timer.as_mut());
                        request_seq = request_seq.saturating_add(1);
                        let (access, job) = if rate.allow() {
                            Job::from_text(&text)
                        } else {
                            Job::immediate(request::error_response(format!(
                                "Rate limit exceeded ({max_msgs_per_sec} msgs/sec)"
                            )))
                        };
                        let span = tracing::info_span!(
                            "ws_request",
                            request_id = request_seq,
                            msg_bytes = text.len()
                        );
                        requests.admit(access, (job, span));
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
                        warn!(error = %e, "ws_protocol_error");
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
            // A request's reply, in request order
            released = requests.next_reply() => {
                touch(idle_timer.as_mut());
                let frames = release_reply(&handler, &session_id, released, &mut subscriptions);
                if !send_frames(&mut sender, frames).await {
                    break;
                }
            }
            // Standing-query evaluation finished
            completion = subscriptions.next_completion() => {
                if let Some(push) = subscriptions.on_completion(completion) {
                    if !send_subscription_push(&mut sender, &push).await {
                        break;
                    }
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
                        if notification_visible(notif, &session_kg, &principal) {
                            if let Ok(json) = serde_json::to_string(&notif) {
                                if sender.send(Message::Text(json)).await.is_err() {
                                    break;
                                }
                            }
                        }
                        subscriptions.on_notification(notif);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        subscriptions.on_missed_notifications();
                        total_lagged += count;
                        if total_lagged > max_lag {
                            warn!(total_lagged, max_lag, "ws_slow_subscriber_disconnected");
                            let err = GlobalWsResponse::Error {
                                message: format!("Disconnected: missed {total_lagged} total notification(s)"),
                                validation_errors: None,
                                code: None,
                            };
                            if let Ok(json) = serde_json::to_string(&err) {
                                let _ = sender.send(Message::Text(json)).await;
                            }
                            break;
                        }
                        let warn = GlobalWsResponse::Error {
                            message: format!("Missed {count} notification(s) due to backpressure"),
                            validation_errors: None,
                            code: None,
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
                            code: None,
                        };
                        if let Ok(json) = serde_json::to_string(&shutdown_msg) {
                            let _ = sender.send(Message::Text(json)).await;
                        }
                        break;
                    }
                }
            }
        }
        start_requests(
            &mut requests,
            &handler,
            &session_id,
            &principal,
            &mut subscriptions,
        );
    }

    // Abort reads in progress, let a started write finish, then stop
    // standing queries before anything else is sent.
    requests.shutdown().await;
    drop(subscriptions);
    notify_if_revoked(&mut sender, &principal).await;
    // Send close frame before cleanup (prevents "connection reset without handshake" warnings)
    let _ = sender.send(Message::Close(None)).await;

    // Auto-close session on disconnect
    let stats = handler.session_stats();
    info!(
        active_sessions = stats.total_sessions,
        "ws_session_disconnecting"
    );
    if let Err(e) = handler.close_session(&session_id) {
        tracing::warn!(error = %e, "session_cleanup_failed");
    }
}

/// Helper: serialize a `GlobalWsResponse` and send it. Returns `false` if the
/// send fails (connection dead).
async fn send_global_response(sender: &mut Outbound, response: &GlobalWsResponse) -> bool {
    sender
        .send(Message::Text(response_frame(response)))
        .await
        .is_ok()
}

/// Serialize `response` as one frame; an oversized one becomes an `error`.
fn response_frame(response: &GlobalWsResponse) -> String {
    let json = match serde_json::to_string(response) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "Failed to serialize GlobalWsResponse");
            let err = GlobalWsResponse::Error {
                message: "Internal server error".to_string(),
                validation_errors: None,
                code: None,
            };
            serde_json::to_string(&err).unwrap_or_else(|_| {
                r#"{"type":"error","message":"Internal serialization error"}"#.to_string()
            })
        }
    };
    // Guard against oversized WS frames (shouldn't happen for streamed chunks,
    // but protects against non-streamed single messages)
    if json.len() > MAX_MESSAGE_SIZE {
        warn!(
            size = json.len(),
            max = MAX_MESSAGE_SIZE,
            "ws_result_too_large"
        );
        let err = GlobalWsResponse::Error {
            message: format!(
                "Result too large ({} bytes, max {})",
                json.len(),
                MAX_MESSAGE_SIZE
            ),
            validation_errors: None,
            code: None,
        };
        serde_json::to_string(&err)
            .unwrap_or_else(|_| r#"{"type":"error","message":"Result too large"}"#.to_string())
    } else {
        json
    }
}

/// The connection's requests: each job with its tracing span.
type Requests = RequestPipeline<(Job, tracing::Span), Reply>;

/// Start every request the pipeline's ordering barriers now allow. Work that
/// computes runs as a pipeline future; connection-state changes happen here, on
/// the loop that owns that state.
fn start_requests(
    requests: &mut Requests,
    handler: &Arc<Handler>,
    session_id: &str,
    principal: &Principal,
    subscriptions: &mut ConnectionSubscriptions,
) {
    while let Some(Startable {
        ticket,
        job: (job, span),
    }) = requests.next_startable()
    {
        let _entered = span.enter();
        match job {
            Job::Immediate(response) => {
                requests.complete(ticket, Reply::Frames(vec![response_frame(&response)]));
            }
            Job::Execute { program } => {
                let work = request::execute(
                    Arc::clone(handler),
                    session_id.to_string(),
                    program,
                    principal.clone(),
                );
                requests.run(ticket, work.map(Reply::Frames).in_current_span());
            }
            Job::Subscribe { id, query } => {
                let started = std::time::Instant::now();
                let opening = handler
                    .session_manager()
                    .session_kg(&session_id.to_string())
                    .and_then(|kg| subscriptions.begin_subscribe(&kg, &id, &query));
                match opening {
                    Ok(opening) => {
                        let work = opening
                            .run()
                            .map(move |opened| Reply::Subscribed { opened, started });
                        requests.run(ticket, work.in_current_span());
                    }
                    Err(message) => {
                        let frame = response_frame(&request::error_response(message));
                        requests.complete(ticket, Reply::Frames(vec![frame]));
                    }
                }
            }
            Job::Unsubscribe { id } => {
                let response = match subscriptions.unsubscribe(&id) {
                    Ok(()) => request::message_rows(
                        vec!["message".to_string()],
                        vec![vec![serde_json::Value::String(format!(
                            "Unsubscribed '{id}'."
                        ))]],
                        std::time::Instant::now(),
                    ),
                    Err(message) => request::error_response(message),
                };
                requests.complete(ticket, Reply::Frames(vec![response_frame(&response)]));
            }
        }
    }
}

/// Apply a released request's effect on the connection; returns its frames.
fn release_reply(
    handler: &Handler,
    session_id: &str,
    released: Released<Reply>,
    subscriptions: &mut ConnectionSubscriptions,
) -> Vec<String> {
    let frames = match released.reply {
        Some(Reply::Frames(frames)) => frames,
        Some(Reply::Subscribed { opened, started }) => {
            let response = match subscriptions.finish_subscribe(opened) {
                Ok(snapshot) => request::message_rows(snapshot.columns, snapshot.inserted, started),
                Err(message) => request::error_response(message),
            };
            vec![response_frame(&response)]
        }
        None => vec![response_frame(&request::error_response(
            "Internal server error".to_string(),
        ))],
    };
    if released.access == Access::Exclusive {
        // Subscriptions are scoped to the connection's KG: switching drops them.
        match handler
            .session_manager()
            .session_kg(&session_id.to_string())
        {
            Ok(kg) => subscriptions.retain_knowledge_graph(&kg),
            Err(_) => subscriptions.clear(),
        }
    }
    frames
}

/// Write `frames` in order; `false` if the connection is dead.
async fn send_frames(sender: &mut Outbound, frames: Vec<String>) -> bool {
    for frame in frames {
        if sender.send(Message::Text(frame)).await.is_err() {
            return false;
        }
    }
    true
}

/// Send a subscription push; an oversized delta becomes a `subscription_error`.
/// Returns `false` if the connection is dead.
async fn send_subscription_push(sender: &mut Outbound, push: &Push) -> bool {
    let json = match serde_json::to_string(push) {
        Ok(json) if json.len() <= MAX_MESSAGE_SIZE => json,
        result => {
            let reason = match result {
                Ok(json) => format!(
                    "Delta too large ({} bytes, max {MAX_MESSAGE_SIZE}); narrow the query",
                    json.len()
                ),
                Err(e) => format!("Failed to serialize delta: {e}"),
            };
            warn!(%reason, "ws_subscription_push_failed");
            let subscription = match push {
                Push::SubscriptionDelta { subscription, .. }
                | Push::SubscriptionError { subscription, .. } => subscription.clone(),
            };
            let error = Push::SubscriptionError {
                subscription,
                message: reason,
            };
            serde_json::to_string(&error).unwrap_or_else(|_| {
                r#"{"type":"subscription_error","message":"Internal serialization error"}"#
                    .to_string()
            })
        }
    };
    sender.send(Message::Text(json)).await.is_ok()
}

/// Maximum characters of a program logged as a preview.
const LOG_PREVIEW_CHARS: usize = 80;

/// Credential-free log preview of a program: the first line, truncated to
/// [`LOG_PREVIEW_CHARS`] characters. `.user` and `.apikey` commands carry
/// secrets, so only their kind is kept.
fn log_preview(program: &str) -> String {
    const SECRET_COMMANDS: [&str; 2] = ["user", "apikey"];
    const SUBCOMMANDS: [&str; 6] = ["list", "create", "drop", "password", "role", "revoke"];
    for line in program.lines() {
        let Some(meta) = line.trim_start().strip_prefix('.') else {
            continue;
        };
        let mut words = meta.trim_start_matches('.').split_whitespace();
        let find = |word: Option<&str>, names: &[&'static str]| {
            word.and_then(|w| names.iter().copied().find(|n| w.eq_ignore_ascii_case(n)))
        };
        let Some(cmd) = find(words.next(), &SECRET_COMMANDS) else {
            continue;
        };
        return match find(words.next(), &SUBCOMMANDS) {
            Some(sub) => format!(".{cmd} {sub} <redacted>"),
            None => format!(".{cmd} <redacted>"),
        };
    }
    program
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .chars()
        .take(LOG_PREVIEW_CHARS)
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_log_preview_redacts_credentials() {
        assert_eq!(
            log_preview(".user create bob s3cret admin"),
            ".user create <redacted>"
        );
        assert_eq!(
            log_preview("  .user password bob n3w"),
            ".user password <redacted>"
        );
        assert_eq!(log_preview("..user bob s3cret"), ".user <redacted>");
        assert_eq!(
            log_preview("?edge(X, Y)\n.apikey create ci"),
            ".apikey create <redacted>"
        );
        assert_eq!(
            log_preview(".USER create bob s3cret admin"),
            ".user create <redacted>"
        );
        assert_eq!(
            log_preview(".User Password bob n3w"),
            ".user password <redacted>"
        );
        assert_eq!(
            log_preview(".ApiKey CREATE ci"),
            ".apikey create <redacted>"
        );
    }

    #[test]
    fn test_log_preview_first_line() {
        assert_eq!(log_preview("  ?edge(X, Y)  \n+edge(1, 2)"), "?edge(X, Y)");
        assert_eq!(log_preview(".users"), ".users");
        assert_eq!(log_preview(""), "");
    }

    #[test]
    fn test_log_preview_truncates_on_char_boundary() {
        let program = format!("{}é{}", "a".repeat(79), "b".repeat(19));
        assert_eq!(program.len(), 100);
        assert!(!program.is_char_boundary(80));
        let preview = log_preview(&program);
        assert_eq!(preview.chars().count(), LOG_PREVIEW_CHARS);
        assert!(preview.ends_with('é'));
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
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
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
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"switched_kg\":\"new_kg\""));
    }

    #[test]
    fn test_global_ws_response_error_serialize() {
        let resp = GlobalWsResponse::Error {
            message: "test error".to_string(),
            validation_errors: None,
            code: None,
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
            code: None,
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
            code: None,
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
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_start\""));
        assert!(json.contains("\"total_count\":10000"));
        assert!(json.contains("\"columns\":[\"x\",\"y\"]"));
        // Optional fields omitted when None
        assert!(!json.contains("metadata"));
        assert!(!json.contains("switched_kg"));
        assert!(!json.contains("proof_trees"));
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
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"type\":\"result_start\""));
        assert!(json.contains("\"truncated\":true"));
        assert!(json.contains("\"metadata\""));
        assert!(json.contains("\"switched_kg\":\"new_kg\""));
        assert!(!json.contains("proof_trees"));
    }

    #[test]
    fn test_global_ws_response_result_start_with_proof_trees() {
        use crate::provenance::proof_tree::*;
        let mut builder = ProofTreeBuilder::new();
        let fact_id = builder.insert(ProofNode {
            kind: NodeKind::Fact,
            conclusion: Conclusion {
                pred: "edge".into(),
                args: vec![crate::value::Value::Int32(1), crate::value::Value::Int32(2)],
            },
            rule_id: None,
            bindings: None,
            aggregate: None,
            negation: None,
            vector_search: None,
            truncated: None,
            why_not: None,
            source: None,
            children: vec![],
        });
        let graph = builder.finish(vec![fact_id]);

        let resp = GlobalWsResponse::ResultStart {
            columns: vec!["x".to_string()],
            total_count: 1,
            truncated: false,
            execution_time_ms: 5,
            metadata: None,
            switched_kg: None,
            proof_trees: Some(vec![graph]),
            timing_breakdown: None,
            errors: Vec::new(),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"proof_trees\""));
        assert!(json.contains("\"kind\":\"fact\""));
        assert!(json.contains("\"pred\":\"edge\""));
    }
}
