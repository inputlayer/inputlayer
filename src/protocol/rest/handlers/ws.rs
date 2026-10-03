//! WebSocket Handler
//!
//! The global `/ws` endpoint: authenticate, then send `execute` messages
//! carrying IQL. Each connection owns an auto-managed session and receives
//! push notifications for its knowledge graph. Standing queries
//! (`.subscribe` / `.unsubscribe`) push `subscription_delta` and
//! `subscription_error`; see [`crate::protocol::subscription`].
//!
//! Frames are the types of [`inputlayer_ws_protocol`]: every reply echoes the
//! `id` of the request it answers, and unsolicited frames (notifications,
//! subscription pushes, notices) never carry one.
//!
//! A connection is bound to the [`Principal`] it authenticated as. Revoking
//! that credential closes the connection, and every outbound data frame is
//! fenced by it (see the `outbound` module).

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
use inputlayer_ws_protocol::{
    probe_request_id, ClientFrame, ErrorCode, NoticeCode, ServerFrame, SubscriptionPush,
    PROTOCOL_VERSION,
};
use serde::Deserialize;
use tracing::{debug, info, warn, Instrument};

mod auth;
mod execute;
mod outbound;
mod replay;

use outbound::Outbound;

use crate::auth::{Principal, Role, INTERNAL_KG};
use crate::protocol::handler::Notification;
use crate::protocol::notification_log::{Cursor, Resumed};
use crate::protocol::rest::error::RestError;
use crate::protocol::rest::{ClientIp, PreAuthSlots, WsSemaphore};
use crate::protocol::subscription::ConnectionSubscriptions;
use crate::protocol::Handler;
use crate::protocol::MAX_MESSAGE_SIZE;

// =============================================================================
// Global WebSocket Endpoint (/ws)
//
// Auto-session lifecycle: connect → authenticate → server creates session →
// all commands via execute → disconnect closes session.
// =============================================================================

/// Query parameters for the global WebSocket connection
#[derive(Debug, Deserialize)]
pub struct WsConnectParams {
    /// Knowledge graph to bind to (defaults to "default")
    #[serde(default = "default_kg")]
    pub kg: String,
    /// Last notification `seq` the client saw. With the matching `epoch`, the
    /// server replays the retained notifications after it, or sends a
    /// `replay_gap` notice when they are not the complete history since it.
    #[serde(default)]
    pub last_seq: Option<u64>,
    /// The `stream_epoch` (from `authenticated`) that `last_seq` belongs to.
    #[serde(default)]
    pub epoch: Option<String>,
}

fn default_kg() -> String {
    "default".to_string()
}

/// Global WebSocket endpoint with auto-session lifecycle.
///
/// Connect to `/ws?kg=<name>` (defaults to "default"), then authenticate; the
/// server creates a session bound to that knowledge graph and closes it on
/// disconnect. The full protocol is `docs/spec/asyncapi.yaml`; the frame
/// types are [`inputlayer_ws_protocol`].
///
/// A reconnecting client may add `&last_seq=<seq>&epoch=<stream_epoch>`: the
/// server replays the retained notifications after that cursor before live
/// ones, or sends one `replay_gap` notice when it cannot (another engine run,
/// or notifications evicted from the ring).
///
/// ## Client → Server
///
/// Any request may carry an `id` (a non-empty string of at most 64 bytes),
/// echoed on every frame answering it:
/// ```json
/// {"type": "authenticate", "id": "1", "api_key": "..."}
/// {"type": "execute", "id": "2", "program": "?edge(X,Y)"}
/// {"type": "ping", "id": "3"}
/// ```
///
/// ## Server → Client
///
/// **Replies**, each with the request's `id`: `authenticated` (with
/// `protocol_version` and `stream_epoch`), `auth_error`, `result`, the streamed
/// `result_start` / `result_chunk` / `result_end`, `error` and `pong`:
/// ```json
/// {"type": "result", "id": "2", "columns": ["col0", "col1"], "rows": [[1, 2]],
///  "row_count": 1, "total_count": 1, "truncated": false, "execution_time_ms": 5, "errors": []}
/// {"type": "error", "id": "2", "message": "...", "code": "not_found"}
/// ```
/// A malformed request gets one `error` with code `invalid_request` (and its
/// `id` when readable); an over-rate one gets code `rate_limited`.
///
/// **Pushes**, never with an `id`: data changes in the connection's KG,
/// ```json
/// {"type": "persistent_update", "knowledge_graph": "default", "relation": "edge",
///  "operation": "insert", "count": 5, "timestamp_ms": 1700000000000, "seq": 42}
/// ```
/// and standing-query results. `.subscribe <name> ?<query>` replies with a
/// `result` holding the snapshot and
/// `"subscribed": {"subscription", "generation", "revision"}`; afterwards the
/// server pushes deltas numbered from 1, each naming the higher revision it
/// brings the result to:
/// ```json
/// {"type": "subscription_delta", "subscription": "<name>", "generation": 1,
///  "knowledge_graph": "default", "seq": 1, "revision": 17, "columns": ["X"],
///  "inserted": [[3]], "retracted": []}
/// {"type": "subscription_error", "subscription": "<name>", "generation": 1, "message": "..."}
/// ```
/// `.unsubscribe <name>` removes one; disconnecting or switching KG removes all.
///
/// **Notices**, never with an `id`: connection events, most of them followed
/// by the server closing the connection:
/// ```json
/// {"type": "notice", "code": "idle_timeout", "message": "Idle timeout"}
/// ```
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
            let cursor = params.last_seq.map(|last_seq| Cursor {
                epoch: params.epoch,
                last_seq,
            });
            async move {
                handle_global_ws_connection(socket, handler, params.kg, cursor, peer, preauth_slot)
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

    fn max_per_sec(&self) -> u32 {
        self.max_per_sec
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

/// Whether a connection bound to `session_kg` may see `notification`: changes
/// to its own KG, plus KG creation and drop for admins. Changes to the
/// internal KG are never visible.
fn notification_visible(
    notification: &Notification,
    session_kg: &str,
    principal: &Principal,
) -> bool {
    let kg = notification.knowledge_graph();
    if kg == INTERNAL_KG {
        return false;
    }
    kg == session_kg
        || (matches!(notification, Notification::KgChange { .. })
            && principal.role() == Ok(Role::Admin))
}

/// Handle a global WebSocket connection: authentication, then the message loop.
async fn handle_global_ws_connection(
    socket: WebSocket,
    handler: Arc<Handler>,
    kg: String,
    cursor: Option<Cursor>,
    peer: IpAddr,
    preauth_slot: crate::protocol::rest::PreAuthSlot,
) {
    let (sink, mut receiver) = socket.split();
    let mut sender = Outbound::new(sink);

    info!(kg = %kg, "ws_connection_start");

    // Per-connection message rate limiting, auth phase included
    let mut rate = MessageRate::new(handler.config().http.rate_limit.ws_max_messages_per_sec);

    let Some(authenticated) =
        auth::authenticate(&handler, &mut receiver, &mut sender, &mut rate, peer).await
    else {
        let _ = sender.send(Message::Close(None)).await;
        return;
    };
    drop(preauth_slot);
    let auth_request = authenticated.request;
    let principal = authenticated.principal;
    sender.bind(principal.clone());

    // ── Authenticated: create session ────────────────────────────────────
    let Ok(auth_identity) = principal.identity() else {
        auth::auth_error(&mut sender, auth_request, "Invalid credentials".to_string()).await;
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
            auth::auth_error(&mut sender, auth_request, e).await;
            sender.close().await;
            return;
        }
    };

    let authenticated = ServerFrame::Authenticated {
        id: auth_request,
        session_id: session_id.clone(),
        knowledge_graph: kg,
        version: env!("CARGO_PKG_VERSION").to_string(),
        role: auth_identity.role.to_string(),
        protocol_version: PROTOCOL_VERSION,
        stream_epoch: handler.notifications().epoch().to_string(),
    };
    if !sender.send_frame(&authenticated).await {
        if let Err(e) = handler.close_session(&session_id) {
            tracing::warn!(error = %e, "session_cleanup_failed");
        }
        let _ = sender.send(Message::Close(None)).await;
        return;
    }

    let mut request_seq: u64 = 0;
    let mut subscriptions =
        ConnectionSubscriptions::new(Arc::clone(&handler), Some(principal.clone()));

    // Missed notifications on reconnect, then live ones without overlap.
    let Resumed {
        replay,
        live: mut notify_rx,
    } = handler.notifications().resume(cursor.as_ref());
    if let Some(cursor) = &cursor {
        let session_kg = handler
            .session_manager()
            .with_session(&session_id, |s| s.knowledge_graph.clone())
            .unwrap_or_default();
        let visible = |n: &Notification| notification_visible(n, &session_kg, &principal);
        if !replay::send_replay(&mut sender, cursor.last_seq, replay, visible).await {
            if let Err(e) = handler.close_session(&session_id) {
                tracing::warn!(error = %e, "session_cleanup_failed");
            }
            notify_if_revoked(&mut sender, &principal).await;
            return;
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

    // Fires once the connection's credential is revoked.
    let mut revocation = principal.revocation();

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
                info!(max_lifetime_secs, "ws_max_lifetime_exceeded");
                let message = format!("Connection lifetime exceeded ({max_lifetime_secs}s)");
                sender
                    .send_notice(NoticeCode::LifetimeExceeded, message)
                    .await;
                break;
            }
        }

        tokio::select! {
            // Credential revoked: stop everything this connection was doing
            () = &mut revocation => {
                info!(credential = %principal.credential(), "ws_credential_revoked");
                break;
            }
            // Idle timeout
            () = idle_sleep => {
                if idle_duration.is_some() {
                    info!(idle_ms, "ws_idle_timeout");
                    sender.send_notice(NoticeCode::IdleTimeout, "Idle timeout".to_string()).await;
                    break;
                }
            }
            // Client message
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        last_activity = std::time::Instant::now();
                        request_seq = request_seq.saturating_add(1);

                        if !rate.allow() {
                            let rejected = ServerFrame::error(
                                probe_request_id(&text),
                                Some(ErrorCode::RateLimited),
                                format!("Rate limit exceeded ({} msgs/sec)", rate.max_per_sec()),
                            );
                            if !sender.send_frame(&rejected).await {
                                break;
                            }
                            continue;
                        }
                        let span = tracing::info_span!(
                            "ws_request",
                            request_id = request_seq,
                            msg_bytes = text.len()
                        );
                        let send_ok = handle_request(
                            &handler, &session_id, &text, &principal, &mut sender,
                            &mut subscriptions,
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
            // Standing-query evaluation finished
            completion = subscriptions.next_completion() => {
                if let Some(push) = subscriptions.on_completion(completion) {
                    if !send_subscription_push(&mut sender, push).await {
                        break;
                    }
                }
            }
            // Push notification
            notification = notify_rx.recv() => {
                match notification {
                    Ok(notif) => {
                        let session_kg = match handler
                            .session_manager()
                            .with_session(&session_id, |s| s.knowledge_graph.clone())
                        {
                            Ok(kg) => kg,
                            Err(_) => break,
                        };
                        subscriptions.on_notification(&notif);
                        if notification_visible(&notif, &session_kg, &principal)
                            && !sender.send_frame(&ServerFrame::Notification(notif)).await
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
                        subscriptions.on_missed_notifications();
                        total_lagged += count;
                        if total_lagged > max_lag {
                            warn!(total_lagged, max_lag, "ws_slow_subscriber_disconnected");
                            let message = format!("Disconnected: missed {total_lagged} total notification(s)");
                            sender.send_notice(NoticeCode::SlowConsumer, message).await;
                            break;
                        }
                        let message = format!("Missed {count} notification(s) due to backpressure");
                        if !sender.send_notice(NoticeCode::NotificationsMissed, message).await {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        sender
                            .send_notice(NoticeCode::ServerShutdown, "Server shutting down".to_string())
                            .await;
                        break;
                    }
                }
            }
        }
    }

    // Stop standing queries before anything else is sent.
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

/// Tell the client its credential was revoked, if it was.
async fn notify_if_revoked(sender: &mut Outbound, principal: &Principal) {
    if principal.is_revoked() {
        sender.send_revocation_notice().await;
    }
}

/// Answer one request of an authenticated connection. Returns `true` if the
/// connection is still alive, `false` if it should close.
async fn handle_request(
    handler: &Arc<Handler>,
    session_id: &str,
    text: &str,
    auth: &Principal,
    sender: &mut Outbound,
    subscriptions: &mut ConnectionSubscriptions,
) -> bool {
    let request = match serde_json::from_str::<ClientFrame>(text) {
        Ok(request) => request,
        Err(e) => {
            debug!(error = %e, "ws_invalid_request");
            let rejected = ServerFrame::error(
                probe_request_id(text),
                Some(ErrorCode::InvalidRequest),
                format!("Invalid message format: {e}"),
            );
            return sender.send_frame(&rejected).await;
        }
    };
    match request {
        ClientFrame::Execute { id, program } => {
            execute::execute(
                handler,
                session_id,
                id,
                program,
                auth,
                sender,
                subscriptions,
            )
            .await
        }
        ClientFrame::Ping { id } => sender.send_frame(&ServerFrame::Pong { id }).await,
        ClientFrame::Login { id, .. } | ClientFrame::Authenticate { id, .. } => {
            let rejected = ServerFrame::error(
                id,
                Some(ErrorCode::InvalidRequest),
                "Already authenticated".to_string(),
            );
            sender.send_frame(&rejected).await
        }
    }
}

/// Send a subscription push; one that cannot be sent becomes a
/// `subscription_error` for the same subscription. Returns `false` if the
/// connection is dead.
async fn send_subscription_push(sender: &mut Outbound, push: SubscriptionPush) -> bool {
    let frame = ServerFrame::Subscription(push);
    let json = match serde_json::to_string(&frame) {
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
            let ServerFrame::Subscription(push) = &frame else {
                unreachable!("constructed as a subscription push above");
            };
            let (subscription, generation) = push.subscription();
            let error = ServerFrame::Subscription(SubscriptionPush::SubscriptionError {
                subscription: subscription.to_string(),
                generation,
                message: reason,
            });
            return sender.send_frame(&error).await;
        }
    };
    sender.send(Message::Text(json)).await.is_ok()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_ws_connect_params_default() {
        let params: WsConnectParams = serde_json::from_str("{}").unwrap();
        assert_eq!(params.kg, "default");
        assert_eq!(params.last_seq, None);
        assert_eq!(params.epoch, None);
    }

    #[test]
    fn test_ws_connect_params_custom_kg_and_cursor() {
        let params: WsConnectParams =
            serde_json::from_str(r#"{"kg": "test", "last_seq": 42, "epoch": "00ff"}"#).unwrap();
        assert_eq!(params.kg, "test");
        assert_eq!(params.last_seq, Some(42));
        assert_eq!(params.epoch.as_deref(), Some("00ff"));
    }
}
