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
//! that credential, or its expiry, closes the connection, and every outbound
//! data frame is fenced by it (see the `outbound` module).
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
use futures_util::{FutureExt, StreamExt};
use inputlayer_ws_protocol::{
    probe_request_id, ErrorCode, NoticeCode, ServerFrame, Subscribed, PROTOCOL_VERSION,
};
use serde::Deserialize;
use tracing::{debug, info, warn, Instrument};

mod access;
mod auth;
mod execute;
mod framing;
mod in_flight;
mod outbound;
mod pipeline;
mod push;
mod replay;
mod request;
mod stream;

use access::KgReadAccess;
use in_flight::InFlight;
use outbound::{encode, Outbound};
use pipeline::{Access, Released, RequestPipeline, Startable};
use request::{Job, Reply, Request};

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
/// Any request may carry an `id` (a non-empty string of at most 64 bytes,
/// unique among the connection's unanswered requests), echoed on every frame
/// answering it:
/// ```json
/// {"type": "authenticate", "id": "1", "api_key": "..."}
/// {"type": "execute", "id": "2", "program": "?edge(X,Y)"}
/// {"type": "ping", "id": "3"}
/// ```
/// An `execute` may set `timeout_ms`; its one deadline covers queueing,
/// admission and computation (see [`crate::execution::RequestControl`]).
/// `{"type": "cancel", "id": "4", "target": "2"}` stops the unanswered request
/// `2` on arrival and is answered by `cancel_ack` with its `outcome`
/// (`cancelled`, `too_late` or `not_found`).
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
/// A malformed request, or one reusing the `id` of an unanswered request,
/// gets one `error` with code `invalid_request` (and its `id` when readable);
/// an over-rate one gets code `rate_limited`.
///
/// Requests may be pipelined. Replies come back in request order; programs
/// made only of queries overlap, and everything else runs alone, in order.
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
/// A result, snapshot or delta too large for one frame is streamed as one
/// logical payload that applies only at its end frame (`result_start` /
/// `result_chunk` / `result_end`, `subscription_delta_start` /
/// `subscription_delta_chunk` / `subscription_delta_end`). What cannot be
/// delivered whole is reported instead of any part of it: an `error` for a
/// reply, or a `subscription_reset` that ends the subscription. A client that
/// leaves a frame unread for `http.ws_send_timeout_ms` is disconnected.
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
    let send_timeout = std::time::Duration::from_millis(handler.config().http.ws_send_timeout_ms);
    let mut sender = Outbound::new(sink).with_send_timeout(send_timeout);

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
    let mut requests: Requests =
        RequestPipeline::new(handler.config().http.rate_limit.ws_max_in_flight_requests);
    let mut in_flight = InFlight::default();

    // Read access to the KG of each pushed frame, re-checked when ACLs change.
    let mut kg_access = KgReadAccess::default();

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
        let visible = |n: &Notification| {
            notification_visible(n, &session_kg, &principal)
                && kg_access.allows(&handler, &principal, n.knowledge_graph())
        };
        if !replay::send_replay(&mut sender, cursor.last_seq, replay, visible).await {
            if let Err(e) = handler.close_session(&session_id) {
                tracing::warn!(error = %e, "session_cleanup_failed");
            }
            notify_if_ended(&mut sender, &principal).await;
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

    // Fires once the connection's credential is revoked or expires.
    let mut credential_end = principal.end_signal();

    // Server-initiated heartbeat: send ping every 30s to detect dead connections
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat_interval.tick().await; // consume the immediate first tick

    loop {
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
            // Credential revoked or expired: stop everything this connection was doing
            () = &mut credential_end => {
                info!(credential = %principal.credential(), "ws_credential_ended");
                break;
            }
            // Idle timeout; a connection with requests in progress is not idle
            () = &mut idle_timer, if idle_duration.is_some() && requests.is_idle() => {
                info!(idle_ms, "ws_idle_timeout");
                sender.send_notice(NoticeCode::IdleTimeout, "Idle timeout".to_string()).await;
                break;
            }
            // Client message, read only while the pipeline has room
            msg = receiver.next(), if requests.has_capacity() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        touch(idle_timer.as_mut());
                        request_seq = request_seq.saturating_add(1);
                        let (access, request) = if rate.allow() {
                            Request::from_text(&text)
                        } else {
                            Request::immediate(ServerFrame::error(
                                probe_request_id(&text),
                                Some(ErrorCode::RateLimited),
                                format!("Rate limit exceeded ({} msgs/sec)", rate.max_per_sec()),
                            ))
                        };
                        let span = tracing::info_span!(
                            "ws_request",
                            request_id = request_seq,
                            msg_bytes = text.len()
                        );
                        admit(&handler, &mut requests, &mut in_flight, access, request, span);
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
                let id = in_flight.release(released.ticket);
                let frames = release_reply(&handler, &session_id, id, released, &mut subscriptions);
                if !send_frames(&mut sender, frames).await {
                    break;
                }
            }
            // A shared view published news for one of this connection's subscriptions
            subscriber = subscriptions.next_delivery() => {
                let push = subscriptions.deliver(subscriber, |kg| {
                    kg_access.allows(&handler, &principal, kg)
                });
                if let Some(push) = push {
                    if !push::deliver(&mut sender, &mut subscriptions, push).await {
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
                        if notification_visible(&notif, &session_kg, &principal)
                            && kg_access.allows(&handler, &principal, notif.knowledge_graph())
                            && !sender.send_frame(&ServerFrame::Notification(notif)).await
                        {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => {
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
        start_requests(
            &mut requests,
            &in_flight,
            &handler,
            &session_id,
            &principal,
            &mut subscriptions,
        );
    }

    // Abort reads in progress and stop their computation, let a started
    // write finish, then stop standing queries before anything else is sent.
    in_flight.cancel_reads();
    requests.shutdown().await;
    drop(subscriptions);
    notify_if_ended(&mut sender, &principal).await;
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

/// Tell the client its credential was revoked or expired, if it was.
async fn notify_if_ended(sender: &mut Outbound, principal: &Principal) {
    if let Some(ended) = principal.ended() {
        sender.send_credential_notice(ended).await;
    }
}

/// The connection's requests: each with its tracing span.
type Requests = RequestPipeline<(Request, tracing::Span), Reply>;

/// Admit `request` to the pipeline. One reusing the id of an unanswered
/// request is answered with `invalid_request` instead of running. An
/// `execute` gets its [`RequestControl`] here, so its deadline counts from
/// arrival; a `cancel` acts here, ahead of the pipeline, and only its
/// `cancel_ack` waits for its turn.
fn admit(
    handler: &Handler,
    requests: &mut Requests,
    in_flight: &mut InFlight,
    access: Access,
    request: Request,
    span: tracing::Span,
) {
    if let Job::Cancel { target } = &request.job {
        let outcome = in_flight.cancel(target);
        debug!(target = target.as_str(), ?outcome, "ws_cancel");
        let ack = ServerFrame::CancelAck {
            id: request.id,
            target: target.clone(),
            outcome,
        };
        let (access, ack) = Request::immediate(ack);
        requests.admit(access, (ack, span));
        return;
    }
    let immediate = matches!(request.job, Job::Immediate(_));
    let duplicate = request
        .id
        .as_ref()
        .filter(|id| !immediate && in_flight.contains(id));
    if let Some(id) = duplicate {
        let rejected = ServerFrame::error(
            Some(id.clone()),
            Some(ErrorCode::InvalidRequest),
            format!(
                "Request id '{}' is already in use by an unanswered request",
                id.as_str()
            ),
        );
        let (access, request) = Request::immediate(rejected);
        requests.admit(access, (request, span));
        return;
    }
    let control = match &request.job {
        Job::Execute { timeout_ms, .. } => Some(handler.request_control(*timeout_ms)),
        _ => None,
    };
    let id = request.id.clone();
    let ticket = requests.admit(access, (request, span));
    if !immediate {
        in_flight.insert(ticket, id, access, control);
    }
}

/// Start every request the pipeline's ordering barriers now allow. Work that
/// computes runs as a pipeline future; connection-state changes happen here, on
/// the loop that owns that state.
fn start_requests(
    requests: &mut Requests,
    in_flight: &InFlight,
    handler: &Arc<Handler>,
    session_id: &str,
    principal: &Principal,
    subscriptions: &mut ConnectionSubscriptions,
) {
    while let Some(Startable {
        ticket,
        job: (Request { id, job }, span),
    }) = requests.next_startable()
    {
        let _entered = span.enter();
        match job {
            Job::Immediate(frame) => {
                requests.complete(ticket, Reply::Frames(vec![encode(&frame)]));
            }
            Job::Execute { program, .. } => {
                let control = match in_flight.control(ticket) {
                    Some(control) => Arc::clone(control),
                    None => handler.request_control(None),
                };
                // Stopped while queued behind the barrier: it never runs.
                if control.expire_if_due() {
                    let stop = control
                        .stopped()
                        .unwrap_or(crate::execution::Stop::Deadline);
                    let error = crate::protocol::handler::stop_error(stop);
                    let frame = ServerFrame::error(id, error.code, error.message);
                    requests.complete(ticket, Reply::Frames(vec![encode(&frame)]));
                    continue;
                }
                let work = execute::execute(
                    Arc::clone(handler),
                    session_id.to_string(),
                    id,
                    program,
                    principal.clone(),
                    control,
                );
                requests.run(ticket, work.map(Reply::Frames).in_current_span());
            }
            Job::Cancel { .. } => {
                // Handled on arrival by `admit`; never queued.
                requests.complete(ticket, Reply::Frames(Vec::new()));
            }
            Job::Subscribe { name, query } => {
                let started = std::time::Instant::now();
                let opening = handler
                    .session_manager()
                    .session_kg(&session_id.to_string())
                    .and_then(|kg| subscriptions.begin_subscribe(&kg, &name, &query));
                match opening {
                    Ok(opening) => {
                        let work = opening.run().map(move |opened| Reply::Subscribed {
                            name,
                            opened,
                            started,
                        });
                        requests.run(ticket, work.in_current_span());
                    }
                    Err(message) => {
                        let frame = encode(&ServerFrame::error(id, None, message));
                        requests.complete(ticket, Reply::Frames(vec![frame]));
                    }
                }
            }
            Job::Unsubscribe { name } => {
                let frame = match subscriptions.unsubscribe(&name) {
                    Ok(()) => ServerFrame::Result(execute::subscription_reply(
                        id,
                        vec!["message".to_string()],
                        vec![vec![serde_json::Value::String(format!(
                            "Unsubscribed '{name}'."
                        ))]],
                        None,
                        std::time::Instant::now(),
                    )),
                    Err(message) => ServerFrame::error(id, None, message),
                };
                requests.complete(ticket, Reply::Frames(vec![encode(&frame)]));
            }
        }
    }
}

/// Apply a released request's effect on the connection; returns its frames.
/// `id` is the request's id, for replies built here.
fn release_reply(
    handler: &Handler,
    session_id: &str,
    id: Option<inputlayer_ws_protocol::RequestId>,
    released: Released<Reply>,
    subscriptions: &mut ConnectionSubscriptions,
) -> Vec<String> {
    let frames = match released.reply {
        Some(Reply::Frames(frames)) => frames,
        Some(Reply::Subscribed {
            name: subscription,
            opened,
            started,
        }) => match subscriptions.finish_subscribe(opened) {
            Ok((snapshot, generation)) => {
                let reply = execute::subscription_reply(
                    id.clone(),
                    snapshot.columns,
                    snapshot.rows,
                    Some(Subscribed {
                        subscription: subscription.clone(),
                        generation,
                        revision: snapshot.revision,
                    }),
                    started,
                );
                stream::result_frames(reply).unwrap_or_else(|reason| {
                    // A snapshot that cannot be delivered whole registers
                    // nothing; no push for it was sent yet.
                    subscriptions.reset(&subscription, generation);
                    vec![encode(&ServerFrame::error(
                        id,
                        Some(ErrorCode::Internal),
                        reason,
                    ))]
                })
            }
            Err(message) => vec![encode(&ServerFrame::error(id, None, message))],
        },
        None => vec![encode(&ServerFrame::error(
            id,
            Some(ErrorCode::Internal),
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
