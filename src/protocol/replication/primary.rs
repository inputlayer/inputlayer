//! The primary's side: `GET /v1/replication/stream` and
//! `GET /v1/replication/status`.

use super::{
    encode_frame, token_matches, FollowerMessage, PrimaryMessage, StartMode, StatusReport,
    FRAME_BYTES,
};
use crate::config::ReplicationRole;
use crate::protocol::rest::ClientIp;
use crate::protocol::Handler;
use crate::replication::resync::{checkpoint_lines, CHUNK_TUPLES};
use crate::replication::{Read, ReplicationLog};
use crate::storage::StorageError;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{info, warn};

/// How long a follower has to say hello after the upgrade.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

/// Shortest time a send may block, with nothing heard from the follower,
/// before the follower counts as stalled.
const MIN_SEND_TIMEOUT: Duration = Duration::from_secs(10);

/// `GET /v1/replication/status`: this server's replication state.
pub async fn status(Extension(handler): Extension<Arc<Handler>>) -> Json<StatusReport> {
    let log = handler.get_storage().replication_log().cloned();
    Json(handler.replication_status().report(log.as_deref()))
}

/// `GET /v1/replication/stream`: a follower's stream. Only a primary serves
/// it, only to a client presenting `replication.token`.
pub async fn stream(
    Extension(handler): Extension<Arc<Handler>>,
    client_ip: Option<Extension<ClientIp>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let config = &handler.config().replication;
    if config.role != ReplicationRole::Primary {
        return (
            StatusCode::CONFLICT,
            "this server is not a replication primary\n",
        )
            .into_response();
    }
    let presented = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let authorized = match (config.token.as_deref(), presented) {
        (Some(expected), Some(presented)) => token_matches(expected, presented),
        _ => false,
    };
    if !authorized {
        warn!("replication_stream_unauthorized");
        return (StatusCode::UNAUTHORIZED, "invalid replication token\n").into_response();
    }
    let Some(log) = handler.get_storage().replication_log().cloned() else {
        return (StatusCode::CONFLICT, "replication log not attached\n").into_response();
    };
    let addr = client_ip.map_or_else(String::new, |Extension(ClientIp(ip))| ip.to_string());
    ws.max_message_size(64 * 1024)
        .on_upgrade(move |socket| async move {
            serve(handler, log, socket, addr).await;
        })
        .into_response()
}

type Sink = Outbound<SplitSink<WebSocket, Message>>;

/// A follower's stream, and when the follower was last heard from.
pub(super) struct Outbound<S> {
    pub(super) sink: S,
    pub(super) heard: Arc<Mutex<Instant>>,
}

/// Why a stream ended.
#[derive(Debug)]
enum End {
    /// The follower went away or stopped reading.
    Gone(String),
    /// The follower fell behind the retained log; it reconnects and resyncs.
    Behind,
    /// Changes made while the checkpoint was sent outgrew the log's pin cap;
    /// the follower retries the resync after a backoff.
    PinOverflowed,
}

async fn serve(handler: Arc<Handler>, log: Arc<ReplicationLog>, socket: WebSocket, addr: String) {
    let (sink, mut incoming) = socket.split();
    let heard = Arc::new(Mutex::new(Instant::now()));
    let mut sink = Outbound {
        sink,
        heard: Arc::clone(&heard),
    };
    let hello = match tokio::time::timeout(HELLO_TIMEOUT, incoming.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => serde_json::from_str::<FollowerMessage>(&text),
        _ => {
            warn!(%addr, "replication_stream_no_hello");
            return;
        }
    };
    let Ok(FollowerMessage::Hello {
        name,
        stream_id,
        lsn,
    }) = hello
    else {
        warn!(%addr, "replication_stream_bad_hello");
        return;
    };
    let resync = !log.can_serve(stream_id, lsn);
    let status = handler.replication_status();
    let id = status.follower_connected(&name, &addr, resync);
    if !resync {
        // A tailing follower already holds everything up to its hello's LSN.
        status.follower_acked(id, lsn);
    }
    info!(follower = %name, %addr, stream_id, lsn, resync, "replication_follower_connected");

    // Acks arrive while events go out: read them on their own task.
    let (gone_tx, mut gone) = watch::channel(false);
    let reader_status = Arc::clone(&handler);
    let reader = tokio::spawn(async move {
        while let Some(Ok(message)) = incoming.next().await {
            // Acks, and the pings of a follower busy applying.
            *heard.lock() = Instant::now();
            match message {
                Message::Text(text) => {
                    if let Ok(FollowerMessage::Ack { lsn }) = serde_json::from_str(&text) {
                        reader_status.replication_status().follower_acked(id, lsn);
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        let _ = gone_tx.send(true);
    });

    let send_timeout =
        Duration::from_millis(handler.config().replication.timeout_ms).max(MIN_SEND_TIMEOUT);
    let ended = stream_to(
        &handler,
        &log,
        &mut sink,
        &mut gone,
        resync,
        lsn,
        id,
        send_timeout,
    )
    .await;
    match &ended {
        End::Behind => {
            warn!(follower = %name, "replication_follower_fell_behind_retained_log");
            let message = PrimaryMessage::Error {
                message: "fell behind the primary's retained log; reconnect to resync".into(),
            };
            let _ = send_json(&mut sink, &message, send_timeout).await;
        }
        End::PinOverflowed => {
            status.resync_pin_overflowed();
            warn!(
                follower = %name,
                pin_cap_bytes = log.pin_cap(),
                "replication_resync_pin_overflowed"
            );
            let message = PrimaryMessage::Error {
                message: format!(
                    "the primary's changes during the checkpoint outgrew {} bytes; \
                     the resync is retried",
                    log.pin_cap()
                ),
            };
            let _ = send_json(&mut sink, &message, send_timeout).await;
        }
        End::Gone(reason) => info!(follower = %name, reason, "replication_follower_disconnected"),
    }
    let _ = sink.sink.close().await;
    reader.abort();
    status.follower_gone(id);
}

#[allow(clippy::too_many_arguments)]
async fn stream_to(
    handler: &Arc<Handler>,
    log: &Arc<ReplicationLog>,
    sink: &mut Sink,
    gone: &mut watch::Receiver<bool>,
    resync: bool,
    lsn: u64,
    id: u64,
    send_timeout: Duration,
) -> End {
    let mut head_changed = log.watch_head();
    let start = PrimaryMessage::Start {
        stream_id: log.stream_id(),
        mode: if resync {
            StartMode::Resync
        } else {
            StartMode::Tail
        },
        head: log.head(),
    };
    if let Err(e) = send_json(sink, &start, send_timeout).await {
        return End::Gone(e);
    }
    // Keep the events the follower tails after the checkpoint until it has
    // caught up with them.
    let mut pin = resync.then(|| log.pin_head());
    let mut cursor = if resync {
        match send_checkpoint(handler, log, sink, send_timeout).await {
            Ok(head) => head,
            Err(e) => return End::Gone(e),
        }
    } else {
        lsn
    };
    let status = handler.replication_status();
    status.follower_sent(id, cursor);
    let heartbeat = Duration::from_millis(handler.config().replication.heartbeat_ms);
    loop {
        if *gone.borrow() {
            return End::Gone("connection closed".into());
        }
        match log.read_after(cursor, FRAME_BYTES) {
            Read::Events { first, lines } => {
                let last = first + lines.len() as u64 - 1;
                let frame = encode_frame(first, log.head(), &lines);
                if let Err(e) = send(sink, Message::Binary(frame), send_timeout).await {
                    return End::Gone(e);
                }
                cursor = last;
                status.follower_sent(id, cursor);
                if let Some(pin) = &mut pin {
                    pin.advance(cursor);
                }
            }
            Read::UpToDate => {
                pin = None;
                head_changed.mark_unchanged();
                if log.head() != cursor {
                    continue;
                }
                status.follower_drained(id, cursor);
                tokio::select! {
                    _ = head_changed.changed() => {}
                    _ = gone.changed() => {}
                    () = tokio::time::sleep(heartbeat) => {
                        let beat = PrimaryMessage::Heartbeat { head: log.head() };
                        if let Err(e) = send_json(sink, &beat, send_timeout).await {
                            return End::Gone(e);
                        }
                    }
                }
            }
            Read::Unavailable if pin.is_some() => return End::PinOverflowed,
            Read::Unavailable => return End::Behind,
        }
    }
}

/// Capture a checkpoint and send it as checkpoint lines; returns the LSN it
/// holds the events up to.
async fn send_checkpoint(
    handler: &Arc<Handler>,
    log: &ReplicationLog,
    sink: &mut Sink,
    send_timeout: Duration,
) -> Result<u64, String> {
    let (frames_tx, mut frames) = mpsc::channel::<Vec<u8>>(4);
    let (head_tx, mut head_rx) = tokio::sync::oneshot::channel();
    let producer_handler = Arc::clone(handler);
    let producer = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let (checkpoint, head) = producer_handler
            .get_storage()
            .capture_checkpoint_at_lsn()
            .map_err(|e| format!("checkpoint capture failed: {e}"))?;
        let _ = head_tx.send((head, checkpoint.revision));
        let empty = || encode_frame(0, 0, std::iter::empty::<&[u8]>());
        let mut frame = empty();
        let result = checkpoint_lines::<Closed>(&checkpoint, head, CHUNK_TUPLES, |line| {
            frame.extend_from_slice(&line);
            if frame.len() >= FRAME_BYTES {
                let full = std::mem::replace(&mut frame, empty());
                frames_tx
                    .blocking_send(full)
                    .map_err(|_| Closed::Receiver)?;
            }
            Ok(())
        });
        match result {
            Ok(()) => {
                if frame.len() > super::FRAME_HEADER {
                    frames_tx
                        .blocking_send(frame)
                        .map_err(|_| "sender closed".to_string())?;
                }
                Ok(())
            }
            Err(Closed::Receiver) => Err("follower went away during checkpoint".into()),
            Err(Closed::Storage(e)) => Err(e.to_string()),
        }
    });
    // Capturing a large state takes a while: keep the follower hearing from
    // the primary meanwhile.
    let heartbeat = Duration::from_millis(handler.config().replication.heartbeat_ms);
    let captured = loop {
        tokio::select! {
            captured = &mut head_rx => break captured,
            () = tokio::time::sleep(heartbeat) => {
                let beat = PrimaryMessage::Heartbeat { head: log.head() };
                send_json(sink, &beat, send_timeout).await?;
            }
        }
    };
    let Ok((head, revision)) = captured else {
        // The capture failed before it had a head: report why.
        producer
            .await
            .map_err(|e| format!("checkpoint task failed: {e}"))??;
        return Err("checkpoint capture failed".into());
    };
    info!(head, revision, "replication_checkpoint_sending");
    while let Some(frame) = frames.recv().await {
        send(sink, Message::Binary(frame), send_timeout).await?;
    }
    producer
        .await
        .map_err(|e| format!("checkpoint task failed: {e}"))??;
    Ok(head)
}

/// Why checkpoint line production stopped.
enum Closed {
    Receiver,
    Storage(StorageError),
}

impl From<StorageError> for Closed {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}

async fn send_json(
    sink: &mut Sink,
    message: &PrimaryMessage,
    timeout: Duration,
) -> Result<(), String> {
    let text = serde_json::to_string(message).map_err(|e| e.to_string())?;
    send(sink, Message::Text(text), timeout).await
}

/// Send `message`. A send blocks while the follower is not reading, which a
/// follower applying a large graph does once its receive buffer is full; it
/// keeps pinging meanwhile. The send fails once it has been blocked, and the
/// follower silent, for `timeout`.
pub(super) async fn send<S, M>(
    sink: &mut Outbound<S>,
    message: M,
    timeout: Duration,
) -> Result<(), String>
where
    S: futures_util::Sink<M> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut sending = sink.sink.send(message);
    let mut deadline = Instant::now() + timeout;
    loop {
        match tokio::time::timeout_at(deadline, &mut sending).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => {
                let alive_until = *sink.heard.lock() + timeout;
                if alive_until <= Instant::now() {
                    return Err(format!(
                        "follower stopped reading and was silent for {} ms",
                        timeout.as_millis()
                    ));
                }
                deadline = alive_until;
            }
        }
    }
}
