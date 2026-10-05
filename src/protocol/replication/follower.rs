//! The follower's side: a task that keeps applying the primary's stream.
//!
//! It connects, says where it is, and applies what arrives: a checkpoint
//! (each graph reconciled, graphs the primary lacks dropped) and then events
//! in LSN order. After each applied frame it saves its position and acks.
//! Silence longer than `replication.timeout_ms`, a gap in LSNs, or any error
//! ends the connection; it reconnects with backoff. A failed apply or a gap
//! clears the position, so the next connection resyncs from a checkpoint.
//! Consecutive resyncs that end before their checkpoint is applied back off
//! further, up to `MAX_RESYNC_BACKOFF`, so a resync that cannot finish does
//! not cost the primary a checkpoint every few seconds.

use super::{
    decode_frame, FollowerMessage, FollowerState, PrimaryMessage, StartMode, MAX_FRAME_BYTES,
};
use crate::config::ReplicationConfig;
use crate::protocol::Handler;
use crate::replication::event::{decode_line, split_lines};
use crate::replication::position::position_path;
use crate::replication::{Event, Line, Position, ResyncMark};
use crate::storage_engine::{GraphEvent, GraphState, ReplicaChange};
use futures_util::{FutureExt, SinkExt, StreamExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

const MIN_BACKOFF: Duration = Duration::from_millis(100);
/// Bytes of already-received events applied as one batch.
const BATCH_BYTES: usize = 8 * 1024 * 1024;
const MAX_BACKOFF: Duration = Duration::from_secs(2);
const MIN_RESYNC_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RESYNC_BACKOFF: Duration = Duration::from_secs(60);

/// Start the follower task (a no-op future when this is not a follower).
pub fn spawn(handler: Arc<Handler>) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !handler.get_storage().is_replica() {
            return;
        }
        run(handler).await;
    })
}

async fn run(handler: Arc<Handler>) {
    let config = handler.config().replication.clone();
    let path = position_path(&handler.config().storage.data_dir);
    let mut follower = Follower {
        position: match Position::load(&path) {
            Ok(position) => position,
            Err(e) => {
                warn!(error = %e, "replication_position_unreadable_resyncing");
                Position::default()
            }
        },
        path,
        handler,
        streamed: false,
        resyncing: false,
    };
    info!(
        primary = config.primary_url.as_deref().unwrap_or(""),
        stream_id = follower.position.stream_id,
        lsn = follower.position.lsn,
        "replication_follower_starting"
    );
    let handler = Arc::clone(&follower.handler);
    let status = handler.replication_status();
    let mut backoff = MIN_BACKOFF;
    let mut resync_backoff = Duration::ZERO;
    loop {
        status.set_state(FollowerState::Connecting);
        let result = follower.session(&config).await;
        let streamed = std::mem::take(&mut follower.streamed);
        let resync_failed = std::mem::take(&mut follower.resyncing);
        if let Err(e) = result {
            warn!(error = %e, "replication_follower_disconnected");
            status.failed(&e);
        }
        backoff = if streamed {
            MIN_BACKOFF
        } else {
            (backoff * 2).min(MAX_BACKOFF)
        };
        if resync_failed {
            status.resync_failed();
            resync_backoff = (resync_backoff * 2).clamp(MIN_RESYNC_BACKOFF, MAX_RESYNC_BACKOFF);
            warn!(
                retry_ms = u64::try_from(resync_backoff.as_millis()).unwrap_or(u64::MAX),
                "replication_resync_failed"
            );
        } else if streamed {
            resync_backoff = Duration::ZERO;
        }
        tokio::time::sleep(backoff.max(resync_backoff)).await;
    }
}

struct Follower {
    handler: Arc<Handler>,
    path: PathBuf,
    position: Position,
    /// The last session applied something (resets the backoff).
    streamed: bool,
    /// The last session began a resync and has not streamed past its
    /// checkpoint yet.
    resyncing: bool,
}

/// A checkpoint being received.
#[derive(Default)]
struct Resync {
    revision: u64,
    head: u64,
    graphs: Vec<String>,
    current: Option<(String, GraphState)>,
}

impl Follower {
    async fn session(&mut self, config: &ReplicationConfig) -> Result<(), String> {
        let timeout = Duration::from_millis(config.timeout_ms);
        let base = config.primary_url.as_deref().unwrap_or_default();
        let url = format!(
            "{}/v1{}",
            base.trim_end_matches('/').replacen("http://", "ws://", 1),
            super::STREAM_PATH
        );
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|e| format!("bad primary_url {url}: {e}"))?;
        let token = config.token.as_deref().unwrap_or_default();
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .map_err(|_| "replication.token is not a valid header value".to_string())?,
        );
        let ws_config = WebSocketConfig {
            max_message_size: Some(MAX_FRAME_BYTES),
            max_frame_size: Some(MAX_FRAME_BYTES),
            ..WebSocketConfig::default()
        };
        let (mut ws, _) = tokio::time::timeout(
            timeout,
            tokio_tungstenite::connect_async_with_config(request, Some(ws_config), true),
        )
        .await
        .map_err(|_| format!("connecting to {url} timed out"))?
        .map_err(|e| format!("connecting to {url}: {e}"))?;

        let hello = FollowerMessage::Hello {
            name: follower_name(),
            stream_id: self.position.stream_id,
            lsn: self.position.lsn,
        };
        ws.send(Message::Text(json(&hello)))
            .await
            .map_err(|e| e.to_string())?;

        let handler = Arc::clone(&self.handler);
        let status = handler.replication_status();
        let mut resync: Option<Resync> = None;
        let mut stream_id = 0;
        loop {
            let message = match tokio::time::timeout(timeout, ws.next()).await {
                Err(_) => {
                    return Err(format!(
                        "no word from the primary for {} ms",
                        timeout.as_millis()
                    ))
                }
                Ok(None) => return Err("the primary closed the stream".into()),
                Ok(Some(Err(e))) => return Err(e.to_string()),
                Ok(Some(Ok(message))) => message,
            };
            match message {
                Message::Text(text) => {
                    let message: PrimaryMessage =
                        serde_json::from_str(&text).map_err(|e| format!("bad message: {e}"))?;
                    match message {
                        PrimaryMessage::Start {
                            stream_id: id,
                            mode,
                            head,
                        } => {
                            stream_id = id;
                            status.contact(head);
                            if mode == StartMode::Resync {
                                info!(stream_id = id, head, "replication_resync_started");
                                status.set_state(FollowerState::Resyncing);
                                // A crash mid-resync must not resume from the old position.
                                self.save_position(Position::default(), true)?;
                                resync = Some(Resync::default());
                                self.resyncing = true;
                            } else {
                                status.set_state(FollowerState::Streaming);
                            }
                        }
                        PrimaryMessage::Heartbeat { head } => {
                            status.contact(head);
                            if resync.is_none() {
                                self.resyncing = false;
                            }
                        }
                        PrimaryMessage::Error { message } => return Err(message),
                    }
                }
                Message::Binary(frame) => {
                    if stream_id == 0 {
                        return Err("stream data before start".into());
                    }
                    let (first, head, lines) = decode_frame(&frame)?;
                    if head > 0 {
                        status.contact(head);
                    }
                    let lsn = if first == 0 {
                        let state = resync.as_mut().ok_or("checkpoint data outside a resync")?;
                        match self.resync_lines(state, lines.to_vec(), stream_id).await? {
                            Some(head) => {
                                resync = None;
                                status.set_state(FollowerState::Streaming);
                                head
                            }
                            None => continue,
                        }
                    } else {
                        if resync.is_some() {
                            return Err("events before the checkpoint ended".into());
                        }
                        // Apply every frame that already arrived as one batch:
                        // one fsync and one position save for all of them.
                        let mut batch = lines.to_vec();
                        let mut next = first + split_lines(lines).count() as u64;
                        while batch.len() < BATCH_BYTES {
                            let Some(message) = ws.next().now_or_never() else {
                                break;
                            };
                            match message {
                                Some(Ok(Message::Binary(frame))) => {
                                    let (more, head, lines) = decode_frame(&frame)?;
                                    if more != next {
                                        return Err(format!(
                                            "frame at LSN {more} does not follow {}",
                                            next - 1
                                        ));
                                    }
                                    status.contact(head);
                                    next += split_lines(lines).count() as u64;
                                    batch.extend_from_slice(lines);
                                }
                                Some(Ok(Message::Text(text))) => {
                                    match serde_json::from_str(&text) {
                                        Ok(PrimaryMessage::Heartbeat { head }) => {
                                            status.contact(head);
                                        }
                                        Ok(PrimaryMessage::Error { message }) => {
                                            return Err(message)
                                        }
                                        _ => return Err(format!("unexpected message: {text}")),
                                    }
                                }
                                Some(Ok(_)) => {}
                                Some(Err(e)) => return Err(e.to_string()),
                                // Closed: apply what arrived; the next read reports it.
                                None => break,
                            }
                        }
                        let lsn = self.apply_events(stream_id, first, batch).await?;
                        self.resyncing = false;
                        lsn
                    };
                    self.streamed = true;
                    let ack = FollowerMessage::Ack { lsn };
                    ws.send(Message::Text(json(&ack)))
                        .await
                        .map_err(|e| e.to_string())?;
                }
                Message::Close(_) => return Err("the primary closed the stream".into()),
                _ => {}
            }
        }
    }

    /// Apply a batch of events starting at LSN `first`; returns the last LSN.
    async fn apply_events(
        &mut self,
        stream_id: u64,
        first: u64,
        lines: Vec<u8>,
    ) -> Result<u64, String> {
        if stream_id != self.position.stream_id || first != self.position.lsn + 1 {
            self.save_position(Position::default(), true)?;
            return Err(format!(
                "stream gap: expected LSN {} of stream {:016x}, got {first} of {stream_id:016x}",
                self.position.lsn + 1,
                self.position.stream_id
            ));
        }
        let handler = Arc::clone(&self.handler);
        let applied = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let storage = handler.get_storage();
            let mut changes = Vec::new();
            let mut count = 0u64;
            let mut revision = 0u64;
            for line in split_lines(&lines) {
                let event = match decode_line(line).map_err(|e| e.to_string())? {
                    Line::Commit(txn) => {
                        revision = revision.max(txn.revision());
                        Event::Commit(txn)
                    }
                    Line::Engine(event) => Event::Engine(event),
                    Line::Resync(_) => return Err("checkpoint mark in an event frame".into()),
                };
                changes.extend(storage.apply_replicated(event).map_err(|e| e.to_string())?);
                count += 1;
            }
            storage.sync_replicated().map_err(|e| e.to_string())?;
            Ok((changes, count, revision))
        })
        .await
        .map_err(|e| format!("apply task failed: {e}"))?;
        let (changes, count, revision) = match applied {
            Ok(applied) => applied,
            Err(e) => {
                // The state may be partly applied: rebuild it from a checkpoint.
                self.save_position(Position::default(), true)?;
                return Err(format!("applying the primary's events failed: {e}"));
            }
        };
        self.publish(&changes);
        let position = Position {
            stream_id,
            lsn: first + count - 1,
            primary_revision: self.position.primary_revision.max(revision),
        };
        self.save_position(position, false)?;
        Ok(position.lsn)
    }

    /// Take a frame of checkpoint lines. Returns the LSN the checkpoint
    /// holds once its end is applied.
    async fn resync_lines(
        &mut self,
        state: &mut Resync,
        lines: Vec<u8>,
        stream_id: u64,
    ) -> Result<Option<u64>, String> {
        let mut finished = None;
        let mut graphs_to_reconcile = Vec::new();
        for line in split_lines(&lines) {
            match decode_line(line).map_err(|e| e.to_string())? {
                Line::Resync(ResyncMark::Begin {
                    revision,
                    head,
                    graphs,
                }) => {
                    state.revision = revision;
                    state.head = head;
                    state.graphs = graphs;
                }
                Line::Resync(ResyncMark::Graph { name, indexes }) => {
                    graphs_to_reconcile.extend(state.current.take());
                    state.current = Some((
                        name,
                        GraphState {
                            indexes,
                            ..GraphState::default()
                        },
                    ));
                }
                Line::Commit(txn) => {
                    let (name, graph) = state
                        .current
                        .as_mut()
                        .ok_or("checkpoint data before its graph")?;
                    graph.absorb(name, txn).map_err(|e| e.to_string())?;
                }
                Line::Resync(ResyncMark::End) => {
                    graphs_to_reconcile.extend(state.current.take());
                    finished = Some(state.graphs.clone());
                }
                Line::Engine(_) => return Err("engine event inside a checkpoint".into()),
            }
        }
        let done = finished.is_some();
        let handler = Arc::clone(&self.handler);
        let reconciled = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let storage = handler.get_storage();
            let mut changes = Vec::new();
            for (name, graph) in graphs_to_reconcile {
                changes.push(
                    storage
                        .reconcile_graph(&name, graph)
                        .map_err(|e| format!("reconciling graph '{name}': {e}"))?,
                );
            }
            if let Some(keep) = finished {
                changes.extend(
                    storage
                        .retain_replica_graphs(&keep)
                        .map_err(|e| e.to_string())?,
                );
            }
            storage.sync_replicated().map_err(|e| e.to_string())?;
            Ok(changes)
        })
        .await
        .map_err(|e| format!("reconcile task failed: {e}"))??;
        self.publish(&reconciled);
        if !done {
            return Ok(None);
        }
        let position = Position {
            stream_id,
            lsn: state.head,
            primary_revision: state.revision,
        };
        self.save_position(position, true)?;
        info!(
            stream_id,
            head = state.head,
            revision = state.revision,
            graphs = state.graphs.len(),
            "replication_resync_finished"
        );
        Ok(Some(state.head))
    }

    /// Record `position`. With `durable` the file is fsynced; without, a
    /// crash may bring back an older position, which only replays events
    /// (the WAL holding them is synced first).
    fn save_position(&mut self, position: Position, durable: bool) -> Result<(), String> {
        position
            .save(&self.path, durable)
            .map_err(|e| format!("saving the replication position: {e}"))?;
        self.position = position;
        self.handler.replication_status().applied(
            position.stream_id,
            position.lsn,
            position.primary_revision,
        );
        Ok(())
    }

    /// Announce applied changes to this follower's subscribers, and refresh
    /// credentials when the primary's `_internal` changed.
    fn publish(&self, changes: &[ReplicaChange]) {
        let mut credentials = false;
        for change in changes.iter().filter(|c| !c.is_empty()) {
            if change.kg == crate::auth::INTERNAL_KG {
                credentials = true;
                continue;
            }
            let handler = &self.handler;
            match change.graph {
                Some(GraphEvent::Created) => handler.notify_kg_change(&change.kg, "created"),
                Some(GraphEvent::Dropped) => handler.notify_kg_change(&change.kg, "dropped"),
                Some(GraphEvent::Restructured) => {
                    handler.notify_kg_change(&change.kg, "replicated");
                }
                None => {}
            }
            for rule in &change.rules {
                handler.notify_rule_change(&change.kg, rule, "replicated");
            }
            for schema in &change.schemas {
                handler.notify_schema_change(&change.kg, schema, "replicated");
            }
            for relation in &change.relations {
                let operation = match (relation.inserted, relation.deleted) {
                    (_, 0) => "insert",
                    (0, _) => "delete",
                    _ => "update",
                };
                handler.notify_persistent_update(
                    &change.kg,
                    &relation.relation,
                    operation,
                    relation.inserted + relation.deleted,
                );
            }
        }
        if credentials {
            self.handler.sync_credentials();
        }
    }
}

fn follower_name() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "follower".to_string())
}

fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("replication message serializes")
}
