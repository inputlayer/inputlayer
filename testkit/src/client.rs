//! Thin client for the engine's `/ws` protocol.
//!
//! A reader task stamps every frame with its arrival [`Instant`] the moment it
//! is read, so latency samples do not include time the test spent elsewhere.
//! The task owns its queue; there is no lock shared between connections.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use inputlayer_ws_protocol::{FrameClass, NoticeCode, RequestId, ServerFrame};
use serde_json::{json, Value};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::contract::{Checked, Violation};
use crate::engine::Engine;

/// Longest wait for any single reply or push.
pub const FRAME_TIMEOUT: Duration = Duration::from_secs(60);

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

/// A server message and when it arrived.
#[derive(Debug, Clone)]
pub struct Frame {
    pub at: Instant,
    pub value: Value,
    /// How the frame routes, or why it is not a protocol frame.
    class: Result<FrameClass, String>,
    /// The `id` it echoes, for replies.
    request_id: Option<RequestId>,
}

impl Frame {
    /// Parse `text`, received at `at`, against the shared protocol types.
    fn parse(at: Instant, text: &str) -> Self {
        let value = serde_json::from_str(text)
            .unwrap_or_else(|e| json!({"type": "malformed", "error": e.to_string(), "text": text}));
        let (class, request_id) = match serde_json::from_str::<ServerFrame>(text) {
            Ok(frame) => (Ok(frame.class()), frame.request_id().cloned()),
            // Unknown types are a protocol change the harness must learn.
            Err(e) => (Err(format!("not a protocol frame ({e}): {text}")), None),
        };
        Self {
            at,
            value,
            class,
            request_id,
        }
    }

    /// The message's `type` tag.
    pub fn kind(&self) -> &str {
        self.value["type"].as_str().unwrap_or_default()
    }

    fn class(&self) -> Checked<FrameClass> {
        self.class.clone().map_err(Violation::Transport)
    }
}

/// A complete query result, reassembled from chunks when streamed.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Value>,
    pub total_count: usize,
    pub truncated: bool,
    /// Failed statements of a multi-statement program.
    pub errors: Vec<Value>,
    /// When the result's last frame arrived.
    pub at: Instant,
    /// For a `.subscribe` reply: the revision the snapshot is the answer at.
    pub subscribed_revision: Option<u64>,
    /// The reply's `revision`: for a write, the revision its changes are
    /// visible at. Queries carry none until V9 (#315).
    pub revision: Option<u64>,
    /// Effective counts of the fact statements a write committed
    /// (`statements`), in program order.
    pub statements: Vec<Value>,
}

impl QueryResult {
    fn from_header(header: &Frame) -> Self {
        let at = header.at;
        let header = &header.value;
        Self {
            columns: header["columns"]
                .as_array()
                .map(|cols| {
                    cols.iter()
                        .filter_map(|c| c.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
            rows: Vec::new(),
            total_count: header["total_count"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or_default(),
            truncated: header["truncated"].as_bool().unwrap_or_default(),
            errors: header["errors"].as_array().cloned().unwrap_or_default(),
            at,
            subscribed_revision: header["subscribed"]["revision"].as_u64(),
            revision: header["revision"].as_u64(),
            statements: header["statements"].as_array().cloned().unwrap_or_default(),
        }
    }

    fn extend_rows(&mut self, frame: &Frame) {
        if let Some(rows) = frame.value["rows"].as_array() {
            self.rows.extend(rows.iter().cloned());
        }
        self.at = frame.at;
    }

    /// Fail unless the engine reported the whole result.
    pub fn complete(self) -> Checked<Self> {
        if self.truncated || self.total_count > self.rows.len() {
            return Err(Violation::IncompleteSnapshot {
                rows: self.rows.len(),
                detail: format!(
                    "truncated={}, total_count={}",
                    self.truncated, self.total_count
                ),
            });
        }
        Ok(self)
    }
}

/// Timing of one acknowledged write.
#[derive(Debug, Clone, Copy)]
pub struct Commit {
    /// Just before the request was sent.
    pub sent_at: Instant,
    /// When the engine's success reply arrived.
    pub acked_at: Instant,
}

/// A `snapshot` reply: results of several queries at one revision.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The knowledge graph revision every result is the exact answer at.
    pub revision: u64,
    /// `(name, rows)` per query, in request order.
    pub results: Vec<(String, Vec<Value>)>,
    /// When the snapshot arrived.
    pub at: Instant,
}

/// How a connection reads from its socket.
#[derive(Debug, Clone, Copy)]
pub struct Pacing {
    /// Frames read ahead of the consumer. A full inbox stops reading the
    /// socket, so a consumer that falls behind pushes back on the server.
    pub inbox_frames: usize,
    /// `SO_RCVBUF` of the socket; `None` keeps the system default.
    pub recv_buffer_bytes: Option<u32>,
}

impl Pacing {
    /// Read everything as it arrives (the default).
    pub const EAGER: Self = Self {
        // The most a bounded channel takes; never reached in practice.
        inbox_frames: usize::MAX >> 4,
        recv_buffer_bytes: None,
    };
}

/// An authenticated `/ws` connection.
///
/// Every request carries an id; a reply that does not echo it is a
/// [`Violation::Uncorrelated`]. Notices are never taken for replies.
/// Requests may be pipelined: [`Self::send_execute`] sends without waiting
/// and [`Self::result`] reads replies in request order, which the engine
/// guarantees. Pushes arriving meanwhile are kept for [`Self::next_push`].
pub struct WsClient {
    sink: Sink,
    inbox: mpsc::Receiver<Frame>,
    /// Pushes read while waiting for a reply, in arrival order.
    pushes: VecDeque<Frame>,
    /// Replies read while waiting for a push, for requests still outstanding.
    replies: VecDeque<Frame>,
    /// Ids of the requests whose reply has not been consumed, oldest first.
    outstanding: VecDeque<String>,
    /// Connection notices that did not close it (`notifications_missed`).
    notices: Vec<Frame>,
    /// Id of the last request sent.
    last_id: u64,
    /// The engine run's `stream_epoch`, from `authenticated`.
    stream_epoch: String,
    reader: JoinHandle<()>,
}

impl Drop for WsClient {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

impl WsClient {
    /// Connect to `knowledge_graph` on `engine` and authenticate with its API key.
    pub async fn connect(engine: &Engine, knowledge_graph: &str) -> Checked<Self> {
        Self::connect_url(&engine.ws_url(knowledge_graph), engine.api_key()).await
    }

    /// Connect to `knowledge_graph` on `engine` and authenticate with `api_key`
    /// (e.g. a scoped key from [`Engine::create_api_key`]).
    pub async fn connect_with_key(
        engine: &Engine,
        knowledge_graph: &str,
        api_key: &str,
    ) -> Checked<Self> {
        Self::connect_url(&engine.ws_url(knowledge_graph), api_key).await
    }

    /// Connect to `url` and authenticate with `api_key`.
    pub async fn connect_url(url: &str, api_key: &str) -> Checked<Self> {
        Self::connect_paced(url, api_key, Pacing::EAGER).await
    }

    /// Connect to `url` and authenticate with `api_key`, reading the socket
    /// only as fast as `pacing` allows: a slow consumer.
    pub async fn connect_paced(url: &str, api_key: &str, pacing: Pacing) -> Checked<Self> {
        let ws = open_socket(url, pacing.recv_buffer_bytes).await?;
        let (sink, mut stream) = ws.split();
        let (tx, inbox) = mpsc::channel(pacing.inbox_frames.max(1));
        let reader = tokio::spawn(async move {
            while let Some(Ok(message)) = stream.next().await {
                let at = Instant::now();
                if let Message::Text(text) = message {
                    if tx.send(Frame::parse(at, &text)).await.is_err() {
                        break;
                    }
                }
            }
        });
        let mut client = Self {
            sink,
            inbox,
            pushes: VecDeque::new(),
            replies: VecDeque::new(),
            outstanding: VecDeque::new(),
            notices: Vec::new(),
            last_id: 0,
            stream_epoch: String::new(),
            reader,
        };
        let reply = client
            .request(json!({"type": "authenticate", "api_key": api_key}))
            .await?;
        if reply.kind() != "authenticated" {
            return Err(Violation::Rejected(format!(
                "authentication: {}",
                reply.value
            )));
        }
        client.stream_epoch = reply.value["stream_epoch"]
            .as_str()
            .ok_or_else(|| Violation::Transport(format!("no stream_epoch: {}", reply.value)))?
            .to_string();
        Ok(client)
    }

    /// Connection notices received so far that did not close the connection.
    pub fn notices(&self) -> &[Frame] {
        &self.notices
    }

    /// The engine run's stream epoch, for a reconnect cursor.
    pub fn stream_epoch(&self) -> &str {
        &self.stream_epoch
    }

    /// Send `request` tagged with a fresh id; returns its whole reply, which
    /// must be one frame.
    async fn request(&mut self, request: Value) -> Checked<Frame> {
        self.send(request).await?;
        let reply = self.next_reply().await;
        self.outstanding.pop_front();
        reply
    }

    /// Send `request` tagged with a fresh id, without waiting for its reply.
    async fn send(&mut self, mut request: Value) -> Checked<()> {
        self.last_id += 1;
        let id = self.last_id.to_string();
        request["id"] = json!(id);
        self.sink
            .send(Message::Text(request.to_string()))
            .await
            .map_err(|e| Violation::Transport(format!("send: {e}")))?;
        self.outstanding.push_back(id);
        Ok(())
    }

    /// Next reply or push. Notices are kept aside; one that closes the
    /// connection is a transport failure.
    async fn next_frame(&mut self, timeout: Duration, what: &str) -> Checked<Frame> {
        loop {
            let frame = match tokio::time::timeout(timeout, self.inbox.recv()).await {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    return Err(Violation::Transport(format!(
                        "connection closed while waiting for {what}"
                    )))
                }
                Err(_) => return Err(Violation::Timeout(what.to_string())),
            };
            if frame.class()? != FrameClass::Notice {
                return Ok(frame);
            }
            let closing = serde_json::from_value::<NoticeCode>(frame.value["code"].clone())
                .map_or(true, NoticeCode::closes_connection);
            if closing {
                return Err(Violation::Transport(format!(
                    "server closed the connection: {}",
                    frame.value
                )));
            }
            self.notices.push(frame);
        }
    }

    /// Next frame answering the oldest outstanding request; pushes read
    /// meanwhile are kept for [`Self::next_push`].
    async fn next_reply(&mut self) -> Checked<Frame> {
        let Some(expected) = self.outstanding.front().cloned() else {
            return Err(Violation::Transport("no request outstanding".to_string()));
        };
        let frame = match self.replies.pop_front() {
            Some(frame) => frame,
            None => loop {
                let frame = self.next_frame(FRAME_TIMEOUT, "a reply").await?;
                if frame.class()? == FrameClass::Push {
                    self.pushes.push_back(frame);
                    continue;
                }
                break frame;
            },
        };
        if frame.request_id.as_ref().map(RequestId::as_str) != Some(expected.as_str()) {
            return Err(Violation::Uncorrelated {
                expected,
                frame: frame.value.to_string(),
            });
        }
        Ok(frame)
    }

    /// Run `program`; returns its complete result or the engine's error.
    pub async fn execute(&mut self, program: &str) -> Checked<QueryResult> {
        self.send_execute(program).await?;
        self.result().await
    }

    /// Run write `program` only if nothing in scope changed after `revision`
    /// (`expect_revision`): the scope is `relations` and everything derived
    /// from them, or the whole knowledge graph when `relations` is empty.
    /// A failed precondition is the engine's error, a [`Violation::Rejected`]
    /// naming the relation that changed and its revision.
    pub async fn execute_expecting(
        &mut self,
        program: &str,
        revision: u64,
        relations: &[&str],
    ) -> Checked<QueryResult> {
        let mut request = json!({
            "type": "execute",
            "program": program,
            "expect_revision": revision,
        });
        if !relations.is_empty() {
            request["expect_relations"] = json!(relations);
        }
        self.send(request).await?;
        self.result().await
    }

    /// Run `program` reading the knowledge graph as of `revision` (`at`,
    /// V13 #316). Until the engine supports `at` it answers at its latest
    /// revision; compare [`QueryResult::revision`] with `revision`.
    pub async fn execute_at(&mut self, program: &str, revision: u64) -> Checked<QueryResult> {
        self.send(json!({"type": "execute", "program": program, "at": revision}))
            .await?;
        self.result().await
    }

    /// Send `program` without waiting; returns its request id (for
    /// [`Self::send_cancel`]). Read its reply with [`Self::result`].
    pub async fn send_execute(&mut self, program: &str) -> Checked<String> {
        self.send(json!({"type": "execute", "program": program}))
            .await?;
        Ok(self.last_id.to_string())
    }

    /// Send a `cancel` of the unanswered request `target` without waiting.
    /// Its acknowledgement comes after the target's reply: read it with
    /// [`Self::cancel_ack`].
    pub async fn send_cancel(&mut self, target: &str) -> Checked<()> {
        self.send(json!({"type": "cancel", "target": target})).await
    }

    /// The `cancel_ack` answering the oldest outstanding request: its
    /// `outcome` (`cancelled`, `too_late` or `not_found`).
    pub async fn cancel_ack(&mut self) -> Checked<String> {
        let reply = self.next_reply().await;
        self.outstanding.pop_front();
        let reply = reply?;
        if reply.kind() != "cancel_ack" {
            return Err(Violation::Transport(format!(
                "expected cancel_ack: {}",
                reply.value
            )));
        }
        Ok(reply.value["outcome"]
            .as_str()
            .unwrap_or_default()
            .to_string())
    }

    /// The reply to the oldest request sent with [`Self::send_execute`]: its
    /// complete result or the engine's error.
    pub async fn result(&mut self) -> Checked<QueryResult> {
        let result = self.read_result().await;
        self.outstanding.pop_front();
        result
    }

    /// Read the reply to the oldest outstanding request.
    async fn read_result(&mut self) -> Checked<QueryResult> {
        let header = self.next_reply().await?;
        match header.kind() {
            "result" => {
                let mut result = QueryResult::from_header(&header);
                result.extend_rows(&header);
                Ok(result)
            }
            // Complete only at a `result_end` its chunks add up to.
            "result_start" => {
                let mut result = QueryResult::from_header(&header);
                let mut chunks = 0;
                loop {
                    let frame = self.next_reply().await?;
                    match frame.kind() {
                        "result_chunk" if frame.value["chunk_index"].as_u64() == Some(chunks) => {
                            result.extend_rows(&frame);
                            chunks += 1;
                        }
                        "result_end" => {
                            let announced = (
                                frame.value["row_count"].as_u64(),
                                frame.value["chunk_count"].as_u64(),
                            );
                            if announced != (Some(result.rows.len() as u64), Some(chunks)) {
                                return Err(Violation::IncompleteSnapshot {
                                    rows: result.rows.len(),
                                    detail: format!(
                                        "{chunks} chunk(s) received, end announces {}",
                                        frame.value
                                    ),
                                });
                            }
                            result.at = frame.at;
                            return Ok(result);
                        }
                        "error" => {
                            return Err(Violation::Rejected(format!(
                                "streamed result failed: {}",
                                frame.value["message"].as_str().unwrap_or_default()
                            )))
                        }
                        _ => {
                            return Err(Violation::Transport(format!(
                                "unexpected frame inside a streamed result: {}",
                                frame.value
                            )))
                        }
                    }
                }
            }
            "error" => Err(Violation::Rejected(
                header.value["message"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            )),
            _ => Err(Violation::Transport(format!(
                "unexpected reply: {}",
                header.value
            ))),
        }
    }

    /// Keep the connection from idling out (`ping`, answered by `pong`).
    pub async fn ping(&mut self) -> Checked<()> {
        let reply = self.request(json!({"type": "ping"})).await?;
        if reply.kind() != "pong" {
            return Err(Violation::Transport(format!(
                "expected pong: {}",
                reply.value
            )));
        }
        Ok(())
    }

    /// Read `queries` (`(name, query)`) at one revision (`read`).
    pub async fn read(&mut self, queries: &[(&str, &str)]) -> Checked<Snapshot> {
        let reply = self
            .request(json!({"type": "read", "queries": named(queries)}))
            .await?;
        snapshot_of(&reply)
    }

    /// Subscribe to `queries` (`(name, query)`) as group `subscription`
    /// (`subscribe`); returns the group's snapshot. Its pushes are
    /// `subscription_group_delta`s, read with [`Self::next_push`].
    pub async fn subscribe_group(
        &mut self,
        subscription: &str,
        queries: &[(&str, &str)],
    ) -> Checked<Snapshot> {
        let reply = self
            .request(json!({
                "type": "subscribe",
                "subscription": subscription,
                "queries": named(queries),
            }))
            .await?;
        snapshot_of(&reply)
    }

    /// Run a write `program`; fails unless every statement succeeded.
    pub async fn commit(&mut self, program: &str) -> Checked<Commit> {
        let sent_at = Instant::now();
        let result = self.execute(program).await?;
        let acked_at = Instant::now();
        if !result.errors.is_empty() {
            return Err(Violation::Rejected(format!(
                "{program:?} failed: {:?}",
                result.errors
            )));
        }
        Ok(Commit { sent_at, acked_at })
    }

    /// Run `query` and require the complete result.
    pub async fn query(&mut self, query: &str) -> Checked<QueryResult> {
        self.execute(query).await?.complete()
    }

    /// Next pushed message, waiting up to `timeout`. A reply arriving
    /// meanwhile is kept for [`Self::result`] while a request is outstanding,
    /// and is a violation otherwise.
    pub async fn next_push(&mut self, timeout: Duration) -> Checked<Frame> {
        if let Some(frame) = self.pushes.pop_front() {
            return Ok(frame);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let frame = self.next_frame(remaining, "a push").await?;
            match frame.class()? {
                FrameClass::Push => return Ok(frame),
                _ if !self.outstanding.is_empty() => self.replies.push_back(frame),
                _ => {
                    return Err(Violation::Transport(format!(
                        "unsolicited reply: {}",
                        frame.value
                    )))
                }
            }
        }
    }

    /// The next push if one arrives within `within`.
    pub async fn poll_push(&mut self, within: Duration) -> Checked<Option<Frame>> {
        match self.next_push(within).await {
            Ok(frame) => Ok(Some(frame)),
            Err(Violation::Timeout(_)) => Ok(None),
            Err(other) => Err(other),
        }
    }

    /// Close the connection cleanly.
    pub async fn close(mut self) {
        let _ = self.sink.send(Message::Close(None)).await;
    }
}

/// Open the WebSocket, with `SO_RCVBUF` set before connecting when given.
async fn open_socket(
    url: &str,
    recv_buffer_bytes: Option<u32>,
) -> Checked<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let transport = |e: &dyn std::fmt::Display| Violation::Transport(format!("connect {url}: {e}"));
    let Some(bytes) = recv_buffer_bytes else {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| transport(&e))?;
        return Ok(ws);
    };
    let uri: Uri = url.parse().map_err(|e| transport(&e))?;
    let host = uri.host().unwrap_or("127.0.0.1");
    let port = uri.port_u16().unwrap_or(80);
    let addr = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| transport(&e))?
        .next()
        .ok_or_else(|| transport(&"no address"))?;
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .map_err(|e| transport(&e))?;
    socket
        .set_recv_buffer_size(bytes)
        .map_err(|e| transport(&e))?;
    let stream = socket.connect(addr).await.map_err(|e| transport(&e))?;
    let (ws, _) = tokio_tungstenite::client_async(url, MaybeTlsStream::Plain(stream))
        .await
        .map_err(|e| transport(&e))?;
    Ok(ws)
}

fn named(queries: &[(&str, &str)]) -> Vec<Value> {
    queries
        .iter()
        .map(|(name, query)| json!({"name": name, "query": query}))
        .collect()
}

/// A one-frame `snapshot` reply; anything else is the engine's refusal.
fn snapshot_of(reply: &Frame) -> Checked<Snapshot> {
    match reply.kind() {
        "snapshot" => {
            let results = reply.value["results"]
                .as_array()
                .ok_or_else(|| {
                    Violation::Transport(format!("snapshot without results: {}", reply.value))
                })?
                .iter()
                .map(|result| {
                    if result["truncated"].as_bool().unwrap_or_default() {
                        return Err(Violation::IncompleteSnapshot {
                            rows: result["rows"].as_array().map_or(0, Vec::len),
                            detail: format!("result {} truncated", result["name"]),
                        });
                    }
                    Ok((
                        result["name"].as_str().unwrap_or_default().to_string(),
                        result["rows"].as_array().cloned().unwrap_or_default(),
                    ))
                })
                .collect::<Checked<_>>()?;
            Ok(Snapshot {
                revision: reply.value["revision"].as_u64().ok_or_else(|| {
                    Violation::Transport(format!("snapshot without revision: {}", reply.value))
                })?,
                results,
                at: reply.at,
            })
        }
        "error" => Err(Violation::Rejected(
            reply.value["message"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        )),
        // Streamed snapshots are for results too large for one frame.
        _ => Err(Violation::Transport(format!(
            "expected a one-frame snapshot: {}",
            reply.value
        ))),
    }
}
