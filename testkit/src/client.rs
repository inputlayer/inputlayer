//! Thin client for the engine's `/ws` protocol.
//!
//! A reader task stamps every frame with its arrival [`Instant`] the moment it
//! is read, so latency samples do not include time the test spent elsewhere.
//! The task owns its queue; there is no lock shared between connections.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
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
}

impl Frame {
    /// The message's `type` tag.
    pub fn kind(&self) -> &str {
        self.value["type"].as_str().unwrap_or_default()
    }
}

/// Whether a frame answers a request or is pushed by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Reply,
    Push,
}

/// Classify a frame; unknown types are a protocol change the harness must learn.
fn role(frame: &Frame) -> Checked<Role> {
    match frame.kind() {
        "authenticated" | "auth_error" | "result" | "result_start" | "result_chunk"
        | "result_end" | "error" | "pong" => Ok(Role::Reply),
        "subscription_delta" | "subscription_error" | "persistent_update" | "rule_change"
        | "kg_change" | "schema_change" => Ok(Role::Push),
        _ => Err(Violation::Transport(format!(
            "unknown message type (update the testkit for the new protocol): {}",
            frame.value
        ))),
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
}

impl QueryResult {
    fn from_header(header: &Value) -> Self {
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
        }
    }

    fn extend_rows(&mut self, frame: &Value) {
        if let Some(rows) = frame["rows"].as_array() {
            self.rows.extend(rows.iter().cloned());
        }
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

/// An authenticated `/ws` connection.
pub struct WsClient {
    sink: Sink,
    inbox: mpsc::UnboundedReceiver<Frame>,
    /// Pushes read while waiting for a reply, in arrival order.
    pushes: VecDeque<Frame>,
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

    /// Connect to `url` and authenticate with `api_key`.
    pub async fn connect_url(url: &str, api_key: &str) -> Checked<Self> {
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| Violation::Transport(format!("connect {url}: {e}")))?;
        let (sink, mut stream) = ws.split();
        let (tx, inbox) = mpsc::unbounded_channel();
        let reader = tokio::spawn(async move {
            while let Some(Ok(message)) = stream.next().await {
                let at = Instant::now();
                if let Message::Text(text) = message {
                    let value = serde_json::from_str(&text).unwrap_or_else(
                        |e| json!({"type": "malformed", "error": e.to_string(), "text": text}),
                    );
                    if tx.send(Frame { at, value }).is_err() {
                        break;
                    }
                }
            }
        });
        let mut client = Self {
            sink,
            inbox,
            pushes: VecDeque::new(),
            reader,
        };
        client
            .send(&json!({"type": "authenticate", "api_key": api_key}))
            .await?;
        let reply = client.next_reply().await?;
        if reply.kind() != "authenticated" {
            return Err(Violation::Rejected(format!(
                "authentication: {}",
                reply.value
            )));
        }
        Ok(client)
    }

    async fn send(&mut self, value: &Value) -> Checked<()> {
        self.sink
            .send(Message::Text(value.to_string()))
            .await
            .map_err(|e| Violation::Transport(format!("send: {e}")))
    }

    async fn next_frame(&mut self, timeout: Duration, what: &str) -> Checked<Frame> {
        match tokio::time::timeout(timeout, self.inbox.recv()).await {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(Violation::Transport(format!(
                "connection closed while waiting for {what}"
            ))),
            Err(_) => Err(Violation::Timeout(what.to_string())),
        }
    }

    /// Next reply frame; pushes read meanwhile are kept for [`Self::next_push`].
    async fn next_reply(&mut self) -> Checked<Frame> {
        loop {
            let frame = self.next_frame(FRAME_TIMEOUT, "a reply").await?;
            match role(&frame)? {
                Role::Reply => return Ok(frame),
                Role::Push => self.pushes.push_back(frame),
            }
        }
    }

    /// Run `program`; returns its complete result or the engine's error.
    pub async fn execute(&mut self, program: &str) -> Checked<QueryResult> {
        self.send(&json!({"type": "execute", "program": program}))
            .await?;
        let header = self.next_reply().await?;
        match header.kind() {
            "result" => {
                let mut result = QueryResult::from_header(&header.value);
                result.extend_rows(&header.value);
                Ok(result)
            }
            "result_start" => {
                let mut result = QueryResult::from_header(&header.value);
                loop {
                    let frame = self.next_reply().await?;
                    match frame.kind() {
                        "result_chunk" => result.extend_rows(&frame.value),
                        "result_end" => return Ok(result),
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

    /// Next pushed message, waiting up to `timeout`.
    pub async fn next_push(&mut self, timeout: Duration) -> Checked<Frame> {
        if let Some(frame) = self.pushes.pop_front() {
            return Ok(frame);
        }
        let frame = self.next_frame(timeout, "a push").await?;
        match role(&frame)? {
            Role::Push => Ok(frame),
            Role::Reply => Err(Violation::Transport(format!(
                "unsolicited reply: {}",
                frame.value
            ))),
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
