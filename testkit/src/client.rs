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

/// An authenticated `/ws` connection.
///
/// Every request carries an id; a reply that does not echo it is a
/// [`Violation::Uncorrelated`]. Notices are never taken for replies.
/// Requests may be pipelined: [`Self::send_execute`] sends without waiting
/// and [`Self::result`] reads replies in request order, which the engine
/// guarantees. Pushes arriving meanwhile are kept for [`Self::next_push`].
pub struct WsClient {
    sink: Sink,
    inbox: mpsc::UnboundedReceiver<Frame>,
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
                    if tx.send(Frame::parse(at, &text)).is_err() {
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
        Ok(client)
    }

    /// Connection notices received so far that did not close the connection.
    pub fn notices(&self) -> &[Frame] {
        &self.notices
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
            "result_start" => {
                let mut result = QueryResult::from_header(&header);
                loop {
                    let frame = self.next_reply().await?;
                    match frame.kind() {
                        "result_chunk" => result.extend_rows(&frame),
                        "result_end" => {
                            result.at = frame.at;
                            return Ok(result);
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
