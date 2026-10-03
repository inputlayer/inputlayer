//! Minimal WebSocket client for `il` - connect, authenticate, execute.
//!
//! Speaks the engine's `/ws` protocol ([`inputlayer_ws_protocol`]), but exposes
//! only the request/response surface the registry commands need. Every request
//! carries an id and only frames echoing it are accepted as its reply. Streaming results
//! (result_start / result_chunk / result_end) are reassembled into a single
//! `Result` before being returned.

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use inputlayer_ws_protocol::{ClientFrame, FrameClass, RequestId, ServerFrame};
use tokio_tungstenite::tungstenite;

/// A failed statement of a multi-statement program.
pub use inputlayer_ws_protocol::StatementError;

pub struct QueryResult {
    #[allow(dead_code)]
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Engine-produced proof trees (present on `.why` results), passed
    /// through untouched.
    pub proof_trees: Option<serde_json::Value>,
    /// Failed statements; empty when every statement succeeded.
    pub errors: Vec<StatementError>,
}

impl QueryResult {
    /// Messages of the statements that failed. A failed single statement is
    /// an error frame instead, so `execute` already returned `Err`.
    pub fn soft_errors(&self) -> Vec<String> {
        self.errors.iter().map(|e| e.message.clone()).collect()
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct Engine {
    stream: WsStream,
    /// Id of the last request sent; its replies must echo it.
    last_id: u64,
}

/// Transport-level failure: the socket is gone (closed, reset, or the server
/// hung up). Attached to the error chain so callers holding a pooled
/// connection can tell "reconnect" apart from "the statement failed".
#[derive(Debug, Clone, Copy)]
pub struct Disconnected;

impl std::fmt::Display for Disconnected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("engine connection lost")
    }
}

impl std::error::Error for Disconnected {}

fn disconnected(detail: impl std::fmt::Display) -> anyhow::Error {
    anyhow::Error::new(Disconnected).context(detail.to_string())
}

/// `http(s)://host:port` -> `ws(s)://host:port/ws`; scheme-less input
/// defaults to `ws://`.
pub fn ws_url(server: &str) -> String {
    let base = server.trim_end_matches('/');
    let converted = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if base.starts_with("ws://") || base.starts_with("wss://") {
        base.to_string()
    } else {
        format!("ws://{base}")
    };
    if converted.ends_with("/ws") {
        converted
    } else {
        format!("{converted}/ws")
    }
}

impl Engine {
    pub async fn connect(server: &str, api_key: &str) -> Result<Self> {
        let url = ws_url(server);
        let (stream, _) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio_tungstenite::connect_async(&url),
        )
        .await
        .map_err(|_| anyhow!("connection timeout (10s): {url}"))?
        .with_context(|| format!("failed to connect to {url}"))?;

        let mut engine = Self { stream, last_id: 0 };
        engine
            .send(ClientFrame::Authenticate {
                id: Some(RequestId::from(0)),
                api_key: api_key.to_string(),
            })
            .await?;
        // Engines before protocol v2 omit `protocol_version`, so their reply
        // does not parse.
        let reply = engine
            .next_reply()
            .await
            .context("authenticate (the engine must speak /ws protocol v2)")?;
        match reply {
            ServerFrame::Authenticated { .. } => Ok(engine),
            ServerFrame::AuthError { message, .. } => bail!("authentication failed: {message}"),
            other => bail!("unexpected reply to authenticate: {other:?}"),
        }
    }

    async fn send(&mut self, request: ClientFrame) -> Result<()> {
        let text = serde_json::to_string(&request)?;
        self.stream
            .send(tungstenite::Message::Text(text))
            .await
            .map_err(|err| disconnected(format!("send failed: {err}")))
    }

    /// Execute a (possibly multi-statement) program and return the final result.
    pub async fn execute(&mut self, program: &str) -> Result<QueryResult> {
        self.last_id += 1;
        self.send(ClientFrame::Execute {
            id: Some(RequestId::from(self.last_id)),
            program: program.to_string(),
        })
        .await?;

        let mut streamed: Option<QueryResult> = None;
        loop {
            match self.next_reply().await? {
                ServerFrame::Result(result) => {
                    return Ok(QueryResult {
                        columns: result.columns,
                        rows: result.rows,
                        proof_trees: result.proof_trees.map(serde_json::Value::Array),
                        errors: result.errors,
                    });
                }
                ServerFrame::ResultStart(start) => {
                    streamed = Some(QueryResult {
                        columns: start.columns,
                        rows: Vec::new(),
                        proof_trees: start.proof_trees.map(serde_json::Value::Array),
                        errors: start.errors,
                    });
                }
                ServerFrame::ResultChunk { rows, .. } => {
                    if let Some(acc) = streamed.as_mut() {
                        acc.rows.extend(rows);
                    }
                }
                ServerFrame::ResultEnd { .. } => {
                    if let Some(acc) = streamed.take() {
                        return Ok(acc);
                    }
                }
                ServerFrame::Error {
                    message,
                    validation_errors,
                    ..
                } => {
                    // Parse failures name the offending lines; without
                    // them "Program has N parse error(s)" is undebuggable.
                    match validation_errors {
                        Some(details) => {
                            let details = serde_json::to_string(&details)?;
                            bail!("{message}: {details}")
                        }
                        None => bail!("{message}"),
                    }
                }
                other => bail!("unexpected reply to execute: {other:?}"),
            }
        }
    }

    /// Drain frames the server pushed while this connection sat idle
    /// (notifications, pings, a close) without blocking. Returns false when
    /// the socket is closed or errored - the connection must not be reused.
    pub fn drain_idle(&mut self) -> bool {
        use futures_util::FutureExt;
        loop {
            match self.stream.next().now_or_never() {
                None => return true,
                Some(Some(Ok(tungstenite::Message::Close(_)) | Err(_)) | None) => return false,
                Some(Some(Ok(_))) => {}
            }
        }
    }

    /// Next frame answering the last request. Pushes are skipped; a notice
    /// is not an answer, and the one closing the connection fails the request
    /// as a disconnect.
    async fn next_reply(&mut self) -> Result<ServerFrame> {
        loop {
            let frame =
                tokio::time::timeout(std::time::Duration::from_secs(120), self.stream.next())
                    .await
                    .map_err(|_| anyhow!("server response timeout (120s)"))?
                    .ok_or_else(|| disconnected("connection closed"))?
                    .map_err(|err| disconnected(format!("websocket error: {err}")))?;
            let text = match frame {
                tungstenite::Message::Text(text) => text,
                tungstenite::Message::Close(_) => {
                    return Err(disconnected("connection closed by server"))
                }
                _ => continue, // binary/ping/pong: skip
            };
            let frame: ServerFrame =
                serde_json::from_str(&text).context("unexpected server message")?;
            match frame {
                ServerFrame::Notice { code, message } if code.closes_connection() => {
                    return Err(disconnected(message));
                }
                ServerFrame::Notice { .. } => {}
                frame if frame.class() == FrameClass::Reply => {
                    let expected = RequestId::from(self.last_id);
                    if frame.request_id() != Some(&expected) {
                        bail!("reply for another request (waiting for {expected}): {frame:?}");
                    }
                    return Ok(frame);
                }
                _ => {} // notifications and subscription pushes
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn soft_errors_are_the_failed_statements() {
        let error = StatementError {
            index: 1,
            code: inputlayer_ws_protocol::ErrorCode::NotFound,
            message: "Rule 'path' not found.".to_string(),
        };
        let result = QueryResult {
            columns: vec!["message".to_string()],
            rows: vec![vec![serde_json::json!("Inserted 1 fact(s) into 'a'.")]],
            proof_trees: None,
            errors: vec![error.clone()],
        };
        assert_eq!(result.soft_errors(), vec![error.message]);

        let result = QueryResult {
            errors: Vec::new(),
            ..result
        };
        assert!(result.soft_errors().is_empty());
    }
}
