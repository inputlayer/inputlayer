//! Minimal WebSocket client for `il` - connect, authenticate, execute.
//!
//! Speaks the same GlobalWs* protocol as `inputlayer-client`, but exposes only
//! the request/response surface the registry commands need. Streaming results
//! (result_start / result_chunk / result_end) are reassembled into a single
//! `Result` before being returned.

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite;

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsRequest {
    Authenticate { api_key: String },
    Execute { program: String },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsResponse {
    Connected {},
    Authenticated {},
    AuthError {
        message: String,
    },
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
        #[serde(default)]
        proof_trees: Option<serde_json::Value>,
    },
    ResultStart {
        columns: Vec<String>,
    },
    ResultChunk {
        rows: Vec<Vec<serde_json::Value>>,
    },
    ResultEnd {},
    Error {
        message: String,
        #[serde(default)]
        validation_errors: Option<serde_json::Value>,
    },
    Pong,
    Notification {},
    PersistentUpdate {},
    RuleChange {},
    KgChange {},
    SchemaChange {},
}

pub struct QueryResult {
    #[allow(dead_code)]
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Engine-produced proof trees (present on `.why` results), passed
    /// through untouched.
    pub proof_trees: Option<serde_json::Value>,
}

impl QueryResult {
    /// The engine reports many per-statement failures as message rows inside
    /// an Ok result rather than as error frames. An allowlist of known
    /// failure phrases is the wrong shape here: a phrase the list has not
    /// seen reads as success, and reporting success for something that did
    /// not happen is the one failure this product must never have. So this
    /// is DENY BY DEFAULT - a single-column message row counts as a problem
    /// unless it matches a known-good phrase.
    pub fn soft_errors(&self) -> Vec<String> {
        const SUCCESS_MARKERS: [&str; 18] = [
            "Inserted ",
            "Deleted ",
            "Updated ",
            "Conditional delete:",
            "Relation ",
            "Rule ",
            "Schema ",
            "Knowledge graph ",
            "Switched to knowledge graph",
            "Registered ",
            "Type ",
            "No facts",
            // `.ontology` command results (engine-owned lifecycle).
            "installed ",
            "removed ",
            "upgraded ",
            "digest ",
            "recorded ",
            "kept ",
        ];
        // Message rows only: real query results are not problem reports.
        if self.columns.len() != 1 || self.columns.first().map(String::as_str) != Some("message") {
            return Vec::new();
        }
        self.rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()))
            .filter(|message| {
                let trimmed = message.trim();
                !trimmed.is_empty()
                    && !SUCCESS_MARKERS
                        .iter()
                        .any(|marker| trimmed.starts_with(marker))
            })
            .map(str::to_string)
            .collect()
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct Engine {
    stream: WsStream,
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
        let (mut stream, _) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio_tungstenite::connect_async(&url),
        )
        .await
        .map_err(|_| anyhow!("connection timeout (10s): {url}"))?
        .with_context(|| format!("failed to connect to {url}"))?;

        let auth = serde_json::to_string(&WsRequest::Authenticate {
            api_key: api_key.to_string(),
        })?;
        stream.send(tungstenite::Message::Text(auth)).await?;

        loop {
            match Self::next_response(&mut stream).await? {
                WsResponse::Authenticated {} => break,
                WsResponse::AuthError { message } => bail!("authentication failed: {message}"),
                _ => {}
            }
        }
        Ok(Self { stream })
    }

    /// Execute a (possibly multi-statement) program and return the final result.
    pub async fn execute(&mut self, program: &str) -> Result<QueryResult> {
        let req = serde_json::to_string(&WsRequest::Execute {
            program: program.to_string(),
        })?;
        self.stream
            .send(tungstenite::Message::Text(req))
            .await
            .map_err(|err| disconnected(format!("send failed: {err}")))?;

        let mut streamed: Option<QueryResult> = None;
        loop {
            match Self::next_response(&mut self.stream).await? {
                WsResponse::Result {
                    columns,
                    rows,
                    proof_trees,
                } => {
                    return Ok(QueryResult {
                        columns,
                        rows,
                        proof_trees,
                    });
                }
                WsResponse::ResultStart { columns } => {
                    streamed = Some(QueryResult {
                        columns,
                        rows: Vec::new(),
                        proof_trees: None,
                    });
                }
                WsResponse::ResultChunk { rows } => {
                    if let Some(acc) = streamed.as_mut() {
                        acc.rows.extend(rows);
                    }
                }
                WsResponse::ResultEnd {} => {
                    if let Some(acc) = streamed.take() {
                        return Ok(acc);
                    }
                }
                WsResponse::Error {
                    message,
                    validation_errors,
                } => {
                    // The global socket pushes connection-level errors that
                    // are not responses to the in-flight request; treating
                    // them as one fails the request spuriously.
                    if message.starts_with("Missed ")
                        || message.starts_with("Idle timeout")
                        || message.starts_with("Connection lifetime")
                    {
                        continue;
                    }
                    // Parse failures name the offending lines; without
                    // them "Program has N parse error(s)" is undebuggable.
                    match validation_errors {
                        Some(details) if !details.is_null() => bail!("{message}: {details}"),
                        _ => bail!("{message}"),
                    }
                }
                _ => {} // notifications, pongs: skip
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

    async fn next_response(stream: &mut WsStream) -> Result<WsResponse> {
        loop {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(120), stream.next())
                .await
                .map_err(|_| anyhow!("server response timeout (120s)"))?
                .ok_or_else(|| disconnected("connection closed"))?
                .map_err(|err| disconnected(format!("websocket error: {err}")))?;
            match frame {
                tungstenite::Message::Text(text) => {
                    return serde_json::from_str(&text).context("unexpected server message");
                }
                tungstenite::Message::Close(_) => {
                    return Err(disconnected("connection closed by server"))
                }
                _ => {} // binary/ping/pong: skip
            }
        }
    }
}
