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
    /// an Ok result rather than as error frames ("Insert rejected for ...",
    /// "Failed to register schema ..."). Anything that must know whether its
    /// statements actually executed has to scan for them.
    pub fn soft_errors(&self) -> Vec<String> {
        const MARKERS: [&str; 4] = [
            "Insert rejected",
            "Failed to register schema",
            "Create failed:",
            "Drop failed:",
        ];
        self.rows
            .iter()
            .filter_map(|row| row.first().and_then(serde_json::Value::as_str))
            .filter(|msg| MARKERS.iter().any(|m| msg.contains(m)))
            .map(str::to_string)
            .collect()
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

pub struct Engine {
    stream: WsStream,
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
        self.stream.send(tungstenite::Message::Text(req)).await?;

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
                WsResponse::Error { message } => {
                    // The global socket pushes connection-level errors that
                    // are not responses to the in-flight request; treating
                    // them as one fails the request spuriously.
                    if message.starts_with("Missed ")
                        || message.starts_with("Idle timeout")
                        || message.starts_with("Connection lifetime")
                    {
                        continue;
                    }
                    bail!("{message}");
                }
                _ => {} // notifications, pongs: skip
            }
        }
    }

    async fn next_response(stream: &mut WsStream) -> Result<WsResponse> {
        loop {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(120), stream.next())
                .await
                .map_err(|_| anyhow!("server response timeout (120s)"))?
                .ok_or_else(|| anyhow!("connection closed"))?
                .context("websocket error")?;
            match frame {
                tungstenite::Message::Text(text) => {
                    return serde_json::from_str(&text).context("unexpected server message");
                }
                tungstenite::Message::Close(_) => bail!("connection closed by server"),
                _ => {} // binary/ping/pong: skip
            }
        }
    }
}
