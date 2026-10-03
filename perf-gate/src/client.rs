//! A minimal `/ws` client, speaking the protocol exactly as an agent does.
//!
//! Every frame is stamped with [`Instant::now`] the moment it is read, before
//! parsing, so client-side JSON work never inflates a measured latency.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Admin user the gate creates in every server it starts.
pub const ADMIN_USER: &str = "admin";

/// Longest wait for any single reply before the fixture is declared broken.
const REPLY_TIMEOUT: Duration = Duration::from_secs(60);

/// One parsed server frame. Row payloads of query results are skipped; only
/// subscription deltas keep their rows.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    Authenticated {},
    AuthError {
        message: String,
    },
    Result {
        row_count: usize,
        #[serde(default)]
        truncated: bool,
        #[serde(default)]
        errors: Vec<Value>,
    },
    ResultStart {
        #[serde(default)]
        truncated: bool,
        #[serde(default)]
        errors: Vec<Value>,
    },
    ResultChunk {},
    ResultEnd {
        row_count: usize,
    },
    Error {
        message: String,
    },
    SubscriptionDelta {
        subscription: String,
        #[serde(default)]
        inserted: Vec<Vec<Value>>,
        #[serde(default)]
        retracted: Vec<Vec<Value>>,
    },
    SubscriptionError {
        subscription: String,
        message: String,
    },
    /// Persistent-change notifications and anything newer than this client.
    #[serde(other)]
    Other,
}

/// A frame and the instant it was read off the socket.
pub struct Stamped {
    pub at: Instant,
    pub frame: Frame,
}

/// Complete reply to an `execute`, rows included.
///
/// Unlike [`Client::execute`], statement errors and truncation are returned
/// to the caller instead of failing, so a harness can record them.
#[derive(Debug, Clone)]
pub struct Answer {
    /// When the final frame of the reply arrived.
    pub at: Instant,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// Failed statements of a multi-statement program, as sent by the server,
    /// or the single `{"message"}` of a whole-program `error` frame.
    pub errors: Vec<Value>,
    pub truncated: bool,
}

/// The row payload of `result`, `result_start` and `result_chunk` frames.
#[derive(Deserialize)]
struct Payload {
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<Value>>,
}

/// Successful reply to an `execute`.
#[derive(Debug, Clone, Copy)]
pub struct Reply {
    /// When the final frame of the reply arrived.
    pub at: Instant,
    pub row_count: usize,
}

pub struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Subscription frames that arrived while waiting for a reply.
    deferred: Vec<Stamped>,
}

impl Client {
    /// Connect to `/ws?kg=<kg>` and log in as the admin user.
    pub async fn connect(addr: SocketAddr, kg: &str, password: &str) -> Result<Self> {
        let url = format!("ws://{addr}/ws?kg={kg}");
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .with_context(|| format!("connect to {addr}"))?;
        let mut client = Self {
            ws,
            deferred: Vec::new(),
        };
        client
            .send(&json!({"type": "login", "username": ADMIN_USER, "password": password}))
            .await?;
        match client.next().await?.frame {
            Frame::Authenticated {} => Ok(client),
            Frame::AuthError { message } => bail!("login failed: {message}"),
            other => bail!("unexpected login reply: {other:?}"),
        }
    }

    /// Send one program without waiting; returns the send instant.
    pub async fn send_execute(&mut self, program: &str) -> Result<Instant> {
        let start = Instant::now();
        self.send(&json!({"type": "execute", "program": program}))
            .await?;
        Ok(start)
    }

    /// Send `program` and wait for its reply. A statement error or a
    /// truncated result is an error: fixtures must measure complete work.
    pub async fn execute(&mut self, program: &str) -> Result<(Instant, Reply)> {
        let start = self.send_execute(program).await?;
        let reply = self.reply().await.with_context(|| preview(program))?;
        Ok((start, reply))
    }

    /// Wait for the reply to the oldest outstanding `execute`.
    pub async fn reply(&mut self) -> Result<Reply> {
        loop {
            let Stamped { at, frame } = self.next().await?;
            match frame {
                Frame::Result {
                    row_count,
                    truncated,
                    errors,
                } => {
                    check_complete(truncated, &errors)?;
                    return Ok(Reply { at, row_count });
                }
                Frame::ResultStart { truncated, errors } => check_complete(truncated, &errors)?,
                Frame::ResultEnd { row_count } => return Ok(Reply { at, row_count }),
                Frame::Error { message } => bail!("server error: {message}"),
                frame @ (Frame::SubscriptionDelta { .. } | Frame::SubscriptionError { .. }) => {
                    self.deferred.push(Stamped { at, frame });
                }
                Frame::ResultChunk {} | Frame::Other => {}
                frame @ (Frame::Authenticated {} | Frame::AuthError { .. }) => {
                    bail!("unexpected frame while waiting for a reply: {frame:?}")
                }
            }
        }
    }

    /// Send `program` and collect its complete reply, rows included. A
    /// whole-program `error` frame becomes the answer's only error; only a
    /// transport failure is an `Err`. See [`Answer`].
    pub async fn query(&mut self, program: &str) -> Result<(Instant, Answer)> {
        let start = self.send_execute(program).await?;
        let mut answer = Answer {
            at: start,
            columns: Vec::new(),
            rows: Vec::new(),
            errors: Vec::new(),
            truncated: false,
        };
        loop {
            let (at, text) = self.next_text().await?;
            let frame = parse(&text)?;
            match frame {
                Frame::Result {
                    truncated, errors, ..
                } => {
                    let payload: Payload = serde_json::from_str(&text)?;
                    answer.columns = payload.columns;
                    answer.rows = payload.rows;
                    answer.errors = errors;
                    answer.truncated = truncated;
                    answer.at = at;
                    return Ok((start, answer));
                }
                Frame::ResultStart { truncated, errors } => {
                    answer.columns = serde_json::from_str::<Payload>(&text)?.columns;
                    answer.errors = errors;
                    answer.truncated = truncated;
                }
                Frame::ResultChunk {} => {
                    answer
                        .rows
                        .extend(serde_json::from_str::<Payload>(&text)?.rows);
                }
                Frame::ResultEnd { .. } => {
                    answer.at = at;
                    return Ok((start, answer));
                }
                Frame::Error { message } => {
                    answer.errors = vec![json!({ "message": message })];
                    answer.at = at;
                    return Ok((start, answer));
                }
                frame @ (Frame::SubscriptionDelta { .. } | Frame::SubscriptionError { .. }) => {
                    self.deferred.push(Stamped { at, frame });
                }
                Frame::Other => {}
                frame @ (Frame::Authenticated {} | Frame::AuthError { .. }) => {
                    bail!("unexpected frame while waiting for a reply: {frame:?}")
                }
            }
        }
    }

    /// Next subscription delta or error, including ones deferred by `reply`.
    pub async fn next_push(&mut self) -> Result<Stamped> {
        if !self.deferred.is_empty() {
            return Ok(self.deferred.remove(0));
        }
        loop {
            let stamped = self.next().await?;
            match stamped.frame {
                Frame::SubscriptionDelta { .. } | Frame::SubscriptionError { .. } => {
                    return Ok(stamped)
                }
                Frame::Error { message } => bail!("server error: {message}"),
                _ => {}
            }
        }
    }

    async fn send(&mut self, value: &Value) -> Result<()> {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .context("websocket send")
    }

    async fn next(&mut self) -> Result<Stamped> {
        let (at, text) = self.next_text().await?;
        Ok(Stamped {
            at,
            frame: parse(&text)?,
        })
    }

    /// Next text frame and the instant it was read, before any parsing.
    async fn next_text(&mut self) -> Result<(Instant, String)> {
        loop {
            let message = tokio::time::timeout(REPLY_TIMEOUT, self.ws.next())
                .await
                .map_err(|_| anyhow!("no frame within {REPLY_TIMEOUT:?}"))?
                .ok_or_else(|| anyhow!("connection closed"))?
                .context("websocket receive")?;
            let at = Instant::now();
            if let Message::Text(text) = message {
                return Ok((at, text));
            }
        }
    }
}

fn parse(text: &str) -> Result<Frame> {
    serde_json::from_str(text).with_context(|| format!("unparseable frame: {}", preview(text)))
}

fn check_complete(truncated: bool, errors: &[Value]) -> Result<()> {
    if let Some(error) = errors.first() {
        bail!("statement failed: {error}");
    }
    if truncated {
        bail!("result truncated");
    }
    Ok(())
}

fn preview(text: &str) -> String {
    text.chars().take(120).collect()
}
