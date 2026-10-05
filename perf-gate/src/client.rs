//! A minimal `/ws` client, speaking the protocol exactly as an agent does.
//!
//! Every frame is stamped with [`Instant::now`] the moment it is read, before
//! parsing, so client-side JSON work never inflates a measured latency.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, Stream, StreamExt};
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
        /// The revision a program's committed writes are visible at.
        #[serde(default)]
        revision: Option<u64>,
    },
    ResultStart {
        #[serde(default)]
        truncated: bool,
        #[serde(default)]
        errors: Vec<Value>,
        #[serde(default)]
        revision: Option<u64>,
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
        /// The revision whose result this delta produces.
        #[serde(default)]
        revision: u64,
        #[serde(default)]
        inserted: Vec<Vec<Value>>,
        #[serde(default)]
        retracted: Vec<Vec<Value>>,
    },
    /// Header of a delta streamed in chunks; it applies at the matching
    /// [`Frame::SubscriptionDeltaEnd`].
    SubscriptionDeltaStart {
        revision: u64,
    },
    SubscriptionDeltaEnd {},
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
    /// The revision the program's committed writes are visible at, for a
    /// program that wrote.
    pub revision: Option<u64>,
}

pub struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Subscription frames that arrived while waiting for a reply.
    deferred: Vec<Stamped>,
    /// Requests sent so far; each carries the next number as its `id`, as SDK
    /// requests do. Engines before protocol v2 ignore it.
    sent: u64,
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
            sent: 0,
        };
        client
            .send(json!({"type": "login", "username": ADMIN_USER, "password": password}))
            .await?;
        match client.next().await?.frame {
            Frame::Authenticated {} => Ok(client),
            Frame::AuthError { message } => bail!("login failed: {message}"),
            other => bail!("unexpected login reply: {other:?}"),
        }
    }

    /// Connect to `/ws?kg=<kg>` and authenticate with an API key. Many
    /// connections from one host log in this way: concurrent password logins
    /// from one address are throttled by design.
    pub async fn connect_with_key(addr: SocketAddr, kg: &str, api_key: &str) -> Result<Self> {
        let url = format!("ws://{addr}/ws?kg={kg}");
        let (ws, _) = tokio_tungstenite::connect_async(url)
            .await
            .with_context(|| format!("connect to {addr}"))?;
        let mut client = Self {
            ws,
            deferred: Vec::new(),
            sent: 0,
        };
        client
            .send(json!({"type": "authenticate", "api_key": api_key}))
            .await?;
        match client.next().await?.frame {
            Frame::Authenticated {} => Ok(client),
            Frame::AuthError { message } => bail!("authentication failed: {message}"),
            other => bail!("unexpected authentication reply: {other:?}"),
        }
    }

    /// Send one program without waiting; returns the send instant.
    pub async fn send_execute(&mut self, program: &str) -> Result<Instant> {
        let start = Instant::now();
        self.send(json!({"type": "execute", "program": program}))
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
        read_reply(&mut self.ws, &mut self.deferred).await
    }

    pub async fn execute_schedule(
        &mut self,
        programs: &[String],
        interval: Duration,
    ) -> Result<Vec<(Instant, Reply)>> {
        let (mut sender, mut receiver) = (&mut self.ws).split();
        let origin = tokio::time::Instant::now();
        let sends = async {
            let mut scheduled = Vec::with_capacity(programs.len());
            for (index, program) in programs.iter().enumerate() {
                let due = origin + interval * u32::try_from(index)?;
                tokio::time::sleep_until(due).await;
                sender
                    .send(Message::Text(
                        json!({"type": "execute", "program": program}).to_string(),
                    ))
                    .await
                    .context("websocket send")?;
                scheduled.push(due.into_std());
            }
            Ok::<_, anyhow::Error>(scheduled)
        };
        let replies = async {
            let mut replies = Vec::with_capacity(programs.len());
            for program in programs {
                replies.push(
                    read_reply(&mut receiver, &mut self.deferred)
                        .await
                        .with_context(|| preview(program))?,
                );
            }
            Ok::<_, anyhow::Error>(replies)
        };
        let (scheduled, replies) = tokio::try_join!(sends, replies)?;
        Ok(scheduled.into_iter().zip(replies).collect())
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
                Frame::ResultStart {
                    truncated, errors, ..
                } => {
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
                frame @ (Frame::SubscriptionDelta { .. }
                | Frame::SubscriptionDeltaStart { .. }
                | Frame::SubscriptionDeltaEnd { .. }
                | Frame::SubscriptionError { .. }) => {
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
        loop {
            let stamped = self.next_frame().await?;
            match stamped.frame {
                Frame::SubscriptionDelta { .. } | Frame::SubscriptionError { .. } => {
                    return Ok(stamped)
                }
                Frame::Error { message } => bail!("server error: {message}"),
                _ => {}
            }
        }
    }

    /// Next frame of any kind (notifications included), starting with the
    /// pushes deferred by `reply`; for a connection that sends nothing more.
    pub async fn next_frame(&mut self) -> Result<Stamped> {
        if !self.deferred.is_empty() {
            return Ok(self.deferred.remove(0));
        }
        self.next().await
    }

    async fn send(&mut self, mut value: Value) -> Result<()> {
        self.sent += 1;
        value["id"] = json!(self.sent.to_string());
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
        next_text(&mut self.ws).await
    }
}

async fn read_reply<S>(ws: &mut S, deferred: &mut Vec<Stamped>) -> Result<Reply>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let mut revision = None;
    loop {
        let (at, text) = next_text(ws).await?;
        let frame = parse(&text)?;
        match frame {
            Frame::Result {
                row_count,
                truncated,
                errors,
                revision,
            } => {
                check_complete(truncated, &errors)?;
                return Ok(Reply {
                    at,
                    row_count,
                    revision,
                });
            }
            Frame::ResultStart {
                truncated,
                errors,
                revision: start_revision,
            } => {
                check_complete(truncated, &errors)?;
                revision = start_revision;
            }
            Frame::ResultEnd { row_count } => {
                return Ok(Reply {
                    at,
                    row_count,
                    revision,
                })
            }
            Frame::Error { message } => bail!("server error: {message}"),
            frame @ (Frame::SubscriptionDelta { .. }
            | Frame::SubscriptionDeltaStart { .. }
            | Frame::SubscriptionDeltaEnd { .. }
            | Frame::SubscriptionError { .. }) => {
                deferred.push(Stamped { at, frame });
            }
            Frame::ResultChunk {} | Frame::Other => {}
            frame @ (Frame::Authenticated {} | Frame::AuthError { .. }) => {
                bail!("unexpected frame while waiting for a reply: {frame:?}")
            }
        }
    }
}

async fn next_text<S>(ws: &mut S) -> Result<(Instant, String)>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let message = tokio::time::timeout(REPLY_TIMEOUT, ws.next())
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
