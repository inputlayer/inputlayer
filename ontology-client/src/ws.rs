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
        #[serde(default)]
        errors: Option<Vec<StatementError>>,
    },
    ResultStart {
        columns: Vec<String>,
        #[serde(default)]
        errors: Option<Vec<StatementError>>,
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

/// A failed statement of a multi-statement program.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StatementError {
    /// 0-based statement index in the program.
    pub index: usize,
    /// `validation`, `not_found`, `conflict`, `unsupported` or `internal`.
    pub code: String,
    pub message: String,
}

pub struct QueryResult {
    #[allow(dead_code)]
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Engine-produced proof trees (present on `.why` results), passed
    /// through untouched.
    pub proof_trees: Option<serde_json::Value>,
    /// Failed statements; `None` from engines that do not report them.
    pub errors: Option<Vec<StatementError>>,
}

/// Every success message the engine writes as a message row. `*` stands for
/// a name or number (no `'`); a trailing `*` for the rest of the line.
const SUCCESS_SHAPES: &[&str] = &[
    "Inserted * fact(s) into '*'.",
    "Deleted * facts from '*'.",
    "Deleted * fact(s) from '*'.",
    "Conditional delete: * fact(s) deleted from '*'.",
    "Update: * deleted, * inserted.",
    "Schema for '*' registered with * columns (persistent)",
    "Schema for '*' registered with * columns (session)",
    "Session fact added for '*'. (Use +*(...) to persist)",
    "Session rule added for '*'.",
    "Rule '*' registered.",
    "Rule '*' dropped.",
    "Rule '*' cleared.",
    "Rule '*' deleted (last clause removed).",
    "Clause * removed from rule '*'.",
    "Dropped * rule(s) with prefix '*': *",
    "No rules matching prefix '*'.",
    "Type '*' declared.",
    "Relation '*' dropped.",
    "Relation '*' is empty.",
    "Cleared * fact(s) from * relation(s) with prefix '*': *",
    "No relations matching prefix '*'.",
    "Knowledge graph '*' created.",
    "Knowledge graph '*' dropped.",
    "Switched to knowledge graph: *",
    "Compaction complete.",
    "Index '*' created on * (* vectors).",
    "Index '*' dropped.",
    "Index '*' rebuilt (* vectors).",
    // `.ontology` command results.
    "installed * into * (* statements)",
    "digest *",
    "recorded * rule(s), * relation(s) in pack_item",
    "removed * from * (* rule(s), * relation(s))",
    "kept * item(s) shared with another installed pack",
    "upgraded * in * (dropped * old rule(s), data kept)",
];

/// Whether `text` has `shape` (see `SUCCESS_SHAPES`).
fn has_shape(shape: &str, text: &str) -> bool {
    let Some((literal, rest)) = shape.split_once('*') else {
        return shape == text;
    };
    let Some(text) = text.strip_prefix(literal) else {
        return false;
    };
    if rest.is_empty() {
        return !text.is_empty();
    }
    let span = text.find(['\'', '\n']).unwrap_or(text.len());
    (1..=span)
        .filter(|&end| text.is_char_boundary(end))
        .any(|end| has_shape(rest, &text[end..]))
}

impl QueryResult {
    /// Messages of the statements that failed. A failed single statement is
    /// an error frame instead, so `execute` already returned `Err`.
    ///
    /// Engines without `errors` report failures as message rows. For those
    /// this is DENY BY DEFAULT: a message row is a problem unless it has an
    /// exact `SUCCESS_SHAPES` shape, because reporting success for something
    /// that did not happen is the one failure this product must never have.
    pub fn soft_errors(&self) -> Vec<String> {
        if let Some(errors) = &self.errors {
            return errors.iter().map(|e| e.message.clone()).collect();
        }
        // Message rows only: real query results are not problem reports.
        if self.columns.len() != 1 || self.columns.first().map(String::as_str) != Some("message") {
            return Vec::new();
        }
        self.rows
            .iter()
            .filter_map(|row| row.first().and_then(|v| v.as_str()))
            .filter(|message| {
                let trimmed = message.trim();
                !trimmed.is_empty() && !SUCCESS_SHAPES.iter().any(|shape| has_shape(shape, trimmed))
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
                    errors,
                } => {
                    return Ok(QueryResult {
                        columns,
                        rows,
                        proof_trees,
                        errors,
                    });
                }
                WsResponse::ResultStart { columns, errors } => {
                    streamed = Some(QueryResult {
                        columns,
                        rows: Vec::new(),
                        proof_trees: None,
                        errors,
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn messages(rows: &[&str], errors: Option<Vec<StatementError>>) -> QueryResult {
        QueryResult {
            columns: vec!["message".to_string()],
            rows: rows.iter().map(|m| vec![serde_json::json!(m)]).collect(),
            proof_trees: None,
            errors,
        }
    }

    /// Every message row the engine writes for a statement that succeeded.
    const ENGINE_SUCCESSES: &[&str] = &[
        "Inserted 2 fact(s) into 'edge'.",
        "Deleted 1 facts from 'edge'.",
        "Deleted 3 fact(s) from 'edge'.",
        "Conditional delete: 4 fact(s) deleted from 'edge'.",
        "Update: 1 deleted, 1 inserted.",
        "Schema for 'person' registered with 2 columns (persistent)",
        "Schema for 'person' registered with 2 columns (session)",
        "Session fact added for 'edge'. (Use +edge(...) to persist)",
        "Session rule added for 'path'.",
        "Rule 'path' registered.",
        "Rule 'path' dropped.",
        "Rule 'path' cleared.",
        "Rule 'path' deleted (last clause removed).",
        "Clause 2 removed from rule 'path'.",
        "Dropped 2 rule(s) with prefix 'tmp_': tmp_a, tmp_b",
        "No rules matching prefix 'tmp_'.",
        "Type 'Email' declared.",
        "Relation 'edge' dropped.",
        "Relation 'edge' is empty.",
        "Cleared 5 fact(s) from 2 relation(s) with prefix 'tmp_': tmp_a (2), tmp_b (3)",
        "No relations matching prefix 'tmp_'.",
        "Knowledge graph 'kg2' created.",
        "Knowledge graph 'kg2' dropped.",
        "Switched to knowledge graph: kg2",
        "Compaction complete.",
        "Index 'emb_idx' created on doc.embedding (10 vectors).",
        "Index 'emb_idx' dropped.",
        "Index 'emb_idx' rebuilt (10 vectors).",
        "installed retail@1.2.0 into default (12 statements)",
        "digest sha256:abc123",
        "recorded 3 rule(s), 4 relation(s) in pack_item",
        "removed retail from default (3 rule(s), 4 relation(s))",
        "  kept 1 item(s) shared with another installed pack",
        "upgraded retail in default (dropped 3 old rule(s), data kept)",
        "  Rule 'path' dropped.",
    ];

    /// Every message row the engine writes for a statement that failed.
    const ENGINE_FAILURES: &[&str] = &[
        "Relation 'edge' not found.",
        "Rule 'path' not found.",
        "Rule 'path' not found: Failed to drop rule: Rule 'path' not found",
        "'path' not found as rule.",
        "Knowledge graph 'kg2' not found: Knowledge graph not found: kg2",
        "Create failed: Knowledge graph already exists: kg2",
        "Drop failed: Cannot drop the default knowledge graph",
        "Cannot drop current knowledge graph. Switch to another first.",
        "String value too long: 70000 bytes (max 65536)",
        "Insert rejected for 'edge': 20000 tuples exceeds max 10000",
        "Insert rejected for 'edge': arity mismatch",
        "Failed to register schema for 'person': conflicting column types",
        "Fact must have at least one argument",
        "Delete error: unsupported term",
        "Error: Failed to drop relation: Relation 'edge' not found.",
        "Compaction error: I/O error: disk full",
        "Debug error: unknown relation",
        "Why error: no derivation",
        "Why-not error: unknown relation",
        "Index error: Index 'emb_idx' not found",
        "Rule editing is not supported in server mode.",
        "Agent commands require async context. Use the GUI chat panel.",
        "Session commands require a WebSocket connection.",
        "User/API key commands require a WebSocket connection with admin privileges.",
        "KG ACL commands require a WebSocket connection with admin or owner privileges.",
        ".ontology commands must be run as a standalone statement.",
        ".subscribe and .unsubscribe are only available as standalone commands on the global /ws endpoint.",
        "This command is client-only and not available via server API.",
        "  warning: Relation 'pack_item' not found.",
        "Relation 'edge' dropped. Rule 'path' not found.",
        "Inserted 2 fact(s) into 'edge'. Insert rejected for 'b': arity mismatch",
    ];

    #[test]
    fn legacy_fallback_accepts_every_engine_success() {
        for message in ENGINE_SUCCESSES {
            let result = messages(&[message], None);
            assert!(
                result.soft_errors().is_empty(),
                "flagged success: {message}"
            );
        }
    }

    #[test]
    fn legacy_fallback_flags_every_engine_failure() {
        for message in ENGINE_FAILURES {
            let result = messages(&[message], None);
            assert_eq!(
                result.soft_errors(),
                vec![message.to_string()],
                "missed failure: {message}"
            );
        }
    }

    #[test]
    fn legacy_fallback_flags_unknown_messages() {
        let result = messages(&["Something new happened", ""], None);
        assert_eq!(result.soft_errors(), vec!["Something new happened"]);
    }

    #[test]
    fn structured_errors_are_authoritative() {
        let error = StatementError {
            index: 1,
            code: "not_found".to_string(),
            message: "Rule 'path' not found.".to_string(),
        };
        let result = messages(
            &["Inserted 1 fact(s) into 'a'.", &error.message],
            Some(vec![error.clone()]),
        );
        assert_eq!(result.soft_errors(), vec![error.message]);

        let result = messages(&["Something new happened"], Some(Vec::new()));
        assert!(result.soft_errors().is_empty());
    }

    #[test]
    fn query_rows_are_not_problems() {
        let result = QueryResult {
            columns: vec!["x".to_string()],
            rows: vec![vec![serde_json::json!("Rule 'p' not found.")]],
            proof_trees: None,
            errors: None,
        };
        assert!(result.soft_errors().is_empty());
    }

    #[test]
    fn result_frames_carry_errors() {
        let frame = r#"{"type":"result","columns":["message"],"rows":[],
            "errors":[{"index":1,"code":"validation","message":"String value too long"}]}"#;
        let WsResponse::Result { errors, .. } = serde_json::from_str(frame).unwrap() else {
            panic!("not a result");
        };
        assert_eq!(
            errors,
            Some(vec![StatementError {
                index: 1,
                code: "validation".to_string(),
                message: "String value too long".to_string(),
            }])
        );

        let frame = r#"{"type":"result","columns":[],"rows":[]}"#;
        let WsResponse::Result { errors, .. } = serde_json::from_str(frame).unwrap() else {
            panic!("not a result");
        };
        assert_eq!(errors, None);
    }
}
