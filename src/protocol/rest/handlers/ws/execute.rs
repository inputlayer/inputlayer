//! Answering `execute`: programs, streamed results and subscription commands.
//!
//! Every frame sent here answers one request and carries its `id`.

use std::sync::Arc;

use axum::extract::ws::Message;
use inputlayer_ws_protocol::{
    ErrorCode, RequestId, ResultFrame, ResultStartFrame, ServerFrame, SessionMetadata, Subscribed,
};
use tracing::{info, warn};

use super::outbound::Outbound;
use crate::auth::Principal;
use crate::protocol::handler::{ProgramError, ValidationError, VALIDATION_ERROR_PREFIX};
use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::subscription::ConnectionSubscriptions;
use crate::protocol::{Handler, QueryResult};
use crate::statement::{MetaCommand, Statement};

/// Results whose single-frame JSON exceeds this many bytes are streamed as
/// `result_start` / `result_chunk` / `result_end`.
const STREAMING_THRESHOLD: usize = 1024 * 1024; // 1 MB

/// Maximum number of rows per `result_chunk`.
const STREAMING_CHUNK_ROWS: usize = 500;

/// Maximum characters of a program logged as a preview.
const LOG_PREVIEW_CHARS: usize = 80;

/// Run `program` for the request `id` and send its reply. Returns `false` if
/// the connection is dead.
pub(super) async fn execute(
    handler: &Arc<Handler>,
    session_id: &str,
    id: Option<RequestId>,
    program: String,
    auth: &Principal,
    sender: &mut Outbound,
    subscriptions: &mut ConnectionSubscriptions,
) -> bool {
    if let Some(command) = subscription_command(&program) {
        return send_subscription_command(handler, session_id, id, command, subscriptions, sender)
            .await;
    }
    let session_kg = || {
        handler
            .session_manager()
            .session_kg(&session_id.to_string())
            .ok()
    };
    let kg_before = session_kg();
    let alive = send_program(handler, session_id, id, program, auth, sender).await;
    // Subscriptions are scoped to the connection's KG: switching drops them.
    if session_kg() != kg_before {
        subscriptions.clear();
    }
    alive
}

/// Extract `.subscribe` / `.unsubscribe` from a program.
fn subscription_command(program: &str) -> Option<MetaCommand> {
    let trimmed = program.trim();
    if !trimmed.starts_with(".subscribe") && !trimmed.starts_with(".unsubscribe") {
        return None;
    }
    match crate::statement::parse_statement(trimmed) {
        Ok(Statement::Meta(
            command @ (MetaCommand::Subscribe { .. } | MetaCommand::Unsubscribe(_)),
        )) => Some(command),
        _ => None,
    }
}

/// Run `.subscribe` / `.unsubscribe` against this connection's subscriptions.
/// A `.subscribe` reply is a `result` holding the snapshot and naming the
/// subscription's generation.
async fn send_subscription_command(
    handler: &Arc<Handler>,
    session_id: &str,
    id: Option<RequestId>,
    command: MetaCommand,
    subscriptions: &mut ConnectionSubscriptions,
    sender: &mut Outbound,
) -> bool {
    let start = std::time::Instant::now();
    let outcome = match command {
        MetaCommand::Subscribe {
            id: subscription,
            query,
        } => match handler
            .session_manager()
            .session_kg(&session_id.to_string())
        {
            Ok(kg) => subscriptions
                .subscribe(&kg, &subscription, &query)
                .await
                .map(|(snapshot, generation)| {
                    let subscribed = Subscribed {
                        subscription,
                        generation,
                    };
                    (snapshot.columns, snapshot.inserted, Some(subscribed))
                }),
            Err(e) => Err(e),
        },
        MetaCommand::Unsubscribe(subscription) => {
            subscriptions.unsubscribe(&subscription).map(|()| {
                let message = format!("Unsubscribed '{subscription}'.");
                (
                    vec!["message".to_string()],
                    vec![vec![serde_json::Value::String(message)]],
                    None,
                )
            })
        }
        _ => Err("Not a subscription command".to_string()),
    };
    let frame = match outcome {
        Ok((columns, rows, subscribed)) => ServerFrame::Result(ResultFrame {
            id,
            columns,
            row_count: rows.len(),
            total_count: rows.len(),
            rows,
            // A subscription snapshot is complete by construction: a capped
            // result fails the `.subscribe` instead.
            truncated: false,
            execution_time_ms: start.elapsed().as_millis() as u64,
            row_provenance: Vec::new(),
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
            subscribed,
        }),
        Err(message) => ServerFrame::error(id, None, message),
    };
    sender.send_frame(&frame).await
}

/// Run a program and send its result: one `result` frame, or for results
/// over [`STREAMING_THRESHOLD`] a `result_start`, `result_chunk`s of up to
/// [`STREAMING_CHUNK_ROWS`] rows and a `result_end`.
async fn send_program(
    handler: &Arc<Handler>,
    session_id: &str,
    id: Option<RequestId>,
    program: String,
    auth: &Principal,
    sender: &mut Outbound,
) -> bool {
    let start = std::time::Instant::now();
    let program_len = program.len();
    let program_preview = log_preview(&program);
    info!(program_len, program_preview = %program_preview, "ws_execute_start");
    let sid = session_id.to_string();
    let result = handler
        .execute_program_status(Some(&sid), None, program, Some(auth))
        .await;
    let elapsed_ms = start.elapsed().as_millis() as u64;
    let slow_query_ms = handler.config().storage.performance.slow_query_log_ms;
    if slow_query_ms > 0 && elapsed_ms >= slow_query_ms {
        warn!(
            elapsed_ms,
            threshold_ms = slow_query_ms,
            program_preview = %program_preview,
            "ws_slow_execute"
        );
    }
    info!(
        program_len,
        elapsed_ms,
        ok = result.is_ok(),
        "ws_execute_end"
    );

    let frame = match result {
        Ok(response) => result_frame(id, response),
        Err(e) => program_error_frame(id, e),
    };
    match frame {
        ServerFrame::Result(result) => send_result(sender, result).await,
        other => sender.send_frame(&other).await,
    }
}

/// Send `result` in one frame, or streamed when over [`STREAMING_THRESHOLD`].
async fn send_result(sender: &mut Outbound, result: ResultFrame) -> bool {
    let frame = ServerFrame::Result(result);
    let json = match serde_json::to_string(&frame) {
        Ok(json) => json,
        // Reports the failure to the client.
        Err(_) => return sender.send_frame(&frame).await,
    };
    if json.len() <= STREAMING_THRESHOLD {
        return sender.send(Message::Text(json)).await.is_ok();
    }
    let json_size = json.len();
    drop(json);
    let ServerFrame::Result(result) = frame else {
        unreachable!("constructed as a result above");
    };
    info!(
        row_count = result.row_count,
        json_size, "ws_streaming_result"
    );
    stream_result(sender, result).await
}

/// The single-frame reply for a program's result.
fn result_frame(id: Option<RequestId>, response: QueryResult) -> ServerFrame {
    let proof_trees = match response
        .proof_trees
        .map(|trees| trees.iter().map(serde_json::to_value).collect())
        .transpose()
    {
        Ok(trees) => trees,
        Err(e) => {
            tracing::error!(error = %e, "ws_proof_tree_serialize_failed");
            let message = "Internal server error".to_string();
            return ServerFrame::error(id, Some(ErrorCode::Internal), message);
        }
    };
    let mut row_provenance = Vec::with_capacity(response.rows.len());
    let mut rows = Vec::with_capacity(response.rows.len());
    for row in response.rows {
        row_provenance.push(
            row.provenance
                .as_ref()
                .map_or_else(|| "unknown".to_string(), ToString::to_string),
        );
        rows.push(row.values.into_iter().map(wire_value_to_json).collect());
    }
    ServerFrame::Result(ResultFrame {
        id,
        columns: response.schema.into_iter().map(|c| c.name).collect(),
        row_count: rows.len(),
        rows,
        total_count: response.total_count,
        truncated: response.truncated,
        execution_time_ms: response.execution_time_ms,
        row_provenance,
        metadata: response.metadata.map(|m| SessionMetadata {
            has_ephemeral: m.has_ephemeral,
            ephemeral_sources: m.ephemeral_sources,
            warnings: m.warnings,
        }),
        switched_kg: response.switched_kg,
        proof_trees,
        timing_breakdown: response.timing_breakdown,
        errors: response.errors,
        subscribed: None,
    })
}

/// Send `result` as `result_start`, chunks and `result_end`.
async fn stream_result(sender: &mut Outbound, result: ResultFrame) -> bool {
    let ResultFrame {
        id,
        columns,
        rows,
        row_count,
        total_count,
        truncated,
        execution_time_ms,
        row_provenance,
        metadata,
        switched_kg,
        proof_trees,
        timing_breakdown,
        errors,
        subscribed: _,
    } = result;
    let start = ServerFrame::ResultStart(ResultStartFrame {
        id: id.clone(),
        columns,
        total_count,
        truncated,
        execution_time_ms,
        metadata,
        switched_kg,
        proof_trees,
        timing_breakdown,
        errors,
    });
    if !sender.send_frame(&start).await {
        return false;
    }
    let mut chunk_count = 0;
    let mut rows = rows.into_iter();
    let mut provenance = row_provenance.into_iter();
    loop {
        let chunk: Vec<_> = rows.by_ref().take(STREAMING_CHUNK_ROWS).collect();
        if chunk.is_empty() {
            break;
        }
        let frame = ServerFrame::ResultChunk {
            id: id.clone(),
            row_provenance: provenance.by_ref().take(chunk.len()).collect(),
            rows: chunk,
            chunk_index: chunk_count,
        };
        if !sender.send_frame(&frame).await {
            return false;
        }
        chunk_count += 1;
    }
    let end = ServerFrame::ResultEnd {
        id,
        row_count,
        chunk_count,
    };
    sender.send_frame(&end).await
}

/// The `error` frame for a failed program, unpacking parse errors.
fn program_error_frame(id: Option<RequestId>, error: ProgramError) -> ServerFrame {
    if let Some(json_str) = error.message.strip_prefix(VALIDATION_ERROR_PREFIX) {
        if let Ok(errors) = serde_json::from_str::<Vec<ValidationError>>(json_str) {
            return ServerFrame::Error {
                id,
                message: format!("Program has {} parse error(s)", errors.len()),
                validation_errors: Some(errors),
                code: Some(ErrorCode::Validation),
            };
        }
    }
    ServerFrame::error(id, error.code, error.message)
}

/// Credential-free log preview of a program: the first line, truncated to
/// [`LOG_PREVIEW_CHARS`] characters. `.user` and `.apikey` commands carry
/// secrets, so only their kind is kept.
fn log_preview(program: &str) -> String {
    const SECRET_COMMANDS: [&str; 2] = ["user", "apikey"];
    const SUBCOMMANDS: [&str; 6] = ["list", "create", "drop", "password", "role", "revoke"];
    for line in program.lines() {
        let Some(meta) = line.trim_start().strip_prefix('.') else {
            continue;
        };
        let mut words = meta.trim_start_matches('.').split_whitespace();
        let find = |word: Option<&str>, names: &[&'static str]| {
            word.and_then(|w| names.iter().copied().find(|n| w.eq_ignore_ascii_case(n)))
        };
        let Some(cmd) = find(words.next(), &SECRET_COMMANDS) else {
            continue;
        };
        return match find(words.next(), &SUBCOMMANDS) {
            Some(sub) => format!(".{cmd} {sub} <redacted>"),
            None => format!(".{cmd} <redacted>"),
        };
    }
    program
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .chars()
        .take(LOG_PREVIEW_CHARS)
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
