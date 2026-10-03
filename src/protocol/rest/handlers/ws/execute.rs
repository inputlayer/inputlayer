//! Answering `execute`: running a program and building its reply frames.
//!
//! Every frame built here answers one request and carries its `id`. Frames
//! are returned encoded, for the connection loop to write when the request is
//! released (see the `pipeline` module).

use std::sync::Arc;
use std::time::Instant;

use inputlayer_ws_protocol::{
    ErrorCode, RequestId, ResultFrame, ResultStartFrame, ServerFrame, SessionMetadata, Subscribed,
};
use tracing::{info, warn};

use super::outbound::encode;
use crate::auth::Principal;
use crate::protocol::handler::{ProgramError, ValidationError, VALIDATION_ERROR_PREFIX};
use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::{Handler, QueryResult};

/// Results whose single-frame JSON exceeds this many bytes are streamed as
/// `result_start` / `result_chunk` / `result_end`.
const STREAMING_THRESHOLD: usize = 1024 * 1024; // 1 MB

/// Maximum number of rows per `result_chunk`.
const STREAMING_CHUNK_ROWS: usize = 500;

/// Results with more rows than this are serialized on the blocking pool, so
/// a large result never stalls the connection loop polling the request.
const INLINE_FRAME_ROWS: usize = 256;

/// Maximum characters of a program logged as a preview.
const LOG_PREVIEW_CHARS: usize = 80;

/// Run `program` in `session_id` as `auth` for the request `id`; returns its
/// reply frames.
pub(super) async fn execute(
    handler: Arc<Handler>,
    session_id: String,
    id: Option<RequestId>,
    program: String,
    auth: Principal,
) -> Vec<String> {
    let start = Instant::now();
    let program_len = program.len();
    let program_preview = log_preview(&program);
    info!(program_len, program_preview = %program_preview, "ws_execute_start");
    let result = handler
        .execute_program_status(Some(&session_id), None, program, Some(&auth))
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
    match result {
        Ok(response) if response.rows.len() <= INLINE_FRAME_ROWS => program_frames(id, response),
        Ok(response) => {
            let reply_id = id.clone();
            tokio::task::spawn_blocking(move || program_frames(reply_id, response))
                .await
                .unwrap_or_else(|e| {
                    tracing::error!(error = %e, "ws_result_serialization_failed");
                    let message = "Internal server error".to_string();
                    vec![encode(&ServerFrame::error(
                        id,
                        Some(ErrorCode::Internal),
                        message,
                    ))]
                })
        }
        Err(e) => vec![encode(&program_error_frame(id, e))],
    }
}

/// Frames of a successful program: one `result` frame, or for results over
/// [`STREAMING_THRESHOLD`] a `result_start`, `result_chunk`s of up to
/// [`STREAMING_CHUNK_ROWS`] rows and a `result_end`.
fn program_frames(id: Option<RequestId>, response: QueryResult) -> Vec<String> {
    let frame = result_frame(id, response);
    let json = match serde_json::to_string(&frame) {
        Ok(json) => json,
        // Reports the failure to the client.
        Err(_) => return vec![encode(&frame)],
    };
    if json.len() <= STREAMING_THRESHOLD {
        return vec![json];
    }
    let json_size = json.len();
    drop(json);
    let ServerFrame::Result(result) = frame else {
        // Only a result frame can be this large.
        return vec![encode(&frame)];
    };
    info!(
        row_count = result.row_count,
        json_size, "ws_streaming_result"
    );
    stream_frames(result)
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

/// `result` as `result_start`, chunks and `result_end`.
fn stream_frames(result: ResultFrame) -> Vec<String> {
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
    let mut frames = vec![encode(&ServerFrame::ResultStart(ResultStartFrame {
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
    }))];
    let mut chunk_count = 0;
    let mut rows = rows.into_iter();
    let mut provenance = row_provenance.into_iter();
    loop {
        let chunk: Vec<_> = rows.by_ref().take(STREAMING_CHUNK_ROWS).collect();
        if chunk.is_empty() {
            break;
        }
        frames.push(encode(&ServerFrame::ResultChunk {
            id: id.clone(),
            row_provenance: provenance.by_ref().take(chunk.len()).collect(),
            rows: chunk,
            chunk_index: chunk_count,
        }));
        chunk_count += 1;
    }
    frames.push(encode(&ServerFrame::ResultEnd {
        id,
        row_count,
        chunk_count,
    }));
    frames
}

/// The `result` frame of a subscription command. A `.subscribe` reply holds
/// the snapshot and names the subscription's generation.
pub(super) fn subscription_reply(
    id: Option<RequestId>,
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    subscribed: Option<Subscribed>,
    started: Instant,
) -> ServerFrame {
    ServerFrame::Result(ResultFrame {
        id,
        columns,
        row_count: rows.len(),
        total_count: rows.len(),
        rows,
        // A subscription snapshot is complete by construction: a capped
        // result fails the `.subscribe` instead.
        truncated: false,
        execution_time_ms: started.elapsed().as_millis() as u64,
        row_provenance: Vec::new(),
        metadata: None,
        switched_kg: None,
        proof_trees: None,
        timing_breakdown: None,
        errors: Vec::new(),
        subscribed,
    })
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
