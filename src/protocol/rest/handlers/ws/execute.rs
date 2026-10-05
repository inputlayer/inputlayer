//! Answering `execute`: running a program and building its reply frames.
//!
//! Every frame built here answers one request and carries its `id`. Frames
//! are returned encoded, for the connection loop to write when the request is
//! released (see the `pipeline` module).

use std::sync::Arc;
use std::time::Instant;

use inputlayer_ws_protocol::{
    ErrorCode, NamedQuery, NamedResult, Params, RequestId, ResultFrame, ServerFrame,
    SessionMetadata, SnapshotFrame, Subscribed,
};
use tracing::{info, warn};

use super::outbound::encode;
use super::stream;
use crate::auth::Principal;
use crate::execution::RequestControl;
use crate::protocol::handler::{ProgramError, ValidationError, VALIDATION_ERROR_PREFIX};
use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::subscription::Snapshot;
use crate::protocol::{Handler, QueryResult};

/// Results with more rows than this are serialized on the blocking pool, so
/// a large result never stalls the connection loop polling the request.
const INLINE_FRAME_ROWS: usize = 256;

/// Maximum characters of a program logged as a preview.
const LOG_PREVIEW_CHARS: usize = 80;

/// Run `program` with `params` in `session_id` as `auth` for the request
/// `id`, under `control`; returns its reply frames. The request log records
/// how many parameters it had, not their values.
pub(super) async fn execute(
    handler: Arc<Handler>,
    session_id: String,
    id: Option<RequestId>,
    program: String,
    params: Params,
    auth: Principal,
    control: Arc<RequestControl>,
) -> Vec<String> {
    let start = Instant::now();
    let program_len = program.len();
    let program_preview = log_preview(&program);
    let param_count = params.len();
    info!(program_len, param_count, program_preview = %program_preview, "ws_execute_start");
    let result = handler
        .execute_program_with_params(
            Some(&session_id),
            None,
            program,
            &params,
            Some(&auth),
            &control,
        )
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

/// Run the `read` of `queries` on the knowledge graph of `session_id` as
/// `auth` for the request `id`, under `control`; returns its reply frames: a
/// `snapshot`, or one `error` when any query fails.
pub(super) async fn read(
    handler: Arc<Handler>,
    session_id: String,
    id: Option<RequestId>,
    queries: Vec<NamedQuery>,
    auth: Principal,
    control: Arc<RequestControl>,
) -> Vec<String> {
    let start = Instant::now();
    info!(queries = queries.len(), "ws_read_start");
    let knowledge_graph = match handler.session_manager().session_kg(&session_id) {
        Ok(kg) => kg,
        Err(message) => return vec![encode(&ServerFrame::error(id, None, message))],
    };
    let result = handler
        .read_snapshot(&knowledge_graph, &queries, Some(&auth), &control)
        .await;
    let elapsed_ms = start.elapsed().as_millis() as u64;
    info!(elapsed_ms, ok = result.is_ok(), "ws_read_end");
    let read = match result {
        Ok(read) => read,
        Err(e) => return vec![encode(&program_error_frame(id, e))],
    };
    let rows: usize = read.results.iter().map(|r| r.rows.len()).sum();
    let reply_id = id.clone();
    let frame = move || {
        let results = queries
            .into_iter()
            .zip(read.results)
            .map(|(query, result)| NamedResult {
                name: query.name,
                columns: result.schema.into_iter().map(|c| c.name).collect(),
                rows: result
                    .rows
                    .into_iter()
                    .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
                    .collect(),
                total_count: result.total_count,
                truncated: result.truncated,
            })
            .collect();
        snapshot_reply_frames(SnapshotFrame {
            id: reply_id,
            knowledge_graph,
            revision: read.revision,
            results,
            execution_time_ms: elapsed_ms,
            subscribed: None,
        })
    };
    if rows <= INLINE_FRAME_ROWS {
        return frame();
    }
    tokio::task::spawn_blocking(frame)
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

/// Frames of the snapshot reply `frame`, or one `error` when it cannot be
/// delivered whole: never part of it.
fn snapshot_reply_frames(frame: SnapshotFrame) -> Vec<String> {
    let id = frame.id.clone();
    stream::snapshot_frames(frame).unwrap_or_else(|reason| {
        vec![encode(&ServerFrame::error(
            id,
            Some(ErrorCode::Internal),
            reason,
        ))]
    })
}

/// The `snapshot` replying to the group `subscribe` request `id`: one result
/// per member, named `members`.
pub(super) fn group_snapshot(
    id: Option<RequestId>,
    knowledge_graph: String,
    members: Vec<String>,
    snapshot: Snapshot,
    subscribed: Subscribed,
    started: Instant,
) -> SnapshotFrame {
    let results = members
        .into_iter()
        .zip(snapshot.results)
        .map(|(name, result)| NamedResult {
            name,
            columns: result.columns,
            total_count: result.rows.len(),
            rows: result.rows,
            // A subscription snapshot is complete by construction: a capped
            // result fails the `subscribe` instead.
            truncated: false,
        })
        .collect();
    SnapshotFrame {
        id,
        knowledge_graph,
        revision: snapshot.revision,
        results,
        execution_time_ms: started.elapsed().as_millis() as u64,
        subscribed: Some(subscribed),
    }
}

/// Frames of a successful program: one `result` frame, or streamed when
/// large (see [`stream::result_frames`]); an `error` for one that cannot be
/// delivered whole.
fn program_frames(id: Option<RequestId>, response: QueryResult) -> Vec<String> {
    match result_frame(id.clone(), response) {
        ServerFrame::Result(result) => reply_frames(id, result),
        other => vec![encode(&other)],
    }
}

/// Frames of the reply `result` to request `id`, or one `error` when it
/// cannot be delivered whole: never part of it.
pub(super) fn reply_frames(id: Option<RequestId>, result: ResultFrame) -> Vec<String> {
    stream::result_frames(result).unwrap_or_else(|reason| {
        vec![encode(&ServerFrame::error(
            id,
            Some(ErrorCode::Internal),
            reason,
        ))]
    })
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
        statements: response.statements,
        revision: response.revision,
        subscribed: None,
    })
}

/// The `result` of a subscription command. A `.subscribe` reply holds the
/// snapshot and names the subscription's generation.
pub(super) fn subscription_reply(
    id: Option<RequestId>,
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    subscribed: Option<Subscribed>,
    started: Instant,
) -> ResultFrame {
    ResultFrame {
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
        statements: Vec::new(),
        revision: None,
        subscribed,
    }
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
    const SUBCOMMANDS: [&str; 7] = [
        "list", "create", "drop", "password", "role", "revoke", "expire",
    ];
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
