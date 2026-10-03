//! Requests of an authenticated `/ws` connection, as pipeline jobs.
//!
//! [`Job::from_text`] classifies a client message by the state it touches
//! ([`Access`]); the connection loop starts the job once the pipeline's
//! barriers allow and writes its [`Reply`] when released. Request futures are
//! polled by the connection loop, so nothing here may block it: computation
//! runs on the blocking pool, and so does serializing a large result.

use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use super::pipeline::Access;
use super::{log_preview, response_frame, GlobalWsRequest, GlobalWsResponse};
use super::{STREAMING_CHUNK_ROWS, STREAMING_THRESHOLD};
use crate::auth::Principal;
use crate::protocol::handler::{
    is_query_program, ProgramError, ValidationError, VALIDATION_ERROR_PREFIX,
};
use crate::protocol::rest::dto::SessionQueryMetadataDto;
use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::subscription::connection::Opened;
use crate::protocol::wire::{ErrorCode, QueryResult};
use crate::protocol::Handler;
use crate::protocol::MAX_MESSAGE_SIZE;
use crate::statement::{MetaCommand, Statement};

/// Results with more rows than this are serialized on the blocking pool.
const INLINE_FRAME_ROWS: usize = 256;

/// One client request.
pub(super) enum Job {
    /// Answered without running anything (pong, malformed message).
    Immediate(GlobalWsResponse),
    /// An IQL program through the handler.
    Execute { program: String },
    /// `.subscribe <id> ?<query>` on the connection's KG.
    Subscribe { id: String, query: String },
    /// `.unsubscribe <id>`.
    Unsubscribe { id: String },
}

/// A finished request, ready to be written.
pub(super) enum Reply {
    /// Serialized frames.
    Frames(Vec<String>),
    /// An evaluated subscription snapshot, registered by the loop on release.
    Subscribed { opened: Opened, started: Instant },
}

impl Job {
    /// Parse a client message of an authenticated connection.
    pub(super) fn from_text(text: &str) -> (Access, Self) {
        let request = match serde_json::from_str::<GlobalWsRequest>(text) {
            Ok(request) => request,
            Err(e) => {
                tracing::debug!(error = %e, "Invalid GlobalWsRequest message");
                return Self::immediate(error_response("Invalid message format".to_string()));
            }
        };
        match request {
            GlobalWsRequest::Execute { program } => match subscription_command(&program) {
                Some(MetaCommand::Subscribe { id, query }) => {
                    (Access::Exclusive, Self::Subscribe { id, query })
                }
                Some(MetaCommand::Unsubscribe(id)) => (Access::Exclusive, Self::Unsubscribe { id }),
                _ => (program_access(&program), Self::Execute { program }),
            },
            GlobalWsRequest::Ping => Self::immediate(GlobalWsResponse::Pong),
            GlobalWsRequest::Login { .. } | GlobalWsRequest::Authenticate { .. } => {
                Self::immediate(error_response("Already authenticated".to_string()))
            }
        }
    }

    /// A reply that touches no state, released in order with the others.
    pub(super) fn immediate(response: GlobalWsResponse) -> (Access, Self) {
        (Access::Shared, Self::Immediate(response))
    }
}

/// Programs made only of queries read the connection's KG and session
/// without changing either, so they may overlap; a malformed query fails
/// without effect. Anything else (writes, rules, session facts, meta
/// commands) runs alone.
fn program_access(program: &str) -> Access {
    // The prefix check skips the statement split for writes and meta commands.
    if program.trim_start().starts_with('?') && is_query_program(program) {
        Access::Shared
    } else {
        Access::Exclusive
    }
}

/// Extract `.subscribe` / `.unsubscribe` from an Execute program.
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

/// An `error` frame without code or validation details.
pub(super) fn error_response(message: String) -> GlobalWsResponse {
    GlobalWsResponse::Error {
        message,
        validation_errors: None,
        code: None,
    }
}

/// The `result` frame of a subscription command.
pub(super) fn message_rows(
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    started: Instant,
) -> GlobalWsResponse {
    GlobalWsResponse::Result {
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
    }
}

/// Run `program` in `session_id` as `auth`; returns its reply frames.
pub(super) async fn execute(
    handler: Arc<Handler>,
    session_id: String,
    program: String,
    auth: Principal,
) -> Vec<String> {
    let start = Instant::now();
    let program_len = program.len();
    let program_preview = log_preview(&program);
    info!(
        program_len,
        program_preview = %program_preview,
        "ws_execute_start"
    );
    let result = handler
        .execute_program_status(Some(&session_id), None, program, Some(&auth))
        .await;
    let elapsed = start.elapsed();
    let slow_query_ms = handler.config().storage.performance.slow_query_log_ms;
    if slow_query_ms > 0 && elapsed.as_millis() as u64 >= slow_query_ms {
        warn!(
            elapsed_ms = elapsed.as_millis() as u64,
            threshold_ms = slow_query_ms,
            program_preview = %program_preview,
            "ws_slow_execute"
        );
    }
    info!(
        program_len,
        elapsed_ms = elapsed.as_millis() as u64,
        ok = result.is_ok(),
        "ws_execute_end"
    );
    match result {
        Ok(response) if response.rows.len() <= INLINE_FRAME_ROWS => result_frames(response),
        Ok(response) => tokio::task::spawn_blocking(move || result_frames(response))
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "ws_result_serialization_failed");
                vec![response_frame(&error_response(
                    "Internal server error".to_string(),
                ))]
            }),
        Err(e) => vec![response_frame(&program_error_response(e))],
    }
}

/// Frames of a successful program.
///
/// A result whose single-message JSON is at most [`STREAMING_THRESHOLD`]
/// bytes is one `result` frame. A larger one is streamed:
/// 1. `result_start` - schema, metadata, totals
/// 2. `result_chunk` (×N) - batches of up to [`STREAMING_CHUNK_ROWS`] rows
/// 3. `result_end` - row_count + chunk_count summary
fn result_frames(response: QueryResult) -> Vec<String> {
    let row_provenance: Vec<String> = response
        .rows
        .iter()
        .map(|row| {
            row.provenance
                .as_ref()
                .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
        })
        .collect();

    let rows: Vec<Vec<serde_json::Value>> = response
        .rows
        .into_iter()
        .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
        .collect();

    let columns: Vec<String> = response.schema.iter().map(|c| c.name.clone()).collect();
    let row_count = rows.len();

    let metadata = response.metadata.map(|m| SessionQueryMetadataDto {
        has_ephemeral: m.has_ephemeral,
        ephemeral_sources: m.ephemeral_sources,
        warnings: m.warnings,
    });

    // Build the single-message response to check its size
    let single_response = GlobalWsResponse::Result {
        columns: columns.clone(),
        rows: rows.clone(),
        row_count,
        total_count: response.total_count,
        truncated: response.truncated,
        execution_time_ms: response.execution_time_ms,
        row_provenance: row_provenance.clone(),
        metadata: metadata.clone(),
        switched_kg: response.switched_kg.clone(),
        proof_trees: response.proof_trees.clone(),
        timing_breakdown: response.timing_breakdown.clone(),
        errors: response.errors.clone(),
    };

    // Check serialized size to decide: single message vs streaming
    let single_json = match serde_json::to_string(&single_response) {
        Ok(j) => j,
        Err(e) => {
            tracing::error!(error = %e, "Failed to serialize result");
            return vec![response_frame(&error_response(
                "Internal server error".to_string(),
            ))];
        }
    };

    if single_json.len() <= STREAMING_THRESHOLD {
        // Small result: send as single message (backward compatible)
        if single_json.len() > MAX_MESSAGE_SIZE {
            warn!(
                size = single_json.len(),
                max = MAX_MESSAGE_SIZE,
                "ws_result_too_large"
            );
            return vec![response_frame(&error_response(format!(
                "Result too large ({} bytes, max {})",
                single_json.len(),
                MAX_MESSAGE_SIZE
            )))];
        }
        return vec![single_json];
    }

    // Large result: stream as chunks
    info!(
        row_count,
        json_size = single_json.len(),
        "ws_streaming_result"
    );
    drop(single_json); // free memory

    let mut frames = vec![response_frame(&GlobalWsResponse::ResultStart {
        columns,
        total_count: response.total_count,
        truncated: response.truncated,
        execution_time_ms: response.execution_time_ms,
        metadata,
        switched_kg: response.switched_kg,
        proof_trees: response.proof_trees,
        timing_breakdown: response.timing_breakdown,
        errors: response.errors,
    })];
    let mut chunk_index: usize = 0;
    let mut row_iter = rows.into_iter();
    let mut prov_iter = row_provenance.into_iter();
    loop {
        let chunk_rows: Vec<Vec<serde_json::Value>> =
            row_iter.by_ref().take(STREAMING_CHUNK_ROWS).collect();
        if chunk_rows.is_empty() {
            break;
        }
        let chunk_prov: Vec<String> = prov_iter.by_ref().take(chunk_rows.len()).collect();
        frames.push(response_frame(&GlobalWsResponse::ResultChunk {
            rows: chunk_rows,
            row_provenance: chunk_prov,
            chunk_index,
        }));
        chunk_index += 1;
    }
    frames.push(response_frame(&GlobalWsResponse::ResultEnd {
        row_count,
        chunk_count: chunk_index,
    }));
    frames
}

/// The `error` frame for a failed program, unpacking parse errors.
pub(super) fn program_error_response(error: ProgramError) -> GlobalWsResponse {
    if let Some(json_str) = error.message.strip_prefix(VALIDATION_ERROR_PREFIX) {
        if let Ok(errors) = serde_json::from_str::<Vec<ValidationError>>(json_str) {
            return GlobalWsResponse::Error {
                message: format!("Program has {} parse error(s)", errors.len()),
                validation_errors: Some(errors),
                code: Some(ErrorCode::Validation),
            };
        }
    }
    GlobalWsResponse::Error {
        message: error.message,
        validation_errors: None,
        code: error.code,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn access(program: &str) -> Access {
        let text = serde_json::json!({"type": "execute", "program": program}).to_string();
        Job::from_text(&text).0
    }

    #[test]
    fn pure_queries_are_shared() {
        assert_eq!(access("?edge(X, Y)"), Access::Shared);
        assert_eq!(access("  ?edge(X, Y)\n?node(X)"), Access::Shared);
        assert_eq!(access("?edge(X,\n  Y)\n// all edges"), Access::Shared);
        // Fails to parse, and so changes nothing.
        assert_eq!(access("?edge(X, "), Access::Shared);
    }

    #[test]
    fn anything_that_changes_state_is_exclusive() {
        for program in [
            "+edge(1, 2).",
            "-edge(1, 2).",
            "?edge(X, Y)\n+edge(1, 2).",
            "path(X, Y) <- edge(X, Y).",
            "edge(1, 2).",
            ".kg use other",
            ".session clear",
            ".subscribe s ?edge(X, Y)",
            ".unsubscribe s",
            "?edge(X, Y)\n% comment\nedge(1, 2).",
            "// only a comment",
            "",
        ] {
            assert_eq!(access(program), Access::Exclusive, "{program:?}");
        }
    }

    #[test]
    fn control_messages_are_answered_in_order() {
        for text in [
            r#"{"type": "ping"}"#,
            "not json",
            r#"{"type": "authenticate", "api_key": "k"}"#,
        ] {
            let (access, job) = Job::from_text(text);
            assert_eq!(access, Access::Shared, "{text}");
            assert!(matches!(job, Job::Immediate(_)), "{text}");
        }
    }

    #[test]
    fn subscription_commands_are_jobs_of_their_own() {
        let text = r#"{"type": "execute", "program": ".subscribe s ?edge(X, Y)"}"#;
        assert!(matches!(
            Job::from_text(text).1,
            Job::Subscribe { id, query } if id == "s" && query.contains("edge")
        ));
        let text = r#"{"type": "execute", "program": ".unsubscribe s"}"#;
        assert!(matches!(Job::from_text(text).1, Job::Unsubscribe { id } if id == "s"));
    }

    #[test]
    fn test_program_error_response_keeps_code() {
        let resp = program_error_response(ProgramError {
            message: "Rule 'x' not found.".to_string(),
            code: Some(ErrorCode::NotFound),
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"code\":\"not_found\""), "{json}");

        let resp = program_error_response(ProgramError::from("Access denied".to_string()));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(!json.contains("\"code\""), "{json}");
    }

    #[test]
    fn test_program_error_response_parse_errors_are_validation() {
        let errors = vec![ValidationError {
            line: 1,
            statement_index: 0,
            error: "bad".to_string(),
        }];
        let message = format!(
            "{VALIDATION_ERROR_PREFIX}{}",
            serde_json::to_string(&errors).unwrap()
        );
        let resp = program_error_response(ProgramError::from(message));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"code\":\"validation\""), "{json}");
        assert!(json.contains("\"validation_errors\""), "{json}");
    }
}
