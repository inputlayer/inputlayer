//! Requests of an authenticated `/ws` connection, as pipeline jobs.
//!
//! [`Request::from_text`] classifies a client frame by the state it touches
//! ([`Access`]); the connection loop starts the job once the pipeline's
//! barriers allow and writes its [`Reply`] when released. Request futures are
//! polled by the connection loop, so nothing here may block it: computation
//! runs on the blocking pool, and so does serializing a large result.

use std::time::Instant;

use inputlayer_ws_protocol::{
    probe_request_id, ClientFrame, ErrorCode, Params, RequestId, ServerFrame,
};

use super::pipeline::Access;
use crate::params::{references_params, META_PARAMS};
use crate::protocol::handler::is_query_program;
use crate::protocol::subscription::connection::Opened;
use crate::statement::{MetaCommand, Statement};

/// One client request: the `id` its replies echo and what it asks for.
pub(super) struct Request {
    pub id: Option<RequestId>,
    pub job: Job,
}

/// What a request asks for.
pub(super) enum Job {
    /// Answered without running anything (pong, malformed frame).
    Immediate(ServerFrame),
    /// An IQL program through the handler, within `timeout_ms` of arrival
    /// when the client set one.
    Execute {
        program: String,
        params: Params,
        timeout_ms: Option<u64>,
    },
    /// Cancel the unanswered request `target`; handled on arrival.
    Cancel { target: RequestId },
    /// `.subscribe <name> ?<query>` on the connection's KG.
    Subscribe { name: String, query: String },
    /// `.unsubscribe <name>`.
    Unsubscribe { name: String },
}

/// A finished request, ready to be written.
pub(super) enum Reply {
    /// Encoded frames.
    Frames(Vec<String>),
    /// An evaluated snapshot of subscription `name`, registered by the loop
    /// on release.
    Subscribed {
        name: String,
        opened: Opened,
        started: Instant,
    },
}

impl Request {
    /// Parse a client frame of an authenticated connection.
    pub(super) fn from_text(text: &str) -> (Access, Self) {
        let frame = match serde_json::from_str::<ClientFrame>(text) {
            Ok(frame) => frame,
            Err(e) => {
                tracing::debug!(error = %e, "ws_invalid_request");
                let rejected = ServerFrame::error(
                    probe_request_id(text),
                    Some(ErrorCode::InvalidRequest),
                    format!("Invalid message format: {e}"),
                );
                return Self::immediate(rejected);
            }
        };
        match frame {
            ClientFrame::Execute {
                id,
                program,
                params,
                timeout_ms,
            } => {
                let command = subscription_command(&program);
                // Standing queries take no parameters: their IQL is
                // re-evaluated long after the request.
                if command.is_some() && (!params.is_empty() || references_params(&program)) {
                    return Self::immediate(ServerFrame::error(
                        id,
                        Some(ErrorCode::Validation),
                        META_PARAMS.to_string(),
                    ));
                }
                let (access, job) = match command {
                    Some(MetaCommand::Subscribe { id: name, query }) => {
                        (Access::Exclusive, Job::Subscribe { name, query })
                    }
                    Some(MetaCommand::Unsubscribe(name)) => {
                        (Access::Exclusive, Job::Unsubscribe { name })
                    }
                    _ => (
                        program_access(&program),
                        Job::Execute {
                            program,
                            params,
                            timeout_ms,
                        },
                    ),
                };
                (access, Self { id, job })
            }
            ClientFrame::Cancel { id, target } => (
                Access::Shared,
                Self {
                    id,
                    job: Job::Cancel { target },
                },
            ),
            ClientFrame::Ping { id } => Self::immediate(ServerFrame::Pong { id }),
            ClientFrame::Login { id, .. } | ClientFrame::Authenticate { id, .. } => {
                Self::immediate(ServerFrame::error(
                    id,
                    Some(ErrorCode::InvalidRequest),
                    "Already authenticated".to_string(),
                ))
            }
        }
    }

    /// A reply that touches no state, released in order with the others.
    pub(super) fn immediate(frame: ServerFrame) -> (Access, Self) {
        let id = frame.request_id().cloned();
        (
            Access::Shared,
            Self {
                id,
                job: Job::Immediate(frame),
            },
        )
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn access(program: &str) -> Access {
        let text = serde_json::json!({"type": "execute", "program": program}).to_string();
        Request::from_text(&text).0
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
    fn control_frames_are_answered_in_order_with_their_id() {
        for (text, id) in [
            (r#"{"type": "ping", "id": "p"}"#, Some("p")),
            ("not json", None),
            (r#"{"type": "execute", "id": 7, "program": "?a(X)"}"#, None),
            (
                r#"{"type": "authenticate", "id": "a", "api_key": "k"}"#,
                Some("a"),
            ),
        ] {
            let (access, request) = Request::from_text(text);
            assert_eq!(access, Access::Shared, "{text}");
            assert!(matches!(request.job, Job::Immediate(_)), "{text}");
            assert_eq!(request.id.as_ref().map(RequestId::as_str), id, "{text}");
        }
    }

    #[test]
    fn subscription_commands_are_jobs_of_their_own() {
        let text = r#"{"type": "execute", "id": "s1", "program": ".subscribe s ?edge(X, Y)"}"#;
        let (_, request) = Request::from_text(text);
        assert_eq!(request.id.unwrap().as_str(), "s1");
        assert!(matches!(
            request.job,
            Job::Subscribe { name, query } if name == "s" && query.contains("edge")
        ));
        let text = r#"{"type": "execute", "program": ".unsubscribe s"}"#;
        assert!(matches!(
            Request::from_text(text).1.job,
            Job::Unsubscribe { name } if name == "s"
        ));
    }
}
