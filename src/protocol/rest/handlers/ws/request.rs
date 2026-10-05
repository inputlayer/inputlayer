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
use crate::storage_engine::Precondition;

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
    /// when the client set one, committing only if `precondition` holds.
    Execute {
        program: String,
        params: Params,
        timeout_ms: Option<u64>,
        precondition: Option<Expectation>,
    },
    /// Cancel the unanswered request `target`; handled on arrival.
    Cancel { target: RequestId },
    /// `.subscribe <name> ?<query>` on the connection's KG.
    Subscribe { name: String, query: String },
    /// `.unsubscribe <name>`.
    Unsubscribe { name: String },
}

/// An `execute`'s `expect_revision` and the stream epoch it belongs to.
pub(super) struct Expectation {
    pub precondition: Precondition,
    /// `expect_epoch`, checked against this engine run's on admission.
    pub epoch: Option<String>,
}

impl Expectation {
    /// The precondition of an `execute` frame's `expect_*` fields: `None`
    /// when it sets none, `Err` with the reason when they are inconsistent.
    fn of(
        revision: Option<u64>,
        relations: Option<Vec<String>>,
        epoch: Option<String>,
    ) -> Result<Option<Self>, String> {
        let Some(revision) = revision else {
            return match (relations, epoch) {
                (None, None) => Ok(None),
                _ => Err("expect_relations and expect_epoch require expect_revision".to_string()),
            };
        };
        if let Some(relations) = &relations {
            if relations.is_empty() {
                return Err("expect_relations names no relation; omit it to expect the \
                            whole knowledge graph unchanged"
                    .to_string());
            }
            if relations.iter().any(String::is_empty) {
                return Err("expect_relations names an empty relation".to_string());
            }
        }
        Ok(Some(Self {
            precondition: Precondition {
                revision,
                relations,
            },
            epoch,
        }))
    }
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
                expect_revision,
                expect_relations,
                expect_epoch,
            } => {
                let precondition =
                    match Expectation::of(expect_revision, expect_relations, expect_epoch) {
                        Ok(precondition) => precondition,
                        Err(message) => return Self::invalid(id, message),
                    };
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
                    Some(_) if precondition.is_some() => {
                        let message = "expect_revision applies to a program that writes, \
                                       not to .subscribe or .unsubscribe";
                        return Self::invalid(id, message.to_string());
                    }
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
                            precondition,
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
                Self::invalid(id, "Already authenticated".to_string())
            }
        }
    }

    /// A request rejected with `invalid_request` before anything ran.
    fn invalid(id: Option<RequestId>, message: String) -> (Access, Self) {
        Self::immediate(ServerFrame::error(
            id,
            Some(ErrorCode::InvalidRequest),
            message,
        ))
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

    fn rejection(text: &str) -> Option<String> {
        match Request::from_text(text).1.job {
            Job::Immediate(ServerFrame::Error {
                code: Some(ErrorCode::InvalidRequest),
                message,
                ..
            }) => Some(message),
            _ => None,
        }
    }

    #[test]
    fn expect_revision_rides_with_its_program() {
        let text = r#"{"type": "execute", "program": "+a(1)", "expect_revision": 9,
            "expect_relations": ["a"], "expect_epoch": "e"}"#;
        let Job::Execute {
            precondition: Some(expectation),
            ..
        } = Request::from_text(text).1.job
        else {
            panic!("expected an execute job with a precondition");
        };
        assert_eq!(
            expectation.precondition,
            Precondition {
                revision: 9,
                relations: Some(vec!["a".to_string()]),
            }
        );
        assert_eq!(expectation.epoch.as_deref(), Some("e"));
        let text = r#"{"type": "execute", "program": "+a(1)"}"#;
        assert!(matches!(
            Request::from_text(text).1.job,
            Job::Execute {
                precondition: None,
                ..
            }
        ));
    }

    #[test]
    fn inconsistent_expectations_are_invalid_requests() {
        for text in [
            r#"{"type": "execute", "program": "+a(1)", "expect_relations": ["a"]}"#,
            r#"{"type": "execute", "program": "+a(1)", "expect_epoch": "e"}"#,
            r#"{"type": "execute", "program": "+a(1)", "expect_revision": 1, "expect_relations": []}"#,
            r#"{"type": "execute", "program": "+a(1)", "expect_revision": 1, "expect_relations": [""]}"#,
            r#"{"type": "execute", "program": ".subscribe s ?a(X)", "expect_revision": 1}"#,
            r#"{"type": "execute", "program": ".unsubscribe s", "expect_revision": 1}"#,
        ] {
            assert!(rejection(text).is_some(), "{text}");
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
