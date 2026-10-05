//! Reading several queries at one revision.
//!
//! A `read` pins its knowledge graph's current snapshot once and runs every
//! query on it, so each result is its query's exact answer at the snapshot's
//! revision, whatever commits meanwhile. The queries run concurrently under
//! the request's one deadline and cancellation, each admitted and authorized
//! as a query of `execute` is; the first failure, stop or deadline stops the
//! queries still computing. A read sees persistent data only, as a
//! subscription does, so a `read` and a `subscribe` of the same queries agree
//! at the same revision. It fails as a whole when any query fails.

use std::sync::Arc;

use futures_util::future::try_join_all;
use inputlayer_ws_protocol::NamedQuery;

use super::{
    parse_program, settle_result, statement, supervise, Handler, ProgramError, ValidationError,
    VALIDATION_ERROR_PREFIX,
};
use crate::execution::RequestControl;
use crate::protocol::subscription::connection::query_names;
use crate::protocol::wire::{ErrorCode, QueryResult};

/// The results of a [`Handler::read_snapshot`].
#[derive(Debug)]
pub struct SnapshotRead {
    /// The knowledge graph revision every result is the exact answer at.
    pub revision: u64,
    /// One per query, in order.
    pub results: Vec<QueryResult>,
}

impl Handler {
    /// Run `queries` (each `?body`) on one snapshot of `knowledge_graph`, as
    /// `auth`, under `control`; see the module docs.
    pub async fn read_snapshot(
        &self,
        knowledge_graph: &str,
        queries: &[NamedQuery],
        auth: Option<&crate::auth::Principal>,
        control: &Arc<RequestControl>,
    ) -> Result<SnapshotRead, ProgramError> {
        query_names(queries, "A read").map_err(|message| ProgramError {
            message,
            code: Some(ErrorCode::Validation),
        })?;
        let max_bytes = self.config.storage.performance.max_query_size_bytes;
        let bytes: usize = queries.iter().map(|q| q.query.len()).sum();
        if max_bytes > 0 && bytes > max_bytes {
            return Err(ProgramError {
                message: format!("Read too large: {bytes} bytes of queries (max {max_bytes})"),
                code: Some(ErrorCode::Validation),
            });
        }
        let identity = auth.map(crate::auth::Principal::identity).transpose()?;
        let mut parsed = Vec::with_capacity(queries.len());
        for query in queries {
            let statements = parse_read_query(query)?;
            self.authorize_program(identity.as_ref(), Some(knowledge_graph), &statements)?;
            parsed.push(statements);
        }
        let snapshot = self.storage.read().get_snapshot_for(knowledge_graph)?;
        let revision = snapshot.revision;
        // Each query runs under a control of its own with the read's
        // deadline: a running query's control finishes when it does, and
        // that must not shield the others from a cancel or the deadline.
        let controls: Vec<Arc<RequestControl>> = queries
            .iter()
            .map(|_| RequestControl::new(control.deadline()))
            .collect();
        let runs = queries.iter().zip(parsed).zip(&controls).map(
            |((query, statements), query_control)| {
                let job = super::QueryJob {
                    pinned: Some(Arc::clone(&snapshot)),
                    ..self.make_query_job()
                };
                async move {
                    let text = query.query.trim().to_string();
                    self.run_job(
                        job,
                        Some(knowledge_graph.to_string()),
                        text,
                        Some(statements),
                        query_control,
                        &self.query_semaphore,
                    )
                    .await
                    .and_then(|result| settle_result(result, auth, true))
                    .map_err(|e| named_error(&query.name, e))
                }
            },
        );
        // The first failure fails the read; the queries still computing are
        // stopped, so each exits at its next check and frees its permit.
        let stop_all = || {
            for query_control in &controls {
                query_control.cancel();
            }
        };
        let runs = try_join_all(runs);
        tokio::pin!(runs);
        let outcome = tokio::select! {
            outcome = &mut runs => outcome,
            stop = control.interrupted() => match stop {
                Some(stop) => {
                    stop_all();
                    return Err(supervise::stop_error(stop));
                }
                // Not interruptible any more: nothing stops it but itself.
                None => runs.await,
            },
        };
        let results = outcome.inspect_err(|_| stop_all())?;
        // A stop that won the race discards results nobody waits for.
        control.finish().map_err(supervise::stop_error)?;
        Ok(SnapshotRead { revision, results })
    }
}

/// The statements of one query of a read: exactly one `?` query.
fn parse_read_query(query: &NamedQuery) -> Result<Vec<statement::Statement>, ProgramError> {
    let invalid = |message: String| ProgramError {
        message: format!("Query '{}': {message}", query.name),
        code: Some(ErrorCode::Validation),
    };
    if !query.query.trim_start().starts_with('?') {
        return Err(invalid("a read query must start with '?'".to_string()));
    }
    let statements = parse_program(&query.query).map_err(|errors| {
        invalid(
            errors
                .iter()
                .map(|e: &ValidationError| e.error.as_str())
                .collect::<Vec<_>>()
                .join("; "),
        )
    })?;
    match statements.as_slice() {
        [statement::Statement::Query(_)] => Ok(statements),
        _ => Err(invalid("a read query must be a single query".to_string())),
    }
}

/// `error` of the query named `name`, saying which query failed.
fn named_error(name: &str, error: ProgramError) -> ProgramError {
    let message = match error.message.strip_prefix(VALIDATION_ERROR_PREFIX) {
        Some(_) => "the query failed to parse".to_string(),
        None => error.message,
    };
    // A stopped request is the request's outcome, not one query's.
    let stopped = matches!(
        error.code,
        Some(ErrorCode::DeadlineExceeded | ErrorCode::Cancelled)
    );
    ProgramError {
        message: if stopped {
            message
        } else {
            format!("Query '{name}': {message}")
        },
        code: error.code,
    }
}
