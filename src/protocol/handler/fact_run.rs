//! A program's consecutive fact statements, committed as one transaction.
//!
//! The statement loop queues `+`, `-` and update statements in a [`FactRun`]
//! instead of executing them one by one. Before a statement with other
//! persistent effects (schema, rules, meta commands) and at the end of the
//! program, [`QueryJob::commit_fact_run`] stages the queued statements and
//! commits them through [`StorageEngine::commit_facts`]: all of them take
//! effect at one revision, with one WAL record and one snapshot publish, or
//! none does. Subscribers are notified once per changed relation, after the
//! publish, so they never observe part of a run.

use super::fact_staging::{FactStatement, InsertLimits, StageError};
use super::{storage_error_code, QueryJob};
use crate::protocol::wire::ErrorCode;
use crate::statement::Statement;
use crate::storage_engine::{
    FactCommit, FactCommitError, FactProgram, KnowledgeGraphSnapshot, StorageEngine,
};
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Stagings of one run before a state-reading statement that keeps going
/// stale under concurrent writes is reported as a conflict.
pub(super) const MAX_STAGE_ATTEMPTS: u32 = 8;

/// Upper bound of the random pause before a re-staging, in milliseconds.
const MAX_BACKOFF_MS: u64 = 32;

/// A queued statement and the message row reserved for it.
struct Queued {
    index: usize,
    statement: FactStatement,
    slot: usize,
}

/// Fact statements queued since the last commit point.
#[derive(Default)]
pub(super) struct FactRun {
    queued: Vec<Queued>,
    /// Reserved message rows that stay empty: statements that failed or
    /// report nothing.
    vacant: Vec<usize>,
}

/// A run that did not commit: statement `index` failed and none of the
/// run's statements took effect.
pub(super) struct RunFailure {
    pub index: usize,
    pub code: ErrorCode,
    pub message: String,
}

impl FactRun {
    /// Whether `statement` can join a run without committing it first:
    /// fact statements, and statements with no persistent effect.
    pub fn joins(statement: &Statement) -> bool {
        matches!(
            statement,
            Statement::Insert(_)
                | Statement::Delete(_)
                | Statement::Update(_)
                | Statement::Fact(_)
                | Statement::SessionRule(_)
                | Statement::Query(_)
                | Statement::TypeDecl(_)
        )
    }

    /// Queue statement `index`, reserving its message row in `messages`.
    pub fn queue(&mut self, index: usize, statement: FactStatement, messages: &mut Vec<String>) {
        self.queued.push(Queued {
            index,
            statement,
            slot: messages.len(),
        });
        messages.push(String::new());
    }

    /// Whether any statement is queued.
    pub fn is_empty(&self) -> bool {
        self.queued.is_empty()
    }

    /// Remove the message rows of statements that report nothing.
    pub fn remove_vacant(mut self, messages: &mut Vec<String>) {
        self.vacant.sort_unstable();
        for slot in self.vacant.into_iter().rev() {
            messages.remove(slot);
        }
    }
}

impl QueryJob {
    /// Commit the statements queued in `run` to `kg` as one transaction and
    /// fill their message rows in `messages`.
    ///
    /// A run whose staging read the KG is staged again, up to
    /// [`MAX_STAGE_ATTEMPTS`] times with a short random pause, if a concurrent
    /// commit changed what it read first. On failure no statement of the run took effect and their
    /// message rows stay vacant.
    pub(super) fn commit_fact_run(
        &self,
        storage: &StorageEngine,
        kg: &str,
        run: &mut FactRun,
        messages: &mut [String],
    ) -> Result<(), RunFailure> {
        let result = self.commit_queued(storage, kg, &run.queued);
        let queued = std::mem::take(&mut run.queued);
        let commit = match result {
            Ok(commit) => commit,
            Err(failure) => {
                run.vacant.extend(queued.iter().map(|q| q.slot));
                return Err(rolled_back(&queued, failure));
            }
        };

        let mut inserted_total = 0;
        for (queued, count) in queued.iter().zip(&commit.statements) {
            inserted_total += count.inserted;
            match queued
                .statement
                .success_message(count.inserted, count.deleted)
            {
                Some(message) => messages[queued.slot] = message,
                None => run.vacant.push(queued.slot),
            }
        }
        self.insert_count
            .fetch_add(inserted_total as u64, Ordering::Relaxed);
        for change in &commit.relations {
            let operation = match (change.inserted, change.deleted) {
                (_, 0) => "insert",
                (0, _) => "delete",
                _ => "update",
            };
            self.notify_persistent_update(
                kg,
                &change.relation,
                operation,
                change.inserted + change.deleted,
            );
        }
        Ok(())
    }

    /// Stage `queued` and commit it, re-staging while it goes stale.
    fn commit_queued(
        &self,
        storage: &StorageEngine,
        kg: &str,
        queued: &[Queued],
    ) -> Result<FactCommit, RunFailure> {
        let cancel = crate::code_generator::current_query_cancel_flag();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let program = self.stage(storage, kg, queued)?;
            match storage.commit_facts(kg, program, cancel.as_deref()) {
                Ok(commit) => return Ok(commit),
                Err(FactCommitError::Stale) if attempts < MAX_STAGE_ATTEMPTS => {
                    // Spread writers contending for the same data apart. This
                    // runs on the blocking pool, never on an async worker.
                    let ceiling = (1 << attempts).min(MAX_BACKOFF_MS);
                    let pause = rand::thread_rng().gen_range(0..=ceiling);
                    std::thread::sleep(Duration::from_millis(pause));
                }
                Err(error) => return Err(commit_failure(kg, queued, error)),
            }
        }
    }

    /// Resolve every queued statement to its changes, in order. Statements
    /// that read the KG read one snapshot, taken when the first of them stages.
    fn stage(
        &self,
        storage: &StorageEngine,
        kg: &str,
        queued: &[Queued],
    ) -> Result<FactProgram, RunFailure> {
        let perf = &self.config.storage.performance;
        let limits = InsertLimits {
            max_string_bytes: perf.max_string_value_bytes,
            max_tuples: perf.max_insert_tuples,
        };
        let mut program = FactProgram::new();
        let mut snapshot: Option<Arc<KnowledgeGraphSnapshot>> = None;
        for q in queued {
            let changes = q
                .statement
                .changes(&limits, |query| {
                    let base = match &snapshot {
                        Some(base) => Arc::clone(base),
                        None => {
                            let base = storage.get_snapshot_for(kg).map_err(|e| StageError {
                                code: storage_error_code(&e, ErrorCode::Internal),
                                message: q.statement.failure_message(&e),
                            })?;
                            snapshot = Some(Arc::clone(&base));
                            base
                        }
                    };
                    program.read(&base, query);
                    Ok(program.view(&base))
                })
                .map_err(|e| RunFailure {
                    index: q.index,
                    code: e.code,
                    message: e.message,
                })?;
            program.push(q.index, changes);
        }
        Ok(program)
    }
}

/// The run failure reporting `error`. A rejection belongs to its statement,
/// a stale read to the first statement that read the KG, and any other
/// failure to the last statement, where the run commits.
fn commit_failure(kg: &str, queued: &[Queued], error: FactCommitError) -> RunFailure {
    let last = queued.last().map_or(0, |q| q.index);
    let (index, code, error) = match error {
        FactCommitError::Rejected { statement, error } => (
            statement,
            storage_error_code(&error, ErrorCode::Validation),
            error.to_string(),
        ),
        FactCommitError::Failed(error) => (
            last,
            storage_error_code(&error, ErrorCode::Internal),
            error.to_string(),
        ),
        FactCommitError::Stale => {
            warn!(kg = %kg, attempts = MAX_STAGE_ATTEMPTS, "fact_run_stale");
            let reader = queued.iter().find(|q| q.statement.reads_state());
            return RunFailure {
                index: reader.map_or(last, |q| q.index),
                code: ErrorCode::Conflict,
                message: format!(
                    "The data this statement reads in knowledge graph '{kg}' kept changing \
                     under concurrent writes ({MAX_STAGE_ATTEMPTS} attempts); nothing was \
                     applied. Retry the program."
                ),
            };
        }
        FactCommitError::Cancelled => {
            return RunFailure {
                index: last,
                code: ErrorCode::Internal,
                message: "Program cancelled before its fact changes were committed; \
                          nothing was applied."
                    .to_string(),
            };
        }
    };
    let message = queued
        .iter()
        .find(|q| q.index == index)
        .map_or_else(|| error.clone(), |q| q.statement.failure_message(&error));
    RunFailure {
        index,
        code,
        message,
    }
}

/// `failure` with a note that the rest of the run was rolled back too, when
/// the run held more than the failed statement.
fn rolled_back(queued: &[Queued], mut failure: RunFailure) -> RunFailure {
    if let [first, .., last] = queued {
        failure.message.push_str(&format!(
            " (rolled back: none of the {} fact statements {}-{} was applied)",
            queued.len(),
            first.index,
            last.index
        ));
    }
    failure
}
