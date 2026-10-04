//! A program's writes, committed as one transaction.
//!
//! The statement loop queues every write of a program (facts, schemas, rules
//! and rule removals; see [`super::program_boundary`]) in a [`WriteRun`]
//! instead of executing it. After the last statement,
//! [`QueryJob::commit_write_run`] stages the queued statements and commits
//! them through [`StorageEngine::commit_program`]: all of them take effect at
//! one revision, with one WAL record and one snapshot publish, or none does.
//! Subscribers are notified after the publish (rule changes per statement,
//! then once per changed relation), so they never observe part of a program.

use super::catalog_staging::CatalogStatement;
use super::fact_staging::{FactStatement, InsertLimits, StageError};
use super::{storage_error_code, QueryJob};
use crate::protocol::wire::{ErrorCode, StatementCounts};
use crate::rule_catalog::RuleCatalog;
use crate::storage_engine::{
    CommitError, KnowledgeGraphSnapshot, ProgramCommit, StagedChanges, StatementEffect,
    StorageEngine, WriteProgram,
};
use rand::Rng;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Stagings of one program before a state-reading statement that keeps going
/// stale under concurrent writes is reported as a conflict.
pub(super) const MAX_STAGE_ATTEMPTS: u32 = 8;

/// Upper bound of the random pause before a re-staging, in milliseconds.
const MAX_BACKOFF_MS: u64 = 32;

/// A statement that writes persistent state.
#[derive(Debug)]
pub(super) enum WriteStatement {
    Facts(FactStatement),
    Catalog(CatalogStatement),
}

impl WriteStatement {
    /// Whether staging this statement reads the knowledge graph.
    fn reads_state(&self) -> bool {
        matches!(self, Self::Facts(statement) if statement.reads_state())
    }

    fn changes_rules(&self) -> bool {
        matches!(self, Self::Catalog(statement) if statement.changes_rules())
    }

    /// The code and message reporting that this statement failed with `error`.
    fn failure(&self, error: &crate::storage::StorageError) -> StageError {
        match self {
            Self::Facts(statement) => StageError {
                code: storage_error_code(error, ErrorCode::Validation),
                message: statement.failure_message(error),
            },
            Self::Catalog(statement) => statement.failure(error),
        }
    }
}

/// A queued statement and the message row reserved for it.
struct Queued {
    index: usize,
    statement: WriteStatement,
    slot: usize,
}

/// The writes of a program, queued until it commits.
#[derive(Default)]
pub(super) struct WriteRun {
    queued: Vec<Queued>,
    /// Reserved message rows that stay empty: statements that failed or
    /// report nothing.
    vacant: Vec<usize>,
}

pub(super) struct RunFailure {
    pub index: usize,
    pub code: ErrorCode,
    pub message: String,
}

impl WriteRun {
    /// Queue statement `index`, reserving its message row in `messages`.
    pub fn queue(&mut self, index: usize, statement: WriteStatement, messages: &mut Vec<String>) {
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

    /// Give up the queued writes because statement `failed` failed: none of
    /// them is applied. Returns the note that says so for the failure's
    /// message, or `None` when the failed statement was the only write.
    pub fn abandon(&mut self, failed: usize) -> Option<String> {
        let queued = std::mem::take(&mut self.queued);
        self.vacant.extend(queued.iter().map(|q| q.slot));
        rolled_back_note(&queued, failed)
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
    /// Commit the statements queued in `run` to `kg` as one transaction, fill
    /// their message rows in `messages` and return the counts of its fact
    /// statements.
    ///
    /// A program whose staging read the KG is staged again, up to
    /// [`MAX_STAGE_ATTEMPTS`] times with a short random pause, if a concurrent
    /// commit changed what it read first.
    pub(super) fn commit_write_run(
        &self,
        storage: &StorageEngine,
        kg: &str,
        run: &mut WriteRun,
        messages: &mut [String],
    ) -> Result<Vec<StatementCounts>, RunFailure> {
        let commit = match self.commit_queued(storage, kg, &run.queued) {
            Ok(commit) => commit,
            Err(mut failure) => {
                let note = run.abandon(failure.index);
                if failure.code != ErrorCode::OutcomeUnknown {
                    if let Some(note) = note {
                        failure.message.push_str(&note);
                    }
                }
                return Err(failure);
            }
        };

        let queued = std::mem::take(&mut run.queued);
        let mut inserted_total = 0;
        let mut counts = Vec::new();
        for (queued, outcome) in queued.iter().zip(&commit.statements) {
            let message = match (&queued.statement, &outcome.effect) {
                (WriteStatement::Facts(statement), StatementEffect::Facts(count)) => {
                    inserted_total += count.inserted;
                    counts.push(StatementCounts {
                        index: queued.index,
                        kind: statement.kind(),
                        inserted: count.inserted,
                        deleted: count.deleted,
                    });
                    statement.success_message(count.inserted, count.deleted)
                }
                (WriteStatement::Catalog(statement), StatementEffect::Catalog(outcome)) => {
                    for (rule, operation) in statement.rule_notices(outcome) {
                        self.notify_rule_change(kg, rule, operation);
                    }
                    Some(statement.success_message(outcome))
                }
                _ => unreachable!("a statement's effect matches its kind"),
            };
            match message {
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
        Ok(counts)
    }

    /// Stage `queued` and commit it, re-staging while it goes stale.
    fn commit_queued(
        &self,
        storage: &StorageEngine,
        kg: &str,
        queued: &[Queued],
    ) -> Result<ProgramCommit, RunFailure> {
        let control = crate::code_generator::current_request_control();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let program = self.stage(storage, kg, queued)?;
            match storage.commit_program(kg, program, control.as_deref()) {
                Ok(commit) => return Ok(commit),
                Err(CommitError::Stale) if attempts < MAX_STAGE_ATTEMPTS => {
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
    /// that read the KG read one snapshot, taken when the first of them
    /// stages, plus the changes staged before them; after a rule change they
    /// evaluate with the staged rules.
    fn stage(
        &self,
        storage: &StorageEngine,
        kg: &str,
        queued: &[Queued],
    ) -> Result<WriteProgram, RunFailure> {
        let perf = &self.config.storage.performance;
        let limits = InsertLimits {
            max_string_bytes: perf.max_string_value_bytes,
            max_tuples: perf.max_insert_tuples,
        };
        let failed = |q: &Queued, e: StageError| RunFailure {
            index: q.index,
            code: e.code,
            message: e.message,
        };
        let reads_after_rule_change = queued
            .iter()
            .position(|q| q.statement.changes_rules())
            .is_some_and(|first| queued[first..].iter().any(|q| q.statement.reads_state()));
        let mut base = Base::default();
        if reads_after_rule_change {
            let (snapshot, rules) = storage.staging_base(kg).map_err(|e| RunFailure {
                index: queued[0].index,
                code: storage_error_code(&e, ErrorCode::Internal),
                message: e.to_string(),
            })?;
            base = Base {
                snapshot: Some(snapshot),
                rules: Some(rules),
            };
        }

        let mut program = WriteProgram::new();
        for q in queued {
            let changes = match &q.statement {
                WriteStatement::Catalog(statement) => {
                    let change = statement.change().map_err(|e| failed(q, e))?;
                    if let Some(rules) = &mut base.rules {
                        change
                            .apply_to_rules(rules)
                            .map_err(|e| failed(q, statement.failure(&e)))?;
                    }
                    StagedChanges::Catalog(change)
                }
                WriteStatement::Facts(statement) => StagedChanges::Facts(
                    statement
                        .changes(&limits, |query| {
                            let snapshot = base.snapshot(storage, kg, statement)?;
                            let view = program.view(&snapshot, base.rules.as_ref());
                            program.read(&snapshot, &view.rules, query);
                            Ok(view)
                        })
                        .map_err(|e| failed(q, e))?,
                ),
            };
            program.push(q.index, changes);
        }
        Ok(program)
    }
}

/// What state-reading statements stage against.
#[derive(Default)]
struct Base {
    /// The KG's snapshot, taken when first needed.
    snapshot: Option<Arc<KnowledgeGraphSnapshot>>,
    /// The snapshot's rules with the rule changes staged so far, when a
    /// statement reads the KG after the program changes rules.
    rules: Option<RuleCatalog>,
}

impl Base {
    fn snapshot(
        &mut self,
        storage: &StorageEngine,
        kg: &str,
        statement: &FactStatement,
    ) -> Result<Arc<KnowledgeGraphSnapshot>, StageError> {
        if let Some(snapshot) = &self.snapshot {
            return Ok(Arc::clone(snapshot));
        }
        let snapshot = storage.get_snapshot_for(kg).map_err(|e| StageError {
            code: storage_error_code(&e, ErrorCode::Internal),
            message: statement.failure_message(&e),
        })?;
        self.snapshot = Some(Arc::clone(&snapshot));
        Ok(snapshot)
    }
}

/// The run failure reporting `error`. A rejection belongs to its statement,
/// a stale read to the first statement that read the KG, and any other
/// failure to the last statement, where the run commits.
fn commit_failure(kg: &str, queued: &[Queued], error: CommitError) -> RunFailure {
    let last = queued.last().map_or(0, |q| q.index);
    match error {
        CommitError::Rejected { statement, error } => {
            match queued.iter().find(|q| q.index == statement) {
                Some(q) => {
                    let StageError { code, message } = q.statement.failure(&error);
                    RunFailure {
                        index: statement,
                        code,
                        message,
                    }
                }
                None => RunFailure {
                    index: statement,
                    code: storage_error_code(&error, ErrorCode::Validation),
                    message: error.to_string(),
                },
            }
        }
        CommitError::StoreReadOnly => RunFailure {
            index: last,
            code: ErrorCode::StoreReadOnly,
            message: crate::storage::StorageError::StoreReadOnly.to_string(),
        },
        CommitError::Failed(error) | CommitError::OutcomeUnknown(error) => RunFailure {
            index: last,
            code: storage_error_code(&error, ErrorCode::Internal),
            message: error.to_string(),
        },
        CommitError::Stale => {
            warn!(kg = %kg, attempts = MAX_STAGE_ATTEMPTS, "write_run_stale");
            let reader = queued.iter().find(|q| q.statement.reads_state());
            RunFailure {
                index: reader.map_or(last, |q| q.index),
                code: ErrorCode::Conflict,
                message: format!(
                    "The data this statement reads in knowledge graph '{kg}' kept changing \
                     under concurrent writes ({MAX_STAGE_ATTEMPTS} attempts); nothing was \
                     applied. Retry the program."
                ),
            }
        }
        CommitError::Cancelled(stop) => RunFailure {
            index: last,
            code: super::supervise::stop_code(stop),
            message: stop.message().to_string(),
        },
        CommitError::Unknown(error) => RunFailure {
            index: last,
            code: ErrorCode::OutcomeUnknown,
            message: format!(
                "The program's changes reached the write-ahead log but failed to apply \
                 ({error}); they may or may not be visible until restart. Read the state \
                 back before retrying."
            ),
        },
    }
}

/// The note that none of `queued` was applied because statement `failed`
/// failed, or `None` when that statement was the only write.
fn rolled_back_note(queued: &[Queued], failed: usize) -> Option<String> {
    match queued {
        [] => None,
        [only] if only.index == failed => None,
        [only] => Some(format!(
            " (rolled back: the program's write statement {} was not applied)",
            only.index
        )),
        [first, .., last] => Some(format!(
            " (rolled back: none of the program's {} write statements {}-{} was applied)",
            queued.len(),
            first.index,
            last.index
        )),
    }
}
