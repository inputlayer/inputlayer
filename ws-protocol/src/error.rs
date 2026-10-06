//! Error payloads of `error` frames and `result.errors`.

use serde::{Deserialize, Serialize};

/// Why a statement or request failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The write was refused: the store is read-only until restart recovery.
    StoreReadOnly,
    /// The input was rejected (schema, size limits, bad values or rules).
    Validation,
    /// The named KG, relation, rule or index does not exist.
    NotFound,
    /// The statement conflicts with current state (already exists, in use).
    Conflict,
    /// The statement cannot run on this path (client-only, WS-only).
    Unsupported,
    /// The engine failed to apply a valid statement.
    Internal,
    /// The request frame or its `id` is malformed; nothing ran.
    InvalidRequest,
    /// The connection is over its message rate; nothing ran.
    RateLimited,
    /// The request's deadline passed before it began committing; nothing it
    /// would have changed was applied.
    DeadlineExceeded,
    /// The client cancelled the request before it began committing; nothing
    /// it would have changed was applied.
    Cancelled,
    /// The program's `expect_revision` precondition did not hold: state in
    /// its scope changed after that revision, or the revision is not one of
    /// this engine run's. Nothing was applied. Read the state again before
    /// deciding whether to retry.
    PreconditionFailed,
    /// The request failed after it began committing: its changes may or may
    /// not be applied. Read the state back before retrying.
    OutcomeUnknown,
    /// The request would go over a memory limit: its query held more than
    /// the per-query limit or grew past the server's query memory budget, or
    /// its writes would grow the knowledge graph past its memory budget. It
    /// was refused and nothing was applied.
    ResourceExhausted,
    /// The request committed on the primary, but no replica confirmed
    /// applying it within `replication.sync_timeout_ms` (synchronous
    /// replication with `on_follower_loss = "block"`). Its changes are
    /// applied on the primary; they could be lost if the primary is lost
    /// before a replica receives them. Do not retry it as if it failed.
    ReplicaUnconfirmed,
    /// The caller may not do this: its role, its write grants or its API
    /// key's scope do not allow the statement, or its credential was revoked
    /// or has expired. Nothing ran.
    AccessDenied,
    /// The server could not admit the request to compute in time: its
    /// lane's queue was full, or no compute permit came within the longest
    /// admission wait (`storage.performance.admission.max_wait_ms`) while
    /// its own deadline was later. Nothing ran and nothing was applied;
    /// retry later, with backoff.
    Overloaded,
}

/// A failed statement of a program.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementError {
    /// 0-based statement index in the program.
    pub index: usize,
    pub code: ErrorCode,
    pub message: String,
}

/// A parse error of one statement in a program.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationError {
    /// 1-based line number in the original program text.
    pub line: usize,
    /// 0-based index of the statement (counting only non-empty lines).
    pub statement_index: usize,
    /// The parse error message.
    pub error: String,
}
