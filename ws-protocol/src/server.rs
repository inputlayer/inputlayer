//! Server → client frames.

use serde::{Deserialize, Serialize};

use crate::{
    ErrorCode, NoticeCode, Notification, RequestId, Row, StatementError, SubscriptionPush,
    TimingBreakdown, ValidationError,
};

/// How a client routes a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameClass {
    /// Answers a request; carries the request's `id` when it had one.
    Reply,
    /// A connection event; never an answer to a request.
    Notice,
    /// A data change or standing-query result; never an answer to a request.
    Push,
}

/// Every frame the server sends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// Authentication succeeded; the connection has a session.
    Authenticated {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        session_id: String,
        knowledge_graph: String,
        /// Engine version.
        version: String,
        role: String,
        /// [`crate::PROTOCOL_VERSION`] of the engine.
        protocol_version: u32,
        /// Identifies this run of the engine. Notification `seq` numbers and
        /// knowledge graph revisions are meaningful only within one epoch; a
        /// restarted engine has a new one. Pass it back with `last_seq` when
        /// reconnecting.
        stream_epoch: String,
    },
    /// Authentication failed, or a request arrived before authentication.
    AuthError {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        message: String,
    },
    /// A complete result in one frame.
    Result(ResultFrame),
    /// Header of a result streamed as `result_chunk` frames. A streamed
    /// result is complete only at its `result_end`; if an `error` answering
    /// the same request comes first, the result failed and the rows received
    /// so far must be discarded.
    ResultStart(ResultStartFrame),
    /// Rows of a streamed result, in order from `chunk_index` 0.
    ResultChunk {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        rows: Vec<Row>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        row_provenance: Vec<String>,
        chunk_index: usize,
    },
    /// End of a streamed result: the counts its chunks must add up to.
    ResultEnd {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        row_count: usize,
        chunk_count: usize,
    },
    /// Results of several queries, all exact at one knowledge graph
    /// revision: the reply to `read`, and to `subscribe` (then naming the
    /// subscription in `subscribed`).
    Snapshot(SnapshotFrame),
    /// Header of a snapshot streamed as `snapshot_chunk` frames: complete only
    /// at its `snapshot_end`; if an `error` answering the same request comes
    /// first, the rows received so far must be discarded.
    SnapshotStart(SnapshotStartFrame),
    /// Rows of result `result` (its index in the request) of a streamed
    /// snapshot, in order from `chunk_index` 0 across all results.
    SnapshotChunk {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        result: usize,
        chunk_index: usize,
        rows: Vec<Row>,
    },
    /// End of a streamed snapshot: how many chunks it had. Each result's
    /// chunks must add up to the `row_count` its header announced.
    SnapshotEnd {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        chunk_count: usize,
    },
    /// The request failed as a whole.
    Error {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        message: String,
        /// Per-statement parse errors.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        validation_errors: Option<Vec<ValidationError>>,
        /// Why it failed, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<ErrorCode>,
    },
    /// Answer to `ping`.
    Pong {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
    },
    /// Answer to `cancel`: what cancelling `target` did. The target's own
    /// reply still follows, in request order, and reports its outcome.
    CancelAck {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<RequestId>,
        target: RequestId,
        outcome: CancelOutcome,
    },
    /// A connection event.
    Notice { code: NoticeCode, message: String },
    /// A standing query's result changed or failed.
    #[serde(untagged)]
    Subscription(SubscriptionPush),
    /// A committed change in the connection's knowledge graph.
    #[serde(untagged)]
    Notification(Notification),
}

impl ServerFrame {
    /// How a client routes this frame.
    pub fn class(&self) -> FrameClass {
        match self {
            Self::Notice { .. } => FrameClass::Notice,
            Self::Subscription(_) | Self::Notification(_) => FrameClass::Push,
            _ => FrameClass::Reply,
        }
    }

    /// The `id` of the request this frame answers. Always `None` for notices
    /// and pushes.
    pub fn request_id(&self) -> Option<&RequestId> {
        match self {
            Self::Authenticated { id, .. }
            | Self::AuthError { id, .. }
            | Self::ResultChunk { id, .. }
            | Self::ResultEnd { id, .. }
            | Self::SnapshotChunk { id, .. }
            | Self::SnapshotEnd { id, .. }
            | Self::Error { id, .. }
            | Self::CancelAck { id, .. }
            | Self::Pong { id } => id.as_ref(),
            Self::Result(frame) => frame.id.as_ref(),
            Self::ResultStart(frame) => frame.id.as_ref(),
            Self::Snapshot(frame) => frame.id.as_ref(),
            Self::SnapshotStart(frame) => frame.id.as_ref(),
            Self::Notice { .. } | Self::Subscription(_) | Self::Notification(_) => None,
        }
    }

    /// An `error` frame without validation details.
    pub fn error(id: Option<RequestId>, code: Option<ErrorCode>, message: String) -> Self {
        Self::Error {
            id,
            message,
            validation_errors: None,
            code,
        }
    }
}

/// What a `cancel` did to its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelOutcome {
    /// The target stopped before it began committing: nothing it would have
    /// changed is applied, and it replies with an error of code `cancelled`
    /// (`deadline_exceeded` if its deadline had already stopped it).
    Cancelled,
    /// The target already finished or began committing, which is not
    /// interrupted; its reply reports what it did.
    TooLate,
    /// No unanswered request of this connection has that id (never sent,
    /// already answered, or a request that cannot be cancelled).
    NotFound,
}

/// A complete result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultFrame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    pub row_count: usize,
    /// Rows before limit/offset or the result cap.
    pub total_count: usize,
    /// Whether a limit or the result cap cut the rows.
    pub truncated: bool,
    pub execution_time_ms: u64,
    /// Per-row origin ("persistent", "ephemeral", ...), parallel to `rows`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_provenance: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SessionMetadata>,
    /// The connection now uses this knowledge graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub switched_kg: Option<String>,
    /// Derivations of `.why` results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof_trees: Option<Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_breakdown: Option<TimingBreakdown>,
    /// Failed statements; empty when every statement succeeded.
    #[serde(default)]
    pub errors: Vec<StatementError>,
    /// Fact statements the program committed, with their effective counts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statements: Vec<StatementCounts>,
    /// Revision the program's committed writes are visible at, the revision
    /// pushes carry: a push at this revision or later reflects them. Set only
    /// when the program changed persistent state of a knowledge graph:
    /// committed fact, schema and rule writes, `.rel drop`, `.clear prefix`,
    /// `.index create|drop|rebuild`, `.kg create` and
    /// `.ontology install|remove|upgrade`; with several, the last one's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    /// Set on the reply to `.subscribe`: `rows` is the subscription's snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribed: Option<Subscribed>,
}

/// Header of a streamed result: [`ResultFrame`] without its rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultStartFrame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub columns: Vec<String>,
    pub total_count: usize,
    pub truncated: bool,
    pub execution_time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SessionMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub switched_kg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proof_trees: Option<Vec<serde_json::Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_breakdown: Option<TimingBreakdown>,
    #[serde(default)]
    pub errors: Vec<StatementError>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statements: Vec<StatementCounts>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
    /// Set on the reply to `.subscribe`: the chunks hold the snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribed: Option<Subscribed>,
}

/// Results of several queries at one revision, in one frame.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotFrame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub knowledge_graph: String,
    /// The knowledge graph revision every result is the exact answer at.
    pub revision: u64,
    /// One per query of the request, in its order.
    pub results: Vec<NamedResult>,
    pub execution_time_ms: u64,
    /// Set on the reply to `subscribe`: the results are the group's snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribed: Option<Subscribed>,
}

/// One query's result in a [`SnapshotFrame`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedResult {
    /// The query's name in the request.
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Row>,
    /// Rows before the result cap.
    pub total_count: usize,
    /// Whether the result cap cut the rows. Never set in a subscription's
    /// snapshot: a capped result fails the `subscribe` instead.
    pub truncated: bool,
}

/// Header of a streamed snapshot: [`SnapshotFrame`] without its rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotStartFrame {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<RequestId>,
    pub knowledge_graph: String,
    pub revision: u64,
    pub results: Vec<NamedResultHeader>,
    pub execution_time_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribed: Option<Subscribed>,
}

/// One query's result in a [`SnapshotStartFrame`]: [`NamedResult`] without
/// its rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedResultHeader {
    pub name: String,
    pub columns: Vec<String>,
    /// Rows the result's chunks carry, in total.
    pub row_count: usize,
    pub total_count: usize,
    pub truncated: bool,
}

/// What a committed fact statement changed.
///
/// Counts are effective: `inserted` counts tuples absent before the statement
/// and present after it, `deleted` tuples present before it and absent after
/// it, each as of the statements before it in the same program. A statement
/// that matched nothing, or inserted facts that were already present, commits
/// with zero counts and is still listed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementCounts {
    /// 0-based statement index in the program.
    pub index: usize,
    pub kind: StatementKind,
    pub inserted: usize,
    pub deleted: usize,
}

/// The form of a fact statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementKind {
    /// `+relation(...)`, one tuple or a bulk list.
    Insert,
    /// `-relation(...)`, literal tuples or a conditional delete.
    Delete,
    /// `-old, +new <- body`.
    Update,
}

/// Session (ephemeral) data that took part in a result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMetadata {
    pub has_ephemeral: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ephemeral_sources: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// The subscription a `.subscribe` or `subscribe` registered.
///
/// A name can be reused after `.unsubscribe`; the generation is unique per
/// connection, so a push whose generation differs from the one returned here
/// belongs to an earlier registration and is stale.
///
/// A subscription lives and dies with its connection: it is never resumed
/// on another one. After a reconnect, subscribe again and start over from
/// the new snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscribed {
    pub subscription: String,
    pub generation: u64,
    /// The knowledge graph revision the snapshot is the exact answer at.
    /// Every later delta names a higher revision. Revisions order the
    /// states of one knowledge graph within one stream epoch; they are not
    /// comparable across epochs or knowledge graphs.
    pub revision: u64,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
