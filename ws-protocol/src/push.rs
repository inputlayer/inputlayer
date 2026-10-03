//! Frames pushed for data changes: notifications and standing-query results.

use serde::{Deserialize, Serialize};

/// One result row, values in column order.
pub type Row = Vec<serde_json::Value>;

/// A committed persistent change, broadcast to the connections bound to its
/// knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Notification {
    /// A base relation was updated (insert or delete).
    PersistentUpdate {
        knowledge_graph: String,
        relation: String,
        operation: String,
        count: usize,
        /// Epoch milliseconds when the change occurred.
        timestamp_ms: u64,
        /// Session that triggered the change (none for API-key or system operations).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        /// Position in the engine's notification stream (see [`Notification::seq`]).
        seq: u64,
    },
    /// A rule was registered, removed or dropped.
    RuleChange {
        knowledge_graph: String,
        rule_name: String,
        /// "registered", "removed" or "dropped".
        operation: String,
        timestamp_ms: u64,
        seq: u64,
    },
    /// A knowledge graph was created or dropped.
    KgChange {
        knowledge_graph: String,
        /// "created" or "dropped".
        operation: String,
        timestamp_ms: u64,
        seq: u64,
    },
    /// An index was created or dropped, or a relation dropped.
    SchemaChange {
        knowledge_graph: String,
        entity: String,
        /// "created" or "dropped".
        operation: String,
        timestamp_ms: u64,
        seq: u64,
    },
}

impl Notification {
    /// The knowledge graph the change happened in.
    pub fn knowledge_graph(&self) -> &str {
        match self {
            Self::PersistentUpdate {
                knowledge_graph, ..
            }
            | Self::RuleChange {
                knowledge_graph, ..
            }
            | Self::KgChange {
                knowledge_graph, ..
            }
            | Self::SchemaChange {
                knowledge_graph, ..
            } => knowledge_graph,
        }
    }

    /// The change's position in the engine's notification stream: strictly
    /// increasing in delivery order within one stream epoch
    /// (`authenticated.stream_epoch`), from 1. A restarted engine starts a new
    /// epoch and numbers from 1 again, so a sequence number means nothing
    /// without its epoch. Notification sequence numbers are unrelated to a
    /// subscription's delta `seq`.
    pub fn seq(&self) -> u64 {
        match self {
            Self::PersistentUpdate { seq, .. }
            | Self::RuleChange { seq, .. }
            | Self::KgChange { seq, .. }
            | Self::SchemaChange { seq, .. } => *seq,
        }
    }
}

/// A change to a standing query's result, addressed by the subscription's name
/// and generation (see [`crate::Subscribed`]).
///
/// A delta is delivered whole or not at all. One that fits a frame is a
/// single `subscription_delta`; a larger one is streamed as
/// `subscription_delta_start`, `subscription_delta_chunk`s and
/// `subscription_delta_end`, which together are that same one delta. A client
/// applies a streamed delta only at its end, after checking that the chunks
/// arrived in order and add up to the announced counts; until then its result
/// stays at the previous revision. Frames of a streamed delta arrive in order,
/// but other frames (replies, notifications, other subscriptions' pushes) may
/// arrive between them. A connection that closes mid-stream delivered nothing
/// of that delta.
///
/// A delta the server cannot deliver whole ends its subscription with a
/// `subscription_reset` instead: the client's rows for it are no longer
/// maintained and it must subscribe again.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionPush {
    /// The result set changed.
    SubscriptionDelta {
        subscription: String,
        generation: u64,
        knowledge_graph: String,
        /// Delta number within this generation, from 1, without gaps: a
        /// client that sees any other number has lost a delta and must
        /// resubscribe.
        seq: u64,
        /// The knowledge graph revision whose result this delta produces: the
        /// previous result plus this delta is the query's exact answer at
        /// `revision` (see [`crate::Subscribed::revision`]).
        revision: u64,
        columns: Vec<String>,
        inserted: Vec<Row>,
        retracted: Vec<Row>,
    },
    /// Header of a delta streamed in chunks: what
    /// [`SubscriptionDelta`](Self::SubscriptionDelta) carries besides its rows.
    SubscriptionDeltaStart {
        subscription: String,
        generation: u64,
        knowledge_graph: String,
        seq: u64,
        revision: u64,
        columns: Vec<String>,
    },
    /// Rows of the streamed delta `seq`, in order from `chunk_index` 0.
    SubscriptionDeltaChunk {
        subscription: String,
        generation: u64,
        seq: u64,
        chunk_index: usize,
        inserted: Vec<Row>,
        retracted: Vec<Row>,
    },
    /// End of the streamed delta `seq`: the counts its chunks must add up to.
    /// Only now does the delta apply.
    SubscriptionDeltaEnd {
        subscription: String,
        generation: u64,
        seq: u64,
        chunk_count: usize,
        inserted_count: usize,
        retracted_count: usize,
    },
    /// Re-evaluation failed; the subscription stays registered and its result
    /// is unchanged, so the next delta applies to the last one delivered.
    SubscriptionError {
        subscription: String,
        generation: u64,
        message: String,
    },
    /// The server ended the subscription because it could not deliver its
    /// next change (the change cannot be framed, or read access was lost).
    /// Discard its rows, including a streamed delta still in progress, and
    /// subscribe again for a fresh snapshot.
    SubscriptionReset {
        subscription: String,
        generation: u64,
        message: String,
    },
}

impl SubscriptionPush {
    /// The subscription's name and generation.
    pub fn subscription(&self) -> (&str, u64) {
        match self {
            Self::SubscriptionDelta {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionDeltaStart {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionDeltaChunk {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionDeltaEnd {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionError {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionReset {
                subscription,
                generation,
                ..
            } => (subscription, *generation),
        }
    }
}
