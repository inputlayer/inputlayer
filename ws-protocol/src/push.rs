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
/// and generation (see [`crate::Subscribed`]). A plain subscription
/// (`.subscribe`) gets `subscription_delta`s, a subscription group (the
/// `subscribe` frame) `subscription_group_delta`s; both get
/// `subscription_error` and `subscription_reset`.
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
/// A group delta streams the same way, as `subscription_group_delta_start`,
/// `subscription_group_delta_chunk`s (each holding rows of one member) and
/// `subscription_group_delta_end`.
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
    /// The results of a subscription group changed: one push per refresh
    /// that changed any member, listing every member in the group's order.
    /// After applying it, every member's result is its query's exact answer
    /// at `revision`; a member marked `unchanged` was already exact there.
    /// The group's pushes share one gapless `seq`, as a plain subscription's
    /// deltas do.
    SubscriptionGroupDelta {
        subscription: String,
        generation: u64,
        knowledge_graph: String,
        seq: u64,
        revision: u64,
        members: Vec<GroupMemberDelta>,
    },
    /// Header of a group delta streamed in chunks: what
    /// [`SubscriptionGroupDelta`](Self::SubscriptionGroupDelta) carries
    /// besides its rows, with each member's row counts.
    SubscriptionGroupDeltaStart {
        subscription: String,
        generation: u64,
        knowledge_graph: String,
        seq: u64,
        revision: u64,
        members: Vec<GroupMemberDeltaHeader>,
    },
    /// Rows of member `member` (its index in the group) of the streamed group
    /// delta `seq`, in order from `chunk_index` 0 across all members.
    SubscriptionGroupDeltaChunk {
        subscription: String,
        generation: u64,
        seq: u64,
        chunk_index: usize,
        member: usize,
        inserted: Vec<Row>,
        retracted: Vec<Row>,
    },
    /// End of the streamed group delta `seq`. Only now does the delta apply,
    /// once the chunks add up to every member's announced counts.
    SubscriptionGroupDeltaEnd {
        subscription: String,
        generation: u64,
        seq: u64,
        chunk_count: usize,
    },
    /// Re-evaluation failed; the subscription stays registered and its result
    /// is unchanged, so the next delta applies to the last one delivered. For
    /// a group, the refresh failed as a whole and every member is unchanged.
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
            | Self::SubscriptionGroupDelta {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionGroupDeltaStart {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionGroupDeltaChunk {
                subscription,
                generation,
                ..
            }
            | Self::SubscriptionGroupDeltaEnd {
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

/// One member of a [`SubscriptionPush::SubscriptionGroupDelta`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupMemberDelta {
    /// The member's name in the `subscribe` request.
    pub name: String,
    /// Whether the member's result did not change: `inserted` and
    /// `retracted` are then empty, and its result is exact at the push's
    /// revision as it is.
    pub unchanged: bool,
    pub columns: Vec<String>,
    pub inserted: Vec<Row>,
    pub retracted: Vec<Row>,
}

/// One member of a [`SubscriptionPush::SubscriptionGroupDeltaStart`]: what
/// [`GroupMemberDelta`] carries besides its rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMemberDeltaHeader {
    pub name: String,
    pub unchanged: bool,
    pub columns: Vec<String>,
    /// Rows the member's chunks insert, in total.
    pub inserted_count: usize,
    /// Rows the member's chunks retract, in total.
    pub retracted_count: usize,
}
