//! Persistent Differential Dataflow Computation
//!
//! `DDComputation` - a long-lived DD computation for one knowledge graph.
//! Owns a timely worker thread, InputSessions for base relations, and shared
//! arrangements for materialized views.
//!
//! ## Architecture
//!
//! ```text
//! Main thread --command_tx--► DD worker thread
//!                              |- Owns timely Worker
//!                              |- Owns InputSessions (one per base relation)
//!                              |- Owns Arrangements (one per relation + view)
//!                              |- Steps worker in event loop
//!                              `- Processes commands between steps
//! ```
//!
//! ## Thread Safety
//!
//! InputSessions and TraceAgents are NOT Send/Sync (they use Rc internally).
//! All DD state lives on the worker thread. The main thread communicates
//! exclusively through the command channel. Queries that need data from
//! arrangements send a response channel and block until the worker replies.

use crate::derived_relations::{CompiledRule, DerivedRelationsManager};
use crate::index_manager::{Index, IndexManager, IndexStats, RegisteredIndex, TupleId};
use crate::value::Tuple;
use crossbeam_channel as channel;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Commands sent from the main thread to the DD worker thread.
pub enum DDCommand {
    /// Feed a batch of updates into a relation's InputSession.
    /// Each update is (data, time, diff) where diff is +1 for insert, -1 for delete.
    InsertDelta {
        relation: String,
        updates: Vec<(Tuple, u64, isize)>,
    },

    /// Advance time on all InputSessions and flush.
    /// The worker will step until computation catches up.
    AdvanceTime(u64),

    /// Block until the probe frontier advances past the given time.
    /// Sends () on the response channel when done.
    WaitUntilCaughtUp {
        time: u64,
        response: channel::Sender<()>,
    },

    /// Read all current tuples from a relation's arrangement.
    /// Returns the tuples via the response channel.
    ReadRelation {
        relation: String,
        response: channel::Sender<Vec<Tuple>>,
    },

    /// Add a new relation dynamically.
    /// Creates a new InputSession and arrangement in a new dataflow.
    AddRelation {
        name: String,
        response: channel::Sender<()>,
    },

    /// Shut down the computation cleanly.
    /// Sends () on the response channel when shutdown is complete.
    Shutdown { response: channel::Sender<()> },

    // === Derived Relations ===
    /// Register a compiled rule for materialization.
    /// The rule is stored but not immediately materialized.
    RegisterRule {
        rule: CompiledRule,
        response: channel::Sender<Result<(), String>>,
    },

    /// Remove a rule and its materialization.
    RemoveRule {
        name: String,
        response: channel::Sender<()>,
    },

    /// Read a derived relation's materialized data.
    /// Returns None if not materialized or invalid.
    ReadDerivedRelation {
        relation: String,
        response: channel::Sender<Option<Vec<Tuple>>>,
    },

    /// Set materialized data for a derived relation.
    /// Called after executing a rule to cache its results.
    SetMaterialized {
        relation: String,
        tuples: Vec<Tuple>,
        response: channel::Sender<()>,
    },

    /// Notify that a base relation has been updated.
    /// Invalidates dependent derived relations.
    NotifyBaseUpdate {
        relation: String,
        response: channel::Sender<Vec<String>>,
    },

    /// Get the current derived relations manager state.
    GetDerivedStats {
        response: channel::Sender<(usize, usize, usize)>, // (total_rules, materialized, invalid)
    },

    // === Index Management Commands ===
    /// Register a new index (metadata only, does not build)
    RegisterIndex {
        index: RegisteredIndex,
        response: channel::Sender<Result<(), String>>,
    },

    /// Remove an index
    RemoveIndex {
        name: String,
        response: channel::Sender<Result<(), String>>,
    },

    /// Store built index data
    SetIndexMaterialized {
        name: String,
        index: Box<dyn Index + Send + Sync>,
        tuple_count: usize,
        response: channel::Sender<()>,
    },

    /// Read an index for query (returns stats, not the index itself)
    GetIndexStats {
        name: Option<String>,
        response: channel::Sender<Vec<IndexStats>>,
    },

    /// Incremental index update (batched inserts and deletes)
    UpdateIndex {
        name: String,
        inserts: Vec<(TupleId, Vec<f32>)>,
        deletes: Vec<TupleId>,
        response: channel::Sender<Result<(), String>>,
    },

    /// Notify indexes that a base relation has been updated
    NotifyIndexesBaseUpdate {
        relation: String,
        response: channel::Sender<Vec<String>>,
    },
}

/// Handle to a persistent DD computation for one knowledge graph.
///
/// Each knowledge graph gets one DDComputation. It owns:
/// - A dedicated worker thread running a timely computation with u64 timestamps
/// - InputSessions for feeding deltas into base relations
/// - Arrangements for each base relation (queryable)
///
/// All DD state is owned by the worker thread (InputSession and TraceAgent
/// are not Send). The main thread communicates through the command channel.
