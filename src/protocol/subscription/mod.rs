//! Standing queries: `.subscribe` / `.unsubscribe` over the global `/ws`.
//!
//! A subscription is a query whose result set the connection keeps current.
//! After every committed persistent change that can affect it, the server
//! pushes the rows that entered (`inserted`) and left (`retracted`) the result.
//!
//! The module separates *protocol* from *evaluation*:
//!
//! - [`registry`] - per-connection state machine: dependency filtering,
//!   coalescing (at most one evaluation in flight per subscription), `seq`
//!   numbering, and the pushed messages. Knows nothing about how results are
//!   computed.
//! - [`standing_query`] - the [`StandingQuery`] trait: "bring this view up to
//!   date and report the change". The registry only talks to this trait.
//! - [`reevaluate`] - today's strategy: re-run the query against a snapshot and
//!   diff with the previous result. A differential-dataflow strategy can
//!   replace it behind the same trait and protocol.
//! - [`dependencies`] - which relations a query reads, through persistent rules.
//! - [`connection`] - glue that drives the registry from the WS loop.
//!
//! Subscriptions read persistent data only: session facts and session rules
//! are excluded, as with `.why`.

pub mod connection;
pub mod dependencies;
pub mod reevaluate;
pub mod registry;
pub mod standing_query;

use std::sync::atomic::{AtomicU64, Ordering};

pub use connection::ConnectionSubscriptions;
pub use dependencies::Dependencies;
pub use reevaluate::ReevaluatingQuery;
pub use registry::{ChangeSet, Completion, Dispatch, Push, SubscriptionRegistry};
pub use standing_query::{Refresh, Row, StandingQuery};

/// Server-wide subscription counters.
#[derive(Debug, Default)]
pub struct SubscriptionMetrics {
    evaluations: AtomicU64,
    active: AtomicU64,
}

impl SubscriptionMetrics {
    /// Evaluations started (initial snapshots plus re-evaluations).
    pub fn evaluations(&self) -> u64 {
        self.evaluations.load(Ordering::SeqCst)
    }

    /// Subscriptions currently registered across all connections.
    pub fn active(&self) -> u64 {
        self.active.load(Ordering::SeqCst)
    }

    pub(crate) fn record_evaluation(&self) {
        self.evaluations.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn add_active(&self, count: u64) {
        self.active.fetch_add(count, Ordering::SeqCst);
    }

    pub(crate) fn remove_active(&self, count: u64) {
        self.active.fetch_sub(count, Ordering::SeqCst);
    }
}
