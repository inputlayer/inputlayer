//! Standing queries: `.subscribe` / `.unsubscribe` over the global `/ws`.
//!
//! A subscription is a query whose result set the connection keeps current.
//! After every committed persistent change that can affect it, the server
//! pushes the rows that entered (`inserted`) and left (`retracted`) the result.
//!
//! Subscriptions to the same query on the same knowledge graph share one
//! *view*, whichever connection or user they belong to: one evaluation per
//! change, one result in memory. A view's rows depend only on the knowledge
//! graph and the query; who may see them is checked per subscriber, when it
//! subscribes and before every push.
//!
//! The module separates *protocol* from *evaluation*:
//!
//! - [`views`] - the shared-view state machine: keys, dependency filtering,
//!   coalescing (at most one evaluation in flight per view, plus an optional
//!   window), and the publications naming the revision each result is at.
//!   Knows nothing about how results are computed.
//! - [`hub`] - the one worker task that owns the views, receives committed
//!   changes and runs evaluations.
//! - [`publication`] - how a view hands results to subscribers without a lock.
//! - [`subscriber`] - one subscription as its connection sees it: read access,
//!   `seq` numbering and the last delivered result.
//! - [`connection`] - a connection's subscriptions, driven by its WS loop.
//! - [`standing_query`] - the [`StandingQuery`] trait: "bring this view up to
//!   date and report the change". The views only talk to this trait.
//! - [`reevaluate`] - today's strategy: re-run the query against a snapshot and
//!   diff with the previous result. A differential-dataflow strategy can
//!   replace it behind the same trait and protocol.
//! - [`result_set`] - a result under set semantics, keyed collision-safely.
//! - [`dependencies`] / [`change`] - which relations a query reads, through
//!   persistent rules, and which ones a commit touched.
//!
//! Subscriptions read persistent data only: session facts and session rules
//! are excluded, as with `.why`.

pub mod change;
pub mod connection;
pub mod dependencies;
pub mod hub;
pub mod publication;
pub mod reevaluate;
pub mod result_set;
pub mod standing_query;
pub mod subscriber;
pub mod views;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod testing;

use std::sync::atomic::{AtomicU64, Ordering};

pub use change::{change_of, changes_rules, ChangeSet};
pub use connection::ConnectionSubscriptions;
pub use dependencies::Dependencies;
pub use hub::SubscriptionHub;
pub use publication::Doorbell;
pub use reevaluate::ReevaluatingQuery;
pub use result_set::ResultSet;
pub use standing_query::{Refresh, Row, StandingQuery};
pub use subscriber::{Snapshot, Subscriber};
pub use views::{Attach, Completed, Dispatch, ViewKey, ViewRegistry};

/// Server-wide subscription counters.
#[derive(Debug, Default)]
pub struct SubscriptionMetrics {
    evaluations: AtomicU64,
    active: AtomicU64,
    views: AtomicU64,
}

impl SubscriptionMetrics {
    /// Evaluations started (initial snapshots plus re-evaluations), one per
    /// shared view however many subscribers it has.
    pub fn evaluations(&self) -> u64 {
        self.evaluations.load(Ordering::SeqCst)
    }

    /// Subscriptions currently registered across all connections.
    pub fn active(&self) -> u64 {
        self.active.load(Ordering::SeqCst)
    }

    /// Shared views currently evaluated.
    pub fn views(&self) -> u64 {
        self.views.load(Ordering::SeqCst)
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

    pub(crate) fn set_views(&self, count: u64) {
        self.views.store(count, Ordering::SeqCst);
    }
}
