//! The evaluation-strategy boundary for standing queries.

use std::sync::Arc;

use futures_util::future::BoxFuture;

use super::{Dependencies, ResultSet};

pub use inputlayer_ws_protocol::Row;

/// Change in one query's result since the previous refresh.
#[derive(Debug, Clone, Default)]
pub struct QueryRefresh {
    /// Result columns (kept from the last non-empty result).
    pub columns: Vec<String>,
    /// Rows now in the result that were not before, sorted.
    pub inserted: Vec<Row>,
    /// Rows no longer in the result, sorted.
    pub retracted: Vec<Row>,
    /// The complete refreshed result: the previous result plus this change.
    pub result: Arc<ResultSet>,
}

impl QueryRefresh {
    /// True when the result set did not change.
    pub fn is_unchanged(&self) -> bool {
        self.inserted.is_empty() && self.retracted.is_empty()
    }
}

/// Change in a standing query's results since its previous refresh.
#[derive(Debug, Clone, Default)]
pub struct Refresh {
    /// One per query of the view, in the view's order.
    pub queries: Vec<QueryRefresh>,
    /// Relations any of the results depends on, as of this refresh.
    pub dependencies: Dependencies,
    /// The knowledge graph revision every refreshed result is the exact answer at.
    pub revision: u64,
}

impl Refresh {
    /// True when no result set changed.
    pub fn is_unchanged(&self) -> bool {
        self.queries.iter().all(QueryRefresh::is_unchanged)
    }
}

/// Query results kept current together by some evaluation strategy.
///
/// A standing query is one or more queries (a *group*) on one knowledge
/// graph. Their results depend only on the knowledge graph and the queries,
/// never on who asks: one view serves every subscriber of the same queries,
/// and each subscriber's read access is checked separately.
///
/// The first `refresh` reports every full result as `inserted`. Each later
/// call reports the set differences against the previous successful refresh.
/// Each refresh evaluates every query against one snapshot of the knowledge
/// graph and reports its revision, so all results are exact at that one
/// revision; revisions never go back. A refresh fails as a whole: a failed
/// refresh leaves every previous result in place. A refresh that cannot see a
/// complete result (e.g. one cut at `max_result_rows`) must fail rather than
/// report a difference against the partial set.
pub trait StandingQuery: Send + 'static {
    /// Bring the result up to date with the knowledge graph.
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>>;
}
