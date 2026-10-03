//! The evaluation-strategy boundary for standing queries.

use std::sync::Arc;

use futures_util::future::BoxFuture;

use super::{Dependencies, ResultSet};

pub use inputlayer_ws_protocol::Row;

/// Change in a standing query's result since its previous refresh.
#[derive(Debug, Clone, Default)]
pub struct Refresh {
    /// Result columns (kept from the last non-empty result).
    pub columns: Vec<String>,
    /// Rows now in the result that were not before, sorted.
    pub inserted: Vec<Row>,
    /// Rows no longer in the result, sorted.
    pub retracted: Vec<Row>,
    /// Relations the result depends on, as of this refresh.
    pub dependencies: Dependencies,
    /// The knowledge graph revision the refreshed result is the exact answer at.
    pub revision: u64,
    /// The complete refreshed result: the previous result plus this change.
    pub result: Arc<ResultSet>,
}

impl Refresh {
    /// True when the result set did not change.
    pub fn is_unchanged(&self) -> bool {
        self.inserted.is_empty() && self.retracted.is_empty()
    }
}

/// A query result kept current by some evaluation strategy.
///
/// The result depends only on the knowledge graph and the query, never on who
/// asks: one view serves every subscriber of the same query, and each
/// subscriber's read access is checked separately.
///
/// The first `refresh` reports the full result as `inserted`. Each later call
/// reports the set difference against the previous successful refresh. Each
/// refresh evaluates one snapshot of the knowledge graph and reports its
/// revision; revisions never go back. A failed refresh leaves the previous
/// result in place. A refresh that cannot see the complete result (e.g. one
/// cut at `max_result_rows`) must fail rather than report a difference against
/// the partial set.
pub trait StandingQuery: Send + 'static {
    /// Bring the result up to date with the knowledge graph.
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>>;
}
