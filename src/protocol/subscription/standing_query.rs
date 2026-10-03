//! The evaluation-strategy boundary for standing queries.

use futures_util::future::BoxFuture;

use super::Dependencies;

/// One result row, in wire JSON form.
pub type Row = Vec<serde_json::Value>;

/// Change in a standing query's result since its previous refresh.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Refresh {
    /// Result columns (kept from the last non-empty result).
    pub columns: Vec<String>,
    /// Rows now in the result that were not before, sorted.
    pub inserted: Vec<Row>,
    /// Rows no longer in the result, sorted.
    pub retracted: Vec<Row>,
    /// Relations the result depends on, as of this refresh.
    pub dependencies: Dependencies,
}

impl Refresh {
    /// True when the result set did not change.
    pub fn is_unchanged(&self) -> bool {
        self.inserted.is_empty() && self.retracted.is_empty()
    }
}

/// A query result kept current by some evaluation strategy.
///
/// The first `refresh` reports the full result as `inserted`. Each later call
/// reports the set difference against the previous successful refresh. A
/// failed refresh leaves the previous result in place. A refresh that cannot
/// see the complete result (e.g. one cut at `max_result_rows`) must fail
/// rather than report a difference against the partial set.
pub trait StandingQuery: Send + 'static {
    /// Bring the result up to date with the knowledge graph.
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>>;
}
