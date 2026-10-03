//! The evaluation-strategy boundary the oracle compares across.
//!
//! An adapter owns one independent copy of a knowledge graph and keeps it in
//! step with a history. Today's adapters are full recomputation, standing
//! queries maintained from subscription deltas, and a naive reference
//! evaluator. A persistent derived-graph adapter (incremental dataflows owned
//! per KG) plugs in behind the same trait: `observe` takes the revision the
//! oracle expects, so an adapter that maintains results asynchronously must
//! answer as of that committed revision, not "whatever it has so far".

use crate::model::{AdapterError, Observation, Outcome, Revision};

pub trait Adapter {
    /// Short stable name used in divergence reports.
    fn name(&self) -> &'static str;

    /// Apply one IQL statement; `revision` is the position after it.
    fn execute(&mut self, statement: &str, revision: Revision) -> Result<Outcome, AdapterError>;

    /// Stop and reopen from durable state, as a server restart would.
    fn restart(&mut self, revision: Revision) -> Result<(), AdapterError>;

    /// The result of `query` as of `revision`.
    fn observe(&mut self, query: &str, revision: Revision) -> Result<Observation, AdapterError>;

    /// The baseline rejected a statement this adapter returned
    /// [`Outcome::Assumed`] for. From now on the adapter must report
    /// `Unsupported` (with `reason`) instead of results.
    ///
    /// Only adapters that ever return `Assumed` need to implement this.
    fn desync(&mut self, reason: String) {
        panic!("adapter {} never assumes validity: {reason}", self.name());
    }
}

/// Builds a fresh set of adapters; the oracle and minimizer rerun histories
/// from scratch, so adapters are never reused across runs.
pub type AdapterFactory<'a> = dyn Fn(&[String]) -> Vec<Box<dyn Adapter>> + 'a;
