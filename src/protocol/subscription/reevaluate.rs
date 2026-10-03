//! Re-evaluation strategy: run the query against the current snapshot and diff
//! with the previous result.
//!
//! Bound queries go through Magic Sets, so a re-run touches only the relevant
//! slice of the KG. Each evaluation pins the KG's current snapshot and runs
//! the query on it through the normal query path ([`Handler::query_snapshot`]),
//! which runs on the blocking pool under the query semaphore.
//!
//! The evaluation runs for no one in particular: its rows depend only on the
//! knowledge graph (data and persistent rules) and the query. Whoever receives
//! them is authorized separately, when subscribing, before the snapshot is
//! sent and before each delivery. That holds because authorization is per
//! knowledge graph; row- or relation-level authorization would have to run
//! the evaluation as the subscriber's scope and add that scope to the
//! [`super::ViewKey`].

use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::Handler;
use crate::statement::{parse_query, QueryGoal};

use super::{Dependencies, Refresh, ResultSet, Row, StandingQuery};

/// A standing query kept current by full re-evaluation.
pub struct ReevaluatingQuery {
    handler: Arc<Handler>,
    knowledge_graph: String,
    query: String,
    goal: QueryGoal,
    columns: Vec<String>,
    /// The last complete result.
    current: Arc<ResultSet>,
}

impl ReevaluatingQuery {
    /// Prepare `query` (`?body`) on `knowledge_graph`.
    pub fn new(handler: Arc<Handler>, knowledge_graph: &str, query: &str) -> Result<Self, String> {
        let body = query
            .trim()
            .strip_prefix('?')
            .ok_or_else(|| format!("Subscription query must start with '?': {query}"))?;
        let goal = parse_query(body).map_err(|e| format!("Failed to parse query: {e}"))?;
        if goal.limit.is_some() || goal.offset.is_some() {
            return Err(
                "Subscriptions track the whole result set; remove limit/offset from the query."
                    .to_string(),
            );
        }
        Ok(Self {
            handler,
            knowledge_graph: knowledge_graph.to_string(),
            query: query.trim().to_string(),
            goal,
            columns: Vec::new(),
            current: Arc::default(),
        })
    }

    /// The parsed query.
    pub fn goal(&self) -> &QueryGoal {
        &self.goal
    }

    async fn evaluate(&mut self) -> Result<Refresh, String> {
        // One snapshot for everything: its rules give the dependencies, the
        // query reads its data, and its revision names the result.
        let snapshot = self
            .handler
            .get_storage()
            .get_snapshot_for(&self.knowledge_graph)
            .map_err(|e| format!("Knowledge graph '{}': {e}", self.knowledge_graph))?;
        let dependencies = Dependencies::for_query(&self.goal, &snapshot.rules);
        let revision = snapshot.revision;

        let result = self
            .handler
            .query_snapshot(&self.knowledge_graph, snapshot, &self.query, None)
            .await?;
        // A capped result is not the result set: adopting it would announce
        // every cut row as retracted. Fail before touching state, so the last
        // complete result stays the base for the next delta.
        if result.truncated {
            return Err(incomplete_result_error(
                self.handler.config().storage.performance.max_result_rows,
            ));
        }
        if !result.schema.is_empty() {
            self.columns = result.schema.into_iter().map(|c| c.name).collect();
        }
        let next: Arc<ResultSet> = Arc::new(
            result
                .rows
                .into_iter()
                .map(|row| -> Row { row.values.into_iter().map(wire_value_to_json).collect() })
                .collect(),
        );
        let inserted = next.difference(&self.current);
        let retracted = self.current.difference(&next);
        self.current = Arc::clone(&next);
        Ok(Refresh {
            columns: self.columns.clone(),
            inserted,
            retracted,
            dependencies,
            revision,
            result: next,
        })
    }
}

/// Error for a result cut at `max_result_rows`.
fn incomplete_result_error(max_result_rows: usize) -> String {
    format!(
        "Subscription result exceeds storage.performance.max_result_rows \
         ({max_result_rows}); no complete result to deliver. Narrow the query."
    )
}

impl StandingQuery for ReevaluatingQuery {
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
        Box::pin(self.evaluate())
    }
}
