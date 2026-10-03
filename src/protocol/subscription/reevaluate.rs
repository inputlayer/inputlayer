//! Re-evaluation strategy: run the query against the current snapshot and diff
//! with the previous result.
//!
//! Bound queries go through Magic Sets, so a re-run touches only the relevant
//! slice of the KG. Evaluation uses the normal query path (`execute_program`),
//! which runs on the blocking pool under the query semaphore and re-checks the
//! subscriber's credential and read permission every time.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;

use crate::auth::Principal;
use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::Handler;
use crate::statement::{parse_query, QueryGoal};

use super::{Dependencies, Refresh, Row, StandingQuery};

/// A standing query kept current by full re-evaluation.
pub struct ReevaluatingQuery {
    handler: Arc<Handler>,
    knowledge_graph: String,
    query: String,
    goal: QueryGoal,
    auth: Option<Principal>,
    columns: Vec<String>,
    /// Current result, keyed by the row's canonical JSON for a deterministic,
    /// set-semantics comparison.
    current: BTreeMap<String, Row>,
}

impl ReevaluatingQuery {
    /// Prepare `query` (`?body`) on `knowledge_graph`, evaluated as `auth`.
    pub fn new(
        handler: Arc<Handler>,
        knowledge_graph: &str,
        query: &str,
        auth: Option<Principal>,
    ) -> Result<Self, String> {
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
            auth,
            columns: Vec::new(),
            current: BTreeMap::new(),
        })
    }

    async fn evaluate(&mut self) -> Result<Refresh, String> {
        // Dependencies come from the rules before the query runs: a rule
        // added in between is announced by its own notification afterwards.
        let rules = {
            let storage = self.handler.get_storage();
            let snapshot = storage
                .get_snapshot_for(&self.knowledge_graph)
                .map_err(|e| format!("Knowledge graph '{}': {e}", self.knowledge_graph))?;
            Arc::clone(&snapshot.rules)
        };
        let dependencies = Dependencies::for_query(&self.goal, &rules);

        let result = self
            .handler
            .execute_program(
                None,
                Some(self.knowledge_graph.clone()),
                self.query.clone(),
                self.auth.as_ref(),
            )
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
        let next: BTreeMap<String, Row> = result
            .rows
            .into_iter()
            .map(|row| {
                let row: Row = row.values.into_iter().map(wire_value_to_json).collect();
                (serde_json::Value::from(row.clone()).to_string(), row)
            })
            .collect();
        let inserted = difference(&next, &self.current);
        let retracted = difference(&self.current, &next);
        self.current = next;
        Ok(Refresh {
            columns: self.columns.clone(),
            inserted,
            retracted,
            dependencies,
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

/// Rows of `a` missing from `b`, in key order.
fn difference(a: &BTreeMap<String, Row>, b: &BTreeMap<String, Row>) -> Vec<Row> {
    a.iter()
        .filter(|(key, _)| !b.contains_key(*key))
        .map(|(_, row)| row.clone())
        .collect()
}

impl StandingQuery for ReevaluatingQuery {
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
        Box::pin(self.evaluate())
    }
}
