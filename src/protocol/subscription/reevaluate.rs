//! Re-evaluation strategy: run the query against the current snapshot and diff
//! with the previous result.
//!
//! Each evaluation pins the KG's current snapshot and runs the query on it
//! through the normal query path ([`Handler::query_snapshot`]), which runs on
//! the blocking pool under the query semaphore and reuses the query's compiled
//! plan while the rules stay the same.
//!
//! The evaluation runs for no one in particular: its rows depend only on the
//! knowledge graph (data and persistent rules) and the query. Whoever receives
//! them is authorized separately, when subscribing, before the snapshot is
//! sent and before each delivery. That holds because authorization is per
//! knowledge graph; row- or relation-level authorization would have to run
//! the evaluation as the subscriber's scope and add that scope to the
//! [`super::ViewKey`].

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;

use crate::protocol::rest::handlers::wire_value_to_json;
use crate::protocol::Handler;
use crate::statement::{parse_query, QueryGoal};
use crate::storage_engine::KnowledgeGraphSnapshot;

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

/// A query's complete result on one snapshot, before it is diffed.
pub struct Evaluated {
    /// Result columns; `None` for an empty result, which names none.
    pub columns: Option<Vec<String>>,
    pub result: Arc<ResultSet>,
    pub dependencies: Dependencies,
    pub revision: u64,
    /// Engine time of the evaluation (excluding waits for a compute permit
    /// and the compile stages the timing breakdown measured).
    pub cost: Duration,
    /// Whether the evaluation reused a compiled plan: `cost` includes no
    /// compilation.
    pub plan_cached: bool,
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

    /// The query text.
    pub fn query(&self) -> &str {
        &self.query
    }

    /// The knowledge graph the query reads.
    pub fn knowledge_graph(&self) -> &str {
        &self.knowledge_graph
    }

    /// The handler the query runs on.
    pub fn handler(&self) -> &Arc<Handler> {
        &self.handler
    }

    /// The knowledge graph's current snapshot.
    pub fn current_snapshot(&self) -> Result<Arc<KnowledgeGraphSnapshot>, String> {
        self.handler
            .get_storage()
            .get_snapshot_for(&self.knowledge_graph)
            .map_err(|e| format!("Knowledge graph '{}': {e}", self.knowledge_graph))
    }

    /// Evaluate the query on `snapshot`: its rules give the dependencies, the
    /// query reads its data, and its revision names the result.
    pub async fn evaluate_on(
        &self,
        snapshot: Arc<KnowledgeGraphSnapshot>,
    ) -> Result<Evaluated, String> {
        evaluate(
            &self.handler,
            &self.knowledge_graph,
            &self.query,
            &self.goal,
            snapshot,
        )
        .await
    }

    /// Make `evaluated` the current result: the change since the previous one.
    pub fn adopt(&mut self, evaluated: Evaluated) -> Refresh {
        let Evaluated {
            columns,
            result: next,
            dependencies,
            revision,
            cost: _,
            plan_cached: _,
        } = evaluated;
        if let Some(columns) = columns {
            self.columns = columns;
        }
        let inserted = next.difference(&self.current);
        let retracted = self.current.difference(&next);
        self.current = Arc::clone(&next);
        Refresh {
            columns: self.columns.clone(),
            inserted,
            retracted,
            dependencies,
            revision,
            result: next,
        }
    }

    async fn reevaluate(&mut self) -> Result<Refresh, String> {
        let snapshot = self.current_snapshot()?;
        let evaluated = self.evaluate_on(snapshot).await?;
        Ok(self.adopt(evaluated))
    }
}

/// Run `query` (parsed: `goal`) on `snapshot` of `knowledge_graph`.
pub async fn evaluate(
    handler: &Handler,
    knowledge_graph: &str,
    query: &str,
    goal: &QueryGoal,
    snapshot: Arc<KnowledgeGraphSnapshot>,
) -> Result<Evaluated, String> {
    let dependencies = Dependencies::for_query(goal, &snapshot.rules);
    let revision = snapshot.revision;
    let ran = run_query(handler, knowledge_graph, query, snapshot, false).await?;
    Ok(Evaluated {
        columns: ran.columns,
        result: Arc::new(ran.rows.into_iter().collect()),
        dependencies,
        revision,
        cost: ran.cost,
        plan_cached: ran.plan_cached,
    })
}

/// A query's complete result rows on one snapshot.
pub struct QueryRows {
    /// Result columns; `None` for an empty result, which names none.
    pub columns: Option<Vec<String>>,
    /// Every row, as the engine returned it (distinct as engine values).
    pub rows: Vec<Row>,
    /// Engine time of the evaluation (excluding waits for a compute permit
    /// and the compile stages the timing breakdown measured), plus converting
    /// its rows.
    pub cost: Duration,
    /// Whether the evaluation reused a compiled plan: `cost` includes no
    /// compilation.
    pub plan_cached: bool,
}

/// Run `query` on `snapshot` of `knowledge_graph`, as a sharing `probe` or
/// not (see [`Handler::query_snapshot`]); a capped result is an error.
pub async fn run_query(
    handler: &Handler,
    knowledge_graph: &str,
    query: &str,
    snapshot: Arc<KnowledgeGraphSnapshot>,
    probe: bool,
) -> Result<QueryRows, String> {
    let started = Instant::now();
    let (result, plan_cached) = handler
        .query_snapshot(knowledge_graph, snapshot, query, None, probe)
        .await?;
    // A capped result is not the result set: adopting it would announce
    // every cut row as retracted. Fail before touching state, so the last
    // complete result stays the base for the next delta.
    if result.truncated {
        return Err(incomplete_result_error(
            handler.config().storage.performance.max_result_rows,
        ));
    }
    let engine = result.timing_breakdown.as_ref().map_or_else(
        || started.elapsed(),
        |timing| {
            let compiling = timing.parse_us
                + timing.sip_us
                + timing.magic_sets_us
                + timing.ir_build_us
                + timing.optimize_us;
            Duration::from_micros(timing.total_us.saturating_sub(compiling))
        },
    );
    let converting = Instant::now();
    let columns =
        (!result.schema.is_empty()).then(|| result.schema.into_iter().map(|c| c.name).collect());
    let rows = result
        .rows
        .into_iter()
        .map(|row| -> Row { row.values.into_iter().map(wire_value_to_json).collect() })
        .collect();
    Ok(QueryRows {
        columns,
        rows,
        cost: engine + converting.elapsed(),
        plan_cached,
    })
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
        Box::pin(self.reevaluate())
    }
}
