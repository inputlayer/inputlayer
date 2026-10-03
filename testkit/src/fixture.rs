//! Named, reproducible knowledge-graph fixtures shared by the reactive e2e
//! suite and the performance benches.
//!
//! A fixture is plain IQL: a knowledge graph name plus the statements that
//! build it. [`Fixture::install`] creates the graph on an engine over the
//! WebSocket, exactly as a deployment would load it.

use crate::client::WsClient;
use crate::contract::{Checked, Violation};
use crate::engine::Engine;

/// Facts per insert statement, keeping every request far below the frame limit.
const FACTS_PER_STATEMENT: usize = 1_000;

/// A knowledge graph and the statements that build it.
#[derive(Debug, Clone)]
pub struct Fixture {
    /// Stable fixture name recorded with every latency sample.
    pub name: String,
    pub knowledge_graph: String,
    pub statements: Vec<String>,
}

impl Fixture {
    /// Empty fixture `name` on `knowledge_graph`.
    pub fn new(name: &str, knowledge_graph: &str) -> Self {
        Self {
            name: name.to_string(),
            knowledge_graph: knowledge_graph.to_string(),
            statements: Vec::new(),
        }
    }

    /// Insert `facts` (IQL tuples such as `(1, 2)`) into `relation`.
    #[must_use]
    pub fn facts(mut self, relation: &str, facts: impl IntoIterator<Item = String>) -> Self {
        let facts: Vec<String> = facts.into_iter().collect();
        for batch in facts.chunks(FACTS_PER_STATEMENT) {
            self.statements
                .push(format!("+{relation}[{}]", batch.join(", ")));
        }
        self
    }

    /// Add a persistent rule clause, e.g. `reach(X, Y) <- edge(X, Y)`.
    #[must_use]
    pub fn rule(mut self, clause: &str) -> Self {
        self.statements.push(format!("+{clause}"));
        self
    }

    /// Create the knowledge graph on `engine` and run every statement.
    pub async fn install(&self, engine: &Engine) -> Checked<()> {
        let mut admin = WsClient::connect(engine, "default").await?;
        admin
            .commit(&format!(".kg create {}", self.knowledge_graph))
            .await?;
        admin.close().await;
        let mut writer = WsClient::connect(engine, &self.knowledge_graph).await?;
        for statement in &self.statements {
            writer.commit(statement).await.map_err(|e| {
                Violation::Rejected(format!("fixture '{}' statement failed: {e}", self.name))
            })?;
        }
        writer.close().await;
        Ok(())
    }
}

/// `edge` is a chain `0 -> 1 -> ... -> nodes-1`; `reach` is its transitive
/// closure. A bound standing query `?reach(0, X)` sees every node.
pub fn reachability_chain(knowledge_graph: &str, nodes: i64) -> Fixture {
    Fixture::new(&format!("reachability_chain_{nodes}"), knowledge_graph)
        .facts("edge", (1..nodes).map(|n| format!("({}, {n})", n - 1)))
        .rule("reach(X, Y) <- edge(X, Y)")
        .rule("reach(X, Z) <- reach(X, Y), edge(Y, Z)")
}
