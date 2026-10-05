//! The gate's fixtures. Each runs against a freshly started server, measures
//! raw samples, and verifies every result it times.

mod delta;
mod engine;
mod first_delta;
mod insert;
mod interference;
mod keyed;
mod query;
mod recursive;
mod shop;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};

use crate::client::Client;
use crate::dataset::Graph;
use crate::profile::Profile;
use crate::schema::Rate;
use crate::server::RunningServer;

/// Knowledge graph every fixture works in.
pub const KG: &str = "perf";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fixture {
    /// Point lookup on a base relation: protocol and dispatch overhead.
    CheapQuery,
    /// Warm bound recursive query (`?reach(1, Y)`, Magic Sets).
    BoundQuery,
    /// Persisted single-fact inserts, serial and concurrent writers.
    InsertSingle,
    /// Persisted 1K-fact batch inserts.
    InsertBatch,
    /// External writer to one subscribed agent.
    DeltaSingle,
    /// External writer to many agents subscribed to the same query.
    DeltaFanout,
    /// First write after subscribing against a warm one, per fresh agent.
    DeltaFirst,
    /// External writer to many agents, each subscribed to its own key of a rule.
    DeltaKeyed,
    /// Writer to a probe agent, beside a long request and a slow consumer.
    Interference,
    /// The scenario suite's shop pack installed into a fresh graph; held to
    /// an absolute ceiling (`policy.toml`), not to the baseline.
    ShopInstall,
    /// Whole deployed recursive view over 100K edges (`?hot_reach(X, Y)`).
    RecursiveQuery,
    // The engine suite (not gated; see `engine`).
    /// Warm bound non-recursive rule (`?two_hop(1, Z)`).
    RuleQuery,
    /// Whole transitive closure (`?reach(X, Y)`).
    UnboundQuery,
    /// Durable deletes, conditional deletes and conditional updates.
    Writes,
    /// Guarded inserts: wins, losses and racing connections.
    Claims,
    /// `.why` proofs of a non-recursive and a recursive rule.
    Why,
    /// Many sessions with their own bound standing queries in one graph.
    Sessions,
    /// Resident memory per base fact.
    MemoryFacts,
    /// Resident memory per knowledge graph.
    MemoryGraphs,
    /// Crash and restart on the same data directory.
    Recovery,
    /// `insert_single` with asynchronous durability: the WAL's share.
    InsertAsync,
}

impl Fixture {
    /// Every fixture, by name.
    pub const ALL: [Fixture; 21] = [
        Fixture::CheapQuery,
        Fixture::BoundQuery,
        Fixture::InsertSingle,
        Fixture::InsertBatch,
        Fixture::DeltaSingle,
        Fixture::DeltaFanout,
        Fixture::DeltaFirst,
        Fixture::DeltaKeyed,
        Fixture::Interference,
        Fixture::ShopInstall,
        Fixture::RecursiveQuery,
        Fixture::RuleQuery,
        Fixture::UnboundQuery,
        Fixture::Writes,
        Fixture::Claims,
        Fixture::Why,
        Fixture::Sessions,
        Fixture::MemoryFacts,
        Fixture::MemoryGraphs,
        Fixture::Recovery,
        Fixture::InsertAsync,
    ];

    /// The gate's fixtures: the default of `run`, and what the policy judges.
    pub const GATE: [Fixture; 11] = [
        Fixture::CheapQuery,
        Fixture::BoundQuery,
        Fixture::InsertSingle,
        Fixture::InsertBatch,
        Fixture::DeltaSingle,
        Fixture::DeltaFanout,
        Fixture::DeltaFirst,
        Fixture::DeltaKeyed,
        Fixture::Interference,
        Fixture::ShopInstall,
        Fixture::RecursiveQuery,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Fixture::CheapQuery => "cheap_query",
            Fixture::BoundQuery => "bound_query",
            Fixture::InsertSingle => "insert_single",
            Fixture::InsertBatch => "insert_batch",
            Fixture::DeltaSingle => "delta_single",
            Fixture::DeltaFanout => "delta_fanout",
            Fixture::DeltaFirst => "delta_first",
            Fixture::DeltaKeyed => "delta_keyed",
            Fixture::Interference => "interference",
            Fixture::ShopInstall => "shop_install",
            Fixture::RecursiveQuery => "recursive_query",
            Fixture::RuleQuery => "rule_query",
            Fixture::UnboundQuery => "unbound_query",
            Fixture::Writes => "writes",
            Fixture::Claims => "claims",
            Fixture::Why => "why",
            Fixture::Sessions => "sessions",
            Fixture::MemoryFacts => "memory_facts",
            Fixture::MemoryGraphs => "memory_graphs",
            Fixture::Recovery => "recovery",
            Fixture::InsertAsync => "insert_async",
        }
    }

    /// Fixtures named by `name`: one fixture, or the group `gate`, `engine`
    /// (the engine suite) or `all`.
    pub fn parse_group(name: &str) -> Result<Vec<Self>> {
        Ok(match name {
            "gate" => Self::GATE.to_vec(),
            "engine" => Self::ALL
                .into_iter()
                .filter(|f| !Self::GATE.contains(f))
                .collect(),
            "all" => Self::ALL.to_vec(),
            _ => vec![Self::parse(name)?],
        })
    }

    /// Environment this fixture's server needs on top of the gate's overrides.
    pub fn server_env(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Fixture::InsertAsync => &[engine::ASYNC_DURABILITY],
            _ => &[],
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|f| f.name() == name)
            .with_context(|| format!("unknown fixture '{name}'"))
    }

    /// Run against `server` (fresh, empty) with `profile`'s parameters.
    pub async fn run(self, server: &mut RunningServer, profile: &Profile) -> Result<Measurement> {
        let engine = &profile.engine;
        let mut measurement = match self {
            Fixture::CheapQuery => query::cheap(server, &profile.cheap_query).await,
            Fixture::BoundQuery => query::bound(server, &profile.bound_query).await,
            // The same workload; `server_env` makes the async server.
            Fixture::InsertSingle | Fixture::InsertAsync => {
                insert::single(server, &profile.insert).await
            }
            Fixture::InsertBatch => insert::batch(server, &profile.insert).await,
            Fixture::DeltaSingle => delta::run(server, &profile.delta_single).await,
            Fixture::DeltaFanout => delta::run(server, &profile.delta_fanout).await,
            Fixture::DeltaFirst => first_delta::run(server, &profile.delta_first).await,
            Fixture::DeltaKeyed => keyed::run(server, &profile.delta_keyed).await,
            Fixture::Interference => interference::run(server, &profile.interference).await,
            Fixture::ShopInstall => shop::install(server, &profile.shop_install).await,
            Fixture::RecursiveQuery => recursive::run(server, &profile.recursive_query).await,
            Fixture::RuleQuery => engine::rule_query(server, &engine.rule_query).await,
            Fixture::UnboundQuery => engine::unbound_query(server, &engine.unbound_query).await,
            Fixture::Writes => engine::writes(server, &engine.writes).await,
            Fixture::Claims => engine::claims(server, &engine.claims).await,
            Fixture::Why => engine::why(server, &engine.why).await,
            Fixture::Sessions => engine::sessions(server, &engine.sessions).await,
            Fixture::MemoryFacts => engine::memory_facts(server, &engine.memory).await,
            Fixture::MemoryGraphs => engine::memory_graphs(server, &engine.memory).await,
            Fixture::Recovery => engine::recovery(server, &engine.recovery).await,
        }?;
        if let Some(rss) = server.peak_rss_kb() {
            measurement.gauges.insert("server_peak_rss_kb".into(), rss);
        }
        Ok(measurement)
    }
}

/// Raw output of one fixture run.
#[derive(Debug, Default)]
pub struct Measurement {
    pub series: BTreeMap<String, Vec<u64>>,
    pub rates: BTreeMap<String, Rate>,
    pub gauges: BTreeMap<String, u64>,
}

impl Measurement {
    fn series(&mut self, name: &str, samples: Vec<u64>) {
        self.series.insert(name.to_string(), samples);
    }

    fn rate(&mut self, name: &str, ops: u64, elapsed: Duration) {
        self.rates.insert(
            name.to_string(),
            Rate {
                ops,
                elapsed_us: micros(elapsed),
            },
        );
    }
}

/// Whole microseconds, saturating.
pub fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Microseconds from `start` to `end`.
pub fn elapsed_us(start: Instant, end: Instant) -> u64 {
    micros(end.saturating_duration_since(start))
}

/// Create [`KG`] and return a client bound to it.
async fn create_kg(server: &RunningServer) -> Result<Client> {
    let mut admin = server.client("default").await?;
    admin.execute(&format!(".kg create {KG}")).await?;
    server.client(KG).await
}

/// Load `graph` into `edge` and run `rules`.
async fn load(client: &mut Client, graph: &Graph, rules: &[&str]) -> Result<()> {
    for program in graph.insert_programs("edge") {
        client.execute(&program).await.context("load edges")?;
    }
    for rule in rules {
        client.execute(rule).await.context("define rule")?;
    }
    Ok(())
}

/// The `i`-th hot chain head of [`load_hot_chains`]: the head of every
/// hundredth chain.
fn hot_head(i: u64) -> u64 {
    4 * 100 * i
}

/// Load `chains` chains of four nodes into `edge` and label every hundredth
/// chain head hot (`label(H, "hot")`); returns the number of hot heads.
async fn load_hot_chains(client: &mut Client, chains: u64) -> Result<u64> {
    for program in Graph::chains(chains).insert_programs("edge") {
        client.execute(&program).await.context("load edges")?;
    }
    let hot = chains.div_ceil(100);
    let labels: Vec<String> = (0..hot)
        .map(|i| format!("({}, \"hot\")", hot_head(i)))
        .collect();
    for chunk in labels.chunks(5_000) {
        client
            .execute(&format!("+label[{}]", chunk.join(", ")))
            .await
            .context("load labels")?;
    }
    Ok(hot)
}

/// Fail unless a reply carried exactly `expected` rows.
fn expect_rows(what: &str, got: usize, expected: usize) -> Result<()> {
    if got != expected {
        bail!("{what}: {got} rows, expected {expected}");
    }
    Ok(())
}
