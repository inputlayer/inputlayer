//! The gate's fixtures. Each runs against a freshly started server, measures
//! raw samples, and verifies every result it times.

mod delta;
mod first_delta;
mod insert;
mod interference;
mod query;

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
    /// Writer to a probe agent, beside a long request and a slow consumer.
    Interference,
}

impl Fixture {
    pub const ALL: [Fixture; 8] = [
        Fixture::CheapQuery,
        Fixture::BoundQuery,
        Fixture::InsertSingle,
        Fixture::InsertBatch,
        Fixture::DeltaSingle,
        Fixture::DeltaFanout,
        Fixture::DeltaFirst,
        Fixture::Interference,
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
            Fixture::Interference => "interference",
        }
    }

    pub fn parse(name: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|f| f.name() == name)
            .with_context(|| format!("unknown fixture '{name}'"))
    }

    /// Run against `server` (fresh, empty) with `profile`'s parameters.
    pub async fn run(self, server: &RunningServer, profile: &Profile) -> Result<Measurement> {
        let mut measurement = match self {
            Fixture::CheapQuery => query::cheap(server, &profile.cheap_query).await,
            Fixture::BoundQuery => query::bound(server, &profile.bound_query).await,
            Fixture::InsertSingle => insert::single(server, &profile.insert).await,
            Fixture::InsertBatch => insert::batch(server, &profile.insert).await,
            Fixture::DeltaSingle => delta::run(server, &profile.delta_single).await,
            Fixture::DeltaFanout => delta::run(server, &profile.delta_fanout).await,
            Fixture::DeltaFirst => first_delta::run(server, &profile.delta_first).await,
            Fixture::Interference => interference::run(server, &profile.interference).await,
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

/// Fail unless a reply carried exactly `expected` rows.
fn expect_rows(what: &str, got: usize, expected: usize) -> Result<()> {
    if got != expected {
        bail!("{what}: {got} rows, expected {expected}");
    }
    Ok(())
}
