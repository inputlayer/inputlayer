//! Reads of a whole deployed recursive view: the unbound recursive read of
//! the views benchmark, as a gated case (issue #384: the runaway-query
//! guards cost it 20%).
//!
//! The KG holds chains of four nodes (three edges each), every hundredth
//! chain head labelled hot, and the views benchmark's recursive rules:
//! `path`, the right-recursive transitive closure (two rows per edge), and
//! `hot_reach(X, Y) <- label(X, "hot"), path(X, Y)` (three rows per hot
//! head). `?hot_reach(X, Y)` evaluates the whole closure for a small answer,
//! so its latency is the engine's recursion, not the reply.

use anyhow::{Context, Result};

use super::{create_kg, elapsed_us, expect_rows, load_hot_chains, Measurement};
use crate::profile::RecursiveParams;
use crate::server::RunningServer;

/// The views benchmark's recursive rules.
const RULES: [&str; 3] = [
    "+path(X, Y) <- edge(X, Y)",
    "+path(X, Z) <- edge(X, Y), path(Y, Z)",
    "+hot_reach(X, Y) <- label(X, \"hot\"), path(X, Y)",
];

const QUERY: &str = "?hot_reach(X, Y)";

/// `?hot_reach(X, Y)`, serially.
pub async fn run(server: &RunningServer, params: &RecursiveParams) -> Result<Measurement> {
    let mut client = create_kg(server).await?;
    let hot = load_hot_chains(&mut client, (params.edges as u64).div_ceil(3)).await?;
    for rule in RULES {
        client.execute(rule).await.context("define rule")?;
    }
    // Every hot head reaches the three other nodes of its chain.
    let expected = 3 * hot as usize;

    for _ in 0..params.warmup {
        let (_, reply) = client.execute(QUERY).await?;
        expect_rows(QUERY, reply.row_count, expected)?;
    }
    let mut latency_us = Vec::with_capacity(params.serial);
    for _ in 0..params.serial {
        let (start, reply) = client.execute(QUERY).await?;
        expect_rows(QUERY, reply.row_count, expected)?;
        latency_us.push(elapsed_us(start, reply.at));
    }

    let mut measurement = Measurement::default();
    measurement.series("latency_us", latency_us);
    Ok(measurement)
}
