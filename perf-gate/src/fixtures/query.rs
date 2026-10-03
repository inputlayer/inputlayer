//! Warm read queries: serial latency, then concurrent throughput.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::Barrier;

use super::{create_kg, elapsed_us, expect_rows, load, Measurement};
use crate::client::Client;
use crate::dataset::{Graph, REACH_RULES};
use crate::profile::{QueryParams, SEED};
use crate::server::RunningServer;

/// Source node of every query.
const SOURCE: u64 = 1;

/// `?edge(1, Y)`: a point lookup returning a handful of rows.
pub async fn cheap(server: &RunningServer, params: &QueryParams) -> Result<Measurement> {
    let graph = Graph::random(params.nodes, params.edges, SEED);
    let query = format!("?edge({SOURCE}, Y)");
    measure(server, params, &graph, &query, graph.out_degree(SOURCE)).await
}

/// `?reach(1, Y)`: bound transitive closure.
pub async fn bound(server: &RunningServer, params: &QueryParams) -> Result<Measurement> {
    let graph = Graph::random(params.nodes, params.edges, SEED);
    let query = format!("?reach({SOURCE}, Y)");
    measure(server, params, &graph, &query, graph.reachable(SOURCE)).await
}

async fn measure(
    server: &RunningServer,
    params: &QueryParams,
    graph: &Graph,
    query: &str,
    expected: usize,
) -> Result<Measurement> {
    let mut client = create_kg(server).await?;
    load(&mut client, graph, &REACH_RULES).await?;

    for _ in 0..params.warmup {
        let (_, reply) = client.execute(query).await?;
        expect_rows(query, reply.row_count, expected)?;
    }
    let mut serial = Vec::with_capacity(params.serial);
    for _ in 0..params.serial {
        let (start, reply) = client.execute(query).await?;
        expect_rows(query, reply.row_count, expected)?;
        serial.push(elapsed_us(start, reply.at));
    }

    let mut clients = Vec::with_capacity(params.clients);
    for _ in 0..params.clients {
        clients.push(server.client(super::KG).await?);
    }
    let barrier = Arc::new(Barrier::new(params.clients + 1));
    let mut tasks = Vec::with_capacity(params.clients);
    for client in clients {
        let barrier = Arc::clone(&barrier);
        let query = query.to_string();
        let count = params.per_client;
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            closed_loop(client, &query, count, expected).await
        }));
    }
    barrier.wait().await;
    let start = Instant::now();
    let mut concurrent = Vec::with_capacity(params.clients * params.per_client);
    let mut end = start;
    for task in tasks {
        let (samples, finished) = task.await.context("query client task")??;
        concurrent.extend(samples);
        end = end.max(finished);
    }

    let mut measurement = Measurement::default();
    measurement.series("latency_us", serial);
    measurement.series("concurrent_latency_us", concurrent);
    measurement.rate(
        "queries_per_sec",
        (params.clients * params.per_client) as u64,
        end - start,
    );
    Ok(measurement)
}

/// Issue `count` queries back to back; samples and the finish instant.
async fn closed_loop(
    mut client: Client,
    query: &str,
    count: usize,
    expected: usize,
) -> Result<(Vec<u64>, Instant)> {
    let mut samples = Vec::with_capacity(count);
    let mut finished = Instant::now();
    for _ in 0..count {
        let (start, reply) = client.execute(query).await?;
        expect_rows(query, reply.row_count, expected)?;
        samples.push(elapsed_us(start, reply.at));
        finished = reply.at;
    }
    Ok((samples, finished))
}
