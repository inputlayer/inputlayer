//! Persisted inserts with the server's default durability.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::Barrier;

use super::{create_kg, elapsed_us, expect_rows, Measurement, KG};
use crate::client::Client;
use crate::profile::InsertParams;
use crate::server::RunningServer;

/// Single-fact inserts: one serial writer, then concurrent writers.
pub async fn single(server: &RunningServer, params: &InsertParams) -> Result<Measurement> {
    let mut client = create_kg(server).await?;
    let mut serial = Vec::with_capacity(params.single);
    let start = Instant::now();
    let mut end = start;
    for id in 0..params.single {
        let (sent, reply) = client.execute(&format!("+event({id}, {id})")).await?;
        serial.push(elapsed_us(sent, reply.at));
        end = reply.at;
    }
    let serial_elapsed = end - start;

    let mut writers = Vec::with_capacity(params.writers);
    for _ in 0..params.writers {
        writers.push(server.client(KG).await?);
    }
    let barrier = Arc::new(Barrier::new(params.writers + 1));
    let mut tasks = Vec::with_capacity(params.writers);
    for (index, writer) in writers.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let first = params.single + index * params.per_writer;
        let ids = first..first + params.per_writer;
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            write_each(writer, ids).await
        }));
    }
    barrier.wait().await;
    let start = Instant::now();
    let mut concurrent = Vec::with_capacity(params.writers * params.per_writer);
    let mut end = start;
    for task in tasks {
        let (samples, finished) = task.await.context("writer task")??;
        concurrent.extend(samples);
        end = end.max(finished);
    }

    let total = params.single + params.writers * params.per_writer;
    let (_, reply) = client.execute("?event(X, Y)").await?;
    expect_rows("committed events", reply.row_count, total)?;

    let mut measurement = Measurement::default();
    measurement.series("ack_us", serial);
    measurement.series("concurrent_ack_us", concurrent);
    measurement.rate("facts_per_sec", params.single as u64, serial_elapsed);
    measurement.rate(
        "concurrent_facts_per_sec",
        (params.writers * params.per_writer) as u64,
        end - start,
    );
    Ok(measurement)
}

/// Batch inserts of `batch_size` facts each.
pub async fn batch(server: &RunningServer, params: &InsertParams) -> Result<Measurement> {
    let mut client = create_kg(server).await?;
    let mut samples = Vec::with_capacity(params.batches);
    let start = Instant::now();
    let mut end = start;
    for batch in 0..params.batches {
        let first = batch * params.batch_size;
        let tuples: Vec<String> = (first..first + params.batch_size)
            .map(|id| format!("({id}, {id})"))
            .collect();
        let (sent, reply) = client
            .execute(&format!("+event[{}]", tuples.join(", ")))
            .await?;
        samples.push(elapsed_us(sent, reply.at));
        end = reply.at;
    }
    let (_, reply) = client.execute("?event(X, Y)").await?;
    expect_rows(
        "committed events",
        reply.row_count,
        params.batches * params.batch_size,
    )?;

    let mut measurement = Measurement::default();
    measurement.series("ack_us", samples);
    measurement.rate(
        "facts_per_sec",
        (params.batches * params.batch_size) as u64,
        end - start,
    );
    Ok(measurement)
}

async fn write_each(
    mut client: Client,
    ids: std::ops::Range<usize>,
) -> Result<(Vec<u64>, Instant)> {
    let mut samples = Vec::with_capacity(ids.len());
    let mut finished = Instant::now();
    for id in ids {
        let (sent, reply) = client.execute(&format!("+event({id}, {id})")).await?;
        samples.push(elapsed_us(sent, reply.at));
        finished = reply.at;
    }
    Ok((samples, finished))
}
