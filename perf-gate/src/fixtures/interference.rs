//! Delta latency to a probe agent while other connections misbehave: one
//! loops a long request, one is a slow consumer that stops reading its socket
//! with large results pending. Measures isolation, not raw speed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use super::delta;
use super::{elapsed_us, Measurement, KG};
use crate::client::Client;
use crate::profile::InterferenceParams;
use crate::server::RunningServer;

/// Two-hop pairs closed by a back edge: a long full join, small result.
const LONG_REQUEST: &str = "?two_hop(X, Z), edge(Z, X)";

/// A large result the slow consumer asks for and never reads.
const LARGE_RESULT: &str = "?two_hop(X, Z)";

pub async fn run(server: &RunningServer, params: &InterferenceParams) -> Result<Measurement> {
    let mut kg = delta::prepare(server, &params.delta).await?;

    // The slow consumer subscribes too, so every write also targets it.
    let (mut slow, _) = kg.subscribe(server, "slow").await?;
    for _ in 0..params.slow_consumer_requests {
        slow.send_execute(LARGE_RESULT).await?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let long_client = server.client(KG).await?;
    let expected = kg.graph.closed_two_hops();
    let long = tokio::spawn(loop_long_request(long_client, expected, Arc::clone(&stop)));

    let (probe, _) = kg.subscribe(server, "probe").await?;
    let probe = delta::spawn_agent(probe, kg.probes);
    let writes = delta::write_schedule(&mut kg.writer, kg.probes, &params.delta).await?;
    let arrivals = delta::drain(vec![probe]).await?.remove(0);
    stop.store(true, Ordering::Relaxed);
    let long_us = long.await.context("long request task")??;
    drop(slow);

    let (delta_us, _) = delta::delivery_latencies(&writes.event_sent(kg.probes), &[arrivals]);
    let mut measurement = Measurement::default();
    measurement.series("delta_us", delta_us);
    measurement.series("ack_us", writes.ack_us);
    measurement.series("long_request_us", long_us);
    Ok(measurement)
}

/// Run [`LONG_REQUEST`] back to back until `stop`; at least once.
async fn loop_long_request(
    mut client: Client,
    expected: usize,
    stop: Arc<AtomicBool>,
) -> Result<Vec<u64>> {
    let mut samples = Vec::new();
    loop {
        let (start, reply) = client.execute(LONG_REQUEST).await?;
        if reply.row_count != expected {
            bail!(
                "long request: {} rows, expected {expected}",
                reply.row_count
            );
        }
        samples.push(elapsed_us(start, reply.at));
        if stop.load(Ordering::Relaxed) {
            return Ok(samples);
        }
    }
}
