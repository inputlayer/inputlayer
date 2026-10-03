//! The first delta after subscribing, against a warm one on the same agent.
//!
//! Each cycle connects a fresh agent and subscribes it to
//! [`delta::SUBSCRIPTION`]; the writer then inserts probe 0 at once, the way
//! a reactive agent's first change follows its subscription. `first_delta_us`
//! is that write's send to its delta at the agent. After the agent has been
//! quiet for `settle_ms`, inserting probe 1 gives `warm_delta_us`. The agent
//! then disconnects and the writer retracts both probes, so every cycle
//! starts from the same KG.
//!
//! A first delta much slower than a warm one means subscribing leaves work
//! (or a transport stall) for the first write to pay.

use std::time::Duration;

use anyhow::{bail, Result};
use serde_json::json;

use super::delta::{self, Probes};
use super::{elapsed_us, Measurement};
use crate::client::{Client, Frame, Stamped};
use crate::profile::FirstDeltaParams;
use crate::server::RunningServer;

/// Probes the cycles write: each is inserted once and retracted once per cycle.
const PROBES: usize = 2;

pub async fn run(server: &RunningServer, params: &FirstDeltaParams) -> Result<Measurement> {
    let mut kg = delta::prepare(server, params.nodes, params.edges, PROBES).await?;
    let settle = Duration::from_millis(params.settle_ms);
    let mut first_us = Vec::with_capacity(params.cycles);
    let mut warm_us = Vec::with_capacity(params.cycles);
    let mut subscribe_us = Vec::with_capacity(params.cycles);
    for cycle in 0..params.cycles {
        let (mut agent, subscribed) = kg.subscribe(server, &format!("first{cycle}")).await?;
        subscribe_us.push(subscribed);
        first_us.push(insert_probe(&mut kg.writer, &mut agent, kg.probes, 0).await?);
        tokio::time::sleep(settle).await;
        warm_us.push(insert_probe(&mut kg.writer, &mut agent, kg.probes, 1).await?);
        drop(agent);
        let (mid0, mid1) = (kg.probes.mid(0), kg.probes.mid(1));
        kg.writer
            .execute(&format!("-edge(1, {mid0})\n-edge(1, {mid1})"))
            .await?;
    }
    let mut measurement = Measurement::default();
    measurement.series("first_delta_us", first_us);
    measurement.series("warm_delta_us", warm_us);
    measurement.series("subscribe_us", subscribe_us);
    Ok(measurement)
}

/// Insert `probe` and time its send to the agent reading exactly its row.
/// The agent is read while the write is in flight, so its stamp is the
/// arrival, not the moment the writer's reply was handled.
async fn insert_probe(
    writer: &mut Client,
    agent: &mut Client,
    probes: Probes,
    probe: usize,
) -> Result<u64> {
    let mid = probes.mid(probe);
    let program = format!("+edge(1, {mid})");
    let (write, push) = tokio::join!(writer.execute(&program), agent.next_push());
    let (sent, _) = write?;
    let Stamped { at, frame } = push?;
    match frame {
        Frame::SubscriptionDelta {
            inserted,
            retracted,
            ..
        } if retracted.is_empty()
            && matches!(inserted.as_slice(), [row] if row.last() == Some(&json!(mid + 1))) =>
        {
            Ok(elapsed_us(sent, at))
        }
        other => bail!(
            "probe {probe}: expected the delta inserting two_hop(1, {}), got {other:?}",
            mid + 1
        ),
    }
}
