//! External writer to subscribed agents: insert-start to delta-at-agent.
//!
//! The KG holds a random graph, the rule `two_hop`, and one "probe" edge pair
//! `edge(P_k, Q_k)` per write. Every agent subscribes to `?two_hop(1, Z)`.
//! Write `k` inserts `edge(1, P_k)`, which adds exactly `two_hop(1, Q_k)`, and
//! retracts the probe inserted [`RETRACT_LAG`] writes earlier, so the result
//! size stays bounded and every row change identifies its write. The lag keeps
//! an insert and its own retraction from coalescing into one no-op refresh.
//!
//! Writes are open-loop on a fixed schedule. Each agent stamps deltas into
//! its own buffer; samples are joined with the writer's send instants only
//! after the run, so the harness adds no shared per-delta synchronization.

use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tokio::task::JoinHandle;

use super::{create_kg, elapsed_us, expect_rows, load, Measurement};
use crate::client::{Client, Frame, Stamped};
use crate::dataset::{Graph, TWO_HOP_RULE};
use crate::profile::{DeltaParams, SEED};
use crate::server::RunningServer;

/// The standing query every agent subscribes to.
pub const SUBSCRIPTION: &str = "?two_hop(1, Z)";

/// Time agents get, after the writer's last acknowledgement, to see every delta.
const DRAIN: Duration = Duration::from_secs(30);

/// Writes between a probe's insert and its retraction.
pub const RETRACT_LAG: usize = 32;

/// The probe edge pairs of a delta KG.
///
/// Row changes ("events") are numbered inserts first: event `k < writes` is
/// the insert of probe `k`, event `writes + k` its retraction.
#[derive(Debug, Clone, Copy)]
pub struct Probes {
    first: u64,
    writes: usize,
}

impl Probes {
    /// The program of write `index`.
    fn program(self, index: usize) -> String {
        let insert = format!("+edge(1, {})", self.mid(index));
        match index.checked_sub(RETRACT_LAG) {
            Some(old) => format!("{insert}\n-edge(1, {})", self.mid(old)),
            None => insert,
        }
    }

    /// The middle node of probe `probe`: `edge(1, mid)` adds `two_hop(1, mid + 1)`.
    pub fn mid(self, probe: usize) -> u64 {
        self.first + 2 * probe as u64
    }

    /// Row changes every agent must see.
    fn events(self) -> usize {
        self.writes + self.writes.saturating_sub(RETRACT_LAG)
    }

    /// The event of `two_hop(1, z)` entering (`inserted`) or leaving the result.
    fn event_of(self, z: u64, inserted: bool) -> Option<usize> {
        let offset = z.checked_sub(self.first + 1)?;
        let probe = usize::try_from(offset / 2).ok()?;
        if offset % 2 != 0 || probe >= self.writes {
            return None;
        }
        if inserted {
            Some(probe)
        } else {
            (probe + RETRACT_LAG < self.writes).then_some(self.writes + probe)
        }
    }

    /// The write that caused `event`.
    fn write_of(self, event: usize) -> usize {
        if event < self.writes {
            event
        } else {
            event - self.writes + RETRACT_LAG
        }
    }
}

/// A prepared delta KG: the writer's client, the probes and the snapshot size.
pub struct DeltaKg {
    pub writer: Client,
    pub probes: Probes,
    pub graph: Graph,
    snapshot_rows: usize,
}

/// Create and load the delta KG: a random graph of `nodes` and `edges`, plus
/// one probe edge pair for each of `probe_count` writes.
pub async fn prepare(
    server: &RunningServer,
    nodes: u64,
    edges: usize,
    probe_count: usize,
) -> Result<DeltaKg> {
    let mut graph = Graph::random(nodes, edges, SEED);
    let probes = Probes {
        first: nodes + 1,
        writes: probe_count,
    };
    for probe in 0..probe_count {
        graph.add(probes.mid(probe), probes.mid(probe) + 1);
    }
    let mut writer = create_kg(server).await?;
    load(&mut writer, &graph, &[TWO_HOP_RULE]).await?;
    let snapshot_rows = graph.two_hop(1);
    Ok(DeltaKg {
        writer,
        probes,
        graph,
        snapshot_rows,
    })
}

impl DeltaKg {
    /// Connect an agent and subscribe it; returns it with its subscribe latency.
    pub async fn subscribe(&self, server: &RunningServer, id: &str) -> Result<(Client, u64)> {
        let mut agent = server.client(super::KG).await?;
        let (start, reply) = agent
            .execute(&format!(".subscribe {id} {SUBSCRIPTION}"))
            .await?;
        expect_rows("subscription snapshot", reply.row_count, self.snapshot_rows)?;
        Ok((agent, elapsed_us(start, reply.at)))
    }
}

/// Collect the arrival instant of every event at one agent; see [`drain`].
pub fn spawn_agent(mut agent: Client, probes: Probes) -> JoinHandle<Result<Vec<Instant>>> {
    tokio::spawn(async move { collect(&mut agent, probes).await })
}

/// Wait for every agent, once the writer is done. The deadline runs from
/// the last write, so a slow writer never counts against the agents.
pub async fn drain(mut agents: Vec<JoinHandle<Result<Vec<Instant>>>>) -> Result<Vec<Vec<Instant>>> {
    let deadline = tokio::time::Instant::now() + DRAIN;
    let mut arrivals = Vec::with_capacity(agents.len());
    for index in 0..agents.len() {
        let Ok(joined) = tokio::time::timeout_at(deadline, &mut agents[index]).await else {
            agents.iter().for_each(JoinHandle::abort);
            bail!("agent {index} missed deltas {DRAIN:?} after the last write");
        };
        arrivals.push(joined.context("agent task")??);
    }
    Ok(arrivals)
}

async fn collect(agent: &mut Client, probes: Probes) -> Result<Vec<Instant>> {
    let mut arrivals: Vec<Option<Instant>> = vec![None; probes.events()];
    let mut remaining = probes.events();
    while remaining > 0 {
        let Stamped { at, frame } = agent.next_push().await?;
        let (inserted, retracted) = match frame {
            Frame::SubscriptionDelta {
                inserted,
                retracted,
                ..
            } => (inserted, retracted),
            Frame::SubscriptionError { message, .. } => bail!("subscription error: {message}"),
            other => bail!("unexpected push {other:?}"),
        };
        let rows = inserted
            .iter()
            .map(|row| (row, true))
            .chain(retracted.iter().map(|row| (row, false)));
        for (row, is_insert) in rows {
            let z = row.last().and_then(serde_json::Value::as_u64);
            let Some(event) = z.and_then(|z| probes.event_of(z, is_insert)) else {
                bail!("delta row {row:?} matches no write");
            };
            if arrivals[event].replace(at).is_some() {
                bail!("event {event} delivered twice");
            }
            remaining -= 1;
        }
    }
    Ok(arrivals.into_iter().flatten().collect())
}

/// Writer timings: send instants and acknowledgement latencies.
pub struct Writes {
    pub sent: Vec<Instant>,
    pub ack_us: Vec<u64>,
}

impl Writes {
    /// Send instant of the write behind each event.
    pub fn event_sent(&self, probes: Probes) -> Vec<Instant> {
        (0..probes.events())
            .map(|event| self.sent[probes.write_of(event)])
            .collect()
    }
}

/// Issue every probe write on its fixed schedule.
pub async fn write_schedule(
    writer: &mut Client,
    probes: Probes,
    params: &DeltaParams,
) -> Result<Writes> {
    let interval = Duration::from_millis(params.interval_ms);
    let origin = tokio::time::Instant::now();
    let mut timings = Writes {
        sent: Vec::with_capacity(params.writes),
        ack_us: Vec::with_capacity(params.writes),
    };
    for index in 0..params.writes {
        tokio::time::sleep_until(origin + interval * index as u32).await;
        let (sent, reply) = writer.execute(&probes.program(index)).await?;
        timings.sent.push(sent);
        timings.ack_us.push(elapsed_us(sent, reply.at));
    }
    Ok(timings)
}

/// Per-delivery latencies and, per event, the latency to the last agent.
pub fn delivery_latencies(sent: &[Instant], agents: &[Vec<Instant>]) -> (Vec<u64>, Vec<u64>) {
    let all = agents
        .iter()
        .flat_map(|arrivals| sent.iter().zip(arrivals).map(|(s, a)| elapsed_us(*s, *a)))
        .collect();
    let last = sent
        .iter()
        .enumerate()
        .map(|(index, s)| {
            agents
                .iter()
                .map(|arrivals| elapsed_us(*s, arrivals[index]))
                .max()
                .unwrap_or(0)
        })
        .collect();
    (all, last)
}

/// `params.subscribers` agents on one query, one external writer.
pub async fn run(server: &RunningServer, params: &DeltaParams) -> Result<Measurement> {
    let mut kg = prepare(server, params.nodes, params.edges, params.writes).await?;
    let mut subscribe_us = Vec::with_capacity(params.subscribers);
    let mut agents = Vec::with_capacity(params.subscribers);
    for index in 0..params.subscribers {
        let (agent, latency) = kg.subscribe(server, &format!("agent{index}")).await?;
        subscribe_us.push(latency);
        agents.push(spawn_agent(agent, kg.probes));
    }
    let writes = write_schedule(&mut kg.writer, kg.probes, params).await?;
    let arrivals = drain(agents).await?;
    let (delta_us, last_agent_us) = delivery_latencies(&writes.event_sent(kg.probes), &arrivals);

    let mut measurement = Measurement::default();
    measurement.series("delta_us", delta_us);
    measurement.series("last_agent_us", last_agent_us);
    measurement.series("ack_us", writes.ack_us);
    measurement.series("subscribe_us", subscribe_us);
    Ok(measurement)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WRITES: usize = RETRACT_LAG + 3;

    fn probes() -> Probes {
        Probes {
            first: 101,
            writes: WRITES,
        }
    }

    #[test]
    fn writes_insert_and_retract_with_a_lag() {
        let probes = probes();
        assert_eq!(probes.program(0), "+edge(1, 101)");
        assert_eq!(
            probes.program(RETRACT_LAG),
            format!("+edge(1, {})\n-edge(1, 101)", 101 + 2 * RETRACT_LAG)
        );
        assert_eq!(probes.events(), WRITES + 3);
    }

    #[test]
    fn every_event_maps_back_from_its_row() {
        let probes = probes();
        let mut seen = 0;
        for probe in 0..WRITES {
            let z = probes.mid(probe) + 1;
            let insert = probes.event_of(z, true).unwrap();
            assert_eq!(probes.write_of(insert), probe);
            seen += 1;
            if let Some(retract) = probes.event_of(z, false) {
                assert_eq!(probes.write_of(retract), probe + RETRACT_LAG);
                seen += 1;
            }
        }
        assert_eq!(seen, probes.events());
    }

    #[test]
    fn foreign_rows_map_to_no_event() {
        let probes = probes();
        assert_eq!(probes.event_of(50, true), None);
        assert_eq!(probes.event_of(101, true), None);
        assert_eq!(probes.event_of(probes.mid(WRITES) + 1, true), None);
        // The last probes are never retracted during the run.
        assert_eq!(probes.event_of(probes.mid(WRITES - 1) + 1, false), None);
    }

    #[test]
    fn last_agent_latency_is_the_slowest_delivery() {
        let t0 = Instant::now();
        let sent = [t0, t0 + Duration::from_millis(10)];
        let fast = vec![
            t0 + Duration::from_millis(1),
            t0 + Duration::from_millis(12),
        ];
        let slow = vec![
            t0 + Duration::from_millis(3),
            t0 + Duration::from_millis(15),
        ];
        let (all, last) = delivery_latencies(&sent, &[fast, slow]);
        assert_eq!(all, [1_000, 2_000, 3_000, 5_000]);
        assert_eq!(last, [3_000, 5_000]);
    }
}
