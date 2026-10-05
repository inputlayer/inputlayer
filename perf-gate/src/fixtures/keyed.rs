//! External writer to many agents, each subscribed to its own key of a
//! deployed rule, as an agent per session subscribes to its session's rows.
//!
//! The KG holds chains of four nodes (three edges each), every hundredth
//! chain head labelled hot, and the views benchmark's non-recursive rule
//! `r1(X, Y) <- edge(X, Y), label(X, "hot")`. Agent `k` subscribes to
//! `?r1(H_k, Y)` for hot head `H_k`. Write `w` inserts `edge(H_k, F_w)` for
//! `k = w mod keys` and a fresh node `F_w`: it adds exactly one row, to agent
//! `k`'s result. Writes are open-loop on a fixed schedule.
//!
//! Every view reads `edge`, so every write refreshes all of them. The views
//! differ only in their constant, so the engine can evaluate the family's
//! query once per write and hand each view its rows, rather than evaluate
//! each view's own query (issue #378: a lost sharing verdict cost a write
//! 66 times the CPU). `writes_per_server_cpu_sec` (writes per second of
//! server CPU over the write phase) shows that even on a host idle enough
//! for the deltas to arrive in time.

use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use tokio::task::JoinHandle;

use super::{create_kg, elapsed_us, expect_rows, hot_head, load_hot_chains, Measurement};
use crate::client::{Client, Frame, Stamped};
use crate::profile::KeyedParams;
use crate::server::RunningServer;

/// The rule every agent's view reads.
const RULE: &str = "+r1(X, Y) <- edge(X, Y), label(X, \"hot\")";

/// Time agents get, after the writer's last acknowledgement, to see every delta.
const DRAIN: Duration = Duration::from_secs(30);

/// The chains, hot heads and fresh nodes of a keyed KG.
#[derive(Debug, Clone, Copy)]
struct Keys {
    chains: u64,
    keys: usize,
}

impl Keys {
    /// Hot chain heads in the graph.
    fn hot(self) -> u64 {
        self.chains.div_ceil(100)
    }

    /// The key write `w` changes.
    fn key_of_write(self, write: usize) -> usize {
        write % self.keys
    }

    /// The fresh node write `w` links to: no chain uses it.
    fn fresh(self, write: usize) -> u64 {
        4 * self.chains + 1_000 + write as u64
    }

    /// The write that linked `node`, if a write did.
    fn write_of(self, node: u64) -> Option<usize> {
        usize::try_from(node.checked_sub(self.fresh(0))?).ok()
    }

    fn program(self, write: usize) -> String {
        let head = hot_head(self.key_of_write(write) as u64);
        format!("+edge({head}, {})", self.fresh(write))
    }
}

/// `params.keys` agents, agent `k` on `?r1(H_k, Y)`, one external writer.
pub async fn run(server: &RunningServer, params: &KeyedParams) -> Result<Measurement> {
    let keys = Keys {
        chains: (params.edges as u64).div_ceil(3),
        keys: params.keys,
    };
    ensure!(
        params.keys as u64 <= keys.hot(),
        "{} keys, but the graph has {} hot heads",
        params.keys,
        keys.hot()
    );
    let mut writer = create_kg(server).await?;
    load_hot_chains(&mut writer, keys.chains).await?;
    writer.execute(RULE).await.context("define rule")?;

    let mut subscribe_us = Vec::with_capacity(params.keys);
    let mut agents = Vec::with_capacity(params.keys);
    for key in 0..params.keys {
        let mut agent = server.client(super::KG).await?;
        let head = hot_head(key as u64);
        let (sent, reply) = agent
            .execute(&format!(".subscribe k{key} ?r1({head}, Y)"))
            .await?;
        expect_rows("keyed snapshot", reply.row_count, 1)?;
        subscribe_us.push(elapsed_us(sent, reply.at));
        let expected: Vec<usize> = (0..params.writes)
            .filter(|&w| keys.key_of_write(w) == key)
            .collect();
        agents.push(tokio::spawn(async move {
            collect(&mut agent, keys, key, expected).await
        }));
    }

    let programs: Vec<String> = (0..params.writes).map(|w| keys.program(w)).collect();
    let cpu = server.cpu_seconds();
    let replies = writer
        .execute_schedule(&programs, Duration::from_millis(params.interval_ms))
        .await?;
    let arrivals = drain(agents).await?;
    let cpu = cpu.zip(server.cpu_seconds()).map(|(a, b)| b - a);

    let sent: Vec<Instant> = replies.iter().map(|(scheduled, _)| *scheduled).collect();
    let ack_us = replies
        .iter()
        .map(|(scheduled, reply)| elapsed_us(*scheduled, reply.at))
        .collect();
    let delta_us = arrivals
        .into_iter()
        .flatten()
        .map(|(write, at)| elapsed_us(sent[write], at))
        .collect();

    let mut measurement = Measurement::default();
    measurement.series("delta_us", delta_us);
    measurement.series("ack_us", ack_us);
    measurement.series("subscribe_us", subscribe_us);
    if let Some(cpu) = cpu {
        // Clock ticks of 10 ms: a phase without one is too short to judge.
        ensure!(cpu > 0.0, "the write phase used no measurable server CPU");
        measurement.rate(
            "writes_per_server_cpu_sec",
            params.writes as u64,
            Duration::from_secs_f64(cpu),
        );
    }
    Ok(measurement)
}

/// Agent `key`'s task: (write, arrival) of each write in `expected`.
type KeyedAgent = JoinHandle<Result<Vec<(usize, Instant)>>>;

/// Wait for every agent, once the writer is done.
async fn drain(mut agents: Vec<KeyedAgent>) -> Result<Vec<Vec<(usize, Instant)>>> {
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

/// Read pushes until every write in `expected` has delivered its row to
/// agent `key`; a row of another key or write fails the run.
async fn collect(
    agent: &mut Client,
    keys: Keys,
    key: usize,
    expected: Vec<usize>,
) -> Result<Vec<(usize, Instant)>> {
    let head = hot_head(key as u64);
    let mut arrivals = Vec::with_capacity(expected.len());
    while arrivals.len() < expected.len() {
        let Stamped { at, frame } = agent.next_push().await?;
        let inserted = match frame {
            Frame::SubscriptionDelta {
                inserted,
                retracted,
                ..
            } => {
                ensure!(retracted.is_empty(), "key {key}: retracted {retracted:?}");
                inserted
            }
            Frame::SubscriptionError { message, .. } => bail!("subscription error: {message}"),
            other => bail!("unexpected push {other:?}"),
        };
        for row in inserted {
            let node = |i: usize| row.get(i).and_then(serde_json::Value::as_u64);
            let write = node(1).and_then(|n| keys.write_of(n));
            match write {
                Some(write)
                    if node(0) == Some(head)
                        && expected.contains(&write)
                        && !arrivals.iter().any(|(w, _)| *w == write) =>
                {
                    arrivals.push((write, at));
                }
                _ => bail!("key {key}: delta row {row:?} matches none of its writes"),
            }
        }
    }
    Ok(arrivals)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_write_changes_one_key_by_a_row_no_chain_holds() {
        let keys = Keys {
            chains: 1_000,
            keys: 4,
        };
        assert_eq!(keys.hot(), 10);
        assert_eq!(keys.program(0), "+edge(0, 5000)");
        assert_eq!(keys.program(5), "+edge(400, 5005)");
        assert_eq!(keys.write_of(5_005), Some(5));
        assert_eq!(keys.write_of(3_999), None, "a chain node");
        assert!(keys.fresh(0) > 4 * keys.chains);
    }
}
