//! The engine suite: what the gate's fixtures leave out. A warm non-recursive
//! rule and an unbound recursive closure, durable deletes and conditional
//! writes, guarded inserts (claims) under contention, `.why` proofs, many
//! sessions with their own standing queries in one graph, resident memory,
//! crash recovery, and inserts without a synchronous WAL. None of these is in
//! the policy's required set: they are measured and reported, not gated.
//!
//! As in the gate's fixtures, every timed reply is checked against an answer
//! computed here from the fixed-seed dataset.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

use super::{create_kg, elapsed_us, expect_rows, load, query, Measurement, KG};
use crate::client::{Client, Frame, Stamped};
use crate::dataset::{Graph, REACH_RULES, TWO_HOP_RULE};
use crate::profile::{
    ClaimParams, MemoryParams, QueryParams, RecoveryParams, SessionParams, WhyParams, WriteParams,
    SEED,
};
use crate::server::RunningServer;

/// Environment of the `insert_async` fixture's server: commits are
/// acknowledged before the WAL reaches the disk.
pub const ASYNC_DURABILITY: (&str, &str) =
    ("INPUTLAYER_STORAGE__PERSIST__DURABILITY_MODE", "async");

/// Time session agents get, after the writer's last acknowledgement, to see
/// every delta.
const SESSION_DRAIN: Duration = Duration::from_secs(60);

/// `?two_hop(1, Z)`: a bound, warm, non-recursive rule.
pub async fn rule_query(server: &RunningServer, params: &QueryParams) -> Result<Measurement> {
    let graph = Graph::random(params.nodes, params.edges, SEED);
    let expected = graph.two_hop(1);
    query::measure(
        server,
        params,
        &graph,
        &[TWO_HOP_RULE],
        "?two_hop(1, Z)",
        expected,
    )
    .await
}

/// `?reach(X, Y)`: the whole transitive closure.
pub async fn unbound_query(server: &RunningServer, params: &QueryParams) -> Result<Measurement> {
    let graph = Graph::random(params.nodes, params.edges, SEED);
    let expected = graph.closure(params.nodes);
    query::measure(
        server,
        params,
        &graph,
        &REACH_RULES,
        "?reach(X, Y)",
        expected,
    )
    .await
}

/// Durable plain deletes, conditional deletes and conditional updates, one
/// fact each, serially against preloaded `event(i, i)` facts.
pub async fn writes(server: &RunningServer, params: &WriteParams) -> Result<Measurement> {
    let mut client = create_kg(server).await?;
    let tuples: Vec<String> = (0..params.preload).map(|i| format!("({i}, {i})")).collect();
    for chunk in tuples.chunks(5_000) {
        client
            .execute(&format!("+event[{}]", chunk.join(", ")))
            .await?;
    }
    if params.preload < 3 * params.each + 1 {
        bail!(
            "writes: preload {} too small for 3 x {}",
            params.preload,
            params.each
        );
    }
    // Keys from 1: event(0, 0) would make the update below a no-op.
    let plain = 1..=params.each;
    let conditional = params.each + 1..=2 * params.each;
    let updated = 2 * params.each + 1..=3 * params.each;

    let mut delete_us = Vec::with_capacity(params.each);
    for i in plain {
        let (us, message) = timed_message(&mut client, &format!("-event({i}, {i})")).await?;
        expect_message(&message, "Deleted 1 facts")?;
        delete_us.push(us);
    }
    let mut conditional_us = Vec::with_capacity(params.each);
    for i in conditional {
        let program = format!("-event(X, Y) <- event(X, Y), X = {i}");
        let (us, message) = timed_message(&mut client, &program).await?;
        expect_message(&message, "1 fact(s) deleted")?;
        conditional_us.push(us);
    }
    let mut update_us = Vec::with_capacity(params.each);
    for i in updated {
        let program = format!("-event(K, V), +event(K, 0) <- event(K, V), K = {i}");
        let (us, message) = timed_message(&mut client, &program).await?;
        expect_message(&message, "1 deleted, 1 inserted")?;
        update_us.push(us);
    }
    let (_, reply) = client.execute("?event(X, Y)").await?;
    expect_rows(
        "events left",
        reply.row_count,
        params.preload - 2 * params.each,
    )?;
    let (_, reply) = client.execute("?event(X, 0)").await?;
    expect_rows("updated events", reply.row_count, params.each + 1)?;

    let mut measurement = Measurement::default();
    measurement.series("delete_ack_us", delete_us);
    measurement.series("conditional_delete_ack_us", conditional_us);
    measurement.series("update_ack_us", update_us);
    Ok(measurement)
}

/// The guarded insert an SDK `claim()` sends: insert `claim(key, owner)`
/// only if the key is a task and nobody holds it, in one atomic update.
fn claim_program(key: usize, owner: &str) -> String {
    format!("-il_ghost(0), +claim(K, \"{owner}\") <- task(K), K = {key}, !claim(K, _)")
}

/// Guarded inserts: serial wins on fresh keys, serial losses on held keys,
/// then `racers` connections claiming each contested key at once.
pub async fn claims(server: &RunningServer, params: &ClaimParams) -> Result<Measurement> {
    let mut client = create_kg(server).await?;
    let keys = params.serial + params.contested_keys;
    let tasks: Vec<String> = (1..=keys).map(|k| format!("({k})")).collect();
    client
        .execute(&format!("+task[{}]", tasks.join(", ")))
        .await?;

    let mut win_us = Vec::with_capacity(params.serial);
    for key in 1..=params.serial {
        let (us, message) = timed_message(&mut client, &claim_program(key, "first")).await?;
        expect_message(&message, "0 deleted, 1 inserted")?;
        win_us.push(us);
    }
    let mut lose_us = Vec::with_capacity(params.serial);
    for key in 1..=params.serial {
        let (us, message) = timed_message(&mut client, &claim_program(key, "second")).await?;
        expect_message(&message, "0 deleted, 0 inserted")?;
        lose_us.push(us);
    }

    let mut racers = Vec::with_capacity(params.racers);
    for _ in 0..params.racers {
        racers.push(server.client(KG).await?);
    }
    let mut race_us = Vec::with_capacity(params.racers * params.contested_keys);
    for key in params.serial + 1..=keys {
        let barrier = Arc::new(Barrier::new(params.racers));
        let tasks: Vec<_> = racers
            .drain(..)
            .enumerate()
            .map(|(index, mut racer)| {
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    let program = claim_program(key, &format!("racer{index}"));
                    let result = timed_message(&mut racer, &program).await;
                    (racer, result)
                })
            })
            .collect();
        let mut winners = 0;
        for task in tasks {
            let (racer, result) = task.await.context("racer task")?;
            let (us, message) = result?;
            if message.contains("0 deleted, 1 inserted") {
                winners += 1;
            } else {
                expect_message(&message, "0 deleted, 0 inserted")?;
            }
            race_us.push(us);
            racers.push(racer);
        }
        if winners != 1 {
            bail!("contested key {key}: {winners} winners, expected exactly 1");
        }
    }
    let (_, reply) = client.execute("?claim(K, O)").await?;
    expect_rows("claims held", reply.row_count, keys)?;

    let mut measurement = Measurement::default();
    measurement.series("win_ack_us", win_us);
    measurement.series("lose_ack_us", lose_us);
    measurement.series("race_ack_us", race_us);
    Ok(measurement)
}

/// `.why` proofs: of a non-recursive rule's answer and of a recursive one.
pub async fn why(server: &RunningServer, params: &WhyParams) -> Result<Measurement> {
    let graph = Graph::random(params.nodes, params.edges, SEED);
    let mut client = create_kg(server).await?;
    let mut rules = REACH_RULES.to_vec();
    rules.push(TWO_HOP_RULE);
    load(&mut client, &graph, &rules).await?;
    let source = 1;
    let cases = [
        (
            "two_hop_us",
            format!(".why ?two_hop({source}, Z)"),
            graph.two_hop(source),
        ),
        (
            "reach_us",
            format!(".why ?reach({source}, Y)"),
            graph.reachable(source),
        ),
    ];
    let mut measurement = Measurement::default();
    for (series, program, expected) in cases {
        for _ in 0..params.warmup {
            let (_, reply) = client.execute(&program).await?;
            expect_rows(&program, reply.row_count, expected)?;
        }
        let mut samples = Vec::with_capacity(params.serial);
        for _ in 0..params.serial {
            let (sent, reply) = client.execute(&program).await?;
            expect_rows(&program, reply.row_count, expected)?;
            samples.push(elapsed_us(sent, reply.at));
        }
        measurement.series(series, samples);
    }
    Ok(measurement)
}

/// `sessions` connections, session `k` subscribed to `?two_hop(k, Z)`, in
/// one graph. Write `w` inserts `edge(k, P_w)` for session `k = w mod
/// sessions + 1`, where the preloaded `edge(P_w, P_w + 1)` makes the change
/// exactly `two_hop(k, P_w + 1)` for that session. (Sessions with an edge
/// into `k` also gain `two_hop(_, P_w)`; their agents ignore those rows.)
/// Writes are open-loop, so every session's evaluation cost shows up in
/// the target session's delta latency.
pub async fn sessions(server: &RunningServer, params: &SessionParams) -> Result<Measurement> {
    let mut graph = Graph::random(params.nodes, params.edges, SEED);
    let first = params.nodes + 1;
    let mid = |write: usize| first + 2 * write as u64;
    for write in 0..params.writes {
        graph.add(mid(write), mid(write) + 1);
    }
    let mut writer = create_kg(server).await?;
    load(&mut writer, &graph, &[TWO_HOP_RULE]).await?;
    let rss_loaded = server.rss_kb();

    let mut subscribe_us = Vec::with_capacity(params.sessions);
    let mut agents: Vec<SessionAgent> = Vec::with_capacity(params.sessions);
    let subscribe_start = Instant::now();
    for session in 1..=params.sessions {
        let mut agent = server.client(KG).await?;
        let (sent, reply) = agent
            .execute(&format!(".subscribe s{session} ?two_hop({session}, Z)"))
            .await?;
        expect_rows(
            "session snapshot",
            reply.row_count,
            graph.two_hop(session as u64),
        )?;
        subscribe_us.push(elapsed_us(sent, reply.at));
        let expected: Vec<usize> = (0..params.writes)
            .filter(|w| w % params.sessions + 1 == session)
            .collect();
        agents.push(tokio::spawn(async move {
            session_agent(&mut agent, first, expected).await
        }));
    }
    let subscribe_all = subscribe_start.elapsed();
    let rss_subscribed = server.rss_kb();

    let programs: Vec<String> = (0..params.writes)
        .map(|w| format!("+edge({}, {})", w % params.sessions + 1, mid(w)))
        .collect();
    let replies = writer
        .execute_schedule(&programs, Duration::from_millis(params.interval_ms))
        .await?;
    let sent: Vec<Instant> = replies.iter().map(|(scheduled, _)| *scheduled).collect();
    let ack_us: Vec<u64> = replies
        .iter()
        .map(|(scheduled, reply)| elapsed_us(*scheduled, reply.at))
        .collect();

    let deadline = tokio::time::Instant::now() + SESSION_DRAIN;
    let mut delta_us = Vec::with_capacity(params.writes);
    for (index, agent) in agents.iter_mut().enumerate() {
        let Ok(joined) = tokio::time::timeout_at(deadline, &mut *agent).await else {
            bail!(
                "session {} missed deltas {SESSION_DRAIN:?} after the last write",
                index + 1
            );
        };
        for (write, at) in joined.context("session agent")?? {
            delta_us.push(elapsed_us(sent[write], at));
        }
    }

    let mut measurement = Measurement::default();
    measurement.series("delta_us", delta_us);
    measurement.series("ack_us", ack_us);
    measurement.series("subscribe_us", subscribe_us);
    measurement.rate(
        "subscriptions_per_sec",
        params.sessions as u64,
        subscribe_all,
    );
    if let (Some(loaded), Some(subscribed)) = (rss_loaded, rss_subscribed) {
        measurement.gauges.insert(
            "rss_per_session_kb".into(),
            subscribed.saturating_sub(loaded) / params.sessions as u64,
        );
    }
    Ok(measurement)
}

/// A session agent's task: (write, arrival) of each of its rows.
type SessionAgent = JoinHandle<Result<Vec<(usize, Instant)>>>;

/// Read pushes until every write in `expected` has delivered its row;
/// returns (write, arrival) pairs.
async fn session_agent(
    agent: &mut Client,
    first: u64,
    expected: Vec<usize>,
) -> Result<Vec<(usize, Instant)>> {
    let mut arrivals = Vec::with_capacity(expected.len());
    while arrivals.len() < expected.len() {
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
        if !retracted.is_empty() {
            bail!("session saw retractions {retracted:?}; writes only insert");
        }
        for row in inserted {
            let Some(offset) = row
                .last()
                .and_then(Value::as_u64)
                .and_then(|z| z.checked_sub(first))
            else {
                bail!("delta row {row:?} matches no write");
            };
            if offset % 2 == 0 {
                // two_hop(_, P_w) through an edge into this session's source.
                continue;
            }
            let write = usize::try_from(offset / 2)?;
            if !expected.contains(&write) {
                bail!("delta row {row:?} is another session's write");
            }
            if arrivals.iter().any(|(seen, _)| *seen == write) {
                bail!("write {write} delivered twice");
            }
            arrivals.push((write, at));
        }
    }
    Ok(arrivals)
}

/// Resident memory: idle; per base fact, after loading `facts` two-integer
/// facts into a fresh server and reading them all back; then after each of
/// `graphs` loaded graphs (edges and the two-hop rule, queried once so its
/// view exists).
pub async fn memory(server: &RunningServer, params: &MemoryParams) -> Result<Measurement> {
    let rss = || server.rss_kb().context("read server RSS");
    // Let startup allocations settle.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let idle = rss()?;
    let mut admin = server.client("default").await?;
    admin.execute(".kg create facts").await?;
    let mut client = server.client("facts").await?;
    let before_facts = rss()?;
    let mut loaded = 0;
    while loaded < params.facts {
        let end = (loaded + 5_000).min(params.facts);
        let tuples: Vec<String> = (loaded..end).map(|i| format!("({i}, {})", i * 7)).collect();
        client
            .execute(&format!("+fact[{}]", tuples.join(", ")))
            .await?;
        loaded = end;
    }
    // Read every fact back, so whatever the engine builds to serve them exists.
    let (_, reply) = client.execute("?fact(X, Y)").await?;
    expect_rows("facts", reply.row_count, params.facts)?;
    let after_facts = rss()?;
    let peak_after = server.peak_rss_kb().context("read server peak RSS")?;

    let graph = Graph::random(params.nodes, params.edges, SEED);
    let before_graphs = rss()?;
    let mut after = Vec::with_capacity(params.graphs);
    for index in 0..params.graphs {
        let name = format!("mem{index}");
        admin.execute(&format!(".kg create {name}")).await?;
        let mut client = server.client(&name).await?;
        load(&mut client, &graph, &[TWO_HOP_RULE]).await?;
        let (_, reply) = client.execute("?two_hop(1, Z)").await?;
        expect_rows("two_hop(1, Z)", reply.row_count, graph.two_hop(1))?;
        after.push(rss()?);
    }

    let mut measurement = Measurement::default();
    let gauges = &mut measurement.gauges;
    gauges.insert("rss_idle_kb".into(), idle);
    gauges.insert(
        "rss_first_graph_kb".into(),
        after[0].saturating_sub(before_graphs),
    );
    if params.graphs > 1 {
        let per = after[params.graphs - 1].saturating_sub(after[0]) / (params.graphs as u64 - 1);
        gauges.insert("rss_per_graph_kb".into(), per);
    }
    let per_fact = |kb: u64| kb * 1024 / params.facts as u64;
    gauges.insert(
        "rss_bytes_per_fact".into(),
        per_fact(after_facts.saturating_sub(before_facts)),
    );
    // From the resident set before loading to the high-water mark after
    // reading: what loading and reading them needed at most. Earlier peaks
    // can hide part of it, so it is a lower bound.
    gauges.insert(
        "peak_bytes_per_fact".into(),
        per_fact(peak_after.saturating_sub(before_facts)),
    );
    Ok(measurement)
}

/// Durable facts and a rule, then crash (SIGKILL) and restart on the same
/// data directory `restarts` times. `restart_ready_us` runs from the kill to
/// the first accepted login; `first_query_us` is the first query of the
/// derived rule after it, which must already see every fact.
pub async fn recovery(server: &mut RunningServer, params: &RecoveryParams) -> Result<Measurement> {
    let graph = Graph::random(params.nodes, params.edges, SEED);
    {
        let mut client = create_kg(server).await?;
        load(&mut client, &graph, &[TWO_HOP_RULE]).await?;
        let mut written = 0;
        while written < params.facts {
            let end = (written + params.batch_size).min(params.facts);
            let tuples: Vec<String> = (written..end).map(|i| format!("({i}, {i})")).collect();
            client
                .execute(&format!("+event[{}]", tuples.join(", ")))
                .await?;
            written = end;
        }
    }
    let two_hop = graph.two_hop(1);
    let mut ready_us = Vec::with_capacity(params.restarts);
    let mut first_query_us = Vec::with_capacity(params.restarts);
    for _ in 0..params.restarts {
        let killed = Instant::now();
        server.crash_and_restart().await?;
        ready_us.push(elapsed_us(killed, Instant::now()));
        let mut client = server.client(KG).await?;
        let (sent, reply) = client.execute("?two_hop(1, Z)").await?;
        expect_rows("two_hop(1, Z) after restart", reply.row_count, two_hop)?;
        first_query_us.push(elapsed_us(sent, reply.at));
        let (_, reply) = client.execute("?event(X, Y)").await?;
        expect_rows("events after restart", reply.row_count, params.facts)?;
    }
    let mut measurement = Measurement::default();
    measurement.series("restart_ready_us", ready_us);
    measurement.series("first_query_us", first_query_us);
    Ok(measurement)
}

/// Send `program` and return its latency and its first reply row as text.
async fn timed_message(client: &mut Client, program: &str) -> Result<(u64, String)> {
    let (sent, answer) = client.query(program).await?;
    if let Some(error) = answer.errors.first() {
        bail!("{program}: {error}");
    }
    let message = answer
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Ok((elapsed_us(sent, answer.at), message))
}

fn expect_message(message: &str, needle: &str) -> Result<()> {
    if !message.contains(needle) {
        bail!("reply '{message}', expected '{needle}'");
    }
    Ok(())
}
