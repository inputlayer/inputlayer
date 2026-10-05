//! Required pass: sessions on one knowledge graph while writers saturate it,
//! at the scale of issue #292 (about 1,000 sessions).
//!
//! Every session subscribes to its own bound standing query (one family of
//! parameterized views, as voice-agent sessions do). Writer connections
//! commit receipts as fast as the engine takes them: each receipt is in every
//! session's dependencies but changes no result, so every view is refreshed
//! continuously. A prober changes one session's result at a time. Overload
//! may slow writes down, but never deliveries: each probe's delta reaches
//! exactly its session, once, within [`DELTA_BOUND`] of the write's
//! acknowledgement, so while the writers run; no session gets a delta no
//! probe caused or a `subscription_error`; every view obeys the agent
//! contract (contiguous `seq`, increasing `revision`, exact inserts and
//! retracts) and ends equal to a fresh query. Run it pinned to few cores
//! (`taskset`) to judge small hosts.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use inputlayer_testkit::{Agent, Checked, Delta, Engine, Fixture, Violation, WsClient};
use serde_json::json;
use tokio::sync::mpsc;

use crate::engine;

const KG: &str = "sessions";
/// Sessions, each with its own connection and subscription: with the
/// writers, the prober and the auditor, within the engine's default 1,024
/// WebSocket connections.
const SESSIONS: usize = 960;
/// Connections committing receipts back to back.
const WRITERS: usize = 32;
/// How long the writers saturate the engine.
const LOAD: Duration = Duration::from_secs(30);
/// Pause between probes.
const PROBE_GAP: Duration = Duration::from_millis(50);
/// Longest a probe's delta may take after its write is acknowledged.
const DELTA_BOUND: Duration = Duration::from_secs(5);
/// How long every session must stay quiet once the probes are settled.
const QUIET: Duration = Duration::from_secs(1);

/// A cut-down voice-agent pack: a session's speech names its order's ETA
/// while no playback of that goal was heard. Receipts are playbacks of a
/// goal no claim reads.
const PACK: [&str; 4] = [
    "conflict(S) <- eta(S, D1, _), eta(S, D2, _), D1 != D2",
    "claim(Sess, G, O, D) <- goal(Sess, G, O), shipment(O, S), eta(S, D, _), !conflict(S)",
    "heard(Sess, G, max<Pb>) <- playback(Sess, G, Pb)",
    "speech(Sess, G, O, D, \"state\") <- claim(Sess, G, O, D), !heard(Sess, G, _)",
];

fn session_query(session: usize) -> String {
    format!("?speech(\"s-{session}\", G, O, D, K), owner(\"s-{session}\", \"sp-{session}\")")
}

/// The speech row of `session` naming `eta`.
fn speech_row(session: usize, eta: &str) -> String {
    json!([
        format!("s-{session}"),
        format!("g-{session}"),
        format!("o-{session}"),
        eta,
        "state"
    ])
    .to_string()
}

fn fixture() -> Fixture {
    let all = 0..SESSIONS;
    let mut fixture = Fixture::new("saturated_sessions", KG)
        .facts(
            "goal",
            all.clone()
                .map(|i| format!("(\"s-{i}\", \"g-{i}\", \"o-{i}\")")),
        )
        .facts(
            "shipment",
            all.clone().map(|i| format!("(\"o-{i}\", \"S-{i}\")")),
        )
        .facts(
            "eta",
            all.clone().map(|i| format!("(\"S-{i}\", \"e-0\", 0)")),
        )
        .facts("owner", all.map(|i| format!("(\"s-{i}\", \"sp-{i}\")")));
    for rule in PACK {
        fixture = fixture.rule(rule);
    }
    fixture
}

/// A delta one session's agent applied.
struct Arrival {
    session: usize,
    delta: Delta,
}

/// Session `session`'s agent: applies its deltas (the agent checks the
/// contract) and reports each one until `stop`; returns the agent.
async fn follow(
    mut agent: Agent,
    session: usize,
    arrivals: mpsc::UnboundedSender<Arrival>,
    stop: Arc<AtomicBool>,
) -> Checked<Agent> {
    while !stop.load(Ordering::SeqCst) {
        if let Some(delta) = agent.poll_delta("s", Duration::from_millis(50)).await? {
            let _ = arrivals.send(Arrival { session, delta });
        }
        agent.clear_notices();
    }
    Ok(agent)
}

/// Commit receipts for session `session` back to back until `stop`.
async fn write_receipts(
    mut writer: WsClient,
    session: usize,
    stop: Arc<AtomicBool>,
    committed: Arc<AtomicU64>,
) -> Checked<()> {
    let mut playback = 0;
    while !stop.load(Ordering::SeqCst) {
        playback += 1;
        writer
            .commit(&format!("+playback(\"s-{session}\", \"g-x\", {playback})"))
            .await?;
        committed.fetch_add(1, Ordering::Relaxed);
        // Change notifications pile up on a connection nobody reads pushes from.
        while writer.poll_push(Duration::ZERO).await?.is_some() {}
    }
    Ok(())
}

/// A probe waiting for its delta.
struct Pending {
    eta: String,
    previous: String,
    sent_at: Instant,
    acked_at: Instant,
}

/// A probe's delta latencies: after its write was acknowledged, and after it
/// was sent.
struct Latency {
    after_ack: Duration,
    after_send: Duration,
}

/// Check `arrival` against the probe it must answer; returns its latency.
fn settle(arrival: &Arrival, pending: &mut BTreeMap<usize, Pending>) -> Checked<Latency> {
    let session = arrival.session;
    let Some(probe) = pending.remove(&session) else {
        return Err(Violation::UnexpectedPush(format!(
            "session s-{session} got a delta no probe caused: seq {}, inserted {:?}, retracted {:?}",
            arrival.delta.seq, arrival.delta.inserted, arrival.delta.retracted
        )));
    };
    arrival.delta.assert_rows(
        &[serde_json::from_str(&speech_row(session, &probe.eta)).unwrap()],
        &[serde_json::from_str(&speech_row(session, &probe.previous)).unwrap()],
    )?;
    Ok(Latency {
        after_ack: arrival.delta.at.saturating_duration_since(probe.acked_at),
        after_send: arrival.delta.at.saturating_duration_since(probe.sent_at),
    })
}

/// The `q` quantile of sorted `samples`.
fn quantile(samples: &[Duration], q: f64) -> Duration {
    samples[((samples.len() - 1) as f64 * q) as usize]
}

async fn saturated_engine() -> Checked<Engine> {
    let engine = engine().start().await.expect("start engine");
    fixture().install(&engine).await?;
    Ok(engine)
}

#[tokio::test(flavor = "multi_thread")]
async fn saturated_sessions_get_every_delta_once_in_time_and_nothing_else() -> Checked<()> {
    let engine = saturated_engine().await?;
    let stop_load = Arc::new(AtomicBool::new(false));
    let stop_agents = Arc::new(AtomicBool::new(false));
    let (arrivals_tx, mut arrivals) = mpsc::unbounded_channel();
    let mut agents = Vec::with_capacity(SESSIONS);
    for session in 0..SESSIONS {
        let mut agent = Agent::connect(&engine, KG).await?;
        agent.subscribe("s", &session_query(session)).await?;
        agent
            .view("s")
            .assert_matches(&[serde_json::from_str(&speech_row(session, "e-0")).unwrap()])?;
        agents.push(tokio::spawn(follow(
            agent,
            session,
            arrivals_tx.clone(),
            Arc::clone(&stop_agents),
        )));
    }
    let committed = Arc::new(AtomicU64::new(0));
    let mut writers = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        writers.push(tokio::spawn(write_receipts(
            WsClient::connect(&engine, KG).await?,
            writer % SESSIONS,
            Arc::clone(&stop_load),
            Arc::clone(&committed),
        )));
    }

    let mut prober = WsClient::connect(&engine, KG).await?;
    let mut etas: Vec<String> = vec!["e-0".to_string(); SESSIONS];
    let mut pending: BTreeMap<usize, Pending> = BTreeMap::new();
    let mut latencies = Vec::new();
    let mut probes = 0;
    let started = Instant::now();
    while started.elapsed() < LOAD {
        while let Ok(arrival) = arrivals.try_recv() {
            latencies.push(settle(&arrival, &mut pending)?);
        }
        // A session with no probe outstanding, so that each probe has a delta
        // of its own.
        let session = (probes * 37 + 11) % SESSIONS;
        probes += 1;
        if pending.contains_key(&session) {
            tokio::time::sleep(PROBE_GAP).await;
            continue;
        }
        let eta = format!("e-{probes}");
        let commit = prober
            .commit(&format!(
                "-eta(\"S-{session}\", D, R) <- eta(\"S-{session}\", D, R)\n\
                 +eta(\"S-{session}\", \"{eta}\", {probes})"
            ))
            .await?;
        let previous = std::mem::replace(&mut etas[session], eta.clone());
        pending.insert(
            session,
            Pending {
                eta,
                previous,
                sent_at: commit.sent_at,
                acked_at: commit.acked_at,
            },
        );
        while prober.poll_push(Duration::ZERO).await?.is_some() {}
        tokio::time::sleep(PROBE_GAP).await;
    }
    let during_load = latencies.len();
    stop_load.store(true, Ordering::SeqCst);
    for writer in writers {
        writer.await.expect("writer task")?;
    }
    let receipts = committed.load(Ordering::Relaxed);

    // Every outstanding probe's delta, within its bound.
    while let Some(oldest) = pending.values().map(|p| p.acked_at).min() {
        let deadline = tokio::time::Instant::from_std(oldest + DELTA_BOUND);
        match tokio::time::timeout_at(deadline, arrivals.recv()).await {
            Ok(Some(arrival)) => latencies.push(settle(&arrival, &mut pending)?),
            Ok(None) => unreachable!("agents hold the sender until stopped"),
            Err(_) => {
                let late: Vec<usize> = pending
                    .iter()
                    .filter(|(_, p)| p.acked_at == oldest)
                    .map(|(s, _)| *s)
                    .collect();
                return Err(Violation::Timeout(format!(
                    "probe deltas of sessions {late:?} within {DELTA_BOUND:?} of their ack"
                )));
            }
        }
    }
    // Nothing else arrives.
    if let Ok(Some(arrival)) = tokio::time::timeout(QUIET, arrivals.recv()).await {
        settle(&arrival, &mut pending)?;
    }
    stop_agents.store(true, Ordering::SeqCst);
    let mut auditor = WsClient::connect(&engine, KG).await?;
    for (session, agent) in agents.into_iter().enumerate() {
        let agent = agent.await.expect("agent task")?;
        let fresh = auditor.query(&session_query(session)).await?;
        agent.view("s").assert_matches(&fresh.rows)?;
        assert_eq!(
            agent.view("s").rows,
            [speech_row(session, &etas[session])].into(),
            "session s-{session}"
        );
    }

    let mut after_ack: Vec<Duration> = latencies.iter().map(|l| l.after_ack).collect();
    let mut after_send: Vec<Duration> = latencies.iter().map(|l| l.after_send).collect();
    after_ack.sort();
    after_send.sort();
    eprintln!(
        "saturated sessions: {SESSIONS} sessions, {receipts} receipts in {LOAD:?}, {} probes, \
         {during_load} delivered during the load; ack->delta p50 {:?} p99 {:?} max {:?}; \
         send->delta p50 {:?} p99 {:?}",
        latencies.len(),
        quantile(&after_ack, 0.5),
        quantile(&after_ack, 0.99),
        quantile(&after_ack, 1.0),
        quantile(&after_send, 0.5),
        quantile(&after_send, 0.99),
    );
    assert!(
        latencies.len() >= 10,
        "too few probes to judge: {}",
        latencies.len()
    );
    Ok(())
}
