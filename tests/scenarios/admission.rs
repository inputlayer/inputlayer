//! Required pass: admission keeps every lane moving under load (#176).
//!
//! Writers saturate one knowledge graph while hundreds of sessions' views
//! refresh on every commit. Meanwhile a reader keeps sending cheap queries
//! from its own connection: they are interactive work on a lane of their
//! own, so they answer within [`READ_BOUND`] however deep the write and
//! refresh queues are; every write is acknowledged, no view reports an
//! error, and every view ends equal to a fresh query. Run it pinned to few
//! cores (`taskset`) to judge small hosts.
//!
//! Separately, a request the engine cannot admit in time is refused at once
//! with `overloaded`, not left to wait out its deadline: a cheap query
//! behind a long one on a one-permit engine gets the refusal after the
//! configured longest wait, and one behind a full lane queue at once.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use inputlayer_testkit::{AdmissionSettings, Agent, Checked, Engine, Fixture, Violation, WsClient};
use serde_json::json;

use crate::engine;

const KG: &str = "admission";
/// Sessions, each with its own bound standing query on one family.
const SESSIONS: usize = 200;
/// Connections committing receipts back to back.
const WRITERS: usize = 16;
/// How long the writers saturate the engine.
const LOAD: Duration = Duration::from_secs(15);
/// Pause between the reader's cheap queries.
const READ_GAP: Duration = Duration::from_millis(20);
/// Longest a cheap query may take, at the 99th percentile, under the load.
const READ_BOUND: Duration = Duration::from_secs(1);

/// A cut-down voice-agent pack (see `saturation`).
const PACK: [&str; 4] = [
    "conflict(S) <- eta(S, D1, _), eta(S, D2, _), D1 != D2",
    "claim(Sess, G, O, D) <- goal(Sess, G, O), shipment(O, S), eta(S, D, _), !conflict(S)",
    "heard(Sess, G, max<Pb>) <- playback(Sess, G, Pb)",
    "speech(Sess, G, O, D, \"state\") <- claim(Sess, G, O, D), !heard(Sess, G, _)",
];

fn session_query(session: usize) -> String {
    format!("?speech(\"s-{session}\", G, O, D, K), owner(\"s-{session}\", \"sp-{session}\")")
}

fn speech_row(session: usize) -> String {
    json!([
        format!("s-{session}"),
        format!("g-{session}"),
        format!("o-{session}"),
        "e-0",
        "state"
    ])
    .to_string()
}

fn fixture() -> Fixture {
    let all = 0..SESSIONS;
    let mut fixture = Fixture::new("admitted_sessions", KG)
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

/// Session `session`'s agent: applies its deltas (the agent checks the
/// contract) until `stop`; returns the agent.
async fn follow(mut agent: Agent, stop: Arc<AtomicBool>) -> Checked<Agent> {
    while !stop.load(Ordering::SeqCst) {
        agent.poll_delta("s", Duration::from_millis(50)).await?;
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
        while writer.poll_push(Duration::ZERO).await?.is_some() {}
    }
    Ok(())
}

/// The `q` quantile of sorted `samples`.
fn quantile(samples: &[Duration], q: f64) -> Duration {
    samples[((samples.len() - 1) as f64 * q) as usize]
}

#[tokio::test(flavor = "multi_thread")]
#[cfg_attr(
    debug_assertions,
    ignore = "timing bounds hold for a release engine; `make e2e-reactive` runs it"
)]
async fn cheap_queries_answer_while_writers_and_refreshes_saturate_the_engine() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    fixture().install(&engine).await?;
    let stop_load = Arc::new(AtomicBool::new(false));
    let stop_agents = Arc::new(AtomicBool::new(false));
    let mut agents = Vec::with_capacity(SESSIONS);
    for session in 0..SESSIONS {
        let mut agent = Agent::connect(&engine, KG).await?;
        agent.subscribe("s", &session_query(session)).await?;
        agent
            .view("s")
            .assert_matches(&[serde_json::from_str(&speech_row(session)).unwrap()])?;
        agents.push(tokio::spawn(follow(agent, Arc::clone(&stop_agents))));
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

    // The reader: cheap point queries, each timed.
    let mut reader = WsClient::connect(&engine, KG).await?;
    let mut latencies = Vec::new();
    let started = Instant::now();
    let mut reads = 0usize;
    while started.elapsed() < LOAD {
        let session = reads % SESSIONS;
        reads += 1;
        let sent = Instant::now();
        let result = reader.query(&format!("?owner(\"s-{session}\", X)")).await?;
        latencies.push(sent.elapsed());
        if result.rows.len() != 1 {
            return Err(Violation::Rejected(format!(
                "cheap query of s-{session} answered {:?}",
                result.rows
            )));
        }
        tokio::time::sleep(READ_GAP).await;
    }
    stop_load.store(true, Ordering::SeqCst);
    for writer in writers {
        writer.await.expect("writer task")?;
    }
    let receipts = committed.load(Ordering::Relaxed);

    // Every view settles on the state the writers left.
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_agents.store(true, Ordering::SeqCst);
    let mut auditor = WsClient::connect(&engine, KG).await?;
    for (session, agent) in agents.into_iter().enumerate() {
        let agent = agent.await.expect("agent task")?;
        let fresh = auditor.query(&session_query(session)).await?;
        agent.view("s").assert_matches(&fresh.rows)?;
    }

    latencies.sort();
    let p50 = quantile(&latencies, 0.5);
    let p99 = quantile(&latencies, 0.99);
    let max = quantile(&latencies, 1.0);
    eprintln!(
        "admission: {SESSIONS} sessions, {WRITERS} writers, {receipts} receipts in {LOAD:?}; \
         {} cheap queries, latency p50 {p50:?} p99 {p99:?} max {max:?}",
        latencies.len()
    );
    assert!(
        latencies.len() >= 50,
        "too few reads to judge: {}",
        latencies.len()
    );
    assert!(
        p99 <= READ_BOUND,
        "cheap queries p99 {p99:?} over {READ_BOUND:?} under the load (max {max:?})"
    );
    Ok(())
}

/// Complete bipartite graph, both directions: `TRIANGLES` finds nothing but
/// its cyclic join runs for seconds (see `ws_cancel_tests`).
const SIDE: i64 = if cfg!(debug_assertions) { 14 } else { 20 };
const TRIANGLES: &str = "?edge(X, Y), edge(Y, Z), edge(Z, X)";
/// The longest admission wait the engine runs with here.
const MAX_WAIT: Duration = Duration::from_millis(300);

fn refusal_code(
    outcome: Result<inputlayer_testkit::QueryResult, inputlayer_testkit::Refusal>,
) -> Checked<String> {
    match outcome {
        Err(refusal) => refusal.code.ok_or_else(|| {
            Violation::Rejected(format!("refused without a code: {}", refusal.message))
        }),
        Ok(result) => Err(Violation::Rejected(format!(
            "ran instead of being refused: {} rows",
            result.rows.len()
        ))),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_the_engine_cannot_admit_in_time_is_refused_as_overloaded() -> Checked<()> {
    let engine: Engine = engine()
        .admission(AdmissionSettings {
            compute_permits: 1,
            max_wait_ms: MAX_WAIT.as_millis() as u64,
            interactive_max_queued: 1,
        })
        .start()
        .await
        .expect("start engine");
    let edges = (0..SIDE).flat_map(|a| {
        (SIDE..2 * SIDE).flat_map(move |b| [format!("({a}, {b})"), format!("({b}, {a})")])
    });
    Fixture::new("bipartite", KG)
        .facts("edge", edges)
        .install(&engine)
        .await?;

    // The one permit goes to a long request.
    let mut long = WsClient::connect(&engine, KG).await?;
    let long_id = long.send_execute(TRIANGLES).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // A cheap query waits for the permit, and is refused once the longest
    // admission wait passes: long before its 30 s deadline.
    let mut waiting = WsClient::connect(&engine, KG).await?;
    waiting.send_execute("?edge(0, X)").await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Another behind a full lane queue is refused at once.
    let mut behind = WsClient::connect(&engine, KG).await?;
    let sent = Instant::now();
    let code = refusal_code(behind.try_execute("?edge(0, X)").await?)?;
    assert_eq!(code, "overloaded", "a full queue refuses at once");
    assert!(
        sent.elapsed() < MAX_WAIT,
        "refused at once, not after the wait: {:?}",
        sent.elapsed()
    );

    let sent = Instant::now();
    let code = refusal_code(waiting.try_result().await?)?;
    let waited = sent.elapsed();
    assert_eq!(code, "overloaded");
    assert!(
        waited < MAX_WAIT * 4,
        "refused after the longest wait, not the deadline: {waited:?}"
    );
    let codes: BTreeSet<String> = engine
        .metrics_text()
        .await
        .expect("metrics")
        .lines()
        .filter(|line| {
            line.starts_with("inputlayer_admission_refused_total") && !line.ends_with(" 0")
        })
        .map(str::to_string)
        .collect();
    assert_eq!(codes.len(), 2, "one refusal per reason: {codes:?}");

    // Cancelled, the long request frees the permit and cheap queries run.
    long.send_cancel(&long_id).await?;
    let outcome = long.try_result().await?;
    assert_eq!(refusal_code(outcome)?, "cancelled");
    let ack = long.cancel_ack().await?;
    assert_eq!(ack, "cancelled");
    let rows = behind.query("?edge(0, X)").await?.rows;
    assert_eq!(rows.len() as i64, SIDE);
    Ok(())
}
