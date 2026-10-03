//! Required passes: the supported agent subscription path.

use std::time::Duration;

use inputlayer_testkit::fixture::reachability_chain;
use inputlayer_testkit::{Agent, Checked, Engine, SampleLog, WsClient};
use serde_json::{json, Value};

use crate::engine;

const KG: &str = "agents";
/// Standing query of the reachability fixture.
const REACH_0: &str = "?reach(0, X)";

/// Engine with the reachability chain `0 -> ... -> nodes-1` installed.
async fn reachability_engine(nodes: i64) -> Checked<Engine> {
    let engine = engine().start().await.expect("start engine");
    reachability_chain(KG, nodes).install(&engine).await?;
    Ok(engine)
}

fn pairs(rows: &[(i64, i64)]) -> Vec<Value> {
    rows.iter().map(|(a, b)| json!([a, b])).collect()
}

/// One write and the exact delta it must produce.
struct Step {
    event: &'static str,
    program: &'static str,
    inserted: &'static [(i64, i64)],
    retracted: &'static [(i64, i64)],
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_receives_exact_deltas_for_facts_and_rules() -> Checked<()> {
    let engine = reachability_engine(4).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    agent.subscribe("r", REACH_0).await?;
    agent
        .view("r")
        .assert_matches(&auditor.query(REACH_0).await?.rows)?;
    assert_eq!(agent.view("r").rows.len(), 3);

    // Not yet derivable: only the rule added below makes it visible.
    writer.commit("+shortcut(0, 7)").await?;
    let steps = [
        Step {
            event: "insert",
            program: "+edge(3, 4)",
            inserted: &[(0, 4)],
            retracted: &[],
        },
        Step {
            event: "insert",
            program: "+edge(1, 10)",
            inserted: &[(0, 10)],
            retracted: &[],
        },
        Step {
            event: "retract",
            program: "-edge(1, 2)",
            inserted: &[],
            retracted: &[(0, 2), (0, 3), (0, 4)],
        },
        Step {
            event: "rule_add",
            program: "+reach(X, Y) <- shortcut(X, Y)",
            inserted: &[(0, 7)],
            retracted: &[],
        },
        Step {
            event: "insert",
            program: "+edge(7, 8)",
            inserted: &[(0, 8)],
            retracted: &[],
        },
        Step {
            event: "rule_remove",
            program: ".rule remove reach 3",
            inserted: &[],
            retracted: &[(0, 7), (0, 8)],
        },
    ];
    let mut log = SampleLog::new("single_subscriber", "reachability_chain_4");
    for (sequence, step) in steps.iter().enumerate() {
        let commit = writer.commit(step.program).await?;
        let delta = agent.next_delta("r").await?;
        delta.assert_rows(&pairs(step.inserted), &pairs(step.retracted))?;
        log.record(step.event, sequence, commit, 0, 1, delta.at);
    }
    agent
        .view("r")
        .assert_matches(&auditor.query(REACH_0).await?.rows)?;
    assert_eq!(agent.view("r").seq, steps.len() as u64);
    log.finish().expect("write samples");
    Ok(())
}

/// Subscribers per run: P00's fanout fixture size.
const SUBSCRIBERS: usize = 64;
const FANOUT_ROUNDS: i64 = 5;

#[tokio::test(flavor = "multi_thread")]
async fn every_subscriber_receives_every_delta() -> Checked<()> {
    // Half the agents share `?reach(0, X)`; the rest each watch another source.
    let sources: Vec<i64> = (0..SUBSCRIBERS as i64)
        .map(|i| if i % 2 == 0 { 0 } else { i })
        .collect();
    let last = SUBSCRIBERS as i64;
    let engine = reachability_engine(last + 1).await?;
    let mut agents = Vec::with_capacity(SUBSCRIBERS);
    for source in &sources {
        let mut agent = Agent::connect(&engine, KG).await?;
        agent
            .subscribe("r", &format!("?reach({source}, X)"))
            .await?;
        agents.push(agent);
    }
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    let mut log = SampleLog::new("fanout_64", &format!("reachability_chain_{}", last + 1));
    for round in 0..FANOUT_ROUNDS {
        let node = last + 1 + round;
        for (event, op) in [("insert", '+'), ("retract", '-')] {
            let commit = writer.commit(&format!("{op}edge({last}, {node})")).await?;
            let sequence = log.samples().len() / SUBSCRIBERS;
            for (subscriber, (agent, source)) in agents.iter_mut().zip(&sources).enumerate() {
                let delta = agent.next_delta("r").await?;
                let row = pairs(&[(*source, node)]);
                if op == '+' {
                    delta.assert_rows(&row, &[])?;
                } else {
                    delta.assert_rows(&[], &row)?;
                }
                log.record(event, sequence, commit, subscriber, SUBSCRIBERS, delta.at);
            }
        }
    }
    for (agent, source) in agents.iter().zip(&sources) {
        let fresh = auditor.query(&format!("?reach({source}, X)")).await?;
        agent.view("r").assert_matches(&fresh.rows)?;
    }
    log.finish().expect("write samples");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn agent_resubscribes_after_reconnect_and_engine_restart() -> Checked<()> {
    let mut engine = reachability_engine(4).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    agent.subscribe("r", REACH_0).await?;
    writer.commit("+edge(3, 4)").await?;
    agent
        .next_delta("r")
        .await?
        .assert_rows(&pairs(&[(0, 4)]), &[])?;
    agent.disconnect().await;

    // Changes while the agent is away arrive in the next snapshot.
    writer.commit("+edge(4, 5)").await?;
    writer.commit("-edge(2, 3)").await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    agent.subscribe("r", REACH_0).await?;
    let expected = auditor.query(REACH_0).await?;
    agent.view("r").assert_matches(&expected.rows)?;
    assert_eq!(agent.view("r").rows.len(), 2, "{:?}", agent.view("r").rows);
    writer.commit("+edge(2, 9)").await?;
    agent
        .next_delta("r")
        .await?
        .assert_rows(&pairs(&[(0, 9)]), &[])?;

    // A crash-restart keeps committed state; a new subscription starts from it.
    let before = auditor.query(REACH_0).await?;
    drop((agent, writer, auditor));
    engine.crash_restart().await.expect("restart engine");
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    agent.subscribe("r", REACH_0).await?;
    agent.view("r").assert_matches(&before.rows)?;
    writer.commit("+edge(9, 11)").await?;
    let delta = agent.next_delta("r").await?;
    delta.assert_rows(&pairs(&[(0, 11)]), &[])?;
    assert_eq!(delta.seq, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unrelated_writes_produce_no_deltas() -> Checked<()> {
    let engine = reachability_engine(4).await?;
    let mut admin = WsClient::connect(&engine, KG).await?;
    admin.commit(".kg create elsewhere").await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    agent.subscribe("r", REACH_0).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut other_kg = WsClient::connect(&engine, "elsewhere").await?;

    for i in 0..20 {
        writer.commit(&format!("+noise({i})")).await?;
        writer
            .commit(&format!("+edge({}, {})", 100 + i, 101 + i))
            .await?;
        other_kg.commit(&format!("+edge(3, {})", 200 + i)).await?;
    }
    writer.commit("-noise(0)").await?;
    // The first delta must be this one: nothing above changed `reach(0, X)`.
    writer.commit("+edge(3, 4)").await?;
    let delta = agent.next_delta("r").await?;
    delta.assert_rows(&pairs(&[(0, 4)]), &[])?;
    assert_eq!(delta.seq, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn write_burst_converges_without_requery() -> Checked<()> {
    let engine = reachability_engine(4).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    agent.subscribe("r", REACH_0).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    const WRITES: i64 = 100;
    for i in 0..WRITES {
        // Grow a branch and retract every third edge of it again.
        writer.commit(&format!("+edge(3, {})", 1000 + i)).await?;
        if i % 3 == 0 {
            writer.commit(&format!("-edge(3, {})", 1000 + i)).await?;
        }
    }
    // Intermediate results can equal the final one; only the final result
    // contains the sentinel.
    writer.commit("+edge(3, 9999)").await?;
    let fresh = auditor.query(REACH_0).await?;
    assert!(fresh.rows.contains(&json!([0, 9999])));
    agent.converge("r", &fresh.rows).await?;
    assert!(
        agent.view("r").seq <= WRITES as u64 * 2,
        "more deltas than writes"
    );
    // Converged: any further delta would move the agent away from the truth.
    agent.expect_quiet("r", Duration::from_millis(300)).await?;
    Ok(())
}
