//! Required passes: the notification stream and the snapshot handoff (W04).
//!
//! Notifications reach every connection in strictly increasing `seq` order
//! however many writers commit at once; a reconnect cursor from before an
//! engine restart is answered with a `replay_gap` notice, never with
//! numerically overlapping notifications of the new run; and commits racing a
//! `.subscribe` all reach the agent, whose view (checked delta by delta for
//! contiguous `seq`, increasing revisions and consistent rows) ends equal to a
//! fresh query.

use inputlayer_testkit::{Agent, Checked, Engine, Fixture, Violation, WsClient};

use crate::engine;

const KG: &str = "agents";

/// Concurrent writer connections per round; half insert into the agent's KG,
/// half create and drop other KGs (their `kg_change` reaches every agent).
const WRITERS: usize = 16;
const WRITES_PER_WRITER: usize = 30;
const ROUNDS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_commits_notify_in_seq_order() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::new("empty", KG).install(&engine).await?;
    for round in 0..ROUNDS {
        notification_order_round(&engine, round).await?;
    }
    Ok(())
}

/// One burst of concurrent commits; the agent must see strictly increasing seqs.
async fn notification_order_round(engine: &Engine, round: usize) -> Checked<()> {
    let mut agent = Agent::connect(engine, KG).await?;
    let mut writers = Vec::with_capacity(WRITERS);
    for _ in 0..WRITERS {
        writers.push(WsClient::connect(engine, KG).await?);
    }
    let tasks: Vec<_> = writers
        .into_iter()
        .enumerate()
        .map(|(w, mut writer)| {
            tokio::spawn(async move {
                for i in 0..WRITES_PER_WRITER {
                    if w % 2 == 0 {
                        writer.commit(&format!("+w{w}({round}, {i})")).await?;
                    } else if i % 2 == 0 {
                        // `.kg create` switches the session into the new KG.
                        writer
                            .commit(&format!(".kg create k{round}_{w}_{i}"))
                            .await?;
                        writer.commit(&format!(".kg use {KG}")).await?;
                    } else {
                        writer
                            .commit(&format!(".kg drop k{round}_{w}_{}", i - 1))
                            .await?;
                    }
                }
                Checked::Ok(())
            })
        })
        .collect();
    for task in tasks {
        task.await.expect("writer task")?;
    }
    agent.wait_notices(WRITERS * WRITES_PER_WRITER).await?;

    let seqs: Vec<u64> = agent
        .notices()
        .iter()
        .filter_map(|n| n.value["seq"].as_u64())
        .collect();
    match seqs.windows(2).find(|pair| pair[1] <= pair[0]) {
        None => Ok(()),
        Some(pair) => Err(Violation::NotificationOrder {
            previous: pair[0],
            got: pair[1],
        }),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_from_before_a_restart_gets_a_replay_gap() -> Checked<()> {
    let mut engine = engine().start().await.expect("start engine");
    Fixture::new("empty", KG).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    for i in 0..5 {
        writer.commit(&format!("+before({i})")).await?;
    }
    agent.wait_notices(5).await?;
    let cursor = agent
        .notices()
        .iter()
        .filter_map(|n| n.value["seq"].as_u64())
        .max()
        .expect("notices carry seq");
    let old_epoch = agent.client().stream_epoch().to_string();
    drop((agent, writer));

    engine.crash_restart().await.expect("restart engine");
    let mut writer = WsClient::connect(&engine, KG).await?;
    // Enough commits that the new run's seqs overlap the old cursor.
    for i in 0..(cursor + 2) {
        writer.commit(&format!("+after({i})")).await?;
    }
    let url = format!("{}&last_seq={cursor}&epoch={old_epoch}", engine.ws_url(KG));
    let mut agent = Agent::over(WsClient::connect_url(&url, engine.api_key()).await?);
    if agent.client().stream_epoch() == old_epoch {
        return Err(Violation::MissedChanges(
            "a restarted engine kept its stream epoch".to_string(),
        ));
    }
    writer.commit("+sentinel(1)").await?;

    // Correct: one replay_gap notice, no replayed notification, then live.
    let mut seen = 0;
    loop {
        seen = agent.wait_notices(seen + 1).await?;
        if agent.notices()[seen - 1].value["relation"] == "sentinel" {
            break;
        }
    }
    let replayed = agent
        .notices()
        .iter()
        .filter(|n| n.value["relation"] == "after")
        .count();
    let gaps = agent
        .client()
        .notices()
        .iter()
        .filter(|n| n.value["code"] == "replay_gap")
        .count();
    if replayed != 0 || gaps != 1 {
        return Err(Violation::MissedChanges(format!(
            "cursor {cursor} of epoch {old_epoch} after a restart: {replayed} notification(s) \
             replayed and {gaps} replay_gap notice(s); expected none and one"
        )));
    }
    Ok(())
}

/// Writers and rows per writer racing one `.subscribe`.
const RACERS: usize = 4;
const RACING_WRITES: usize = 50;

#[tokio::test(flavor = "multi_thread")]
async fn commits_racing_subscribe_all_reach_the_agent() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::new("empty", KG).install(&engine).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    for round in 0..ROUNDS {
        let mut writers = Vec::with_capacity(RACERS);
        for _ in 0..RACERS {
            writers.push(WsClient::connect(&engine, KG).await?);
        }
        let mut agent = Agent::connect(&engine, KG).await?;
        let tasks: Vec<_> = writers
            .into_iter()
            .enumerate()
            .map(|(w, mut writer)| {
                tokio::spawn(async move {
                    for i in 0..RACING_WRITES {
                        writer.commit(&format!("+ev{round}({w}, {i})")).await?;
                    }
                    Checked::Ok(())
                })
            })
            .collect();
        let query = format!("?ev{round}(W, I)");
        agent.subscribe("ev", &query).await?;
        for task in tasks {
            task.await.expect("writer task")?;
        }
        let fresh = auditor.query(&query).await?;
        if fresh.rows.len() != RACERS * RACING_WRITES {
            return Err(Violation::MissedChanges(format!(
                "round {round}: {} of {} rows committed",
                fresh.rows.len(),
                RACERS * RACING_WRITES
            )));
        }
        agent.converge("ev", &fresh.rows).await?;
        agent.disconnect().await;
    }
    Ok(())
}
