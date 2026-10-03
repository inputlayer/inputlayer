//! Expected failures for defects the reactive plan tracks (W04, W05).
//!
//! Each test sets up strictly, then runs the check of the *correct* contract
//! and hands its outcome to [`KnownDefect::judge`]: the defect's own violation
//! is an expected failure; a holding contract fails as XPASS so the marker is
//! removed once the plan item lands. Protocol changes that come with the fix
//! (reset or chunked-delta frames) surface as unknown frames in the testkit
//! client, which must learn them in the same change.

use inputlayer_testkit::{
    Agent, Checked, Engine, Fixture, KnownDefect, Reproduction, Violation, WsClient,
};

use crate::engine;

const KG: &str = "agents";

const W05_OVERSIZED_DELTA: KnownDefect = KnownDefect {
    plan_item: "W05",
    summary: "an undeliverable oversized delta still advances the subscription",
    signature: |v| matches!(v, Violation::SeqGap { .. }),
    reproduction: Reproduction::Deterministic,
};

/// Rows of `big` once switched on, each carrying a `PAD_BYTES` string: the
/// delta exceeds the 16 MiB frame limit while the result stays under the cap.
const BIG_ROWS: i64 = 4_000;
const PAD_BYTES: usize = 5_000;

#[tokio::test(flavor = "multi_thread")]
async fn w05_oversized_delta_advances_seq_without_delivery() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::new("oversized_delta", KG)
        .facts("n", (0..BIG_ROWS).map(|i| format!("({i})")))
        .facts("pad", [format!("(\"{}\")", "x".repeat(PAD_BYTES))])
        .facts("switch", ["(0)".to_string()])
        .rule("big(X, P) <- n(X), switch(1), pad(P)")
        .install(&engine)
        .await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    agent.subscribe("big", "?big(X, P)").await?;
    assert!(agent.view("big").rows.is_empty());

    writer.commit("+switch(1)").await?;
    // The engine reports the delta as undeliverable; the contract still
    // requires the agent to end up with the complete current result.
    match agent.next_delta("big").await {
        Ok(_) | Err(Violation::SubscriptionError { .. }) => {}
        Err(other) => return Err(other),
    }
    writer.commit(&format!("+n({BIG_ROWS})")).await?;
    let fresh = auditor.query("?big(X, P)").await?;
    assert_eq!(fresh.rows.len(), BIG_ROWS as usize + 1);
    let outcome = agent.converge("big", &fresh.rows).await;
    W05_OVERSIZED_DELTA.judge(outcome);
    Ok(())
}

const W04_NOTIFICATION_ORDER: KnownDefect = KnownDefect {
    plan_item: "W04",
    summary: "concurrent commits publish change notifications out of seq order",
    signature: |v| matches!(v, Violation::NotificationOrder { .. }),
    reproduction: Reproduction::Racy,
};

/// Concurrent writer connections per probe round; half insert into the
/// agent's KG, half create and drop other KGs (their `kg_change` reaches
/// every agent).
const WRITERS: usize = 16;
const WRITES_PER_WRITER: usize = 30;
const PROBE_ROUNDS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
async fn w04_concurrent_commits_notify_out_of_order() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::new("empty", KG).install(&engine).await?;
    let mut outcome = Ok(());
    for round in 0..PROBE_ROUNDS {
        outcome = notification_order_round(&engine, round).await?;
        if outcome.is_err() {
            break;
        }
    }
    W04_NOTIFICATION_ORDER.judge(outcome);
    Ok(())
}

/// One burst of concurrent commits; `Err` inside when the agent saw a
/// notification seq not greater than the one before it.
async fn notification_order_round(engine: &Engine, round: usize) -> Checked<Checked<()>> {
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

    // Correct: notifications arrive in strictly increasing seq order.
    let seqs: Vec<u64> = agent
        .notices()
        .iter()
        .filter_map(|n| n.value["seq"].as_u64())
        .collect();
    Ok(seqs
        .windows(2)
        .find(|pair| pair[1] <= pair[0])
        .map_or(Ok(()), |pair| {
            Err(Violation::NotificationOrder {
                previous: pair[0],
                got: pair[1],
            })
        }))
}

const W04_RESTART_CURSOR: KnownDefect = KnownDefect {
    plan_item: "W04",
    summary: "a reconnect cursor from before an engine restart silently skips new changes",
    signature: |v| matches!(v, Violation::MissedChanges(_)),
    reproduction: Reproduction::Deterministic,
};

#[tokio::test(flavor = "multi_thread")]
async fn w04_restart_cursor_skips_changes() -> Checked<()> {
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
    drop((agent, writer));

    engine.crash_restart().await.expect("restart engine");
    let mut writer = WsClient::connect(&engine, KG).await?;
    writer.commit("+after(1)").await?;
    writer.commit("+after(2)").await?;
    let url = format!("{}&last_seq={cursor}", engine.ws_url(KG));
    let mut agent = Agent::over(WsClient::connect_url(&url, engine.api_key()).await?);
    writer.commit("+sentinel(1)").await?;

    // Correct: both `after` changes are replayed (or a reset is signalled)
    // before the live `sentinel` notification.
    let mut seen = 0;
    loop {
        seen = agent.wait_notices(seen + 1).await?;
        let last = &agent.notices()[seen - 1].value;
        if last["relation"] == "sentinel" {
            break;
        }
    }
    let replayed = agent
        .notices()
        .iter()
        .filter(|n| n.value["relation"] == "after")
        .count();
    let outcome = if replayed == 2 {
        Ok(())
    } else {
        Err(Violation::MissedChanges(format!(
            "cursor {cursor} from before the restart; {replayed} of 2 post-restart \
             changes replayed before the live sentinel"
        )))
    };
    W04_RESTART_CURSOR.judge(outcome);
    Ok(())
}
