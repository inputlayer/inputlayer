//! S12, a restart mid-scenario preserves what was committed.
//!
//! Captain's table row 3 (subscriptions read the rows that changed between
//! revisions) across an engine crash, with the readiness features catalog
//! fsync (#297), fast restart (EN-12) and `expect_revision` (#288). Gates V18
//! #330 (kill -9 and restart keeps every resubscribe snapshot equal to
//! recompute) and B14.
//!
//! S1 runs up to the agent's claim; the engine is killed (no clean shutdown)
//! before the agent reads the claim's delta and restarted on the same data.
//! The agent reconnects with its notification cursor and is told the cursor
//! is from another engine run (`replay_gap`, nothing replayed); it
//! resubscribes and gets its pre-crash views plus the committed claim, equal
//! to a fresh query; the writer continues and deltas name the new run's
//! revisions, strictly increasing, and the agent's change notifications are
//! exactly the new run's writes, with contiguous `seq`; a write still
//! expecting a pre-crash revision is refused.
//!
//! Revisions restart with the engine and are paired with the run's stream
//! epoch (`ClientFrame::Execute::expect_epoch`), so the scenario asserts a new
//! epoch where the strategy's table asked for the first post-restart revision
//! to exceed the last pre-crash one, and a pre-crash revision pinned to its
//! epoch is refused. A bare pre-crash `expect_revision` (no epoch) must be
//! refused too: the writer advances the new run until it has issued the
//! pre-crash claim's revision, and a claim expecting that revision, scoped to
//! `claim` so the stock writes are not what refuses it, is accepted today
//! (expected failure, #380).
use inputlayer_testkit::{Agent, Checked, Expect, KnownDefect, Reproduction, Violation, WsClient};
use serde_json::json;

use crate::lifecycle::{up_to_claim, Lifecycle, KG, MINE, OFFERS};
use crate::support::{committed, fresh, refused, write_revision, write_revision_matches_delta};

const STALE_REVISION_ACCEPTED: KnownDefect = KnownDefect {
    plan_item: "#380",
    summary: "a pre-crash expect_revision without expect_epoch passes on the new run's revision",
    signature: |v| matches!(v, Violation::Transport(m) if m.contains("the request succeeded")),
    reproduction: Reproduction::Deterministic,
};

#[tokio::test(flavor = "multi_thread")]
async fn s12_restart_mid_scenario_preserves_revisions() -> Checked<()> {
    let Lifecycle {
        mut engine,
        agent,
        writer,
        auditor,
        decider_key,
        claim,
        ..
    } = up_to_claim().await?;
    let mine_before = agent.view("mine").rows.clone();
    let mut offers_expected = agent.view("offers").rows.clone();
    assert!(offers_expected.remove(&json!(["o-42", "i7"]).to_string()));
    let cursor = agent
        .notices()
        .iter()
        .chain(agent.client().notices())
        .filter_map(|n| n.value["seq"].as_u64())
        .max()
        .unwrap_or(0);
    let old_epoch = agent.client().stream_epoch().to_string();
    let claimed_at = write_revision(&claim)?;
    drop((agent, writer, auditor));

    engine.crash_restart().await.expect("restart engine");

    // Reconnect with the cursor: told it is from another run, nothing replayed.
    let url = format!("{}&last_seq={cursor}&epoch={old_epoch}", engine.ws_url(KG));
    let mut agent = Agent::over(WsClient::connect_url(&url, &decider_key).await?);
    if agent.client().stream_epoch() == old_epoch {
        return Err(Violation::MissedChanges(
            "a restarted engine kept its stream epoch".to_string(),
        ));
    }
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    // Resubscribe: the pre-crash views plus the committed claim.
    let snapshot = agent.subscribe("mine", MINE).await?.revision;
    assert_eq!(agent.view("mine").rows, mine_before);
    agent.subscribe("offers", OFFERS).await?;
    assert_eq!(
        agent.view("offers").rows,
        offers_expected,
        "the claim survived the crash"
    );
    agent
        .view("mine")
        .assert_matches(&fresh(&mut auditor, MINE).await?)?;
    agent
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;

    // The writer continues until the new run has issued the pre-crash claim's
    // revision: deltas name the new run's revisions, increasing.
    let mut previous = snapshot;
    let mut operations = Vec::new();
    let i13 = [json!(["o-42", "i13", "in_stock"])];
    let i14 = [json!(["o-42", "i14"])];
    let writes = [
        (
            r#"-stock("i13", 4)"#,
            "delete",
            (&[][..], &i13[..]),
            (&[][..], &i14[..]),
        ),
        (
            r#"+stock("i13", 4)"#,
            "insert",
            (&i13[..], &[][..]),
            (&i14[..], &[][..]),
        ),
    ];
    for (program, operation, (inserted, retracted), (offered, withdrawn)) in writes.iter().cycle() {
        let write = committed(writer.try_execute(program).await?)?;
        let delta = agent.next_delta("mine").await?;
        delta.assert_rows(inserted, retracted)?;
        write_revision_matches_delta(&write, &delta)?;
        assert!(
            delta.revision > previous,
            "revisions increase after the restart"
        );
        previous = delta.revision;
        let offers = agent.next_delta("offers").await?;
        offers.assert_rows(offered, withdrawn)?;
        write_revision_matches_delta(&write, &offers)?;
        operations.push(*operation);
        if operations.len() >= writes.len() && previous >= claimed_at {
            break;
        }
    }

    // One replay_gap notice, then exactly this run's writes, seq contiguous.
    let gaps = agent
        .client()
        .notices()
        .iter()
        .filter(|n| n.value["code"] == "replay_gap")
        .count();
    assert_eq!(gaps, 1, "one replay_gap notice for a cursor of another run");
    agent.wait_notices(operations.len()).await?;
    let notified: Vec<_> = agent
        .notices()
        .iter()
        .map(|n| (n.value["relation"].clone(), n.value["operation"].clone()))
        .collect();
    let expected: Vec<_> = operations
        .iter()
        .map(|operation| (json!("stock"), json!(operation)))
        .collect();
    assert_eq!(
        notified, expected,
        "the notifications after the restart are this run's writes, nothing replayed"
    );
    let seqs: Vec<_> = agent
        .notices()
        .iter()
        .map(|n| {
            n.value["seq"]
                .as_u64()
                .expect("a notification names its seq")
        })
        .collect();
    assert!(
        seqs.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "notification seqs are contiguous within the new epoch: {seqs:?}"
    );

    // A write expecting the pre-crash revision of its epoch is refused,
    // nothing applied.
    let claim_i4 = r#"+claim("o-42", "i4", "a1")"#;
    let stale = Expect::at(claimed_at).epoch(&old_epoch);
    let reply = agent
        .client_mut()
        .try_execute_expecting(claim_i4, &stale)
        .await?;
    refused(reply, "precondition_failed", "expect_epoch")?;
    agent
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;

    // The same pre-crash revision without its epoch, now issued again by the
    // new run, must be refused as well.
    let bare = Expect::at(claimed_at).relations(&["claim"]);
    let reply = agent
        .client_mut()
        .try_execute_expecting(claim_i4, &bare)
        .await?;
    STALE_REVISION_ACCEPTED.judge(refused(reply, "precondition_failed", "revision").map(drop));
    Ok(())
}
