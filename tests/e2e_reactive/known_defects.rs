//! Expected failures for defects the reactive plan tracks (W05).
//!
//! Each test sets up strictly, then runs the check of the *correct* contract
//! and hands its outcome to [`KnownDefect::judge`]: the defect's own violation
//! is an expected failure; a holding contract fails as XPASS so the marker is
//! removed once the plan item lands. Protocol changes that come with the fix
//! (reset or chunked-delta frames) surface as unknown frames in the testkit
//! client, which must learn them in the same change.

use inputlayer_testkit::{Agent, Checked, Fixture, KnownDefect, Reproduction, Violation, WsClient};

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
