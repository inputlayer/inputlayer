//! Required passes: results and deltas too large for one frame arrive whole.
//!
//! A delta over the 16 MiB frame limit (but under the result cap) is streamed
//! as one logical delta; the agent applies it only at its end and converges
//! with a fresh query. A snapshot over the streaming threshold arrives as a
//! streamed `.subscribe` reply naming its subscription.

use inputlayer_testkit::{Agent, Checked, Fixture, WsClient};

use crate::engine;

const KG: &str = "agents";

/// Rows of `big` once switched on, each carrying a `PAD_BYTES` string: the
/// delta exceeds the 16 MiB frame limit while the result stays under the cap.
const BIG_ROWS: i64 = 4_000;
const PAD_BYTES: usize = 5_000;

#[tokio::test(flavor = "multi_thread")]
async fn oversized_delta_and_snapshot_arrive_whole() -> Checked<()> {
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

    // ~20 MiB of inserted rows: one streamed delta, applied at its end.
    writer.commit("+switch(1)").await?;
    let delta = agent.next_delta("big").await?;
    assert_eq!(delta.inserted.len(), BIG_ROWS as usize);

    writer.commit(&format!("+n({BIG_ROWS})")).await?;
    let fresh = auditor.query("?big(X, P)").await?;
    assert_eq!(fresh.rows.len(), BIG_ROWS as usize + 1);
    agent.converge("big", &fresh.rows).await?;

    // The same rows as a snapshot: a streamed reply that names the subscription.
    let snapshot = agent.subscribe("again", "?big(X, P)").await?;
    assert_eq!(snapshot.rows.len(), BIG_ROWS as usize + 1);
    agent.view("again").assert_matches(&fresh.rows)
}
