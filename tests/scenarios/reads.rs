//! S2 and S3, reads of deployed rules.
//!
//! S2: a one-row read of a deployed view is a lookup. Captain's table rows 1
//! (a deployed rule is a live view) and 2 (one-row reads cost the same at any
//! graph size). Fifty reads of `eligible("o-42", ..)` and one of the
//! recursive `related("i1", X)` return the pack's rows (required), and the
//! engine's counters show no rule evaluated and every read served from a
//! view. The counter part is an expected failure: the counters do not exist
//! until V1 #308, and today every read re-derives the rule, which V9 #315
//! ends. The latency half (p50 under 1 ms at lab size) is the perf tier's B1/B2
//! on the benchmark host, not this debug suite.
//!
//! S3: an ad-hoc query joins facts, a session rule and deployed views without
//! re-deriving the views (row 6). The rows equal the same question asked of
//! base relations only, at the same revision (required); the counters show
//! only the query's own work (expected failure, #308 then V9 #315).
//!
//! Both counter checks hold in `maintained` mode once V9 lands. Until V20
//! deletes recompute, a `recompute` run keeps evaluating rules, so the matrix
//! run of that mode keeps the #315 marker when this one flips.

use inputlayer_testkit::agent::row_keys;
use inputlayer_testkit::{
    Checked, Counters, Engine, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, MINE};
use crate::support::{no_rule_evaluations, NO_VIEW_COUNTERS};

/// Reads of a deployed rule evaluate it instead of reading its view (V9 #315).
pub const READS_EVALUATE_RULES: KnownDefect = KnownDefect {
    plan_item: "#315",
    summary: "a read of a deployed rule re-derives it instead of reading its view",
    signature: |v| matches!(v, Violation::UnexpectedWork(_)),
    reproduction: Reproduction::Deterministic,
};

/// Reads S2 repeats.
const READS: u64 = 50;

async fn started() -> Checked<(Engine, WsClient)> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let reader = WsClient::connect(&engine, KG).await?;
    Ok((engine, reader))
}

#[tokio::test(flavor = "multi_thread")]
async fn s2_one_row_view_read_is_a_lookup() -> Checked<()> {
    let (engine, mut reader) = started().await?;
    let before = engine.metrics().await.expect("scrape metrics");
    for _ in 0..READS {
        let rows = reader.query(MINE).await?.rows;
        assert_eq!(
            row_keys(&rows),
            row_keys(&[
                json!(["o-42", "i1", "in_stock"]),
                json!(["o-42", "i5", "in_stock"])
            ]),
            "o-42's eligible items"
        );
    }
    let related = reader.query(r#"?related("i1", X)"#).await?.rows;
    assert_eq!(
        row_keys(&related),
        row_keys(&[
            json!(["i1", "i2"]),
            json!(["i1", "i3"]),
            json!(["i1", "i4"])
        ]),
        "i1 reaches the rest of its chain through the recursive rule"
    );
    let after = engine.metrics().await.expect("scrape metrics");

    KnownDefect::judge_first(
        &[NO_VIEW_COUNTERS, READS_EVALUATE_RULES],
        no_rule_evaluations(&Counters::delta(&before, &after), READS + 1),
    );
    Ok(())
}

/// The first two columns of a row: S3's `(Item, Qty)`.
fn item_qty(row: &Value) -> Value {
    json!([row[0], row[1]])
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_ad_hoc_query_joins_facts_and_views() -> Checked<()> {
    let (engine, mut agent) = started().await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    // A session rule on the agent's connection, joined with the `offer` view.
    agent.query("big(I) <- stock(I, Q), Q > 8").await?;
    let before = engine.metrics().await.expect("scrape metrics");
    let in_stock = agent
        .query(r#"?stock(I, Q), eligible("o-42", I, _), Q > 5"#)
        .await?
        .rows;
    let big_offers = agent.query(r#"?offer("o-42", I), big(I)"#).await?.rows;
    let after = engine.metrics().await.expect("scrape metrics");

    // The same questions of base relations only: `eligible` inlined, and the
    // session rule inlined against the deployed `offer`.
    let base = auditor
        .query(r#"?stock(I, Q), order_item("o-42", I), stock(I, Q2), Q2 > 0, !blocked(I), Q > 5"#)
        .await?
        .rows;
    assert_eq!(
        row_keys(&in_stock.iter().map(item_qty).collect::<Vec<_>>()),
        row_keys(&base.iter().map(item_qty).collect::<Vec<_>>()),
        "facts joined with the eligible view equal the base-relation answer"
    );
    assert!(
        !in_stock.is_empty(),
        "o-42's items i1 and i5 hold more than 5"
    );
    let inlined = auditor
        .query(r#"?offer("o-42", I), stock(I, Q), Q > 8"#)
        .await?
        .rows;
    let items = |rows: &[Value]| row_keys(&rows.iter().map(|r| r[1].clone()).collect::<Vec<_>>());
    assert_eq!(
        items(&big_offers),
        items(&inlined),
        "the session rule joined with the offer view equals it inlined"
    );
    assert_eq!(items(&big_offers), row_keys(&[json!("i7"), json!("i9")]));

    KnownDefect::judge_first(
        &[NO_VIEW_COUNTERS, READS_EVALUATE_RULES],
        no_rule_evaluations(&Counters::delta(&before, &after), 2),
    );
    Ok(())
}
