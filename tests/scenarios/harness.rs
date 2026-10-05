//! Required passes: the testkit pieces every scenario builds on, against a
//! real engine: the shop pack derives its anchors, counters scrape, scoped
//! keys and `expect_revision` reach the engine as scenarios send them.

use std::collections::BTreeSet;
use std::time::Instant;

use inputlayer_testkit::{
    Checked, Counters, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::Value;

use crate::engine;

/// Rows as strings, for set comparisons.
fn rows(rows: &[Value]) -> BTreeSet<String> {
    rows.iter().map(Value::to_string).collect()
}

fn expected(rows: &[&str]) -> BTreeSet<String> {
    rows.iter().map(|row| (*row).to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn shop_pack_installs_and_derives_its_anchors() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    let pack = Fixture::shop_pack(Size::Small);
    let started = Instant::now();
    pack.install(&engine).await?;
    let install = started.elapsed();
    println!(
        "shop_pack(Small): {} statements in {install:?}",
        pack.statements.len()
    );

    let mut reader = WsClient::connect(&engine, &pack.knowledge_graph).await?;
    let eligible = reader.query(r#"?eligible("o-42", Item, Why)"#).await?;
    assert_eq!(
        rows(&eligible.rows),
        expected(&[r#"["o-42","i1","in_stock"]"#, r#"["o-42","i5","in_stock"]"#])
    );
    let offer = reader.query(r#"?offer("o-42", X)"#).await?;
    assert_eq!(
        rows(&offer.rows),
        expected(&[
            r#"["o-42","i2"]"#,
            r#"["o-42","i3"]"#,
            r#"["o-42","i4"]"#,
            r#"["o-42","i6"]"#,
            r#"["o-42","i7"]"#,
            r#"["o-42","i8"]"#,
            r#"["o-42","i9"]"#,
        ])
    );
    let count = reader.query(r#"?n_eligible("o-42", N)"#).await?;
    assert_eq!(rows(&count.rows), expected(&[r#"["o-42",2]"#]));
    let related = reader.query(r#"?related("i0", X)"#).await?;
    assert_eq!(
        related.rows.len(),
        4,
        "i0 reaches i1-i4: {:?}",
        related.rows
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shop_pack_vector_serves_the_near_rule() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    let pack = Fixture::shop_pack(Size::Vector);
    pack.install(&engine).await?;
    let mut reader = WsClient::connect(&engine, &pack.knowledge_graph).await?;
    let near = reader.query("?near(1, Other, D)").await?;
    assert_eq!(near.rows.len(), 5, "five neighbours: {:?}", near.rows);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn counters_scrape_the_running_engine() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let before = engine.metrics().await.expect("scrape metrics");
    let mut reader = WsClient::connect(&engine, "shop").await?;
    reader.query(r#"?eligible("o-42", Item, Why)"#).await?;
    let after = engine.metrics().await.expect("scrape metrics");
    let delta = Counters::delta(&before, &after);
    assert!(
        Counters::require("queries", delta.queries)? >= 1,
        "a query is counted: {delta:?}"
    );

    KnownDefect {
        plan_item: "#308",
        summary: "rule_evaluations is not exported, so a read cannot prove it skipped evaluation",
        signature: |v| matches!(v, Violation::NotMeasurable(_)),
        reproduction: Reproduction::Deterministic,
    }
    .judge(Counters::require("rule_evaluations", delta.rule_evaluations).map(drop));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scoped_keys_users_and_grants_reach_the_engine() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;

    let key = engine
        .create_api_key("agent-a1", "decider", "shop", &["claim"])
        .await?;
    let mut decider = WsClient::connect_with_key(&engine, "shop", &key).await?;
    decider.commit(r#"+claim("o-42", "i7", "a1")"#).await?;
    let refused = decider.execute(r#"+stock("i7", 99)"#).await;
    let refused = match refused {
        Ok(result) => result.errors,
        Err(Violation::Rejected(message)) => vec![Value::String(message)],
        Err(other) => return Err(other),
    };
    assert!(
        !refused.is_empty(),
        "a decider key limited to claim cannot write stock"
    );

    engine
        .create_user("auditor", "auditor-password-1", "viewer")
        .await?;
    engine.grant("shop", "auditor", "viewer", &[]).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn expect_revision_and_at_reach_the_engine() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut writer = WsClient::connect(&engine, "shop").await?;

    let first = writer.execute(r#"+stock("i13", 5)"#).await?;
    let revision = first.revision.expect("a write reply carries its revision");
    let current = writer
        .execute_expecting(r#"+claim("o-42", "i6", "a1")"#, revision, &["claim"])
        .await?;
    assert!(current.errors.is_empty(), "{:?}", current.errors);
    let landed = current
        .revision
        .expect("a write reply carries its revision");
    assert!(landed > revision);

    let stale = writer
        .execute_expecting(r#"+claim("o-42", "i7", "a1")"#, revision, &["claim"])
        .await;
    match stale {
        Err(Violation::Rejected(message)) if message.contains("Precondition failed") => {}
        other => panic!("a claim against a stale revision must be refused: {other:?}"),
    }

    let read = writer
        .execute_at(r#"?eligible("o-42", Item, _)"#, landed)
        .await?;
    assert_eq!(read.rows.len(), 3, "i13 is now in stock: {:?}", read.rows);
    Ok(())
}
