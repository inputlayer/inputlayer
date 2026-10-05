//! S8, a rule replaced while agents are subscribed.
//!
//! Captain's table row 1 (a deployed rule is a live view, across rule
//! changes). Gates V7 #314.
//!
//! An agent subscribes to `offer("o-42", X)` while an auditor keeps reading
//! it. A writer replaces `offer` with a stricter body (offered items must
//! hold more than three) in one program together with a fact that the new
//! body admits: the agent gets one delta at the program's revision carrying
//! exactly the old-vs-new difference, never a reset, and every concurrent
//! read sees the old rows or the new rows, never a mix (required). Removing
//! `related`'s recursive clause is one more exact delta (required). Dropping
//! the base relation `link`, which `related` (and so `offer`) depends on,
//! must be refused with `conflict` naming the dependents; today the drop
//! succeeds and the rules silently read it as empty (expected failure, V7
//! #314).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use inputlayer_testkit::agent::row_keys;
use inputlayer_testkit::{
    Agent, Checked, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, OFFERS};
use crate::support::{committed, fresh, refused, write_revision_matches_delta, QUIET};

/// Dropping a relation a deployed rule reads is accepted (V7 #314).
const DROP_UNDER_VIEW: KnownDefect = KnownDefect {
    plan_item: "#314",
    summary: "dropping a relation a deployed view depends on is accepted and empties the view",
    signature: |v| matches!(v, Violation::Transport(m) if m.contains("the request succeeded")),
    reproduction: Reproduction::Deterministic,
};

/// `offer` replaced in one program: the stricter body, and a fact it admits.
const REPLACE: &str = r#".rule clear offer
+offer(Order, Other) <- eligible(Order, Item, _), related(Item, Other), !blocked(Other), !claim(Order, Other, _), stock(Other, Q), Q > 3
+stock("i4", 9)"#;

fn offers(items: &[&str]) -> Vec<Value> {
    items.iter().map(|i| json!(["o-42", i])).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn s8_rule_replaced_while_subscribed() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    agent.subscribe("offers", OFFERS).await?;
    let old = row_keys(&offers(&["i2", "i3", "i4", "i6", "i7", "i8", "i9"]));
    agent
        .view("offers")
        .assert_matches(&offers(&["i2", "i3", "i4", "i6", "i7", "i8", "i9"]))?;
    // Stock above 3: i3 (8), i7 (10), i8 (4), i9 (11), and i4 through the new fact.
    let new_items = ["i3", "i4", "i7", "i8", "i9"];
    let new = row_keys(&offers(&new_items));

    // A concurrent reader: every read is one generation or the other.
    let stop = Arc::new(AtomicBool::new(false));
    let mut reader = WsClient::connect(&engine, KG).await?;
    let reading = {
        let (stop, old, new) = (stop.clone(), old.clone(), new.clone());
        tokio::spawn(async move {
            let mut reads = 0_u32;
            while !stop.load(Ordering::Relaxed) || reads == 0 {
                let rows: BTreeSet<String> = row_keys(&reader.query(OFFERS).await?.rows);
                if rows != old && rows != new {
                    return Err(Violation::Diverged {
                        subscription: "concurrent offer read".to_string(),
                        missing: new.difference(&rows).cloned().collect(),
                        unexpected: rows.difference(&old).cloned().collect(),
                    });
                }
                reads += 1;
            }
            Ok(reads)
        })
    };

    let replaced = committed(writer.try_execute(REPLACE).await?)?;
    let delta = agent.next_delta("offers").await?;
    delta.assert_rows(&[], &offers(&["i2", "i6"]))?;
    write_revision_matches_delta(&replaced, &delta)?;
    agent.expect_quiet("offers", QUIET).await?;
    stop.store(true, Ordering::Relaxed);
    let reads = reading.await.expect("reader task")?;
    println!("S8: {reads} concurrent reads saw one generation each");
    agent
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;

    // Without the recursive clause `related` holds direct links only: o-42's
    // items i1 and i5 reach i2 and i6, neither of which holds more than 3.
    let removed = committed(writer.try_execute(".rule remove related 2").await?)?;
    let delta = agent.next_delta("offers").await?;
    delta.assert_rows(&[], &offers(&new_items))?;
    write_revision_matches_delta(&removed, &delta)?;
    committed(
        writer
            .try_execute("+related(Item, Other) <- related(Item, Mid), link(Mid, Other)")
            .await?,
    )?;
    agent.converge("offers", &offers(&new_items)).await?;

    // `link` feeds `related` and `offer`: dropping it must be refused.
    let dropped = writer.try_execute(".rel drop link").await?;
    DROP_UNDER_VIEW.judge(refused(dropped, "conflict", "related").map(drop));
    Ok(())
}
