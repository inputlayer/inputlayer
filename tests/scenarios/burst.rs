//! S17, a write burst with read-your-writes.
//!
//! Captain's table row 1 (a write is confirmed once the views include it,
//! captain call D2) under load, the session-throughput readiness feature.
//! Gates V12 #319 and V14 #320.
//!
//! [`WRITERS`] writers each commit [`WRITES_EACH`] programs as fast as their
//! replies come back, each adding a new in-stock item to order o-42; after
//! every acknowledged write the writer reads its item through the deployed
//! `eligible` view on its own connection and on another one, and must see
//! it (required, read-your-writes). Halfway through, another connection
//! replaces `eligible` with an equivalent body, so the views rebuild during
//! the burst. An agent subscribed to o-42's rows receives every new row
//! exactly once and ends equal to a fresh query (required: no missed or
//! stray deltas; the agent checks contiguous `seq` and increasing
//! `revision`).
//!
//! The rebuild's acknowledgement must carry `views_at`, the revision the
//! views have reached while the rebuild runs (expected failure until V14
//! #320). When this flips, set the view acknowledgement budget to zero on
//! the engine so the rebuild always outlasts it.

use std::collections::BTreeSet;

use futures_util::future::try_join_all;
use inputlayer_testkit::{
    Agent, Checked, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, MINE};
use crate::support::{committed, fresh, write_revision, QUIET};

const WRITERS: usize = 4;
const WRITES_EACH: usize = 100;

/// `eligible` with its body reordered: the same rows, a new generation.
const REPLACE_ELIGIBLE: &str = r#".rule clear eligible
+eligible(Order, Item, Why) <- stock(Item, Q), order_item(Order, Item), Q > 0, !blocked(Item), Why = "in_stock""#;

/// Text of the violation an acknowledgement without `views_at` produces.
const NO_VIEWS_AT_FIELD: &str = "the rebuild's acknowledgement names no views_at";

/// Write acknowledgements never carry `views_at` (V14 #320).
const NO_VIEWS_AT: KnownDefect = KnownDefect {
    plan_item: "#320",
    summary: "a write acknowledged during a view rebuild does not carry views_at",
    signature: |v| matches!(v, Violation::Transport(m) if m.starts_with(NO_VIEWS_AT_FIELD)),
    reproduction: Reproduction::Deterministic,
};

/// One writer's burst: write, then read the item back on both connections.
async fn burst(writer: usize, mut own: WsClient, mut other: WsClient) -> Checked<Vec<Value>> {
    let mut rows = Vec::with_capacity(WRITES_EACH);
    for n in 0..WRITES_EACH {
        let item = format!("b{writer}_{n}");
        let program = format!("+stock(\"{item}\", 1)\n+order_item(\"o-42\", \"{item}\")");
        let write = committed(own.try_execute(&program).await?)?;
        write_revision(&write)?;
        let read = format!(r#"?eligible("o-42", "{item}", Why)"#);
        for (connection, client) in [("own", &mut own), ("another", &mut other)] {
            if client.query(&read).await?.rows.is_empty() {
                return Err(Violation::MissedChanges(format!(
                    "writer {writer}'s acknowledged write {n} is not visible on {connection} \
                     connection"
                )));
            }
        }
        rows.push(json!(["o-42", item, "in_stock"]));
    }
    Ok(rows)
}

#[tokio::test(flavor = "multi_thread")]
async fn s17_write_burst_reads_its_writes() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    agent.subscribe("mine", MINE).await?;
    let mut expected: Vec<Value> = fresh(&mut auditor, MINE).await?;

    let mut writers = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let own = WsClient::connect(&engine, KG).await?;
        let other = WsClient::connect(&engine, KG).await?;
        writers.push(tokio::spawn(burst(writer, own, other)));
    }
    // Rebuild the views mid-burst: once the subscription has seen writes.
    let mut seen = 0;
    while seen < WRITERS * WRITES_EACH / 2 {
        seen += agent.next_delta("mine").await?.inserted.len();
    }
    let mut rule_writer = WsClient::connect(&engine, KG).await?;
    let replaced = committed(rule_writer.try_execute(REPLACE_ELIGIBLE).await?)?;
    let views_at = match (replaced.views_at, write_revision(&replaced)?) {
        (Some(at), revision) if at < revision => Ok(()),
        (Some(at), revision) => Err(Violation::Transport(format!(
            "views_at {at} is not behind the rebuild's revision {revision}"
        ))),
        (None, revision) => Err(Violation::Transport(format!(
            "{NO_VIEWS_AT_FIELD} (revision {revision})"
        ))),
    };

    for rows in try_join_all(writers).await.expect("writer task") {
        expected.extend(rows?);
    }
    agent.converge("mine", &expected).await?;
    agent.expect_quiet("mine", QUIET).await?;
    let unique: BTreeSet<String> = expected.iter().map(Value::to_string).collect();
    assert_eq!(unique.len(), expected.len(), "every write added a new row");
    agent
        .view("mine")
        .assert_matches(&fresh(&mut auditor, MINE).await?)?;
    NO_VIEWS_AT.judge(views_at);
    Ok(())
}
