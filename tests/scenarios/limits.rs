//! S14, limits refuse cleanly while other agents continue.
//!
//! The readiness limits (#296's runaway-query guard, PR 304; parser nesting;
//! size caps; graph memory budgets) on a running deployment. Gates V15 #321,
//! whose memory budget for derived state must refuse the same way.
//!
//! Twenty agents subscribe to `eligible("o-42", ..)` on an engine with a
//! per-query memory limit and a graph memory budget. One connection then is
//! refused four times, three ways: a cross product over that memory limit
//! (`resource_exhausted`), a term nested deeper than the parser allows
//! (`validation`), a `top_k` far above its cap (`validation`) and a write
//! that would grow the graph past its budget (`resource_exhausted`). The
//! cross product is in flight while a writer stocks i13. Required: each
//! refusal carries its code and names its limit, nothing of the refused
//! write is applied, the refused connection stays usable, and every one of
//! the twenty agents receives the writer's delta.

use inputlayer_testkit::agent::row_keys;
use inputlayer_testkit::{Agent, Checked, Fixture, Size, WsClient};
use serde_json::json;

use crate::engine;
use crate::lifecycle::{KG, MINE};
use crate::support::{committed, fresh, others_unaffected, refused};

/// Agents subscribed while one connection hits the limits.
const AGENTS: usize = 20;
/// `max_query_memory_bytes`: far above what any scenario query needs.
const QUERY_BYTES: u64 = 32 << 20;
/// `max_graph_memory_bytes`: room for the shop pack (a few hundred KB of
/// facts), not for [`BULK`] more.
const GRAPH_BYTES: u64 = 1 << 20;
/// Rows per side of the cross product: 400^3 rows cannot fit [`QUERY_BYTES`].
const SIDE: usize = 400;
/// Facts of the write that would outgrow [`GRAPH_BYTES`]: over 1 MB of
/// facts in a program under the 1 MiB request limit.
const BULK: usize = 6_000;
/// Padding of each [`BULK`] fact's item name.
const PAD: usize = 100;
/// Nesting of the refused term: past the default limit (128).
const DEEP: usize = 1_025;

fn side(relation: &str) -> String {
    let facts: Vec<String> = (0..SIDE).map(|n| format!("({n})")).collect();
    format!("+{relation}[{}]", facts.join(", "))
}

#[tokio::test(flavor = "multi_thread")]
async fn s14_limits_refuse_cleanly_while_other_agents_continue() -> Checked<()> {
    let engine = engine()
        .memory_limits(QUERY_BYTES, GRAPH_BYTES)
        .max_query_cost(0)
        .start()
        .await
        .expect("start engine");
    let pack = Fixture::shop_pack(Size::Small)
        .statement(&side("cart_a"))
        .statement(&side("cart_b"))
        .statement(&side("cart_c"));
    pack.install(&engine).await?;

    let mut agents = Vec::with_capacity(AGENTS);
    for _ in 0..AGENTS {
        let mut agent = Agent::connect(&engine, KG).await?;
        agent.subscribe("mine", MINE).await?;
        agents.push(agent);
    }
    let mut offender = WsClient::connect(&engine, KG).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    // The cross product runs while the writer commits and the agents hear it.
    offender
        .send_execute("?cart_a(X), cart_b(Y), cart_c(Z)")
        .await?;
    committed(writer.try_execute(r#"+stock("i13", 5)"#).await?)?;
    let mut waiting: Vec<(&mut Agent, &str)> = agents.iter_mut().map(|a| (a, "mine")).collect();
    for delta in others_unaffected(&mut waiting).await? {
        delta.assert_rows(&[json!(["o-42", "i13", "in_stock"])], &[])?;
    }
    refused(
        offender.try_result().await?,
        "resource_exhausted",
        "max_query_memory_bytes",
    )?;

    let deep = format!("{}Y{}", "abs(".repeat(DEEP), ")".repeat(DEEP));
    refused(
        offender
            .try_execute(&format!("?stock(I, Y), X = {deep}"))
            .await?,
        "validation",
        "nesting exceeds the limit",
    )?;
    refused(
        offender
            .try_execute("best(top_k<300000000000000000, I, Q:desc>) <- stock(I, Q)\n?best(I, Q)")
            .await?,
        "validation",
        "top_k: k must be at most",
    )?;
    let pad = "x".repeat(PAD);
    let bulk: Vec<String> = (0..BULK)
        .map(|n| format!(r#"("bulk-{n}-{pad}", 1)"#))
        .collect();
    let before = row_keys(&fresh(&mut auditor, "?stock(I, Q)").await?);
    refused(
        offender
            .try_execute(&format!("+stock[{}]", bulk.join(", ")))
            .await?,
        "resource_exhausted",
        "max_graph_memory_bytes",
    )?;
    assert_eq!(
        row_keys(&fresh(&mut auditor, "?stock(I, Q)").await?),
        before,
        "nothing of the refused write is applied"
    );

    // The refused connection still answers, and the agents still hear writes.
    let mine = offender.query(MINE).await?.rows;
    assert_eq!(mine.len(), 3, "o-42's eligible items: {mine:?}");
    committed(writer.try_execute(r#"+blocked("i13")"#).await?)?;
    let mut waiting: Vec<(&mut Agent, &str)> = agents.iter_mut().map(|a| (a, "mine")).collect();
    for delta in others_unaffected(&mut waiting).await? {
        delta.assert_rows(&[], &[json!(["o-42", "i13", "in_stock"])])?;
    }
    for agent in &agents {
        agent
            .view("mine")
            .assert_matches(&fresh(&mut auditor, MINE).await?)?;
    }
    Ok(())
}
