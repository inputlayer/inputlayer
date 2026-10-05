//! S5, many agents on one view, each hearing only its key.
//!
//! Captain's table row 4 (many agents on one deployed rule, one maintenance
//! pass per commit). Gates V10 #317, V11 #318 and V12 #319 (B8).
//!
//! The pack's orders plus generated ones make [`KEYED`] orders; one agent
//! per order subscribes to its own `eligible(order, ..)` rows and [`UNKEYED`]
//! agents to every order's. One write stocks three items, each held by one
//! generated order: exactly those three keyed agents and every unkeyed agent
//! get a delta of their rows, every other agent stays quiet (required, key
//! isolation). A write that changes no `eligible` row reaches nobody.
//!
//! One pass is an expected failure: across the write the counters must show
//! the view maintained once and no rule evaluated to refresh the 210
//! subscriptions. The counters do not exist until V1 #308, and today every
//! subscription refresh re-runs its query, which V10 #317 replaces with
//! diffs of the view between revisions.

use futures_util::future::join_all;
use inputlayer_testkit::{
    Agent, Checked, Counters, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::json;

use crate::engine;
use crate::lifecycle::KG;
use crate::support::{committed, write_revision_matches_delta, NO_VIEW_COUNTERS, QUIET};

/// Keyed agents, one order each.
const KEYED: usize = 200;
/// Agents subscribed to every order's rows.
const UNKEYED: usize = 10;
/// Generated orders `o-g{n}` holding item `g{n}`; the first three start out
/// of stock.
const TOUCHED: [usize; 3] = [0, 1, 2];

/// Subscription refreshes re-run their query instead of reading the view's
/// diff (V10 #317).
const REFRESH_EVALUATES: KnownDefect = KnownDefect {
    plan_item: "#317",
    summary: "subscription refreshes re-evaluate the rule instead of diffing the view",
    signature: |v| matches!(v, Violation::UnexpectedWork(_)),
    reproduction: Reproduction::Deterministic,
};

/// One maintenance pass for the commit: maintenance time was spent and no
/// deployed rule was evaluated to refresh the subscriptions.
fn one_pass(delta: &Counters) -> Checked<()> {
    let evaluations = Counters::require("rule_evaluations", delta.rule_evaluations)?;
    let maintained = Counters::require("view_maintenance_us", delta.view_maintenance_us)?;
    if evaluations > 0 || maintained == 0 {
        return Err(Violation::UnexpectedWork(format!(
            "{evaluations} rule evaluation(s) and {maintained} us of view maintenance for one \
             commit with {} subscribers: {delta:?}",
            KEYED + UNKEYED
        )));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn s5_many_agents_each_hear_only_their_key() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut writer = WsClient::connect(&engine, KG).await?;

    let mut orders: Vec<String> = writer
        .query("?order(O, _, _)")
        .await?
        .rows
        .iter()
        .map(|r| r[0].as_str().expect("order id").to_string())
        .collect();
    let generated = KEYED - orders.len();
    let order_items: Vec<String> = (0..generated)
        .map(|n| format!(r#"("o-g{n}", "g{n}")"#))
        .collect();
    let stock: Vec<String> = (0..generated)
        .map(|n| format!(r#"("g{n}", {})"#, u8::from(!TOUCHED.contains(&n))))
        .collect();
    committed(
        writer
            .try_execute(&format!(
                "+order_item[{}]\n+stock[{}]",
                order_items.join(", "),
                stock.join(", ")
            ))
            .await?,
    )?;
    orders.extend((0..generated).map(|n| format!("o-g{n}")));
    assert_eq!(orders.len(), KEYED);

    let mut keyed = Vec::with_capacity(KEYED);
    for order in &orders {
        let mut agent = Agent::connect(&engine, KG).await?;
        agent
            .subscribe("mine", &format!(r#"?eligible("{order}", Item, Why)"#))
            .await?;
        keyed.push((order.clone(), agent));
    }
    let mut unkeyed = Vec::with_capacity(UNKEYED);
    for _ in 0..UNKEYED {
        let mut agent = Agent::connect(&engine, KG).await?;
        agent
            .subscribe("all", "?eligible(Order, Item, Why)")
            .await?;
        unkeyed.push(agent);
    }

    // One write touching three keys.
    let before = engine.metrics().await.expect("scrape metrics");
    let stocked: Vec<String> = TOUCHED.iter().map(|n| format!(r#"("g{n}", 2)"#)).collect();
    let write = committed(
        writer
            .try_execute(&format!("+stock[{}]", stocked.join(", ")))
            .await?,
    )?;
    let rows: Vec<_> = TOUCHED
        .iter()
        .map(|n| json!([format!("o-g{n}"), format!("g{n}"), "in_stock"]))
        .collect();
    let heard = join_all(keyed.iter_mut().map(|(order, agent)| {
        let expected = TOUCHED
            .iter()
            .position(|n| *order == format!("o-g{n}"))
            .map(|i| rows[i].clone());
        let write = &write;
        async move {
            match expected {
                Some(row) => {
                    let delta = agent.next_delta("mine").await?;
                    delta.assert_rows(&[row], &[])?;
                    write_revision_matches_delta(write, &delta)?;
                    agent.expect_quiet("mine", QUIET).await
                }
                None => agent.expect_quiet("mine", QUIET).await,
            }
        }
    }))
    .await;
    heard.into_iter().collect::<Checked<Vec<()>>>()?;
    let heard = join_all(unkeyed.iter_mut().map(|agent| {
        let (rows, write) = (&rows, &write);
        async move {
            let delta = agent.next_delta("all").await?;
            delta.assert_rows(rows, &[])?;
            write_revision_matches_delta(write, &delta)
        }
    }))
    .await;
    heard.into_iter().collect::<Checked<Vec<()>>>()?;
    let after = engine.metrics().await.expect("scrape metrics");

    // A write that changes no eligible row reaches nobody.
    committed(writer.try_execute(r#"+stock("g10", 7)"#).await?)?;
    let quiet = join_all(
        keyed
            .iter_mut()
            .map(|(_, agent)| agent.expect_quiet("mine", QUIET))
            .chain(
                unkeyed
                    .iter_mut()
                    .map(|agent| agent.expect_quiet("all", QUIET)),
            ),
    )
    .await;
    quiet.into_iter().collect::<Checked<Vec<()>>>()?;

    KnownDefect::judge_first(
        &[NO_VIEW_COUNTERS, REFRESH_EVALUATES],
        one_pass(&Counters::delta(&before, &after)),
    );
    Ok(())
}
