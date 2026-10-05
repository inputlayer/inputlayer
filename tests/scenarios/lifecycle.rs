//! S1, agent lifecycle on a deployed rule.
//!
//! Captain's table row 1 (a deployed rule is a live view) and row 3
//! (subscriptions read the rows that changed between revisions). Gates V10
//! #317, V11 #318 and V14 #320: their maintained-mode work must keep every
//! assertion here green.
//!
//! A decider-key agent subscribes to its key's `eligible` rows and `offer`
//! rows; a writer key scoped to `stock` stocks o-42's out-of-stock item i13
//! and another order's out-of-stock item in one write; the agent gets one
//! delta for o-42 only (the other order's agent gets the other row), at the
//! writer's revision, and i13's successor i14 joins its offers; it claims the
//! offered item i7 with `expect_revision` set to that delta's revision, and
//! i7 leaves its `offer` view through the negation `!claim(..)`, at the
//! claim's revision. Its views equal fresh queries throughout.

use inputlayer_testkit::{
    Agent, Checked, Engine, Expect, Fixture, QueryResult, Size, Violation, WsClient,
};
use serde_json::json;

use crate::engine;
use crate::support::{
    committed, fresh, others_unaffected, write_revision, write_revision_matches_delta, QUIET,
};

pub const KG: &str = "shop";
/// The agent's key rows.
pub const MINE: &str = r#"?eligible("o-42", Item, Why)"#;
pub const OFFERS: &str = r#"?offer("o-42", X)"#;

/// S1's state just after the agent's claim committed.
pub struct Lifecycle {
    pub engine: Engine,
    pub agent: Agent,
    pub writer: WsClient,
    pub auditor: WsClient,
    pub decider_key: String,
    /// The committed claim's reply.
    pub claim: QueryResult,
}

/// Run S1 up to and including the agent's claim; the claim's `offer` delta
/// is not read yet.
pub async fn up_to_claim() -> Checked<Lifecycle> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let decider_key = engine
        .create_api_key("agent-a1", "decider", KG, &["claim"])
        .await?;
    let writer_key = engine
        .create_api_key("stock-feed", "writer", KG, &["stock"])
        .await?;
    let mut agent = Agent::over(WsClient::connect_with_key(&engine, KG, &decider_key).await?);
    let mut other = Agent::over(WsClient::connect_with_key(&engine, KG, &decider_key).await?);
    let mut writer = WsClient::connect_with_key(&engine, KG, &writer_key).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;
    // Another agent's key: an order holding an out-of-stock item.
    let other_key = auditor
        .query(r#"?order_item(O, I), stock(I, 0), O != "o-42", I != "i13""#)
        .await?
        .rows
        .first()
        .cloned()
        .expect("the pack has another order with an out-of-stock item");
    let (other_order, other_item) = (&other_key[0], &other_key[1]);
    let theirs = format!("?eligible({other_order}, Item, Why)");

    // Snapshot rows = fresh query.
    agent.subscribe("mine", MINE).await?;
    agent.subscribe("offers", OFFERS).await?;
    other.subscribe("theirs", &theirs).await?;
    agent
        .view("mine")
        .assert_matches(&fresh(&mut auditor, MINE).await?)?;
    agent
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;
    assert_eq!(
        agent.view("mine").rows.len(),
        2,
        "o-42 starts with i1 and i5 eligible"
    );
    assert_eq!(
        agent.view("offers").rows.len(),
        7,
        "o-42 starts with i2-i4 and i6-i9 offered"
    );

    // One write touching two keys: each agent gets its own row only.
    let write = committed(
        writer
            .try_execute(&format!(r#"+stock[("i13", 4), ({other_item}, 2)]"#))
            .await?,
    )?;
    let mut agents = [(&mut agent, "mine"), (&mut other, "theirs")];
    let deltas = others_unaffected(&mut agents).await?;
    let (mine, theirs) = (&deltas[0], &deltas[1]);
    mine.assert_rows(&[json!(["o-42", "i13", "in_stock"])], &[])?;
    theirs.assert_rows(&[json!([other_order, other_item, "in_stock"])], &[])?;
    write_revision_matches_delta(&write, mine)?;
    write_revision_matches_delta(&write, theirs)?;
    let stock_delta = mine.clone();
    agent.expect_quiet("mine", QUIET).await?;
    // i13 links to i14: o-42 is offered i14, in the same revision.
    let offers = agent.next_delta("offers").await?;
    offers.assert_rows(&[json!(["o-42", "i14"])], &[])?;
    write_revision_matches_delta(&write, &offers)?;

    // Claim an offered item as of the revision the agent last saw.
    let expect = Expect::at(stock_delta.revision).relations(&["offer"]);
    let claim = committed(
        agent
            .client_mut()
            .try_execute_expecting(r#"+claim("o-42", "i7", "a1")"#, &expect)
            .await?,
    )?;
    let revision = write_revision(&claim)?;
    if revision <= stock_delta.revision {
        return Err(Violation::StaleRevision {
            subscription: "claim reply".to_string(),
            previous: stock_delta.revision,
            got: revision,
        });
    }
    assert_eq!(
        claim.statements,
        vec![json!({"index": 0, "kind": "insert", "inserted": 1, "deleted": 0})],
        "the claim's statement counts report the landed token"
    );
    other.disconnect().await;
    Ok(Lifecycle {
        engine,
        agent,
        writer,
        auditor,
        decider_key,
        claim,
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn s1_agent_lifecycle_on_a_deployed_rule() -> Checked<()> {
    // Bind the engine: dropping it kills the process.
    let Lifecycle {
        engine: _engine,
        mut agent,
        mut auditor,
        claim,
        ..
    } = up_to_claim().await?;

    // The claimed item leaves the offers through `!claim(..)`, at the claim's revision.
    let delta = agent.next_delta("offers").await?;
    delta.assert_rows(&[], &[json!(["o-42", "i7"])])?;
    write_revision_matches_delta(&claim, &delta)?;
    agent.expect_quiet("mine", QUIET).await?;

    agent
        .view("mine")
        .assert_matches(&fresh(&mut auditor, MINE).await?)?;
    agent
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;
    Ok(())
}
