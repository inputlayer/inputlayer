//! S6, a subscription is to a deployed rule; anything else is a query.
//!
//! Captain's table row 5 (subscriptions only on deployed rules; captain call
//! D1). Gates V11 #318.
//!
//! An agent subscribes to two bodies that are not one atom of a deployed
//! rule: a join of facts with a view, and a view with a comparison. Each must
//! be refused with `invalid_request` and a message containing
//! `deploy a rule first` (expected failure: the engine accepts both until
//! V11 #318). The same bodies as `?` queries answer (required), and a
//! subscription to a deployed rule on the same connection still works
//! (required).

use inputlayer_testkit::{
    Agent, Checked, Fixture, KnownDefect, Reproduction, Size, Violation, WsClient,
};
use serde_json::json;

use crate::engine;
use crate::lifecycle::{KG, MINE};
use crate::support::{fresh, refused};

/// `.subscribe` accepts bodies that are not a deployed rule (V11 #318).
const ARBITRARY_SUBSCRIBE: KnownDefect = KnownDefect {
    plan_item: "#318",
    summary: "a .subscribe whose body is not one deployed-rule atom is accepted",
    signature: |v| matches!(v, Violation::Transport(m) if m.contains("the request succeeded")),
    reproduction: Reproduction::Deterministic,
};

/// Bodies that join or filter: a query, never a subscription.
const NOT_A_RULE: [(&str, &str); 2] = [
    ("x", r#"?stock(I, Q), eligible("o-42", I, _)"#),
    ("y", r#"?eligible("o-42", I, _), I != "i1""#),
];

#[tokio::test(flavor = "multi_thread")]
async fn s6_subscribe_to_a_query_is_refused() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let mut agent = Agent::connect(&engine, KG).await?;
    let mut auditor = WsClient::connect(&engine, KG).await?;

    for (id, body) in NOT_A_RULE {
        let reply = agent
            .client_mut()
            .try_execute(&format!(".subscribe {id} {body}"))
            .await?;
        ARBITRARY_SUBSCRIBE
            .judge(refused(reply, "invalid_request", "deploy a rule first").map(drop));
        let rows = agent.client_mut().query(body).await?.rows;
        assert!(!rows.is_empty(), "the same body as a query answers: {body}");
    }
    let joined = agent.client_mut().query(NOT_A_RULE[1].1).await?.rows;
    assert_eq!(
        joined,
        vec![json!(["o-42", "i5", "in_stock"])],
        "the filtered view as a query"
    );

    agent.subscribe("mine", MINE).await?;
    agent
        .view("mine")
        .assert_matches(&fresh(&mut auditor, MINE).await?)?;
    Ok(())
}
