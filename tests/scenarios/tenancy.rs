//! S15, multi-tenant isolation and scoped keys.
//!
//! Captain's table row 1 (a deployed rule is a live view) per tenant, with the
//! readiness features scoped roles and per-relation grants (#259) and
//! credential revocation (#137). Gates V10 #317 (access checks unchanged when
//! views are maintained) and #279.
//!
//! Two knowledge graphs carry the shop pack; graph B's o-42 has other data.
//! Three keys: the admin's, a decider on graph A, and a writer on graph A
//! limited to `claim`. The decider subscribes on A. The writer's attempts to
//! write `stock`, to switch to B and to connect to B are each refused, naming
//! what is missing, and the decider's next delta still arrives. Writes on B
//! reach B's subscriber and nothing of them reaches A's (no delta, no notice).
//! Revoking the decider's key ends its connection with `credential_revoked`
//! and no further push.
//!
//! Deviations from the strategy's table, which this scenario asserts the
//! documented contract for instead: a writer key may subscribe on its own graph
//! (`writer` includes `viewer`, docs/content/docs/guides/authentication.mdx),
//! so its refused subscription is on graph B.

use inputlayer_testkit::{Agent, Checked, Fixture, Size, Violation, WsClient};
use serde_json::json;

use crate::engine;
use crate::lifecycle::MINE;
use crate::support::{committed, fresh, others_unaffected, refused, QUIET};

const KG_A: &str = "tenant_a";
const KG_B: &str = "tenant_b";

#[tokio::test(flavor = "multi_thread")]
async fn s15_tenants_and_scoped_keys_are_isolated() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    for knowledge_graph in [KG_A, KG_B] {
        let mut pack = Fixture::shop_pack(Size::Small);
        pack.knowledge_graph = knowledge_graph.to_string();
        pack.install(&engine).await?;
    }
    let decider_key = engine
        .create_api_key("decider-a", "decider", KG_A, &["claim"])
        .await?;
    let writer_key = engine
        .create_api_key("writer-a", "writer", KG_A, &["claim"])
        .await?;
    let mut decider = Agent::over(WsClient::connect_with_key(&engine, KG_A, &decider_key).await?);
    let mut tenant_b = Agent::connect(&engine, KG_B).await?;
    let mut admin_a = WsClient::connect(&engine, KG_A).await?;
    let mut admin_b = WsClient::connect(&engine, KG_B).await?;

    decider.subscribe("mine", MINE).await?;
    tenant_b.subscribe("mine", MINE).await?;
    // Same query, another graph: another view once B's data differs.
    committed(admin_b.try_execute(r#"+stock("i13", 1)"#).await?)?;
    tenant_b.next_delta("mine").await?;
    tenant_b
        .view("mine")
        .assert_matches(&fresh(&mut admin_b, MINE).await?)?;
    assert_ne!(decider.view("mine").rows, tenant_b.view("mine").rows);
    decider.expect_quiet("mine", QUIET).await?;

    let stock_before = fresh(&mut admin_a, r#"?stock("i1", Q)"#).await?;

    // The writer key: allowed to read its own graph, refused everything else.
    let mut writer = WsClient::connect_with_key(&engine, KG_A, &writer_key).await?;
    writer.query(&format!(".subscribe w {MINE}")).await?;
    let refusals = [
        (
            writer.try_execute(r#"+stock("i1", 9)"#).await?,
            "no write grant for relation 'stock'",
        ),
        (
            writer.try_execute(&format!(".kg use {KG_B}")).await?,
            "scoped to knowledge graph 'tenant_a'",
        ),
    ];
    for (reply, names) in refusals {
        refused(reply, "access_denied", names)?;
    }
    match WsClient::try_connect_with_key(&engine, KG_B, &writer_key).await? {
        Err(refusal) if refusal.code.as_deref() == Some("access_denied") => {}
        Err(refusal) => {
            return Err(Violation::Rejected(format!(
                "a key scoped to tenant_a was refused on tenant_b without access_denied: \
                 {refusal:?}"
            )))
        }
        Ok(_) => {
            return Err(Violation::Transport(
                "a key scoped to tenant_a authenticated on tenant_b".to_string(),
            ))
        }
    }
    // Nothing of the refused write applied; the decider is unaffected.
    assert_eq!(
        fresh(&mut admin_a, r#"?stock("i1", Q)"#).await?,
        stock_before
    );
    committed(admin_a.try_execute(r#"+stock("i13", 2)"#).await?)?;
    let deltas = others_unaffected(&mut [(&mut decider, "mine")]).await?;
    deltas[0].assert_rows(&[json!(["o-42", "i13", "in_stock"])], &[])?;

    // B's writes reach B only: no delta and no notice on A's connection.
    committed(admin_b.try_execute(r#"+blocked("i1")"#).await?)?;
    tenant_b.next_delta("mine").await?;
    decider.expect_quiet("mine", QUIET).await?;
    let leaked: Vec<_> = decider
        .notices()
        .iter()
        .filter(|n| n.value["knowledge_graph"] != KG_A)
        .map(|n| n.value.to_string())
        .collect();
    assert!(
        leaked.is_empty(),
        "graph B notices reached graph A: {leaked:?}"
    );

    // Revoking the decider's key ends its subscription: no further push.
    let mut admin = WsClient::connect(&engine, "default").await?;
    admin.query(".apikey revoke decider-a").await?;
    committed(admin_a.try_execute(r#"-stock("i13", 2)"#).await?)?;
    match decider.next_delta("mine").await {
        Err(Violation::Transport(message)) if message.contains("credential_revoked") => {}
        other => {
            return Err(Violation::Transport(format!(
                "a revoked key's subscription must end with credential_revoked: {other:?}"
            )))
        }
    }
    Ok(())
}
