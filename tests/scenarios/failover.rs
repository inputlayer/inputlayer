//! S13, failover mid-scenario to a warm standby.
//!
//! Captain's table row 3 (subscriptions read the rows that changed between
//! revisions) across losing the primary. Gates V18 #330 (replication with
//! view maintainers) and EN-12 part 3, #285 (promotion with a fenced writer
//! epoch).
//!
//! A primary ships its commits synchronously to a follower (PR 283's
//! builder, sync shipping from EN-12 part 2), both on the shop pack. One
//! agent subscribes to `offer("o-42", X)` on the primary, another to the
//! same view on the follower. A writer stocks i13, which offers i14, and the
//! agent claims i7 with `expect_revision`, which withdraws it; both agents
//! hear both changes. Then the primary is killed. Required today: the
//! follower holds every acknowledged write, its view equals both agents'
//! views, and it refuses writes (`store_read_only`), a claim carrying the
//! dead primary's revision included, applying nothing.
//!
//! Expected failure (#285): the follower is promoted, and the agent's next
//! claim commits on it exactly once, its retry with the same
//! `expect_revision` refused `precondition_failed`. There is no promotion
//! yet.

use std::time::Duration;

use inputlayer_testkit::agent::row_keys;
use inputlayer_testkit::{
    Agent, Checked, Engine, EngineBuilder, Expect, Fixture, KnownDefect, Replication, Reproduction,
    Size, Violation, WsClient,
};
use serde_json::{json, Value};

use crate::lifecycle::{KG, OFFERS};
use crate::support::{committed, fresh, refused, write_revision};

const TOKEN: &str = "scenario-replication-token-0123456789";
/// Longest wait for the follower to catch up or notice the lost primary.
const CONVERGE: Duration = Duration::from_secs(30);

/// No promotion exists (EN-12 part 3, #285).
const NO_PROMOTION: KnownDefect = KnownDefect {
    plan_item: "#285",
    summary: "a follower cannot be promoted to primary",
    signature: |v| matches!(v, Violation::Unavailable(m) if m.contains("promote")),
    reproduction: Reproduction::Deterministic,
};

fn replication(role: &'static str, primary_url: Option<String>) -> Replication {
    Replication {
        role,
        token: TOKEN.into(),
        primary_url,
        retain_bytes: None,
        heartbeat_ms: Some(100),
        timeout_ms: Some(800),
        mode: (role == "primary").then_some("sync"),
        sync_timeout_ms: (role == "primary").then_some(5_000),
        on_follower_loss: (role == "primary").then_some("block"),
    }
}

fn builder() -> EngineBuilder {
    crate::engine()
}

/// `GET /v1/replication/status`'s `follower` object, once readable.
async fn follower_status(engine: &Engine) -> Option<Value> {
    let response = reqwest::Client::new()
        .get(format!("{}/v1/replication/status", engine.http_url()))
        .bearer_auth(engine.api_key())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let status: Value = response.json().await.ok()?;
    Some(status["follower"].clone())
}

/// Wait until the follower's status satisfies `ok`.
async fn wait_follower(follower: &Engine, what: &str, ok: impl Fn(&Value) -> bool) -> Checked<()> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(status) = follower_status(follower).await {
            if ok(&status) {
                return Ok(());
            }
            last = Some(status);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Violation::Timeout(format!(
        "follower {what} within {CONVERGE:?}; last status {last:?}"
    )))
}

/// `POST /v1/replication/promote` (#285's interface).
async fn promote(follower: &Engine) -> Checked<()> {
    let response = reqwest::Client::new()
        .post(format!("{}/v1/replication/promote", follower.http_url()))
        .bearer_auth(follower.api_key())
        .json(&json!({}))
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| Violation::Transport(format!("promote: {e}")))?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    if status == reqwest::StatusCode::NOT_FOUND || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
    {
        return Err(Violation::Unavailable(format!(
            "POST /v1/replication/promote answered {status}: {body}"
        )));
    }
    Err(Violation::Rejected(format!(
        "promote answered {status}: {body}"
    )))
}

fn offers(items: &[&str]) -> Vec<Value> {
    items.iter().map(|item| json!(["o-42", item])).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn s13_failover_mid_scenario_keeps_every_acknowledged_change() -> Checked<()> {
    let mut primary = builder()
        .replication(replication("primary", None))
        .start()
        .await
        .expect("start primary");
    let mut follower = builder()
        .replication(replication("follower", Some(primary.http_url())))
        .start()
        .await
        .expect("start follower");
    follower.set_api_key(primary.api_key());
    Fixture::shop_pack(Size::Small).install(&primary).await?;
    wait_follower(&follower, "streaming with no lag", |f| {
        f["state"] == "streaming" && f["lag_events"] == 0
    })
    .await?;

    let mut on_primary = Agent::connect(&primary, KG).await?;
    let mut on_follower = Agent::connect(&follower, KG).await?;
    let mut writer = WsClient::connect(&primary, KG).await?;
    let pack = ["i2", "i3", "i4", "i6", "i7", "i8", "i9"];
    for agent in [&mut on_primary, &mut on_follower] {
        agent.subscribe("offers", OFFERS).await?;
        agent.view("offers").assert_matches(&offers(&pack))?;
    }

    // i13 in stock: o-42 is offered i13's successor i14.
    let stocked = committed(writer.try_execute(r#"+stock("i13", 5)"#).await?)?;
    for agent in [&mut on_primary, &mut on_follower] {
        agent
            .next_delta("offers")
            .await?
            .assert_rows(&offers(&["i14"]), &[])?;
    }
    // The agent claims i7 at the revision it last heard; i7 leaves the offers.
    let claim = on_primary
        .client_mut()
        .try_execute_expecting(
            r#"+claim("o-42", "i7", "a1")"#,
            &Expect::at(write_revision(&stocked)?).relations(&["claim"]),
        )
        .await?;
    let claim = committed(claim)?;
    for agent in [&mut on_primary, &mut on_follower] {
        agent
            .next_delta("offers")
            .await?
            .assert_rows(&[], &offers(&["i7"]))?;
    }

    primary.stop().await.expect("kill the primary");
    wait_follower(&follower, "noticing the lost primary", |f| {
        f["state"] == "connecting"
    })
    .await?;

    // Every acknowledged write is on the follower; every view agrees.
    let mut auditor = WsClient::connect(&follower, KG).await?;
    let held = fresh(&mut auditor, OFFERS).await?;
    on_primary.view("offers").assert_matches(&held)?;
    on_follower.view("offers").assert_matches(&held)?;
    let expected: Vec<&str> = ["i2", "i3", "i4", "i6", "i8", "i9", "i14"].to_vec();
    on_follower
        .view("offers")
        .assert_matches(&offers(&expected))?;

    // The follower is read-only. A claim carrying the dead primary's
    // revision is refused too: the follower counts revisions of its own, so
    // it either has not issued that revision (`precondition_failed`) or
    // refuses the write as read-only.
    let plain = auditor.try_execute(r#"+claim("o-42", "i8", "a1")"#).await?;
    refused(plain, "store_read_only", "read-only")?;
    let stale = auditor
        .try_execute_expecting(
            r#"+claim("o-42", "i8", "a1")"#,
            &Expect::at(write_revision(&claim)?).relations(&["claim"]),
        )
        .await?;
    match &stale {
        Err(refusal) if refusal.code.as_deref() == Some("precondition_failed") => {
            refused(stale, "precondition_failed", "has not been issued")?;
        }
        _ => {
            refused(stale, "store_read_only", "read-only")?;
        }
    }
    assert_eq!(
        row_keys(&fresh(&mut auditor, OFFERS).await?),
        row_keys(&held),
        "nothing applied"
    );

    // Promotion (#285): the claim then commits once.
    NO_PROMOTION.judge(promoted_claim(&mut follower).await);
    Ok(())
}

/// Promote `follower`, then claim i8 at its latest revision twice: the first
/// commits, the retry is refused `precondition_failed`.
async fn promoted_claim(follower: &mut Engine) -> Checked<()> {
    promote(follower).await?;
    let mut agent = WsClient::connect(follower, KG).await?;
    let probe = committed(agent.try_execute(r#"+stock("i8", 9)"#).await?)?;
    let at = Expect::at(write_revision(&probe)?).relations(&["claim"]);
    committed(
        agent
            .try_execute_expecting(r#"+claim("o-42", "i8", "a1")"#, &at)
            .await?,
    )?;
    let retry = agent
        .try_execute_expecting(r#"+claim("o-42", "i8", "a1")"#, &at)
        .await?;
    refused(retry, "precondition_failed", "changed at revision").map(drop)
}
