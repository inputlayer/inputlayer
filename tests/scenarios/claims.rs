//! S7, a concurrent writer causes a refused claim with the reason.
//!
//! Captain's table row 1 (a deployed rule is a live view) through the
//! readiness feature `expect_revision` (#288) on scoped decider keys (#259).
//! Gates V14 #320: read-your-writes through the view after the winning claim.
//!
//! Two agents hold the same key (o-42) and saw the same revision r in their
//! `offer` snapshots. Both claim i7 as of r at the same time: exactly one
//! commits; the other is refused with `precondition_failed` naming the
//! relation and the winner's revision, and nothing of it is applied. The
//! loser's view moves to the winner's revision, and its retry on another
//! offered item, as of that revision, commits. The winner reads its own write
//! through the view on its own and another connection.

use inputlayer_testkit::{Agent, Checked, Expect, Fixture, Size, Violation, WsClient};
use serde_json::{json, Value};

use crate::engine;
use crate::lifecycle::{KG, OFFERS};
use crate::support::{committed, fresh, refused, write_revision, write_revision_matches_delta};

#[tokio::test(flavor = "multi_thread")]
async fn s7_concurrent_claim_is_refused_with_the_reason() -> Checked<()> {
    let engine = engine().start().await.expect("start engine");
    Fixture::shop_pack(Size::Small).install(&engine).await?;
    let key = engine
        .create_api_key("agents-o42", "decider", KG, &["claim"])
        .await?;
    let mut a = Agent::over(WsClient::connect_with_key(&engine, KG, &key).await?);
    let mut b = Agent::over(WsClient::connect_with_key(&engine, KG, &key).await?);
    let mut auditor = WsClient::connect(&engine, KG).await?;

    let r = a.subscribe("offers", OFFERS).await?.revision;
    let r_b = b.subscribe("offers", OFFERS).await?.revision;
    assert_eq!(r, r_b, "both agents read the same revision");
    let i7 = json!(["o-42", "i7"]);
    assert!(a.view("offers").rows.contains(&i7.to_string()));

    // Both claim i7 as of r at once.
    let expect = Expect::at(r).relations(&["offer"]);
    let (reply_a, reply_b) = tokio::join!(
        a.client_mut()
            .try_execute_expecting(r#"+claim("o-42", "i7", "a")"#, &expect),
        b.client_mut()
            .try_execute_expecting(r#"+claim("o-42", "i7", "b")"#, &expect),
    );
    let (reply_a, reply_b) = (reply_a?, reply_b?);
    let (winner, loser, won, lost) = match (reply_a.is_ok(), reply_b.is_ok()) {
        (true, false) => (&mut a, &mut b, reply_a, reply_b),
        (false, true) => (&mut b, &mut a, reply_b, reply_a),
        _ => {
            return Err(Violation::Transport(format!(
                "exactly one claim must commit: {reply_a:?} / {reply_b:?}"
            )))
        }
    };
    let won = committed(won)?;
    let won_at = write_revision(&won)?;
    assert_eq!(
        won.statements,
        vec![json!({"index": 0, "kind": "insert", "inserted": 1, "deleted": 0})],
        "the winner's statement counts report the landed token"
    );
    let refusal = refused(lost, "precondition_failed", "relation 'claim'")?;
    assert!(
        refusal.message.contains(&format!("revision {won_at}")),
        "the refusal names the newer revision {won_at}: {}",
        refusal.message
    );

    // Read-your-writes: the winner's own read, and another connection's.
    let claimed = |rows: &[Value]| rows.contains(&i7);
    let own = winner.client_mut().query(OFFERS).await?;
    assert!(
        !claimed(&own.rows),
        "winner reads its claim back: {:?}",
        own.rows
    );
    assert!(!claimed(&fresh(&mut auditor, OFFERS).await?));

    // Both views move to the winner's revision; nothing of the loser's claim applied.
    for agent in [&mut *winner, &mut *loser] {
        let delta = agent.next_delta("offers").await?;
        delta.assert_rows(&[], std::slice::from_ref(&i7))?;
        write_revision_matches_delta(&won, &delta)?;
    }
    let holders = fresh(&mut auditor, r#"?claim("o-42", "i7", Agent)"#).await?;
    assert_eq!(holders.len(), 1, "one claim on i7: {holders:?}");

    // The loser re-reads at the new revision and retries on another offer.
    let view = loser.view("offers");
    let seen = view.revision;
    let retry_item = view
        .rows
        .iter()
        .find_map(|row| {
            serde_json::from_str::<Value>(row).ok()?[1]
                .as_str()
                .map(String::from)
        })
        .expect("o-42 has more offers");
    let retry = committed(
        loser
            .client_mut()
            .try_execute_expecting(
                &format!(r#"+claim("o-42", "{retry_item}", "loser")"#),
                &Expect::at(seen).relations(&["offer"]),
            )
            .await?,
    )?;
    let delta = loser.next_delta("offers").await?;
    delta.assert_rows(&[], &[json!(["o-42", retry_item])])?;
    write_revision_matches_delta(&retry, &delta)?;
    loser
        .view("offers")
        .assert_matches(&fresh(&mut auditor, OFFERS).await?)?;
    Ok(())
}
