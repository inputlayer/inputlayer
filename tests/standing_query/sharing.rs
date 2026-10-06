//! Shared evaluation: identical subscriptions share one view across
//! connections and users, yet each subscriber sees only what it may read.

use std::time::Duration;

use serde_json::{json, Value};

use crate::harness::{admin, rows, start_server, Client, Server, KG, TIMEOUT};

const MALLORY_PASSWORD: &str = "pw1234567890";

/// A server with `+a(1)` and a viewer `mallory` granted read access.
async fn server_with_mallory() -> Server {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    admin(
        &server,
        &format!(".user create mallory {MALLORY_PASSWORD} viewer"),
    )
    .await;
    admin(&server, &format!(".kg acl grant {KG} mallory viewer")).await;
    server
}

fn views(server: &Server) -> u64 {
    server.handler.subscription_metrics().views()
}

async fn expect_quiet(client: &mut Client, within: Duration) {
    if let Ok(frame) = tokio::time::timeout(within, client.next_push()).await {
        panic!("expected no push, got {frame}");
    }
}

fn assert_delta(push: &Value, seq: u64, inserted: &[Value]) {
    assert_eq!(push["type"], "subscription_delta", "{push}");
    assert_eq!(push["seq"], seq, "{push}");
    assert_eq!(rows(&push["inserted"]), inserted, "{push}");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_auth_scopes_share_one_evaluation_and_only_the_readable_one_gets_rows() {
    let server = server_with_mallory().await;
    let mut owner = Client::connect(&server).await;
    let mut mallory = Client::connect_as(&server, KG, "mallory", MALLORY_PASSWORD).await;
    owner.subscribe("o", "?a(X)").await;
    let snapshot = mallory.subscribe("m", "?a(X)").await;
    assert_eq!(rows(&snapshot["rows"]), [json!([1])]);
    assert_eq!(server.evaluations(), 1, "mallory joined the owner's view");
    assert_eq!(views(&server), 1);

    server.write("+a(2)").await;
    assert_delta(&owner.next_push_for("o").await, 1, &[json!([2])]);
    assert_delta(&mallory.next_push_for("m").await, 1, &[json!([2])]);
    assert_eq!(server.evaluations(), 2, "one shared refresh");

    // Revoked: the shared refresh still runs once; mallory gets no rows.
    admin(&server, &format!(".kg acl revoke {KG} mallory")).await;
    server.write("+a(3)").await;
    assert_delta(&owner.next_push_for("o").await, 2, &[json!([3])]);
    let denied = mallory.next_push_for("m").await;
    assert_eq!(denied["type"], "subscription_error", "{denied}");
    assert!(
        denied["message"]
            .as_str()
            .unwrap()
            .contains("Access denied"),
        "{denied}"
    );
    assert!(denied.get("inserted").is_none(), "{denied}");
    assert_eq!(server.evaluations(), 3);
    // Nor can mallory attach to the existing view while denied.
    let refused = mallory.execute(".subscribe again ?a(X)").await;
    assert_eq!(refused["type"], "error", "{refused}");

    // Restored: mallory's next delta follows the last result it received.
    admin(&server, &format!(".kg acl grant {KG} mallory viewer")).await;
    server.write("+a(4)").await;
    assert_delta(&owner.next_push_for("o").await, 3, &[json!([4])]);
    assert_delta(
        &mallory.next_push_for("m").await,
        2,
        &[json!([3]), json!([4])],
    );
    assert_eq!(server.evaluations(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_query_on_another_graph_is_another_view() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    server
        .handler
        .get_storage()
        .create_knowledge_graph("other")
        .unwrap();
    server
        .handler
        .execute_program(None, Some("other".to_string()), "+a(100)".to_string(), None)
        .await
        .unwrap();
    let mut here = Client::connect(&server).await;
    let mut there = Client::connect_as(&server, "other", "admin", crate::harness::PASSWORD).await;
    let mine = here.subscribe("s", "?a(X)").await;
    let theirs = there.subscribe("s", "?a(X)").await;
    assert_eq!(rows(&mine["rows"]), [json!([1])]);
    assert_eq!(rows(&theirs["rows"]), [json!([100])]);
    assert_eq!(views(&server), 2);

    let before = server.evaluations();
    server.write("+a(2)").await;
    assert_delta(&here.next_push_for("s").await, 1, &[json!([2])]);
    assert_eq!(server.evaluations(), before + 1, "only this graph's view");
    expect_quiet(&mut there, Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_sharer_is_cut_off_and_the_view_lives_on() {
    let server = server_with_mallory().await;
    let mut owner = Client::connect(&server).await;
    let mut mallory = Client::connect_as(&server, KG, "mallory", MALLORY_PASSWORD).await;
    owner.subscribe("o", "?a(X)").await;
    mallory.subscribe("m", "?a(X)").await;
    server.wait_for_active(2).await;

    server.handler.handle_user_drop("mallory").unwrap();
    let notice = tokio::time::timeout(TIMEOUT, async {
        loop {
            let frame = mallory.recv().await;
            if frame["type"] == "notice" {
                return frame;
            }
            assert_ne!(frame["type"], "subscription_delta", "{frame}");
        }
    })
    .await
    .unwrap();
    assert_eq!(notice["code"], "credential_revoked", "{notice}");
    server.wait_for_active(1).await;

    server.write("+a(2)").await;
    assert_delta(&owner.next_push_for("o").await, 1, &[json!([2])]);
    assert_eq!(views(&server), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_view_goes_with_its_last_subscriber() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    let mut first = Client::connect(&server).await;
    let mut second = Client::connect(&server).await;
    first.subscribe("s", "?a(X)").await;
    second.subscribe("s", "?a(X)").await;
    assert_eq!(views(&server), 1);

    first.execute(".unsubscribe s").await;
    second.ws.close(None).await.unwrap();
    drop(second);
    server.wait_for_active(0).await;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while views(&server) != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "view outlived its subscribers"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let before = server.evaluations();
    server.write("+a(2)").await;
    // Resubscribing reuses the name on a fresh view and sees the write.
    let snapshot = first.subscribe("s", "?a(X)").await;
    assert_eq!(rows(&snapshot["rows"]), [json!([1]), json!([2])]);
    assert_eq!(
        server.evaluations(),
        before + 1,
        "only the new view's snapshot"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_demoted_admin_stops_receiving_rows_at_once() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    admin(&server, ".user create boss pw-boss-12345 admin").await;
    let mut owner = Client::connect(&server).await;
    let mut boss = Client::connect_as(&server, KG, "boss", "pw-boss-12345").await;
    owner.subscribe("o", "?a(X)").await;
    boss.subscribe("b", "?a(X)").await;
    server.write("+a(2)").await;
    assert_delta(&owner.next_push_for("o").await, 1, &[json!([2])]);
    assert_delta(&boss.next_push_for("b").await, 1, &[json!([2])]);

    // Demoted without a grant on this graph: no rows from the shared view.
    admin(&server, ".user role boss viewer").await;
    server.write("+a(3)").await;
    assert_delta(&owner.next_push_for("o").await, 2, &[json!([3])]);
    let denied = boss.next_push_for("b").await;
    assert_eq!(denied["type"], "subscription_error", "{denied}");
}
