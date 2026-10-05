//! Standing queries (`.subscribe`) over a real `/ws` connection.
//!
//! Writes go straight through the `Handler`; the subscriber observes them via
//! the notification broadcast, like any other client's commits.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod harness;
mod parameterized;
mod sharing;

use std::collections::BTreeSet;
use std::sync::Arc;

use harness::{admin, rows, start_server, start_server_with, Client, Server, KG};
use serde_json::{json, Value};

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_returns_snapshot_and_insert_produces_delta() {
    let server = start_server(64).await;
    server.write("+edge(1, 2)\n+edge(2, 3)").await;
    server
        .write("+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)")
        .await;
    let mut client = Client::connect(&server).await;

    let snapshot = client.subscribe("r1", "?reach(1, X)").await;
    assert_eq!(rows(&snapshot["rows"]), vec![json!([1, 2]), json!([1, 3])]);
    assert_eq!(snapshot["columns"].as_array().unwrap().len(), 2);

    server.write("+edge(3, 4)").await;
    let delta = client.next_push_for("r1").await;
    assert_eq!(delta["type"], "subscription_delta");
    assert_eq!(delta["knowledge_graph"], KG);
    assert_eq!(delta["seq"], 1);
    assert_eq!(delta["columns"], snapshot["columns"]);
    assert_eq!(rows(&delta["inserted"]), vec![json!([1, 4])]);
    assert!(rows(&delta["retracted"]).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_and_deltas_name_strictly_increasing_revisions() {
    let server = start_server(64).await;
    server.write("+n(1)").await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.subscribe("n", "?n(X)").await;
    let mut revision = snapshot["subscribed"]["revision"].as_u64().unwrap();
    assert!(revision > 0, "{snapshot}");

    for (i, write) in ["+n(2)", "-n(1)", "+other(1)\n+n(3)"].iter().enumerate() {
        server.write(write).await;
        let delta = client.next_push_for("n").await;
        assert_eq!(delta["seq"], i + 1, "{delta}");
        let next = delta["revision"].as_u64().unwrap();
        assert!(next > revision, "revision {next} after {revision}: {delta}");
        revision = next;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_reply_names_the_revision_its_push_carries() {
    let server = start_server(64).await;
    let mut subscriber = Client::connect(&server).await;
    let snapshot = subscriber.subscribe("n", "?n(X)").await;
    let subscribed_at = snapshot["subscribed"]["revision"].as_u64().unwrap();
    let mut writer = Client::connect(&server).await;

    let reply = writer.execute("+n(1)\n+other(1)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    let committed = reply["revision"].as_u64().expect("write reply revision");
    assert!(committed > subscribed_at, "{reply}");
    let delta = subscriber.next_push_for("n").await;
    assert_eq!(delta["revision"].as_u64(), Some(committed), "{delta}");

    // A write that changes nothing names the revision that already holds it.
    let reply = writer.execute("+n(1)").await;
    assert_eq!(reply["revision"].as_u64(), Some(committed), "{reply}");

    // A program that writes and queries names its commit; a query alone,
    // or a write that failed, names none.
    let reply = writer.execute("+n(2)\n?n(X)").await;
    let committed = reply["revision"]
        .as_u64()
        .expect("write and query revision");
    assert_eq!(rows(&reply["rows"]).len(), 2, "{reply}");
    let delta = subscriber.next_push_for("n").await;
    assert_eq!(delta["revision"].as_u64(), Some(committed), "{delta}");
    let reply = writer.execute("?n(X)").await;
    assert!(reply.get("revision").is_none(), "{reply}");
    let reply = writer.execute("+n(3)\n+n(\"three\", 4)").await;
    assert!(!reply["errors"].as_array().unwrap().is_empty(), "{reply}");
    assert!(reply.get("revision").is_none(), "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_relation_commands_name_the_revision_their_push_carries() {
    let server = start_server(64).await;
    server.write("+p_n(1)\n+gone(1)").await;
    let mut subscriber = Client::connect(&server).await;
    subscriber.subscribe("n", "?p_n(X)").await;
    let mut writer = Client::connect(&server).await;

    let reply = writer.execute(".clear prefix p_").await;
    assert!(reply["errors"].as_array().unwrap().is_empty(), "{reply}");
    let cleared = reply["revision"].as_u64().expect("clear revision");
    let delta = subscriber.next_push_for("n").await;
    assert_eq!(rows(&delta["retracted"]), vec![json!([1])], "{delta}");
    assert_eq!(delta["revision"].as_u64(), Some(cleared), "{delta}");

    subscriber.subscribe("g", "?gone(X)").await;
    // A session schema is a write, so it cannot share a program with
    // `.rel drop`: nothing runs and nothing is named.
    let reply = writer.execute(".rel drop gone\ns(a: int)").await;
    assert_eq!(reply["errors"][0]["index"], 0, "{reply}");
    assert!(reply.get("revision").is_none(), "{reply}");
    let reply = writer.execute("?gone(X)").await;
    assert_eq!(rows(&reply["rows"]), vec![json!([1])], "{reply}");
    let reply = writer.execute(".rel drop gone").await;
    assert!(reply["errors"].as_array().unwrap().is_empty(), "{reply}");
    let dropped = reply["revision"].as_u64().expect("drop revision");
    assert!(dropped > cleared, "{reply}");
    let delta = subscriber.next_push_for("g").await;
    assert_eq!(delta["revision"].as_u64(), Some(dropped), "{delta}");

    // Session schemas and inspection change nothing persistent.
    let reply = writer.execute("s(a: int)").await;
    assert!(reply["errors"].as_array().unwrap().is_empty(), "{reply}");
    assert!(reply.get("revision").is_none(), "{reply}");
    let reply = writer.execute(".rel").await;
    assert!(reply.get("revision").is_none(), "{reply}");

    let reply = writer.execute(".kg create fresh").await;
    let created = reply["revision"].as_u64().expect("create revision");
    assert!(created > dropped, "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delete_produces_retraction() {
    let server = start_server(64).await;
    server.write("+likes(1, 10)\n+likes(1, 11)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("l", "?likes(1, X)").await;

    server.write("-likes(1, 10)").await;
    let delta = client.next_push_for("l").await;
    assert_eq!(delta["seq"], 1);
    assert!(rows(&delta["inserted"]).is_empty());
    assert_eq!(rows(&delta["retracted"]), vec![json!([1, 10])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replacement_program_is_one_atomic_delta() {
    let server = start_server(64).await;
    server.write("+status(1, \"old\")").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("s", "?status(K, V)").await;

    // Retract-and-assert in one program: the subscriber sees the swap as one
    // delta, never the retraction alone.
    server
        .write("-status(1, \"old\")\n+status(1, \"new\")")
        .await;
    let delta = client.next_push_for("s").await;
    assert_eq!(delta["seq"], 1);
    assert_eq!(rows(&delta["retracted"]), vec![json!([1, "old"])]);
    assert_eq!(rows(&delta["inserted"]), vec![json!([1, "new"])]);

    // A program that fails half way applies nothing and pushes nothing: the
    // next delta is seq 2 and carries only the later write.
    server
        .write("-status(1, \"new\")\n+status(1, \"bad\", \"arity\")")
        .await;
    server.write("+status(2, \"two\")").await;
    let delta = client.next_push_for("s").await;
    assert_eq!(delta["seq"], 2);
    assert!(rows(&delta["retracted"]).is_empty(), "{delta}");
    assert_eq!(rows(&delta["inserted"]), vec![json!([2, "two"])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_path_derivation_retracts_only_when_last_path_goes() {
    let server = start_server(64).await;
    server.write("+a(1)\n+b(1)").await;
    server.write("+ok(X) <- a(X)\n+ok(X) <- b(X)").await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.subscribe("ok", "?ok(X)").await;
    assert_eq!(rows(&snapshot["rows"]), vec![json!([1])]);

    // First path gone: still derivable, so no delta. The next delta must be
    // seq 1, proving nothing was pushed for this commit.
    server.write("-a(1)").await;
    server.write("-b(1)").await;
    let delta = client.next_push_for("ok").await;
    assert_eq!(delta["seq"], 1);
    assert_eq!(rows(&delta["retracted"]), vec![json!([1])]);
    assert!(rows(&delta["inserted"]).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rule_added_changes_results_and_dependencies() {
    let server = start_server(64).await;
    server.write("+a(1)\n+b(2)").await;
    server.write("+ok(X) <- a(X)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("ok", "?ok(X)").await;

    server.write("+ok(X) <- b(X)").await;
    let delta = client.next_push_for("ok").await;
    assert_eq!(delta["seq"], 1);
    assert_eq!(rows(&delta["inserted"]), vec![json!([2])]);

    // `b` is now a dependency.
    server.write("+b(3)").await;
    let delta = client.next_push_for("ok").await;
    assert_eq!(delta["seq"], 2);
    assert_eq!(rows(&delta["inserted"]), vec![json!([3])]);

    // Dropping the rule retracts what it derived.
    server.write(".rule drop ok").await;
    let delta = client.next_push_for("ok").await;
    assert_eq!(delta["seq"], 3);
    assert_eq!(
        rows(&delta["retracted"]),
        vec![json!([1]), json!([2]), json!([3])]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unrelated_write_triggers_no_evaluation() {
    let server = start_server(64).await;
    server.write("+a(1)\n+noise(1)").await;
    server.write("+ok(X) <- a(X)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("ok", "?ok(X)").await;
    let after_subscribe = server.evaluations();

    server.write("+noise(2)").await;
    server.write("+noise(3)").await;
    server.write("+a(2)").await;
    let delta = client.next_push_for("ok").await;
    assert_eq!(rows(&delta["inserted"]), vec![json!([2])]);
    // Notifications are handled in commit order, and evaluations are counted
    // when dispatched, so by the time this delta arrives the noise writes
    // would already have been counted.
    assert_eq!(server.evaluations(), after_subscribe + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unsubscribe_stops_deltas_and_evaluations() {
    let server = start_server(64).await;
    server.write("+a(1)\n+b(1)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("gone", "?a(X)").await;
    client.subscribe("fence", "?b(X)").await;
    server.wait_for_active(2).await;

    let reply = client.execute(".unsubscribe gone").await;
    assert_eq!(reply["type"], "result", "{reply}");
    let reply = client.execute(".unsubscribe gone").await;
    assert_eq!(reply["type"], "error", "{reply}");
    server.wait_for_active(1).await;
    let before = server.evaluations();

    server.write("+a(2)").await;
    server.write("+b(2)").await;
    let delta = client.next_push_for("fence").await;
    assert_eq!(rows(&delta["inserted"]), vec![json!([2])]);
    assert_eq!(server.evaluations(), before + 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_disconnect_and_kg_switch_remove_subscriptions() {
    let server = start_server(64).await;
    server.write("+a(1)").await;

    let mut client = Client::connect(&server).await;
    client.subscribe("s1", "?a(X)").await;
    client.subscribe("s2", "?a(X)").await;
    server.wait_for_active(2).await;
    client.ws.close(None).await.unwrap();
    drop(client);
    server.wait_for_active(0).await;

    let mut client = Client::connect(&server).await;
    client.subscribe("s1", "?a(X)").await;
    server.wait_for_active(1).await;
    let reply = client.execute(".kg create other").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(0).await;
    // The id is free again in the new KG.
    client.subscribe("s1", "?a(X)").await;
    server.wait_for_active(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_limit_duplicates_and_invalid_queries_are_errors() {
    let server = start_server(2).await;
    server.write("+a(1)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("s1", "?a(X)").await;

    let dup = client.execute(".subscribe s1 ?a(X)").await;
    assert_eq!(dup["type"], "error");
    assert!(dup["message"].as_str().unwrap().contains("already exists"));

    client.subscribe("s2", "?a(X)").await;
    let over = client.execute(".subscribe s3 ?a(X)").await;
    assert_eq!(over["type"], "error");
    assert!(
        over["message"].as_str().unwrap().contains("limit"),
        "{over}"
    );

    client.execute(".unsubscribe s2").await;
    let bad = client.execute(".subscribe s3 ?a(X) limit 1").await;
    assert_eq!(bad["type"], "error", "{bad}");
    let bad = client.execute(".subscribe s3 a(X)").await;
    assert_eq!(bad["type"], "error", "{bad}");
    server.wait_for_active(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_access_is_checked_on_subscribe_and_every_evaluation() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    admin(&server, ".user create mallory pw12345678 viewer").await;
    admin(&server, &format!(".kg acl grant {KG} mallory viewer")).await;

    let mut client = Client::connect_as(&server, KG, "mallory", "pw12345678").await;
    let snapshot = client.subscribe("s", "?a(X)").await;
    assert_eq!(rows(&snapshot["rows"]), vec![json!([1])]);

    // Revoked: re-evaluation fails, the error is pushed, the subscription stays.
    admin(&server, &format!(".kg acl revoke {KG} mallory")).await;
    server.write("+a(2)").await;
    let push = client.next_push_for("s").await;
    assert_eq!(push["type"], "subscription_error", "{push}");
    assert!(
        push["message"].as_str().unwrap().contains("denied"),
        "{push}"
    );
    assert_eq!(server.handler.subscription_metrics().active(), 1);
    let denied = client.execute(".subscribe t ?a(X)").await;
    assert_eq!(denied["type"], "error", "{denied}");

    // Granted again: the next evaluation resumes from the last good result.
    admin(&server, &format!(".kg acl grant {KG} mallory viewer")).await;
    server.write("+a(3)").await;
    let delta = client.next_push_for("s").await;
    assert_eq!(delta["type"], "subscription_delta", "{delta}");
    assert_eq!(delta["seq"], 1);
    assert_eq!(rows(&delta["inserted"]), vec![json!([2]), json!([3])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_burst_of_writes_coalesces_to_correct_final_state() {
    const N: i64 = 60;
    let server = start_server(64).await;
    server.write("+seed(0)").await;
    server.write("+ok(X) <- n(X)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("burst", "?ok(X)").await;
    let before = server.evaluations();

    let writer = {
        let handler = Arc::clone(&server.handler);
        tokio::spawn(async move {
            for i in 0..N {
                handler
                    .execute_program(None, Some(KG.to_string()), format!("+n({i})"), None)
                    .await
                    .unwrap();
            }
            // Retract a few so the final state includes retractions.
            for i in 0..5 {
                handler
                    .execute_program(None, Some(KG.to_string()), format!("-n({i})"), None)
                    .await
                    .unwrap();
            }
        })
    };
    writer.await.unwrap();

    let expected: BTreeSet<i64> = (5..N).collect();
    let mut state: BTreeSet<i64> = BTreeSet::new();
    let mut last_seq = 0;
    while state != expected {
        let delta = client.next_push_for("burst").await;
        assert_eq!(delta["type"], "subscription_delta", "{delta}");
        let seq = delta["seq"].as_u64().unwrap();
        assert_eq!(seq, last_seq + 1, "seq must increase by one");
        last_seq = seq;
        for row in rows(&delta["inserted"]) {
            assert!(state.insert(row[0].as_i64().unwrap()), "duplicate insert");
        }
        for row in rows(&delta["retracted"]) {
            assert!(
                state.remove(&row[0].as_i64().unwrap()),
                "retract of absent row"
            );
        }
    }
    let evaluations = server.evaluations() - before;
    assert!(
        evaluations <= (N + 5) as u64,
        "at most one evaluation per commit, got {evaluations}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mutual_recursion_subscription_tracks_joint_fixpoint() {
    let server = start_server(64).await;
    server
        .write("+succ[(0, 1), (1, 2), (2, 3)]\n+zero[(0)]")
        .await;
    server
        .write(
            "+is_even(N) <- zero(N)\n+is_even(N) <- succ(M, N), is_odd(M)\n+is_odd(N) <- succ(M, N), is_even(M)",
        )
        .await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.subscribe("ev", "?is_even(X)").await;
    assert_eq!(rows(&snapshot["rows"]), vec![json!([0]), json!([2])]);

    // Extending the chain derives across both relations
    server.write("+succ[(3, 4), (4, 5)]").await;
    let delta = client.next_push_for("ev").await;
    assert_eq!(rows(&delta["inserted"]), vec![json!([4])]);
    assert!(rows(&delta["retracted"]).is_empty());

    server.write("-succ(1, 2)").await;
    let delta = client.next_push_for("ev").await;
    assert_eq!(rows(&delta["retracted"]), vec![json!([2]), json!([4])]);
}

async fn start_capped_server(max_result_rows: usize) -> Server {
    start_server_with(64, |config| {
        config.storage.performance.max_result_rows = max_result_rows;
    })
    .await
}

fn assert_cap_error(message: &Value) {
    assert!(
        message["message"]
            .as_str()
            .unwrap()
            .contains("max_result_rows (3)"),
        "{message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_over_result_cap_fails_without_registering() {
    let server = start_capped_server(3).await;
    let facts: Vec<String> = (1..=10).map(|i| format!("({i})")).collect();
    server.write(&format!("+a[{}]", facts.join(", "))).await;
    server.write("+b[(1), (2), (3)]").await;
    let mut client = Client::connect(&server).await;

    let reply = client.execute(".subscribe s ?a(X)").await;
    assert_eq!(reply["type"], "error", "{reply}");
    assert_cap_error(&reply);
    assert_eq!(server.handler.subscription_metrics().active(), 0);

    // Exactly at the cap is complete; the failed id is free.
    let snapshot = client.subscribe("s", "?b(X)").await;
    assert_eq!(snapshot["truncated"], false);
    assert_eq!(rows(&snapshot["rows"]).len(), 3);
    server.wait_for_active(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_crossing_result_cap_keeps_last_complete_result() {
    let server = start_capped_server(3).await;
    server.write("+a[(1), (2)]").await;
    let mut client = Client::connect(&server).await;
    client.subscribe("s", "?a(X)").await;

    server.write("+a(3)").await;
    let delta = client.next_push_for("s").await;
    assert_eq!(delta["seq"], 1, "{delta}");
    assert_eq!(rows(&delta["inserted"]), vec![json!([3])]);

    // Over the cap: errors, never a delta computed from the capped rows.
    for program in ["+a(4)", "+a(5)", "-a(1)"] {
        server.write(program).await;
        let push = client.next_push_for("s").await;
        assert_eq!(push["type"], "subscription_error", "{program}: {push}");
        assert_cap_error(&push);
    }
    assert_eq!(server.handler.subscription_metrics().active(), 1);

    // Back under the cap: one delta from the last complete result {1, 2, 3}.
    server.write("-a(5)").await;
    let delta = client.next_push_for("s").await;
    assert_eq!(delta["type"], "subscription_delta", "{delta}");
    assert_eq!(delta["seq"], 2);
    assert_eq!(rows(&delta["inserted"]), vec![json!([4])]);
    assert_eq!(rows(&delta["retracted"]), vec![json!([1])]);
}
