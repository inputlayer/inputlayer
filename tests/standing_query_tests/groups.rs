//! Subscription groups (the `subscribe` frame): several queries kept current
//! together, every push leaving all members exact at one revision.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::harness::{admin, rows, start_server, start_server_with, Client, Server, KG};

const ORDERS: [(&str, &str); 3] = [("a", "?a(X)"), ("b", "?b(X)"), ("c", "?c(X)")];

fn names(push: &Value) -> Vec<String> {
    push["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap().to_string())
        .collect()
}

async fn expect_quiet(client: &mut Client, within: Duration) {
    if let Ok(frame) = tokio::time::timeout(within, client.next_push()).await {
        panic!("expected no push, got {frame}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_subscribe_returns_one_snapshot_of_every_member() {
    let server = start_server(64).await;
    server.write("+a[(1,), (2,)]\n+c(9)").await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.subscribe_group("w", &ORDERS).await;

    assert_eq!(snapshot["knowledge_graph"], KG);
    let revision = snapshot["revision"].as_u64().unwrap();
    assert!(revision > 0);
    assert_eq!(snapshot["subscribed"]["subscription"], "w");
    assert_eq!(snapshot["subscribed"]["revision"], revision);
    assert!(snapshot["subscribed"]["generation"].as_u64().unwrap() > 0);
    let results = snapshot["results"].as_array().unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0],
        json!({"name": "a", "columns": ["X"], "rows": [[1], [2]], "total_count": 2,
            "truncated": false})
    );
    assert_eq!(results[1]["name"], "b");
    assert!(rows(&results[1]["rows"]).is_empty());
    assert_eq!(rows(&results[2]["rows"]), [json!([9])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn one_commit_touching_several_members_is_one_group_delta() {
    let server = start_server(64).await;
    server.write("+a(1)\n+b(1)").await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.subscribe_group("w", &ORDERS).await;
    let generation = snapshot["subscribed"]["generation"].clone();

    server.write("+a(2)\n-b(1)\n+c(3)").await;
    let push = client.next_push_for("w").await;
    assert_eq!(push["type"], "subscription_group_delta", "{push}");
    assert_eq!(push["generation"], generation);
    assert_eq!(push["knowledge_graph"], KG);
    assert_eq!(push["seq"], 1);
    assert!(push["revision"].as_u64() > snapshot["revision"].as_u64());
    assert_eq!(
        push["members"],
        json!([
            {"name": "a", "unchanged": false, "columns": ["X"], "inserted": [[2]], "retracted": []},
            {"name": "b", "unchanged": false, "columns": ["X"], "inserted": [], "retracted": [[1]]},
            {"name": "c", "unchanged": false, "columns": ["X"], "inserted": [[3]], "retracted": []},
        ])
    );

    // One member changes: every member is listed, the others marked unchanged.
    server.write("+b(5)").await;
    let push = client.next_push_for("w").await;
    assert_eq!(push["seq"], 2);
    assert_eq!(names(&push), ["a", "b", "c"]);
    let unchanged: Vec<bool> = push["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["unchanged"].as_bool().unwrap())
        .collect();
    assert_eq!(unchanged, [true, false, true]);
    assert_eq!(rows(&push["members"][1]["inserted"]), [json!([5])]);
    assert!(rows(&push["members"][0]["inserted"]).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn commits_that_change_no_member_push_nothing() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    let mut client = Client::connect(&server).await;
    client.subscribe_group("w", &ORDERS).await;

    server.write("+unrelated(1)").await;
    server.write("+a(1)").await; // already there: the result does not change
    expect_quiet(&mut client, Duration::from_millis(300)).await;
    server.write("+c(1)").await;
    let push = client.next_push_for("w").await;
    assert_eq!(push["seq"], 1, "no push in between: {push}");
}

#[tokio::test(flavor = "multi_thread")]
async fn rules_derive_members_and_rule_changes_refresh_them() {
    let server = start_server(64).await;
    server.write("+edge(1, 2)\n+edge(2, 3)").await;
    server
        .write("+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)")
        .await;
    let mut client = Client::connect(&server).await;
    let snapshot = client
        .subscribe_group("w", &[("from1", "?reach(1, Y)"), ("edges", "?edge(X, Y)")])
        .await;
    assert_eq!(
        rows(&snapshot["results"][0]["rows"]),
        [json!([1, 2]), json!([1, 3])]
    );

    server.write("+edge(3, 4)").await;
    let push = client.next_push_for("w").await;
    assert_eq!(rows(&push["members"][0]["inserted"]), [json!([1, 4])]);
    assert_eq!(rows(&push["members"][1]["inserted"]), [json!([3, 4])]);

    // A new rule for `reach` changes one member only.
    server
        .write("+reach(X, Y) <- shortcut(X, Y)\n+shortcut(1, 9)")
        .await;
    let push = client.next_push_for("w").await;
    assert_eq!(rows(&push["members"][0]["inserted"]), [json!([1, 9])]);
    assert_eq!(push["members"][1]["unchanged"], true);
}

/// Writers commit to `a` and `b` together (inserts and deletes), and to `c`
/// alone, concurrently. After every push the subscriber's `a` and `b` must be
/// equal: members published at different revisions would show one without
/// the other. Pushes are gapless, list every member, and name strictly
/// increasing revisions; once the writers stop, the assembled results equal a
/// fresh read.
#[tokio::test(flavor = "multi_thread")]
async fn group_deltas_stay_coherent_under_concurrent_writers() {
    const ROUNDS: i64 = 80;
    let server = start_server(64).await;
    server.write("+a(0)\n+b(0)").await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.subscribe_group("w", &ORDERS).await;
    let mut state: Vec<BTreeSet<String>> = (0..3)
        .map(|i| {
            rows(&snapshot["results"][i]["rows"])
                .iter()
                .map(Value::to_string)
                .collect()
        })
        .collect();
    assert_eq!(state[0], state[1]);

    let writers = spawn_writers(&server, ROUNDS);
    let (mut seq, mut revision) = (0, snapshot["revision"].as_u64().unwrap());
    let mut pushes = 0;
    let mut finished = false;
    loop {
        if finished && converged(&mut client, &state).await {
            break;
        }
        let push = tokio::select! {
            push = client.next_push_for("w") => push,
            () = writers_done(&writers), if !finished => {
                finished = true;
                continue;
            }
        };
        assert_eq!(push["type"], "subscription_group_delta", "{push}");
        seq += 1;
        assert_eq!(push["seq"], seq, "gapless: {push}");
        let next = push["revision"].as_u64().unwrap();
        assert!(next > revision, "revision {next} after {revision}");
        revision = next;
        assert_eq!(names(&push), ["a", "b", "c"]);
        for (i, member) in push["members"].as_array().unwrap().iter().enumerate() {
            let inserted = rows(&member["inserted"]);
            let retracted = rows(&member["retracted"]);
            assert_eq!(
                member["unchanged"].as_bool().unwrap(),
                inserted.is_empty() && retracted.is_empty(),
                "{member}"
            );
            for row in retracted {
                assert!(state[i].remove(&row.to_string()), "retracted absent {row}");
            }
            for row in inserted {
                assert!(state[i].insert(row.to_string()), "inserted present {row}");
            }
        }
        assert_eq!(
            state[0], state[1],
            "a and b diverged at revision {revision}: one commit seen in part"
        );
        pushes += 1;
    }
    assert!(pushes > 1, "the writers produced only {pushes} pushes");
}

/// Writer tasks: `a` and `b` together (a sliding window, so deletes too), and
/// `c` alone.
fn spawn_writers(server: &Server, rounds: i64) -> Vec<tokio::task::JoinHandle<()>> {
    let together = Arc::clone(&server.handler);
    let alone = Arc::clone(&server.handler);
    let write = |handler: Arc<inputlayer::protocol::Handler>, program: String| async move {
        handler
            .execute_program(None, Some(KG.to_string()), program.clone(), None)
            .await
            .unwrap_or_else(|e| panic!("{program:?} failed: {e}"));
    };
    vec![
        tokio::spawn(async move {
            for i in 1..=rounds {
                let mut program = format!("+a({i})\n+b({i})");
                if i % 3 == 0 {
                    program.push_str(&format!("\n-a({})\n-b({})", i - 2, i - 2));
                }
                write(Arc::clone(&together), program).await;
            }
        }),
        tokio::spawn(async move {
            for i in 1..=rounds {
                write(Arc::clone(&alone), format!("+c({i})")).await;
                if i % 4 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }),
    ]
}

async fn writers_done(writers: &[tokio::task::JoinHandle<()>]) {
    while writers.iter().any(|w| !w.is_finished()) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Whether the assembled `state` equals a fresh read of the members.
async fn converged(client: &mut Client, state: &[BTreeSet<String>]) -> bool {
    let read = client.read(&ORDERS).await;
    assert_eq!(read["type"], "snapshot", "{read}");
    (0..3).all(|i| {
        let fresh: BTreeSet<String> = rows(&read["results"][i]["rows"])
            .iter()
            .map(Value::to_string)
            .collect();
        fresh == state[i]
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_over_the_result_cap_fails_the_group_as_a_whole() {
    let server =
        start_server_with(64, |config| config.storage.performance.max_result_rows = 2).await;
    server.write("+a(1)\n+b(1)").await;
    let mut client = Client::connect(&server).await;

    server.write("+big[(1,), (2,), (3,)]").await;
    let refused = client
        .try_subscribe_group("w", &[("a", "?a(X)"), ("big", "?big(X)")])
        .await;
    assert_eq!(refused["type"], "error", "{refused}");
    assert!(
        refused["message"]
            .as_str()
            .unwrap()
            .contains("max_result_rows"),
        "{refused}"
    );
    server.wait_for_active(0).await;

    // Registered, then a commit pushes one member past the cap: one error for
    // the group, and the next delta follows the last delivered results.
    client
        .subscribe_group("w", &[("a", "?a(X)"), ("b", "?b(X)")])
        .await;
    server.write("+a[(2,), (3,)]\n+b(2)").await;
    let error = client.next_push_for("w").await;
    assert_eq!(error["type"], "subscription_error", "{error}");
    assert!(
        error["message"].as_str().unwrap().starts_with("?a(X): "),
        "{error}"
    );
    server.write("-a(3)").await;
    // The commit's two relations may each have refreshed (and failed) once.
    let mut push = client.next_push_for("w").await;
    while push["type"] == "subscription_error" {
        assert_eq!(push["message"], error["message"]);
        push = client.next_push_for("w").await;
    }
    assert_eq!(push["type"], "subscription_group_delta", "{push}");
    assert_eq!(push["seq"], 1);
    assert_eq!(rows(&push["members"][0]["inserted"]), [json!([2])]);
    assert_eq!(rows(&push["members"][1]["inserted"]), [json!([2])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_subscribe_frames_are_refused_and_the_connection_stays_usable() {
    let server = start_server(64).await;
    let mut client = Client::connect(&server).await;
    for (queries, expected) in [
        (json!([]), "at least one query"),
        (
            json!([{"name": "a", "query": "?a(X)"}, {"name": "a", "query": "?b(X)"}]),
            "names two queries 'a'",
        ),
        (
            json!([{"name": "a", "query": "a(X)"}]),
            "must start with '?'",
        ),
        (json!([{"name": "a", "query": "?a(X"}]), "Failed to parse"),
    ] {
        client
            .send(json!({"type": "subscribe", "id": "s", "subscription": "w", "queries": queries}))
            .await;
        let reply = client.reply().await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert_eq!(reply["id"], "s");
        assert!(
            reply["message"].as_str().unwrap().contains(expected),
            "{expected}: {reply}"
        );
    }
    for name in ["", "two words", &"x".repeat(129)] {
        let reply = client.try_subscribe_group(name, &ORDERS).await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert!(
            reply["message"]
                .as_str()
                .unwrap()
                .contains("Invalid subscription id"),
            "{reply}"
        );
    }
    client
        .send(json!({"type": "subscribe", "id": "s", "subscription": "w"}))
        .await;
    let reply = client.reply().await;
    assert_eq!(reply["code"], "invalid_request", "{reply}");
    server.wait_for_active(0).await;
    client.subscribe_group("w", &ORDERS).await;
    server.wait_for_active(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unsubscribe_and_switching_graphs_end_a_group() {
    let server = start_server(64).await;
    let mut client = Client::connect(&server).await;
    client.subscribe_group("w", &ORDERS).await;
    let reply = client.execute(".unsubscribe w").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(0).await;
    server.write("+a(1)").await;
    expect_quiet(&mut client, Duration::from_millis(200)).await;

    client.subscribe_group("w", &ORDERS).await;
    admin(&server, ".kg create elsewhere").await;
    assert_eq!(client.execute(".kg use elsewhere").await["type"], "result");
    server.wait_for_active(0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_and_a_plain_subscription_push_their_own_shapes() {
    let server = start_server(64).await;
    let mut client = Client::connect(&server).await;
    client.subscribe("p", "?a(X)").await;
    client.subscribe_group("g", &[("a", "?a(X)")]).await;
    server.write("+a(1)").await;
    let mut pushes = [client.next_push().await, client.next_push().await];
    pushes.sort_by_key(|p| p["subscription"].as_str().unwrap().to_string());
    assert_eq!(
        pushes[0]["type"], "subscription_group_delta",
        "{}",
        pushes[0]
    );
    assert_eq!(pushes[1]["type"], "subscription_delta", "{}", pushes[1]);
    assert_eq!(pushes[0]["revision"], pushes[1]["revision"]);
    assert_eq!(rows(&pushes[1]["inserted"]), [json!([1])]);
    assert_eq!(rows(&pushes[0]["members"][0]["inserted"]), [json!([1])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_needs_read_access_on_subscribe_and_every_refresh() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    admin(&server, ".user create eve pw12345678 viewer").await;
    admin(&server, &format!(".kg acl grant {KG} eve viewer")).await;
    let mut eve = Client::connect_as(&server, KG, "eve", "pw12345678").await;
    eve.subscribe_group("w", &ORDERS).await;

    admin(&server, &format!(".kg acl revoke {KG} eve")).await;
    server.write("+a(2)").await;
    let push = eve.next_push_for("w").await;
    assert_eq!(push["type"], "subscription_error", "{push}");
    assert!(
        push["message"].as_str().unwrap().contains("denied"),
        "{push}"
    );
    let refused = eve.try_subscribe_group("v", &ORDERS).await;
    assert_eq!(refused["type"], "error", "{refused}");

    // Granted again: the next push resumes from the last delivered results.
    admin(&server, &format!(".kg acl grant {KG} eve viewer")).await;
    server.write("+b(1)").await;
    let push = eve.next_push_for("w").await;
    assert_eq!(push["type"], "subscription_group_delta", "{push}");
    assert_eq!(push["seq"], 1);
    assert_eq!(rows(&push["members"][0]["inserted"]), [json!([2])]);
    assert_eq!(rows(&push["members"][1]["inserted"]), [json!([1])]);
}
