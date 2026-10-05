//! Snapshot reads (the `read` frame): several queries answered at one revision.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::harness::{rows, start_server, start_server_with, Client, KG};

/// Sort the rows of one result: a read keeps the engine's row order.
fn sort_rows(result: &mut Value) {
    let mut rows = rows(&result["rows"]);
    rows.sort_by_key(Value::to_string);
    result["rows"] = Value::Array(rows);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_answers_every_query_at_one_revision() {
    let server = start_server(64).await;
    server.write("+a[(1,), (2,)]\n+b(7)").await;
    server.write("+r(X) <- a(X), X > 1").await;
    let mut client = Client::connect(&server).await;
    let reply = client
        .read(&[
            ("as", "?a(X)"),
            ("bs", "?b(Y)"),
            ("rs", "?r(Z)"),
            ("none", "?nothing(X)"),
        ])
        .await;
    assert_eq!(reply["type"], "snapshot", "{reply}");
    assert_eq!(reply["knowledge_graph"], KG);
    assert!(reply.get("subscribed").is_none(), "{reply}");
    let revision = reply["revision"].as_u64().unwrap();
    assert!(revision > 0);
    let mut results = reply["results"].clone();
    sort_rows(&mut results[0]);
    assert_eq!(
        results,
        json!([
            {"name": "as", "columns": ["X"], "rows": [[1], [2]], "total_count": 2, "truncated": false},
            {"name": "bs", "columns": ["Y"], "rows": [[7]], "total_count": 1, "truncated": false},
            {"name": "rs", "columns": ["Z"], "rows": [[2]], "total_count": 1, "truncated": false},
            {"name": "none", "columns": [], "rows": [], "total_count": 0, "truncated": false},
        ])
    );

    // The same queries as a group start from the same revision and results.
    let snapshot = client
        .subscribe_group("w", &[("as", "?a(X)"), ("bs", "?b(Y)")])
        .await;
    assert_eq!(snapshot["revision"], revision);
    assert_eq!(snapshot["results"][0], results[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_carries_its_request_id_and_sees_no_session_state() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    let mut client = Client::connect(&server).await;
    assert_eq!(client.execute("+a(2)").await["type"], "result");
    // Session facts and rules stay out, as for a subscription.
    assert_eq!(client.execute("a(99)").await["type"], "result");
    let session_query = client.execute("?a(X)").await;
    assert_eq!(rows(&session_query["rows"]).len(), 3, "{session_query}");
    client
        .send(json!({"type": "read", "id": "r1", "queries": [{"name": "a", "query": "?a(X)"}]}))
        .await;
    let mut reply = client.reply().await;
    assert_eq!(reply["id"], "r1");
    sort_rows(&mut reply["results"][0]);
    assert_eq!(rows(&reply["results"][0]["rows"]), [json!([1]), json!([2])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_fails_as_a_whole_naming_the_failing_query() {
    let server = start_server(64).await;
    let mut client = Client::connect(&server).await;
    for (queries, code, expected) in [
        (
            vec![("a", "?a(X)"), ("bad", "?b(X")],
            "validation",
            "Query 'bad'",
        ),
        (
            vec![("a", "?a(X)"), ("w", "+a(1)")],
            "validation",
            "must start with '?'",
        ),
        (
            vec![("a", "?a(X)"), ("two", "?a(X)\n?b(X)")],
            "validation",
            "single query",
        ),
        (
            vec![("a", "?a(X)"), ("a", "?b(X)")],
            "validation",
            "names two queries 'a'",
        ),
        (vec![], "validation", "at least one query"),
    ] {
        let reply = client.read(&queries).await;
        assert_eq!(reply["type"], "error", "{queries:?}: {reply}");
        assert_eq!(reply["code"], code, "{queries:?}: {reply}");
        assert!(
            reply["message"].as_str().unwrap().contains(expected),
            "{queries:?}: {reply}"
        );
    }
    // Nothing was written by the refused `+a(1)`.
    let reply = client.read(&[("a", "?a(X)")]).await;
    assert!(rows(&reply["results"][0]["rows"]).is_empty(), "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_reports_a_capped_result_as_truncated() {
    let server =
        start_server_with(64, |config| config.storage.performance.max_result_rows = 2).await;
    server.write("+a[(1,), (2,), (3,)]\n+b(1)").await;
    let mut client = Client::connect(&server).await;
    let reply = client.read(&[("a", "?a(X)"), ("b", "?b(X)")]).await;
    assert_eq!(reply["type"], "snapshot", "{reply}");
    assert_eq!(reply["results"][0]["truncated"], true);
    assert_eq!(rows(&reply["results"][0]["rows"]).len(), 2);
    assert_eq!(reply["results"][1]["truncated"], false);
}

/// While writers commit to `a` and `b` together, every read sees them equal,
/// and one connection's reads name non-decreasing revisions.
#[tokio::test(flavor = "multi_thread")]
async fn reads_stay_coherent_under_concurrent_writers() {
    let server = start_server(64).await;
    let handler = Arc::clone(&server.handler);
    let (start, started) = tokio::sync::oneshot::channel::<()>();
    let writer = tokio::spawn(async move {
        started.await.unwrap();
        for i in 1..=150 {
            let mut program = format!("+a({i})\n+b({i})");
            if i % 5 == 0 {
                program.push_str(&format!("\n-a({})\n-b({})", i - 3, i - 3));
            }
            handler
                .execute_program(None, Some(KG.to_string()), program, None)
                .await
                .unwrap();
        }
    });
    let mut client = Client::connect(&server).await;
    let mut start = Some(start);
    let (mut revision, mut reads, mut revisions) = (0, 0, std::collections::BTreeSet::new());
    while !writer.is_finished() || reads < 2 {
        let reply = client.read(&[("a", "?a(X)"), ("b", "?b(X)")]).await;
        assert_eq!(reply["type"], "snapshot", "{reply}");
        let at = reply["revision"].as_u64().unwrap();
        assert!(at >= revision, "revision {at} after {revision}");
        revision = at;
        revisions.insert(at);
        let set = |i: usize| -> Vec<String> {
            let mut rows: Vec<String> = rows(&reply["results"][i]["rows"])
                .iter()
                .map(Value::to_string)
                .collect();
            rows.sort();
            rows
        };
        assert_eq!(set(0), set(1), "a and b differ at revision {at}");
        reads += 1;
        // The writer starts after the first read, so reads overlap it.
        if let Some(start) = start.take() {
            start.send(()).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    writer.await.unwrap();
    assert!(revisions.len() > 1, "every read saw revision {revision}");
}
