//! Parameterized views: subscriptions that differ only in bound constants
//! share one evaluation per commit, and each subscriber still gets exactly the
//! rows its own query returns.

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::{json, Value};

use crate::harness::{rows, start_server, start_server_with, Client, Server, TIMEOUT};

fn shared(server: &Server) -> u64 {
    server.handler.subscription_metrics().shared_evaluations()
}

async fn expect_quiet(client: &mut Client, within: Duration) {
    if let Ok(frame) = tokio::time::timeout(within, client.next_push()).await {
        panic!("expected no push, got {frame}");
    }
}

/// A subscriber and the result its pushes add up to.
struct Member {
    client: Client,
    query: String,
    rows: BTreeSet<String>,
    revision: u64,
}

impl Member {
    async fn subscribe(server: &Server, query: &str) -> Self {
        let mut client = Client::connect(server).await;
        let snapshot = client.subscribe("s", query).await;
        let rows = rows(&snapshot["rows"])
            .iter()
            .map(Value::to_string)
            .collect();
        let revision = snapshot["subscribed"]["revision"].as_u64().unwrap();
        Self {
            client,
            query: query.to_string(),
            rows,
            revision,
        }
    }

    /// The query's answer now, run as a one-off query on this connection.
    async fn oracle(&mut self) -> BTreeSet<String> {
        let reply = self.client.execute(&self.query).await;
        assert_eq!(reply["type"], "result", "{reply}");
        rows(&reply["rows"]).iter().map(Value::to_string).collect()
    }

    /// Apply pushes until the result is `expected`.
    async fn converge(&mut self, expected: &BTreeSet<String>) {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while self.rows != *expected {
            let push = tokio::time::timeout_at(deadline, self.client.next_push())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{}: stuck at {:?}, expected {expected:?}",
                        self.query, self.rows
                    )
                });
            assert_eq!(push["type"], "subscription_delta", "{}: {push}", self.query);
            let revision = push["revision"].as_u64().unwrap();
            assert!(revision > self.revision, "{}: {push}", self.query);
            self.revision = revision;
            for row in rows(&push["retracted"]) {
                assert!(self.rows.remove(&row.to_string()), "{}: {push}", self.query);
            }
            for row in rows(&push["inserted"]) {
                assert!(self.rows.insert(row.to_string()), "{}: {push}", self.query);
            }
        }
    }
}

const RULES: &str = "+view(S, X, Kind) <- item(S, X), kind(X, Kind)";

#[tokio::test(flavor = "multi_thread")]
async fn bound_subscriptions_share_one_evaluation_and_get_only_their_rows() {
    let server = start_server(256).await;
    server.write(RULES).await;
    server
        .write("+item[(\"s1\", 1), (\"s2\", 2)]\n+kind[(1, \"a\"), (2, \"a\")]")
        .await;
    let mut one = Member::subscribe(&server, r#"?view("s1", X, "a")"#).await;
    let mut two = Member::subscribe(&server, r#"?view("s2", X, "a")"#).await;
    let mut three = Member::subscribe(&server, r#"?view("s3", X, "a")"#).await;
    assert_eq!(one.rows.len(), 1);
    assert!(three.rows.is_empty());
    // More bindings than compute permits: their own evaluations no longer
    // all run at once, so a round pays even on a many-core host.
    let mut idle = Vec::new();
    for session in 4..=server.handler.compute_permits() + 1 {
        let query = format!(r#"?view("s{session}", X, "a")"#);
        idle.push(Member::subscribe(&server, &query).await);
    }

    // The views' first evaluations compiled their plans; the next ones give
    // the cost a shared evaluation is compared with.
    server
        .write("+item[(\"s1\", 9), (\"s2\", 9), (\"s3\", 9)]\n+kind(9, \"a\")")
        .await;
    for member in [&mut one, &mut two, &mut three] {
        let expected = member.oracle().await;
        member.converge(&expected).await;
    }
    // That cost starts a probe no view waits for; let it be judged.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while shared(&server) == 0 {
        assert!(tokio::time::Instant::now() < deadline, "no probe");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let before = shared(&server);
    server.write("+item(\"s1\", 3)\n+kind(3, \"a\")").await;
    let expected = one.oracle().await;
    one.converge(&expected).await;
    assert_eq!(one.rows.len(), 3);
    expect_quiet(&mut two.client, Duration::from_millis(300)).await;
    expect_quiet(&mut three.client, Duration::from_millis(1)).await;
    assert_eq!(
        shared(&server),
        before + 1,
        "one evaluation for three views"
    );

    // A write that moves a row from one subscriber to another.
    server.write("-item(\"s1\", 1)\n+item(\"s3\", 1)").await;
    for member in [&mut one, &mut two, &mut three] {
        let expected = member.oracle().await;
        member.converge(&expected).await;
    }
    assert!(one.rows.iter().all(|row| row.contains("s1")));
    assert_eq!(three.rows.len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_subscriber_follows_its_own_query_through_mixed_writes() {
    let server = start_server(64).await;
    server.write(RULES).await;
    server
        .write("+score_of(S, X, N) <- item(S, X), points(X, N)")
        .await;
    let mut members = Vec::new();
    for session in ["s1", "s2", "s3"] {
        for query in [
            format!(r#"?view("{session}", X, K)"#),
            format!(r#"?view("{session}", X, "a"), !blocked("{session}", X)"#),
            format!(r#"?score_of("{session}", X, 1)"#),
            format!(r#"?score_of("{session}", X, 2)"#),
        ] {
            members.push(Member::subscribe(&server, &query).await);
        }
    }
    let writes = [
        r#"+item[("s1", 1), ("s2", 2), ("s3", 3)]"#,
        r#"+kind[(1, "a"), (2, "b"), (3, "a")]"#,
        // The engine matches an integer constant numerically: a float 1.0
        // and an int 1 both match `1`, 1.5 matches nothing.
        "+points[(1, 1), (2, 1.0), (3, 2)]",
        "+points(1, 1.5)",
        r#"+blocked("s1", 1)"#,
        r#"+item[("s1", 2), ("s2", 3)]"#,
        r#"-kind(2, "b")"#,
        r#"+kind(2, "a")"#,
        r#"-blocked("s1", 1)"#,
        "+score_of(S, X, N) <- item(S, X), bonus(X, N)",
        "+bonus(3, 1)",
        r#"-item("s3", 3)"#,
    ];
    for write in writes {
        server.write(write).await;
        for member in &mut members {
            let expected = member.oracle().await;
            member.converge(&expected).await;
        }
    }
    assert!(shared(&server) > 0, "the members shared evaluations");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_family_of_one_binding_evaluates_on_its_own() {
    let server = start_server(64).await;
    server.write(RULES).await;
    server.write("+item(\"s1\", 1)\n+kind(1, \"a\")").await;
    let mut one = Member::subscribe(&server, r#"?view("s1", X, "a")"#).await;
    // The same binding spelled differently is another view of one binding.
    let mut same = Member::subscribe(&server, r#"?view("s1",X,"a")"#).await;
    let mut two = Member::subscribe(&server, r#"?view("s2", X, "a")"#).await;
    for write in [
        "+item[(\"s1\", 2), (\"s2\", 2)]\n+kind(2, \"a\")",
        "+item(\"s1\", 4)\n+kind(4, \"a\")",
    ] {
        server.write(write).await;
        for member in [&mut one, &mut same, &mut two] {
            let expected = member.oracle().await;
            member.converge(&expected).await;
        }
    }
    let after_shared = shared(&server);
    assert!(after_shared > 0);

    // With s2 gone, one binding is left: nothing to share.
    let reply = two.client.execute(".unsubscribe s").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(2).await;
    server.write("+item(\"s1\", 3)\n+kind(3, \"a\")").await;
    for member in [&mut one, &mut same] {
        let expected = member.oracle().await;
        member.converge(&expected).await;
    }
    assert_eq!(shared(&server), after_shared);
}

#[tokio::test(flavor = "multi_thread")]
async fn results_over_the_row_cap_fail_as_the_views_own_query_does() {
    let server = start_server_with(64, |config| {
        config.storage.performance.max_result_rows = 3;
    })
    .await;
    server.write(RULES).await;
    server
        .write(r#"+item[("s1", 1), ("s1", 2), ("s2", 3), ("s2", 4)]"#)
        .await;
    server
        .write(r#"+kind[(1, "a"), (2, "a"), (3, "a"), (4, "a")]"#)
        .await;
    // Together the bindings exceed the cap; each alone does not.
    let mut one = Member::subscribe(&server, r#"?view("s1", X, "a")"#).await;
    let mut two = Member::subscribe(&server, r#"?view("s2", X, "a")"#).await;
    server.write(r#"+item("s1", 5)"#).await;
    server.write(r#"+kind(5, "a")"#).await;
    let expected = two.oracle().await;
    two.converge(&expected).await;
    one.converge(&expected_rows(&["s1"], &[1, 2, 5])).await;

    // Past the cap on its own: the view's own error.
    server.write(r#"+item("s1", 6)"#).await;
    server.write(r#"+kind(6, "a")"#).await;
    let error = one.client.next_push().await;
    assert_eq!(error["type"], "subscription_error", "{error}");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("max_result_rows (3)"),
        "{error}"
    );
}

fn expected_rows(sessions: &[&str], xs: &[i64]) -> BTreeSet<String> {
    sessions
        .iter()
        .flat_map(|s| xs.iter().map(move |x| json!([s, x, "a"]).to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn sharing_can_be_switched_off() {
    let server = start_server_with(64, |config| {
        config.http.rate_limit.subscription_share_parameterized = false;
    })
    .await;
    server.write(RULES).await;
    let mut one = Member::subscribe(&server, r#"?view("s1", X, "a")"#).await;
    let _two = Member::subscribe(&server, r#"?view("s2", X, "a")"#).await;
    server.write("+item(\"s1\", 1)\n+kind(1, \"a\")").await;
    let expected = one.oracle().await;
    one.converge(&expected).await;
    assert_eq!(shared(&server), 0);
}
