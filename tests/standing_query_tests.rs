//! Standing queries (`.subscribe`) over a real `/ws` connection.
//!
//! Writes go straight through the `Handler`; the subscriber observes them via
//! the notification broadcast, like any other client's commits.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const KG: &str = "subs";
const PASSWORD: &str = "standing-query-test-pw";
const TIMEOUT: Duration = Duration::from_secs(20);

struct Server {
    handler: Arc<Handler>,
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
    _tmp: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start_server(max_subscriptions: usize) -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_subscriptions = max_subscriptions;
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.gui.enabled = false;
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth();
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    let app = create_router(Arc::clone(&handler), &handler.config().http);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        handler,
        addr,
        task,
        _tmp: tmp,
    }
}

impl Server {
    /// Commit a persistent change as another client would.
    async fn write(&self, program: &str) {
        self.handler
            .execute_program(None, Some(KG.to_string()), program.to_string(), None)
            .await
            .unwrap_or_else(|e| panic!("write {program:?} failed: {e}"));
    }

    fn evaluations(&self) -> u64 {
        self.handler.subscription_metrics().evaluations()
    }

    async fn wait_for_active(&self, expected: u64) {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while self.handler.subscription_metrics().active() != expected {
            assert!(
                tokio::time::Instant::now() < deadline,
                "active subscriptions stuck at {}, expected {expected}",
                self.handler.subscription_metrics().active()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Subscription pushes received while waiting for a reply.
    pushes: VecDeque<Value>,
}

impl Client {
    async fn connect(server: &Server) -> Self {
        let url = format!("ws://{}/ws?kg={KG}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self {
            ws,
            pushes: VecDeque::new(),
        };
        client
            .send(json!({"type": "login", "username": "admin", "password": PASSWORD}))
            .await;
        let reply = client.recv().await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        client
    }

    async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    async fn recv(&mut self) -> Value {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for a message")
                .expect("connection closed")
                .unwrap();
            if let Message::Text(text) = msg {
                return serde_json::from_str(&text).unwrap();
            }
        }
    }

    /// Run a program; returns its `result` or `error` reply.
    async fn execute(&mut self, program: &str) -> Value {
        self.send(json!({"type": "execute", "program": program}))
            .await;
        loop {
            let msg = self.recv().await;
            match msg["type"].as_str() {
                Some("result" | "error") => return msg,
                Some("subscription_delta" | "subscription_error") => self.pushes.push_back(msg),
                _ => {}
            }
        }
    }

    async fn next_push(&mut self) -> Value {
        if let Some(push) = self.pushes.pop_front() {
            return push;
        }
        loop {
            let msg = self.recv().await;
            if msg["type"]
                .as_str()
                .is_some_and(|t| t.starts_with("subscription_"))
            {
                return msg;
            }
        }
    }

    /// Next push for `subscription`, failing on a push for any other.
    async fn next_push_for(&mut self, subscription: &str) -> Value {
        let push = self.next_push().await;
        assert_eq!(push["subscription"], subscription, "unexpected push {push}");
        push
    }

    async fn subscribe(&mut self, id: &str, query: &str) -> Value {
        let reply = self.execute(&format!(".subscribe {id} {query}")).await;
        assert_eq!(reply["type"], "result", "subscribe failed: {reply}");
        reply
    }
}

fn rows(value: &Value) -> Vec<Value> {
    value.as_array().unwrap().clone()
}

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

async fn admin(server: &Server, program: &str) {
    let identity = inputlayer::auth::AuthIdentity {
        username: "admin".to_string(),
        role: inputlayer::auth::Role::Admin,
    };
    server
        .handler
        .execute_program(None, None, program.to_string(), Some(&identity))
        .await
        .unwrap_or_else(|e| panic!("{program:?} failed: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_access_is_checked_on_subscribe_and_every_evaluation() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    admin(&server, ".user create mallory pw12345678 viewer").await;
    admin(&server, &format!(".kg acl grant {KG} mallory viewer")).await;

    let url = format!("ws://{}/ws?kg={KG}", server.addr);
    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let mut client = Client {
        ws,
        pushes: VecDeque::new(),
    };
    client
        .send(json!({"type": "login", "username": "mallory", "password": "pw12345678"}))
        .await;
    let reply = client.recv().await;
    assert_eq!(reply["type"], "authenticated", "{reply}");
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
