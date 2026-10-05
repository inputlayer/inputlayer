//! `expect_revision` over a real `/ws` connection.
//!
//! A client that saw the knowledge graph at revision `R` (here: from a
//! subscription's snapshot) commits a write only if nothing in its scope
//! changed after `R`. A refused write applies nothing and fails with
//! `precondition_failed`; of several clients racing on one revision of one
//! scope, exactly one commits.

#![allow(clippy::unwrap_used, clippy::expect_used)]

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

const KG: &str = "expect";
const PASSWORD: &str = "ws-expect-revision-test-pw";
const TIMEOUT: Duration = Duration::from_secs(60);

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

async fn start_server() -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
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
    fn count(&self, relation: &str) -> usize {
        let storage = self.handler.get_storage();
        let snapshot = storage.get_snapshot_for(KG).unwrap();
        snapshot.input_tuples.get(relation).map_or(0, |r| r.len())
    }
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    epoch: String,
}

impl Client {
    async fn connect(server: &Server) -> Self {
        let url = format!("ws://{}/ws?kg={KG}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self {
            ws,
            epoch: String::new(),
        };
        client
            .send(json!({"type": "login", "username": "admin", "password": PASSWORD}))
            .await;
        let authenticated = client.reply().await;
        assert_eq!(authenticated["type"], "authenticated", "{authenticated}");
        client.epoch = authenticated["stream_epoch"].as_str().unwrap().to_string();
        client
    }

    async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    /// The next reply; pushes are skipped.
    async fn reply(&mut self) -> Value {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for a reply")
                .expect("connection closed")
                .unwrap();
            let Message::Text(text) = msg else { continue };
            let frame: Value = serde_json::from_str(&text).unwrap();
            let push = frame.get("id").is_none()
                && !matches!(frame["type"].as_str(), Some("authenticated" | "error"));
            if !push {
                return frame;
            }
        }
    }

    /// Send `frame` (an `execute` without its `type` and `id`) as request
    /// `id` and return its reply.
    async fn request(&mut self, id: &str, mut frame: Value) -> Value {
        frame["type"] = json!("execute");
        frame["id"] = json!(id);
        self.send(frame).await;
        let reply = self.reply().await;
        assert_eq!(reply["id"], id, "{reply}");
        reply
    }

    async fn write(&mut self, id: &str, program: &str) {
        let reply = self.request(id, json!({"program": program})).await;
        assert_eq!(reply["type"], "result", "{reply}");
    }

    /// The revision a new subscription to `query` is exact at.
    async fn subscribe(&mut self, name: &str, query: &str) -> u64 {
        let program = format!(".subscribe {name} {query}");
        let reply = self.request(name, json!({"program": program})).await;
        reply["subscribed"]["revision"]
            .as_u64()
            .unwrap_or_else(|| panic!("no revision: {reply}"))
    }
}

fn assert_refused(reply: &Value, code: &str) {
    assert_eq!(reply["type"], "error", "{reply}");
    assert_eq!(reply["code"], code, "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_strict_commit_holds_until_its_window_changes() {
    let server = start_server().await;
    let mut decider = Client::connect(&server).await;
    let mut writer = Client::connect(&server).await;
    writer.write("w1", "+eta(\"s1\", 3)").await;

    let seen = decider.subscribe("window", "?eta(S, D)").await;
    let strict = |revision: u64, epoch: &str| {
        json!({
            "program": "+claim(\"s1\", \"call_carrier\")",
            "expect_revision": revision,
            "expect_relations": ["eta"],
            "expect_epoch": epoch,
        })
    };

    // A write outside the window does not refuse the decision.
    writer.write("w2", "+noise(1)").await;
    let epoch = decider.epoch.clone();
    let reply = decider.request("d1", strict(seen, &epoch)).await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert_eq!(server.count("claim"), 1);

    // A write to the window does.
    writer.write("w3", "+eta(\"s2\", 5)").await;
    let reply = decider
        .request(
            "d2",
            json!({
                "program": "+claim(\"s1\", \"say_eta\")",
                "expect_revision": seen,
                "expect_relations": ["eta"],
            }),
        )
        .await;
    assert_refused(&reply, "precondition_failed");
    assert!(
        reply["message"]
            .as_str()
            .unwrap()
            .contains("Nothing was applied"),
        "{reply}"
    );
    assert_eq!(server.count("claim"), 1);

    // So does any write at all, when the scope is the whole graph.
    let reply = decider
        .request(
            "d3",
            json!({"program": "+claim(\"s1\", \"say_eta\")", "expect_revision": seen}),
        )
        .await;
    assert_refused(&reply, "precondition_failed");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_revision_of_another_engine_run_is_refused() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    client.write("w1", "+eta(\"s1\", 3)").await;
    let seen = client.subscribe("window", "?eta(S, D)").await;
    let reply = client
        .request(
            "d1",
            json!({
                "program": "+claim(\"s1\", \"a\")",
                "expect_revision": seen,
                "expect_epoch": "not-this-run",
            }),
        )
        .await;
    assert_refused(&reply, "precondition_failed");
    assert_eq!(server.count("claim"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_expectations_are_invalid_requests() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    client.write("w1", "+eta(\"s1\", 3)").await;
    for (id, frame) in [
        (
            "relations-alone",
            json!({"program": "+a(1)", "expect_relations": ["eta"]}),
        ),
        (
            "empty-scope",
            json!({"program": "+a(1)", "expect_revision": 1, "expect_relations": []}),
        ),
        (
            "a-query",
            json!({"program": "?eta(S, D)", "expect_revision": 1}),
        ),
        (
            "a-subscription",
            json!({"program": ".subscribe s ?eta(S, D)", "expect_revision": 1}),
        ),
    ] {
        let reply = client.request(id, frame).await;
        assert_refused(&reply, "invalid_request");
    }
    // The connection stays usable.
    client.write("w2", "+a(1)").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn of_clients_racing_on_one_revision_exactly_one_commits() {
    const CLIENTS: usize = 6;
    let server = start_server().await;
    let mut setup = Client::connect(&server).await;
    setup.write("w1", "+slot(0)").await;
    let seen = setup.subscribe("slot", "?slot(X)").await;

    let mut clients = Vec::new();
    for _ in 0..CLIENTS {
        clients.push(Client::connect(&server).await);
    }
    let racers: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(i, mut client)| {
            tokio::spawn(async move {
                let frame = json!({
                    "program": format!("+slot({})", i + 1),
                    "expect_revision": seen,
                    "expect_relations": ["slot"],
                });
                client.request("claim", frame).await
            })
        })
        .collect();
    let mut committed = 0;
    for racer in racers {
        let reply = racer.await.unwrap();
        match reply["type"].as_str() {
            Some("result") => committed += 1,
            _ => assert_refused(&reply, "precondition_failed"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(server.count("slot"), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_decider_that_reads_again_can_commit() {
    let server = start_server().await;
    let mut decider = Client::connect(&server).await;
    let mut watcher = Client::connect(&server).await;
    let mut writer = Client::connect(&server).await;
    writer.write("w1", "+eta(\"s1\", 9)").await;
    writer.write("rule", "+late(S) <- eta(S, D), D > 4").await;
    // Another client keeps the views shared.
    watcher.subscribe("all", "?eta(S, D)").await;
    watcher.subscribe("late", "?late(S)").await;

    // (subscription, query, scope, a write after the first read)
    let cases = [
        // Outside the view: its result is untouched, and it is not refreshed.
        ("w1", "?eta(S, D)", None, "+noise(1)"),
        // Refreshes the view to the same result.
        ("w2", "?late(S)", Some(json!(["late"])), "+eta(\"s2\", 1)"),
        // A rule the view does not read.
        ("w3", "?eta(S, D)", None, "+early(S) <- eta(S, D), D < 2"),
    ];
    for (name, query, scope, write) in cases {
        let decide = |revision: u64| {
            let mut frame = json!({"program": "+claim(\"s1\")", "expect_revision": revision});
            if let Some(scope) = &scope {
                frame["expect_relations"] = scope.clone();
            }
            frame
        };
        let seen = decider.subscribe(name, query).await;
        writer.write("w", write).await;
        let reply = decider.request("d", decide(seen)).await;
        assert_refused(&reply, "precondition_failed");

        // Read the state again and decide again: nothing changed meanwhile.
        let reply = decider
            .request("u", json!({"program": format!(".unsubscribe {name}")}))
            .await;
        assert_eq!(reply["type"], "result", "{reply}");
        let again = decider.subscribe(name, query).await;
        assert!(
            again > seen,
            "{name}: read again at {again}, first at {seen}"
        );
        let reply = decider.request("d", decide(again)).await;
        assert_eq!(reply["type"], "result", "{name}: {reply}");
        let reply = decider
            .request("u", json!({"program": format!(".unsubscribe {name}")}))
            .await;
        assert_eq!(reply["type"], "result", "{reply}");
    }
}
