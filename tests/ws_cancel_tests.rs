//! Deadlines and cancel-by-id over a real `/ws` connection.
//!
//! One deadline spans a request's queueing, admission and computation, and a
//! `cancel` naming its id reaches it wherever it is. A request stopped before
//! it began committing applied nothing and never runs later; one that began
//! committing is not interrupted and reports what it committed, so a client
//! can retry a write whatever the race, without loss or duplicate.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const KG: &str = "cancel";
const PASSWORD: &str = "ws-cancel-test-pw";
const TIMEOUT: Duration = Duration::from_secs(60);

/// Complete bipartite graph, both directions: [`TRIANGLES`] finds nothing
/// but its cyclic join runs for seconds.
/// The join grows with `SIDE^3`; a release build runs it many times faster,
/// so it needs a larger graph to still run well past the timing budgets.
const SIDE: i64 = if cfg!(debug_assertions) { 14 } else { 20 };
const TRIANGLES: &str = "?edge(X, Y), edge(Y, Z), edge(Z, X)";
/// A cancelled or expired computation must answer within this.
const STOP_BUDGET: Duration = Duration::from_millis(2_000);

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
    handler.bootstrap_auth().unwrap();
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    let app = create_router(Arc::clone(&handler), &handler.config().http);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let server = Server {
        handler,
        addr,
        task,
        _tmp: tmp,
    };
    let edges: Vec<String> = (0..SIDE)
        .flat_map(|a| {
            (SIDE..2 * SIDE).flat_map(move |b| [format!("({a}, {b})"), format!("({b}, {a})")])
        })
        .collect();
    server.write(&format!("+edge[{}]", edges.join(", "))).await;
    server
}

impl Server {
    async fn write(&self, program: &str) {
        self.handler
            .execute_program(None, Some(KG.to_string()), program.to_string(), None)
            .await
            .unwrap();
    }

    /// Rows of `relation`, read by another client.
    fn count(&self, relation: &str) -> usize {
        let storage = self.handler.get_storage();
        let snapshot = storage.get_snapshot_for(KG).unwrap();
        snapshot.input_tuples.get(relation).map_or(0, |r| r.len())
    }
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    async fn connect(server: &Server) -> Self {
        let url = format!("ws://{}/ws?kg={KG}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self { ws };
        client
            .send(json!({"type": "login", "username": "admin", "password": PASSWORD}))
            .await;
        assert_eq!(client.reply().await["type"], "authenticated");
        client
    }

    async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    async fn execute(&mut self, id: &str, program: &str) {
        self.send(json!({"type": "execute", "id": id, "program": program}))
            .await;
    }

    async fn cancel(&mut self, id: &str, target: &str) {
        self.send(json!({"type": "cancel", "id": id, "target": target}))
            .await;
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

    /// The next reply, which must answer request `id`.
    async fn reply_to(&mut self, id: &str) -> Value {
        let frame = self.reply().await;
        assert_eq!(frame["id"], id, "{frame}");
        frame
    }
}

fn assert_stopped(reply: &Value, code: &str) {
    assert_eq!(reply["type"], "error", "{reply}");
    assert_eq!(reply["code"], code, "{reply}");
    assert!(
        reply["message"]
            .as_str()
            .unwrap()
            .contains("nothing was applied"),
        "{reply}"
    );
}

fn assert_ack(ack: &Value, target: &str, outcome: &str) {
    assert_eq!(ack["type"], "cancel_ack", "{ack}");
    assert_eq!(ack["target"], target, "{ack}");
    assert_eq!(ack["outcome"], outcome, "{ack}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_stops_a_running_query_promptly() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    client.execute("q", TRIANGLES).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let cancelled_at = Instant::now();
    client.cancel("c", "q").await;

    let reply = client.reply_to("q").await;
    let stopped_in = cancelled_at.elapsed();
    assert_stopped(&reply, "cancelled");
    assert!(stopped_in < STOP_BUDGET, "cancel took {stopped_in:?}");
    assert_ack(&client.reply_to("c").await, "q", "cancelled");

    // The connection stays usable, and the answered id is gone.
    client.cancel("c2", "q").await;
    assert_ack(&client.reply_to("c2").await, "q", "not_found");
    client.execute("again", "?edge(0, Y)").await;
    assert_eq!(client.reply_to("again").await["type"], "result");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_query_stops_at_its_deadline() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    let sent = Instant::now();
    client
        .send(json!({"type": "execute", "id": "q", "program": TRIANGLES, "timeout_ms": 150}))
        .await;
    let reply = client.reply_to("q").await;
    let took = sent.elapsed();
    assert_stopped(&reply, "deadline_exceeded");
    assert!(
        took >= Duration::from_millis(150),
        "stopped early: {took:?}"
    );
    assert!(
        took < Duration::from_millis(150) + STOP_BUDGET,
        "took {took:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_queued_write_never_runs_and_retries_once() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    // The write waits behind the long query (an ordering barrier).
    client.execute("slow", TRIANGLES).await;
    client.execute("w", "+marker(1)").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.cancel("cw", "w").await;
    client.cancel("cs", "slow").await;

    assert_stopped(&client.reply_to("slow").await, "cancelled");
    assert_stopped(&client.reply_to("w").await, "cancelled");
    assert_ack(&client.reply_to("cw").await, "w", "cancelled");
    assert_ack(&client.reply_to("cs").await, "slow", "cancelled");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.count("marker"), 0, "the cancelled write ran");

    // Retrying applies it exactly once.
    client.execute("retry", "+marker(1)").await;
    assert_eq!(client.reply_to("retry").await["type"], "result");
    client.execute("retry2", "+marker(1)").await;
    assert_eq!(client.reply_to("retry2").await["type"], "result");
    assert_eq!(server.count("marker"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_whose_deadline_passes_in_the_queue_never_runs() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    client.execute("slow", TRIANGLES).await;
    client
        .send(json!({"type": "execute", "id": "w", "program": "+marker(1)", "timeout_ms": 50}))
        .await;
    // Let the write's deadline pass while it is queued, then free the queue.
    tokio::time::sleep(Duration::from_millis(300)).await;
    client.cancel("cs", "slow").await;

    assert_stopped(&client.reply_to("slow").await, "cancelled");
    assert_stopped(&client.reply_to("w").await, "deadline_exceeded");
    assert_ack(&client.reply_to("cs").await, "slow", "cancelled");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(server.count("marker"), 0, "an expired write ran later");
}

/// Cancel a large write at increasing delays, so the cancel lands before,
/// during and after its commit. Whatever the race, the ack, the reply and the
/// data agree, and a retry leaves exactly one copy.
#[tokio::test(flavor = "multi_thread")]
async fn cancel_across_the_commit_boundary_is_consistent_and_retry_is_idempotent() {
    const ROWS: usize = 10_000;
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    let mut outcomes = Vec::new();
    for (round, delay_ms) in [0u64, 1, 2, 5, 10, 20, 50, 200].into_iter().enumerate() {
        let relation = format!("bulk{round}");
        let facts: Vec<String> = (0..ROWS).map(|i| format!("({i})")).collect();
        let write = format!("+{relation}[{}]", facts.join(", "));
        let (w, c) = (format!("w{round}"), format!("c{round}"));
        client.execute(&w, &write).await;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        client.cancel(&c, &w).await;

        let reply = client.reply_to(&w).await;
        let ack = client.reply_to(&c).await;
        let outcome = ack["outcome"].as_str().unwrap().to_string();
        let count = server.count(&relation);
        match outcome.as_str() {
            "cancelled" => {
                assert_stopped(&reply, "cancelled");
                assert_eq!(count, 0, "cancelled write applied ({delay_ms} ms)");
            }
            "too_late" | "not_found" => {
                assert_eq!(reply["type"], "result", "{reply}");
                assert_eq!(count, ROWS, "committed write lost ({delay_ms} ms)");
            }
            other => panic!("unexpected outcome {other}"),
        }
        outcomes.push(outcome);

        // Retry blindly: exactly one copy either way.
        let r = format!("r{round}");
        client.execute(&r, &write).await;
        let retried = client.reply_to(&r).await;
        assert_eq!(retried["type"], "result", "{retried}");
        assert_eq!(server.count(&relation), ROWS, "after retry ({delay_ms} ms)");
    }
    // Which side of the boundary each cancel landed on depends on the host;
    // every round above holds either way.
    eprintln!("cancel outcomes by delay: {outcomes:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_of_unknown_or_uncancellable_requests_is_not_found() {
    let server = start_server().await;
    let mut client = Client::connect(&server).await;
    client.cancel("c", "never-sent").await;
    assert_ack(&client.reply_to("c").await, "never-sent", "not_found");

    client.execute("p", "?edge(0, Y)").await;
    assert_eq!(client.reply_to("p").await["type"], "result");
    client.cancel("c2", "p").await;
    assert_ack(&client.reply_to("c2").await, "p", "not_found");
}
