//! Per-connection request pipeline over a real `/ws` connection.
//!
//! Requests on one connection run as tasks while the connection keeps
//! pushing: a long query must not hold up subscription deltas, replies come
//! back in request order, and KG switches and session changes act as ordering
//! barriers, so a pipelined write never lands on the wrong KG.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
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

const KG: &str = "pipeline";
const OTHER_KG: &str = "pipeline_other";
const PASSWORD: &str = "ws-pipeline-test-pw";
const TIMEOUT: Duration = Duration::from_secs(60);

/// Complete bipartite graph, both directions: no odd cycle, so [`TRIANGLES`]
/// returns nothing, yet the cyclic join runs for seconds.
const SIDE: i64 = 10;
const TRIANGLES: &str = "?edge(X, Y), edge(Y, Z), edge(Z, X)";
/// Below this, [`TRIANGLES`] proves nothing about blocking.
const LONG_QUERY: Duration = Duration::from_millis(500);
/// Writer commit to the subscriber's delta, while its own query runs.
const DELTA_BUDGET: Duration = Duration::from_millis(1_000);

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

async fn start_server(configure: impl FnOnce(&mut Config)) -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.gui.enabled = false;
    configure(&mut config);
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth();
    for kg in [KG, OTHER_KG] {
        handler.get_storage().create_knowledge_graph(kg).unwrap();
    }
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
    /// Commit a persistent change to `kg` as another client would.
    async fn write_to(&self, kg: &str, program: &str) {
        self.handler
            .execute_program(None, Some(kg.to_string()), program.to_string(), None)
            .await
            .unwrap_or_else(|e| {
                panic!("write {:?} failed: {e}", &program[..program.len().min(80)])
            });
    }

    async fn write(&self, program: &str) {
        self.write_to(KG, program).await;
    }

    /// Install the graph [`TRIANGLES`] searches.
    async fn install_bipartite_graph(&self) {
        let edges: Vec<String> = (0..SIDE)
            .flat_map(|a| {
                (SIDE..2 * SIDE).flat_map(move |b| [format!("({a}, {b})"), format!("({b}, {a})")])
            })
            .collect();
        self.write(&format!("+edge[{}]", edges.join(", "))).await;
    }

    /// Rows of a fresh `query` on `kg`, from another connection.
    async fn query_rows(&self, kg: &str, query: &str) -> Vec<Value> {
        let mut auditor = Client::connect_to(self, kg).await;
        let reply = auditor.execute(query).await;
        assert_eq!(reply["type"], "result", "{reply}");
        rows(&reply)
    }

    /// Wait until `done` holds, polling.
    async fn wait_until(&self, what: &str, done: impl Fn(&Handler) -> bool) {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while !done(&self.handler) {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// A received frame and when it arrived.
struct Frame {
    at: Instant,
    value: Value,
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    async fn connect(server: &Server) -> Self {
        Self::connect_to(server, KG).await
    }

    async fn connect_to(server: &Server, kg: &str) -> Self {
        let url = format!("ws://{}/ws?kg={kg}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self { ws };
        client
            .send(json!({"type": "login", "username": "admin", "password": PASSWORD}))
            .await;
        let reply = client.recv().await;
        assert_eq!(reply.value["type"], "authenticated", "{}", reply.value);
        client
    }

    async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    /// Send `program` without waiting for its reply.
    async fn send_execute(&mut self, program: &str) {
        self.send(json!({"type": "execute", "program": program}))
            .await;
    }

    async fn recv(&mut self) -> Frame {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for a message")
                .expect("connection closed")
                .unwrap();
            if let Message::Text(text) = msg {
                return Frame {
                    at: Instant::now(),
                    value: serde_json::from_str(&text).unwrap(),
                };
            }
        }
    }

    /// The next `n` replies, in arrival order; pushes are returned separately.
    async fn replies(&mut self, n: usize) -> (Vec<Frame>, VecDeque<Frame>) {
        let mut replies = Vec::new();
        let mut pushes = VecDeque::new();
        while replies.len() < n {
            let frame = self.recv().await;
            match frame.value["type"].as_str() {
                Some("result" | "error" | "pong") => replies.push(frame),
                _ => pushes.push_back(frame),
            }
        }
        (replies, pushes)
    }

    async fn execute(&mut self, program: &str) -> Value {
        self.send_execute(program).await;
        let (mut replies, _) = self.replies(1).await;
        replies.remove(0).value
    }
}

fn message(reply: &Value) -> &str {
    reply["rows"][0][0].as_str().unwrap_or_default()
}

fn kind(frame: &Frame) -> &str {
    frame.value["type"].as_str().unwrap_or_default()
}

/// Rows of a `result` reply.
fn rows(reply: &Value) -> Vec<Value> {
    reply["rows"].as_array().cloned().unwrap_or_default()
}

/// Whether a reply answers [`TRIANGLES`]: its empty result, or the query
/// timeout on a host too loaded to finish it.
fn answers_triangles(reply: &Value) -> bool {
    match reply["type"].as_str() {
        Some("result") => rows(reply).is_empty() && reply["errors"] == json!([]),
        Some("error") => reply["code"] == "deadline_exceeded",
        _ => false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn long_query_does_not_delay_deltas_on_its_connection() {
    let server = start_server(|_| {}).await;
    server.install_bipartite_graph().await;
    server.write("+seen(0)").await;
    let mut client = Client::connect(&server).await;
    let snapshot = client.execute(".subscribe s ?seen(X)").await;
    assert_eq!(rows(&snapshot), vec![json!([0])], "{snapshot}");

    let sent = Instant::now();
    client.send_execute(TRIANGLES).await;
    client.send_execute("?seen(X)").await;
    client.send(json!({"type": "ping"})).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let committed = Instant::now();
    server.write("+seen(1)").await;

    let (replies, pushes) = client.replies(3).await;
    let long = &replies[0];
    assert!(answers_triangles(&long.value), "{}", long.value);
    assert!(
        long.at - sent >= LONG_QUERY,
        "fixture too fast to show blocking: {:?}",
        long.at - sent
    );
    // Replies in request order, although the later ones finished first.
    assert_eq!(kind(&replies[1]), "result");
    assert_eq!(kind(&replies[2]), "pong");

    let delta = pushes
        .iter()
        .find(|frame| kind(frame) == "subscription_delta")
        .expect("no delta while the query ran");
    assert_eq!(delta.value["subscription"], "s");
    assert_eq!(
        rows(&json!({"rows": delta.value["inserted"]})),
        vec![json!([1])]
    );
    assert!(delta.at < long.at, "delta waited for the long query");
    assert!(
        delta.at - committed <= DELTA_BUDGET,
        "delta took {:?}",
        delta.at - committed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn kg_switch_orders_pipelined_writes() {
    let server = start_server(|_| {}).await;
    server.install_bipartite_graph().await;
    let mut client = Client::connect(&server).await;

    // No waiting between requests: the switch queues behind a running query
    // and the insert behind the switch.
    let program = [
        TRIANGLES.to_string(),
        format!(".kg use {OTHER_KG}"),
        "+marker(1)".to_string(),
        "?marker(X)".to_string(),
        format!(".kg use {KG}"),
        "+marker(2)".to_string(),
        "?marker(X)".to_string(),
    ];
    for request in &program {
        client.send_execute(request).await;
    }
    let (replies, _) = client.replies(program.len()).await;
    let replies: Vec<&Value> = replies.iter().map(|f| &f.value).collect();
    assert!(answers_triangles(replies[0]), "{}", replies[0]);
    assert_eq!(replies[1]["switched_kg"], OTHER_KG, "{}", replies[1]);
    assert_eq!(replies[2]["type"], "result", "{}", replies[2]);
    assert_eq!(rows(replies[3]), vec![json!([1])], "{}", replies[3]);
    assert_eq!(replies[4]["switched_kg"], KG, "{}", replies[4]);
    assert_eq!(rows(replies[6]), vec![json!([2])], "{}", replies[6]);

    assert_eq!(
        server.query_rows(OTHER_KG, "?marker(X)").await,
        vec![json!([1])]
    );
    assert_eq!(server.query_rows(KG, "?marker(X)").await, vec![json!([2])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn session_changes_are_ordered_with_queries() {
    let server = start_server(|_| {}).await;
    server.write("+base(1)").await;
    let mut client = Client::connect(&server).await;
    let program = [
        "?base(X)",
        "extra(X) <- base(X)",
        "?extra(X)",
        ".session clear",
        "?base(X)",
    ];
    for request in program {
        client.send_execute(request).await;
    }
    let (replies, _) = client.replies(program.len()).await;
    let replies: Vec<&Value> = replies.iter().map(|f| &f.value).collect();
    assert_eq!(rows(replies[0]), vec![json!([1])]);
    assert!(
        message(replies[1]).contains("Session rule added"),
        "{}",
        replies[1]
    );
    assert_eq!(
        rows(replies[2]),
        vec![json!([1])],
        "query ran before its rule"
    );
    assert!(message(replies[3]).contains("Cleared"), "{}", replies[3]);
    assert_eq!(rows(replies[4]), vec![json!([1])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn pipelined_subscribe_and_unsubscribe_keep_their_order() {
    let server = start_server(|_| {}).await;
    server.write("+seen(0)").await;
    let mut client = Client::connect(&server).await;
    let program = [
        ".subscribe s ?seen(X)",
        ".unsubscribe s",
        ".subscribe s ?seen(X)",
        "+seen(1)",
    ];
    for request in program {
        client.send_execute(request).await;
    }
    let (replies, mut pushes) = client.replies(program.len()).await;
    for reply in &replies {
        assert_eq!(kind(reply), "result", "{}", reply.value);
    }
    let delta = loop {
        let push = match pushes.pop_front() {
            Some(push) => push,
            None => client.recv().await,
        };
        if kind(&push).starts_with("subscription_") {
            break push;
        }
    };
    assert_eq!(kind(&delta), "subscription_delta", "{}", delta.value);
    assert_eq!(delta.value["seq"], 1);
    assert_eq!(delta.value["inserted"], json!([[1]]));
    server
        .wait_until("one subscription", |h| {
            h.subscription_metrics().active() == 1
        })
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bounded_pipeline_answers_every_request_in_order() {
    let server = start_server(|config| config.http.rate_limit.ws_max_in_flight_requests = 2).await;
    server.write("+seen(0)").await;
    let mut client = Client::connect(&server).await;
    const REQUESTS: usize = 60;
    for i in 0..REQUESTS {
        match i % 3 {
            0 => client.send(json!({"type": "ping"})).await,
            1 => client.send_execute("?seen(X)").await,
            _ => client.send_execute(&format!("+seen({i})")).await,
        }
    }
    let (replies, _) = client.replies(REQUESTS).await;
    for (i, reply) in replies.iter().enumerate() {
        let expected = if i % 3 == 0 { "pong" } else { "result" };
        assert_eq!(kind(reply), expected, "reply {i}: {}", reply.value);
    }
    let all = server.query_rows(KG, "?seen(X)").await;
    assert_eq!(all.len(), 1 + REQUESTS / 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_during_a_long_query_releases_the_connection() {
    let server = start_server(|_| {}).await;
    server.install_bipartite_graph().await;
    server.write("+seen(0)").await;
    let mut client = Client::connect(&server).await;
    client.execute(".subscribe s ?seen(X)").await;
    assert_eq!(server.handler.session_stats().total_sessions, 1);

    client.send_execute(TRIANGLES).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let closed = Instant::now();
    client.ws.close(None).await.unwrap();
    drop(client);
    server
        .wait_until("session closed", |h| {
            h.session_stats().total_sessions == 0 && h.subscription_metrics().active() == 0
        })
        .await;
    assert!(
        closed.elapsed() < LONG_QUERY,
        "cleanup waited for the query: {:?}",
        closed.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_lets_a_started_write_finish_whole() {
    const FACTS: i64 = 2_000;
    let server = start_server(|_| {}).await;
    let mut client = Client::connect(&server).await;
    let facts: Vec<String> = (0..FACTS).map(|i| format!("({i})")).collect();
    client
        .send_execute(&format!("+bulk[{}]", facts.join(", ")))
        .await;
    client.ws.close(None).await.unwrap();
    drop(client);
    server
        .wait_until("session closed", |h| h.session_stats().total_sessions == 0)
        .await;
    let committed = server.query_rows(KG, "?bulk(X)").await.len();
    assert!(
        committed == 0 || committed == FACTS as usize,
        "write cut in half: {committed} of {FACTS}"
    );
}

/// Subscribers that pipeline their own queries and writes, beside each other,
/// all end on the final result, with contiguous deltas.
#[tokio::test(flavor = "multi_thread")]
async fn saturated_mixed_traffic_converges() {
    const CLIENTS: i64 = 4;
    const ROUNDS: i64 = 15;
    let server = start_server(|_| {}).await;
    server.write("+item(-1)").await;
    let mut tasks = Vec::new();
    for c in 0..CLIENTS {
        let mut client = Client::connect(&server).await;
        tasks.push(tokio::spawn(async move {
            let snapshot = client.execute(".subscribe all ?item(X)").await;
            let mut view: std::collections::BTreeSet<String> =
                rows(&snapshot).iter().map(Value::to_string).collect();
            let mut seq = 0;
            for r in 0..ROUNDS {
                client.send_execute("?item(X)").await;
                client
                    .send_execute(&format!("+item({})", c * 1_000 + r))
                    .await;
                client.send_execute("?item(X), X > 100").await;
            }
            let (replies, mut pushes) = client.replies(3 * ROUNDS as usize).await;
            for reply in &replies {
                assert_eq!(kind(reply), "result", "{}", reply.value);
            }
            let expected = 1 + CLIENTS * ROUNDS;
            let deadline = Instant::now() + TIMEOUT;
            while view.len() < expected as usize {
                assert!(Instant::now() < deadline, "view stuck at {}", view.len());
                let push = match pushes.pop_front() {
                    Some(push) => push,
                    None => client.recv().await,
                };
                if kind(&push) != "subscription_delta" {
                    continue;
                }
                seq += 1;
                assert_eq!(push.value["seq"], seq, "seq gap");
                for row in push.value["retracted"].as_array().unwrap() {
                    assert!(view.remove(&row.to_string()), "retracted absent {row}");
                }
                for row in push.value["inserted"].as_array().unwrap() {
                    assert!(view.insert(row.to_string()), "inserted present {row}");
                }
            }
            view
        }));
    }
    let fresh: std::collections::BTreeSet<String> = {
        let mut views = Vec::new();
        for task in tasks {
            views.push(task.await.unwrap());
        }
        let fresh: std::collections::BTreeSet<String> = server
            .query_rows(KG, "?item(X)")
            .await
            .iter()
            .map(Value::to_string)
            .collect();
        for view in views {
            assert_eq!(view, fresh);
        }
        fresh
    };
    assert_eq!(fresh.len() as i64, 1 + CLIENTS * ROUNDS);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_spares_a_connection_waiting_on_its_request() {
    const IDLE: Duration = Duration::from_millis(300);
    let server = start_server(|config| config.http.ws_idle_timeout_ms = 300).await;
    server.install_bipartite_graph().await;

    let mut busy = Client::connect(&server).await;
    let sent = Instant::now();
    busy.send_execute(TRIANGLES).await;
    let reply = busy.recv().await;
    assert!(answers_triangles(&reply.value), "{}", reply.value);
    assert!(
        reply.at - sent > IDLE,
        "fixture too fast: {:?}",
        reply.at - sent
    );

    // Idle from here on: the next frame is the timeout.
    let idle = busy.recv().await;
    assert_eq!(idle.value["message"], "Idle timeout", "{}", idle.value);
    // Not fired at once from the time spent waiting on the request.
    assert!(idle.at - reply.at >= IDLE.saturating_sub(Duration::from_millis(50)));
}
