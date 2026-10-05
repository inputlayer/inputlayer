//! Memory limits over a real `/ws` connection.
//!
//! A query that grows past `max_query_memory_bytes` is stopped and refused
//! with `resource_exhausted` instead of growing the server, and a write that
//! would grow its knowledge graph past `max_graph_memory_bytes` is refused
//! the same way. Neither applies anything, and the connection stays usable.

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

const KG: &str = "memory";
const PASSWORD: &str = "ws-memory-test-pw";
const TIMEOUT: Duration = Duration::from_secs(120);

/// Nodes of a strongly connected graph of small diameter: its transitive
/// closure has `NODES^2` pairs (about 2.25 million), several hundred MB in
/// the evaluator, so the unbound closure query runs far past a 32 MiB limit.
const NODES: i64 = 1500;
const CLOSURE: &str = "reach(X, Y) <- edge(X, Y)\n\
                       reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
                       ?reach(X, Y)";

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
    handler.bootstrap_auth().unwrap();
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
    async fn write(&self, program: &str) {
        self.handler
            .execute_program(None, Some(KG.to_string()), program.to_string(), None)
            .await
            .unwrap();
    }

    /// Rows of `relation`, read straight from the store.
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

    /// Run `program` and return its reply.
    async fn execute(&mut self, id: &str, program: &str) -> Value {
        self.send(json!({"type": "execute", "id": id, "program": program}))
            .await;
        let reply = self.reply().await;
        assert_eq!(reply["id"], id, "{reply}");
        reply
    }

    /// Run `program` with a deadline `timeout_ms` from now; return its reply.
    async fn execute_within(&mut self, id: &str, program: &str, timeout_ms: u64) -> Value {
        self.send(
            json!({"type": "execute", "id": id, "program": program, "timeout_ms": timeout_ms}),
        )
        .await;
        let reply = self.reply().await;
        assert_eq!(reply["id"], id, "{reply}");
        reply
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
}

fn assert_exhausted(reply: &Value, limit: &str) {
    assert_eq!(reply["type"], "error", "{reply}");
    assert_eq!(reply["code"], "resource_exhausted", "{reply}");
    let message = reply["message"].as_str().unwrap();
    assert!(message.contains("nothing was applied"), "{reply}");
    assert!(message.contains(limit), "{reply}");
}

fn edges(from: i64, to: i64) -> String {
    let edges: Vec<String> = (from..to)
        .flat_map(|i| {
            [(i + 1) % NODES, (2 * i) % NODES, (3 * i + 1) % NODES].map(|j| format!("({i}, {j})"))
        })
        .collect();
    format!("+edge[{}]", edges.join(", "))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_runaway_query_is_refused_instead_of_growing_the_server() {
    let server = start_server(|config| {
        config.storage.performance.max_query_memory_bytes = 32 << 20;
        config.storage.performance.query_timeout_ms = 0;
    })
    .await;
    server.write(&edges(0, NODES)).await;
    let mut client = Client::connect(&server).await;

    let started = std::time::Instant::now();
    let reply = client.execute("closure", CLOSURE).await;
    assert_exhausted(&reply, "max_query_memory_bytes");
    // Stopped inside the fixpoint, not after it: the whole closure takes
    // minutes in a debug build.
    let stopped_in = started.elapsed();
    assert!(stopped_in < Duration::from_secs(60), "took {stopped_in:?}");

    // A query that fits runs on the same connection, and so does the same
    // closure bound to one start node.
    let reply = client.execute("bound", "?edge(0, Y)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert!(reply["row_count"].as_u64().unwrap() > 0, "{reply}");
}

/// The query's failure in a program whose earlier statements committed:
/// those keep their results, the query is refused for memory.
fn assert_query_exhausted(reply: &Value, query_index: u64) {
    assert_eq!(reply["type"], "result", "{reply}");
    let errors = reply["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1, "{reply}");
    assert_eq!(errors[0]["index"], query_index, "{reply}");
    assert_eq!(errors[0]["code"], "resource_exhausted", "{reply}");
    let message = errors[0]["message"].as_str().unwrap();
    assert!(message.contains("max_query_memory_bytes"), "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_runaway_query_after_a_commit_in_the_same_program_is_still_refused() {
    let server = start_server(|config| {
        config.storage.performance.max_query_memory_bytes = 32 << 20;
        config.storage.performance.query_timeout_ms = 0;
    })
    .await;
    server.write(&edges(0, NODES)).await;
    let mut client = Client::connect(&server).await;

    // A durable meta command enters the commit before the query runs.
    let started = std::time::Instant::now();
    let reply = client
        .execute("compact", &format!(".compact\n{CLOSURE}"))
        .await;
    assert_query_exhausted(&reply, 3);
    let stopped_in = started.elapsed();
    assert!(stopped_in < Duration::from_secs(60), "took {stopped_in:?}");

    // So does a write; it stays committed.
    let reply = client
        .execute("write", &format!("+note(1)\n{CLOSURE}"))
        .await;
    assert_query_exhausted(&reply, 3);
    assert_eq!(server.count("note"), 1);

    let reply = client.execute("bound", "?edge(0, Y)").await;
    assert_eq!(reply["type"], "result", "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_proof_after_a_commit_is_refused_as_resource_exhausted() {
    let server = start_server(|config| {
        config.storage.performance.max_query_memory_bytes = 32 << 20;
        config.storage.performance.query_timeout_ms = 0;
    })
    .await;
    server.write(&edges(0, NODES)).await;
    server
        .write("+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)")
        .await;
    let mut client = Client::connect(&server).await;

    let reply = client.execute("why", ".compact\n.why ?reach(X, Y)").await;
    assert_query_exhausted(&reply, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_conditional_delete_after_the_commit_began_is_refused_as_resource_exhausted() {
    let server = start_server(|config| {
        config.storage.performance.max_query_memory_bytes = 32 << 20;
        config.storage.performance.query_timeout_ms = 0;
    })
    .await;
    server.write(&edges(0, NODES)).await;
    server
        .write(
            "+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             +spare(X) <- edge(X, 0)",
        )
        .await;
    let stored = server.count("edge");
    let mut client = Client::connect(&server).await;

    // `.compact` cannot share a program with writes; dropping a rule is a
    // write that enters the commit before the delete stages.
    let reply = client
        .execute("delete", ".rule drop spare\n-edge(X, Y) <- reach(X, Y)")
        .await;
    assert_query_exhausted(&reply, 1);
    assert_eq!(server.count("edge"), stored, "nothing was deleted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_under_the_limit_is_unaffected() {
    let server = start_server(|config| {
        config.storage.performance.max_query_memory_bytes = 32 << 20;
    })
    .await;
    server.write("+edge[(1, 2), (2, 3), (3, 4)]").await;
    let mut client = Client::connect(&server).await;
    let reply = client.execute("closure", CLOSURE).await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert_eq!(reply["row_count"], 6, "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_past_the_graph_budget_is_refused_and_deletes_still_work() {
    let server = start_server(|config| {
        config.storage.performance.max_graph_memory_bytes = 64 << 10;
    })
    .await;
    let mut client = Client::connect(&server).await;

    // A few hundred tuples fit in 64 KiB; a thousand more do not.
    let reply = client.execute("small", &edges(0, 100)).await;
    assert_eq!(reply["type"], "result", "{reply}");
    let stored = server.count("edge");
    assert!(stored > 250, "{stored}");

    let reply = client.execute("big", &edges(100, 500)).await;
    assert_exhausted(&reply, "max_graph_memory_bytes");
    assert_eq!(server.count("edge"), stored, "nothing was applied");

    // In a program, the refusal is the inserting statement's and nothing
    // of the program is applied.
    let reply = client
        .execute("program", &format!("+note(1)\n{}", edges(100, 500)))
        .await;
    assert_eq!(reply["type"], "result", "{reply}");
    let errors = reply["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1, "{reply}");
    assert_eq!(errors[0]["index"], 1, "{reply}");
    assert_eq!(errors[0]["code"], "resource_exhausted", "{reply}");
    assert_eq!(server.count("note"), 0, "nothing was applied");

    // Deletes always pass; the space they free takes new facts.
    let reply = client.execute("delete", "-edge(X, Y) <- edge(X, Y)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert_eq!(server.count("edge"), 0);
    let reply = client.execute("again", &edges(100, 200)).await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert!(server.count("edge") > 250);
}

/// One-column relations `a`, `b` and `c` of `rows` rows each, written in
/// inserts the default insert limit takes.
async fn wide(server: &Server, rows: i64) {
    for relation in ["a", "b", "c"] {
        for start in (0..rows).step_by(10_000) {
            let chunk: Vec<String> = (start..(start + 10_000).min(rows))
                .map(|i| format!("({i})"))
                .collect();
            server
                .write(&format!("+{relation}[{}]", chunk.join(", ")))
                .await;
        }
    }
}

/// Every pair of `a` and `b`, and the same pairs filtered by a negation.
const CROSS_PRODUCTS: [&str; 2] = ["?a(X), b(Y)", "?a(X), b(Y), !c(Y)"];

#[tokio::test(flavor = "multi_thread")]
async fn a_large_cross_product_is_refused_before_it_runs_on_shipped_defaults() {
    let server = start_server(|_| {}).await;
    wide(&server, 30_000).await;
    let mut client = Client::connect(&server).await;

    for program in CROSS_PRODUCTS {
        let started = std::time::Instant::now();
        let reply = client.execute("cross", program).await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert_eq!(reply["code"], "validation", "{reply}");
        let message = reply["message"].as_str().unwrap();
        assert!(
            message.contains("a cross product of about 900000000 rows")
                && message.contains("max_query_cost"),
            "{reply}"
        );
        let refused_in = started.elapsed();
        assert!(refused_in < Duration::from_secs(5), "took {refused_in:?}");
    }

    // The same relations joined on a shared variable run.
    let reply = client.execute("keyed", "?a(X), b(X), X < 3").await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert_eq!(reply["row_count"], 3, "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cross_product_is_estimated_with_the_rows_its_filters_keep() {
    let server = start_server(|_| {}).await;
    wide(&server, 30_000).await;
    let mut client = Client::connect(&server).await;

    // One row of `a` matches the constant, one the comparison: each side
    // meets `b` with a few rows, not 30,000.
    for (program, rows) in [("?a(7), b(Y)", 30_000), ("?a(X), X > 29998, b(Y)", 30_000)] {
        let reply = client.execute("filtered", program).await;
        assert_eq!(reply["type"], "result", "{program}: {reply}");
        assert_eq!(reply["row_count"], rows, "{program}: {reply}");
    }

    // Filters that still leave 20,000 rows of `a` to pair with all of `b`.
    let started = std::time::Instant::now();
    let reply = client.execute("wide", "?a(X), X < 20000, b(Y)").await;
    assert_eq!(reply["type"], "error", "{reply}");
    assert_eq!(reply["code"], "validation", "{reply}");
    let message = reply["message"].as_str().unwrap();
    assert!(
        message.contains("a cross product of about 600000000 rows")
            && message.contains("max_query_cost"),
        "{reply}"
    );
    let refused_in = started.elapsed();
    assert!(refused_in < Duration::from_secs(5), "took {refused_in:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cross_product_past_the_memory_limit_is_stopped_while_it_forms_pairs() {
    let server = start_server(|config| {
        config.storage.performance.max_query_cost = 0;
        config.storage.performance.max_query_memory_bytes = 32 << 20;
        config.storage.performance.query_timeout_ms = 0;
    })
    .await;
    wide(&server, 3000).await;
    let mut client = Client::connect(&server).await;

    for program in CROSS_PRODUCTS {
        // A join forms all 9 million pairs (about 1 GB) in one dataflow
        // step: the limit must stop it inside that step.
        let started = std::time::Instant::now();
        let reply = client.execute("cross", program).await;
        assert_exhausted(&reply, "max_query_memory_bytes");
        let stopped_in = started.elapsed();
        assert!(stopped_in < Duration::from_secs(20), "took {stopped_in:?}");
    }

    let reply = client.execute("after", "?a(X), X < 3").await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert_eq!(reply["row_count"], 3, "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cross_product_past_its_deadline_is_stopped_while_others_answer() {
    let server = start_server(|config| {
        config.storage.performance.max_query_cost = 0;
        config.storage.performance.max_query_memory_bytes = 0;
    })
    .await;
    wide(&server, 3000).await;
    let mut client = Client::connect(&server).await;
    let mut other = Client::connect(&server).await;

    for program in CROSS_PRODUCTS {
        let started = std::time::Instant::now();
        let reply = client.execute_within("cross", program, 300).await;
        assert_eq!(reply["type"], "error", "{reply}");
        assert_eq!(reply["code"], "deadline_exceeded", "{reply}");
        let stopped_in = started.elapsed();
        assert!(stopped_in < Duration::from_secs(20), "took {stopped_in:?}");

        let reply = other.execute("other", "?c(X), X < 3").await;
        assert_eq!(reply["type"], "result", "{reply}");
        assert_eq!(reply["row_count"], 3, "{reply}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recursion_without_a_bound_stops_at_the_iteration_ceiling() {
    let server = start_server(|config| {
        config.storage.performance.max_recursion_iterations = 200;
        config.storage.performance.query_timeout_ms = 0;
    })
    .await;
    server.write("+seed[(0)]").await;
    let mut client = Client::connect(&server).await;

    // Session rules, then the query: the query's statement fails.
    let reply = client
        .execute(
            "counter",
            "nat(N) <- seed(N)\nnat(N) <- nat(M), N = M + 1\n?nat(X)",
        )
        .await;
    assert_eq!(reply["type"], "result", "{reply}");
    let errors = reply["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1, "{reply}");
    assert_eq!(errors[0]["index"], 2, "{reply}");
    assert_eq!(errors[0]["code"], "validation", "{reply}");
    let message = errors[0]["message"].as_str().unwrap();
    assert!(message.contains("within 200 iterations"), "{reply}");

    // Recursion that converges under the ceiling is unaffected.
    server
        .write("+upto(N) <- seed(N)\n+upto(N) <- upto(M), N = M + 1, N < 100")
        .await;
    let reply = client.execute("bounded", "?upto(X)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    assert_eq!(reply["row_count"], 100, "{reply}");
}
