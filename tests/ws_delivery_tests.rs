//! Large results and deltas over a real `/ws` connection: delivered whole,
//! or reported, never cut.
//!
//! - a delta over the frame limit streams as one logical delta;
//! - a row no frame can carry resets its subscription (and fails `.subscribe`
//!   without registering), after which nothing more is pushed for it;
//! - a client that disconnects mid-stream, or stops reading, loses only its
//!   own connection: the server releases it and other subscribers keep
//!   receiving.
//!
//! Writes go straight through the `Handler`, as another client's commits.

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

const KG: &str = "delivery";
const PASSWORD: &str = "delivery-test-password";
const TIMEOUT: Duration = Duration::from_secs(60);
/// The engine's frame limit (`MAX_MESSAGE_SIZE`).
const MAX_FRAME: usize = 16 * 1024 * 1024;
/// Streamed chunk frames stay near this size (`FRAME_BUDGET`).
const FRAME_BUDGET: usize = 1024 * 1024;

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
            .unwrap_or_else(|e| panic!("write failed: {e}"));
    }

    /// `n` rows of `big` once `switch(1)` holds, each with a `pad`-byte string.
    async fn install_big(&self, n: usize, pad: usize) {
        let facts: Vec<String> = (0..n).map(|i| format!("({i})")).collect();
        self.write(&format!("+n[{}]", facts.join(", "))).await;
        self.write(&format!("+pad(\"{}\")", "x".repeat(pad))).await;
        self.write("+switch(0)").await;
        self.write("+big(X, P) <- n(X), switch(1), pad(P)").await;
    }

    fn active(&self) -> u64 {
        self.handler.subscription_metrics().active()
    }

    async fn wait_until(&self, what: &str, done: impl Fn(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while !done(self) {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
        let (_, reply) = client.recv().await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        client
    }

    async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    /// The next text frame and its size; skips change notifications.
    async fn recv(&mut self) -> (usize, Value) {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for a frame")
                .expect("connection closed")
                .unwrap();
            if let Message::Text(text) = msg {
                let value: Value = serde_json::from_str(&text).unwrap();
                let kind = value["type"].as_str().unwrap_or_default();
                if !kind.ends_with("_update") && !kind.ends_with("_change") {
                    return (text.len(), value);
                }
            }
        }
    }

    /// Run `program`; returns its reply frames (one, or a whole stream).
    async fn execute(&mut self, program: &str) -> Vec<Value> {
        self.send(json!({"type": "execute", "program": program}))
            .await;
        let (_, first) = self.recv().await;
        if first["type"] != "result_start" {
            return vec![first];
        }
        let mut frames = vec![first];
        loop {
            let (bytes, frame) = self.recv().await;
            assert!(bytes <= MAX_FRAME, "{bytes}-byte frame");
            let end = frame["type"] != "result_chunk";
            frames.push(frame);
            if end {
                return frames;
            }
        }
    }

    /// The next push, or `None` when nothing arrives within `within`.
    async fn push_within(&mut self, within: Duration) -> Option<Value> {
        tokio::time::timeout(within, self.recv())
            .await
            .ok()
            .map(|(_, frame)| frame)
    }
}

fn rows(value: &Value) -> &Vec<Value> {
    value.as_array().unwrap()
}

/// Read one streamed delta for `subscription` and check it is whole.
async fn read_streamed_delta(client: &mut Client, subscription: &str) -> (Value, usize) {
    let (_, start) = client.recv().await;
    assert_eq!(start["type"], "subscription_delta_start", "{start}");
    assert_eq!(start["subscription"], subscription);
    let mut inserted = 0;
    let mut chunks = 0;
    loop {
        let (bytes, frame) = client.recv().await;
        assert!(bytes <= FRAME_BUDGET * 2, "a {bytes}-byte chunk");
        assert_eq!(frame["seq"], start["seq"], "{frame}");
        match frame["type"].as_str().unwrap() {
            "subscription_delta_chunk" => {
                assert_eq!(frame["chunk_index"], chunks);
                inserted += rows(&frame["inserted"]).len();
                chunks += 1;
            }
            "subscription_delta_end" => {
                assert_eq!(frame["chunk_count"], chunks);
                assert_eq!(frame["inserted_count"], inserted);
                return (start, inserted);
            }
            other => panic!("{other} inside a streamed delta"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delta_over_the_frame_limit_streams_whole_and_numbering_continues() {
    let server = start_server(|_| {}).await;
    server.install_big(4_000, 5_000).await;
    let mut client = Client::connect(&server).await;
    let reply = client.execute(".subscribe big ?big(X, P)").await;
    assert_eq!(reply[0]["type"], "result", "{:?}", reply[0]);

    server.write("+switch(1)").await;
    let (start, inserted) = read_streamed_delta(&mut client, "big").await;
    assert_eq!((start["seq"].clone(), inserted), (json!(1), 4_000));

    // A small change afterwards is one ordinary frame with the next seq.
    server.write("+n(4000)").await;
    let (_, delta) = client.recv().await;
    assert_eq!(delta["type"], "subscription_delta", "{delta}");
    assert_eq!(delta["seq"], 2);
    assert_eq!(rows(&delta["inserted"]).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_no_frame_can_carry_resets_the_subscription() {
    let server = start_server(|config| {
        // Lets one fact carry a string longer than any frame.
        config.storage.performance.max_string_value_bytes = 2 * MAX_FRAME;
        config.storage.performance.max_query_size_bytes = 2 * MAX_FRAME;
    })
    .await;
    server.install_big(1, MAX_FRAME + 1).await;
    let mut client = Client::connect(&server).await;
    let reply = client.execute(".subscribe big ?big(X, P)").await;
    assert_eq!(reply[0]["type"], "result", "{:?}", reply[0]);
    assert_eq!(server.active(), 1);

    server.write("+switch(1)").await;
    let (_, reset) = client.recv().await;
    assert_eq!(reset["type"], "subscription_reset", "{reset}");
    assert_eq!(reset["subscription"], "big");
    assert!(
        reset["message"].as_str().unwrap().contains("message limit"),
        "{reset}"
    );
    assert_eq!(server.active(), 0, "the subscription is gone");

    // Nothing more is pushed for it, and the id is free again.
    server.write("+n(1)").await;
    assert_eq!(client.push_within(Duration::from_millis(300)).await, None);
    let reply = client.execute(".unsubscribe big").await;
    assert_eq!(reply[0]["type"], "error", "{:?}", reply[0]);

    // Its snapshot cannot be delivered either: `.subscribe` fails and
    // registers nothing, rather than replying with part of the result.
    let reply = client.execute(".subscribe big ?big(X, P)").await;
    assert_eq!(reply.len(), 1, "no partial stream: {reply:?}");
    assert_eq!(reply[0]["type"], "error", "{:?}", reply[0]);
    assert_eq!(server.active(), 0);
    server.write("+n(2)").await;
    assert_eq!(client.push_within(Duration::from_millis(300)).await, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_snapshot_streams_as_the_subscribe_reply() {
    let server = start_server(|_| {}).await;
    server.install_big(3_000, 1_000).await;
    server.write("+switch(1)").await;
    let mut client = Client::connect(&server).await;
    let reply = client.execute(".subscribe big ?big(X, P)").await;
    let start = &reply[0];
    assert_eq!(start["type"], "result_start", "{start}");
    assert_eq!(start["subscribed"]["subscription"], "big");
    let end = reply.last().unwrap();
    assert_eq!(end["type"], "result_end", "{end}");
    assert_eq!(end["row_count"], 3_000);
    let received: usize = reply[1..reply.len() - 1]
        .iter()
        .map(|chunk| rows(&chunk["rows"]).len())
        .sum();
    assert_eq!(received, 3_000);

    server.write("-n(0)").await;
    let (_, delta) = client.recv().await;
    assert_eq!(delta["type"], "subscription_delta", "{delta}");
    assert_eq!(delta["seq"], 1);
    assert_eq!(rows(&delta["retracted"]).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disconnect_mid_stream_releases_only_that_connection() {
    let server = start_server(|_| {}).await;
    server.install_big(4_000, 5_000).await;
    let mut leaving = Client::connect(&server).await;
    let mut staying = Client::connect(&server).await;
    leaving.execute(".subscribe big ?big(X, P)").await;
    staying.execute(".subscribe big ?big(X, P)").await;
    assert_eq!(server.active(), 2);

    server.write("+switch(1)").await;
    let (_, start) = leaving.recv().await;
    assert_eq!(start["type"], "subscription_delta_start", "{start}");
    drop(leaving);

    let (_, inserted) = read_streamed_delta(&mut staying, "big").await;
    assert_eq!(inserted, 4_000);
    server
        .wait_until("the leaving connection is released", |s| s.active() == 1)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_reading_is_disconnected_and_isolated() {
    let server = start_server(|config| config.http.ws_send_timeout_ms = 300).await;
    // ~40 MiB of delta: more than the socket buffers between server and client.
    server.install_big(8_000, 5_000).await;
    let mut stalled = Client::connect(&server).await;
    let mut reader = Client::connect(&server).await;
    stalled.execute(".subscribe big ?big(X, P)").await;
    reader.execute(".subscribe big ?big(X, P)").await;
    reader.execute(".subscribe small ?switch(X)").await;
    assert_eq!(server.active(), 3);

    server.write("+switch(1)").await;
    // `stalled` never reads again; the reader gets everything meanwhile.
    let mut big = false;
    let mut small = false;
    while !(big && small) {
        let (_, frame) = reader.recv().await;
        match (
            frame["type"].as_str().unwrap(),
            frame["subscription"].as_str(),
        ) {
            ("subscription_delta", Some("small")) => small = true,
            ("subscription_delta_start", Some("big")) => {
                let mut chunks = 0;
                loop {
                    let (_, frame) = reader.recv().await;
                    if frame["type"] == "subscription_delta_end" {
                        assert_eq!(frame["inserted_count"], 8_000);
                        break;
                    }
                    assert_eq!(frame["chunk_index"], chunks, "{}", frame["type"]);
                    chunks += 1;
                }
                big = true;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    server
        .wait_until("the stalled connection is dropped", |s| s.active() == 2)
        .await;
    drop(stalled);
}

impl Client {
    /// Send `frame`, a `read` or `subscribe`; returns its reply frames (one
    /// `snapshot` or `error`, or a whole stream).
    async fn snapshot_request(&mut self, frame: Value) -> Vec<Value> {
        self.send(frame).await;
        let (_, first) = self.recv().await;
        if first["type"] != "snapshot_start" {
            return vec![first];
        }
        let mut frames = vec![first];
        loop {
            let (bytes, frame) = self.recv().await;
            assert!(bytes <= FRAME_BUDGET * 2, "a {bytes}-byte chunk");
            let end = frame["type"] != "snapshot_chunk";
            frames.push(frame);
            if end {
                return frames;
            }
        }
    }
}

/// Rows per result of a streamed snapshot, checked whole against its header.
fn streamed_snapshot_rows(frames: &[Value]) -> Vec<usize> {
    let start = &frames[0];
    assert_eq!(start["type"], "snapshot_start", "{start}");
    let end = frames.last().unwrap();
    assert_eq!(end["type"], "snapshot_end", "{end}");
    assert_eq!(end["chunk_count"], frames.len() - 2);
    let mut counts = vec![0; start["results"].as_array().unwrap().len()];
    for (index, chunk) in frames[1..frames.len() - 1].iter().enumerate() {
        assert_eq!(chunk["type"], "snapshot_chunk", "{chunk}");
        assert_eq!(chunk["chunk_index"], index);
        counts[chunk["result"].as_u64().unwrap() as usize] += rows(&chunk["rows"]).len();
    }
    for (i, header) in start["results"].as_array().unwrap().iter().enumerate() {
        assert_eq!(header["row_count"], counts[i], "{header}");
    }
    counts
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_group_snapshot_and_read_stream_whole() {
    let server = start_server(|_| {}).await;
    server.install_big(3_000, 1_000).await;
    server.write("+switch(1)").await;
    let mut client = Client::connect(&server).await;
    let queries = json!([
        {"name": "big", "query": "?big(X, P)"},
        {"name": "n", "query": "?n(X)"},
    ]);
    let reply = client
        .snapshot_request(json!({"type": "read", "id": "r", "queries": queries}))
        .await;
    assert_eq!(reply[0]["id"], "r");
    assert_eq!(streamed_snapshot_rows(&reply), [3_000, 3_000]);

    let reply = client
        .snapshot_request(json!({"type": "subscribe", "subscription": "w", "queries": queries}))
        .await;
    assert_eq!(reply[0]["subscribed"]["subscription"], "w");
    assert_eq!(streamed_snapshot_rows(&reply), [3_000, 3_000]);

    server.write("-n(0)").await;
    let (_, delta) = client.recv().await;
    assert_eq!(delta["type"], "subscription_group_delta", "{delta}");
    assert_eq!(delta["seq"], 1);
    assert_eq!(rows(&delta["members"][0]["retracted"]).len(), 1);
    assert_eq!(rows(&delta["members"][1]["retracted"]).len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_delta_over_the_frame_limit_streams_whole() {
    let server = start_server(|_| {}).await;
    server.install_big(4_000, 5_000).await;
    let mut client = Client::connect(&server).await;
    let reply = client
        .snapshot_request(
            json!({"type": "subscribe", "subscription": "w", "queries": [
                {"name": "switch", "query": "?switch(X)"},
                {"name": "big", "query": "?big(X, P)"},
            ]}),
        )
        .await;
    assert_eq!(reply[0]["type"], "snapshot", "{:?}", reply[0]);

    server.write("+switch(1)").await;
    let (_, start) = client.recv().await;
    assert_eq!(start["type"], "subscription_group_delta_start", "{start}");
    assert_eq!(start["members"][0]["inserted_count"], 1);
    assert_eq!(start["members"][1]["inserted_count"], 4_000);
    let mut inserted = [0, 0];
    let mut chunks = 0;
    loop {
        let (bytes, frame) = client.recv().await;
        assert!(bytes <= FRAME_BUDGET * 2, "a {bytes}-byte chunk");
        match frame["type"].as_str().unwrap() {
            "subscription_group_delta_chunk" => {
                assert_eq!(frame["chunk_index"], chunks);
                inserted[frame["member"].as_u64().unwrap() as usize] +=
                    rows(&frame["inserted"]).len();
                chunks += 1;
            }
            "subscription_group_delta_end" => {
                assert_eq!(frame["chunk_count"], chunks);
                break;
            }
            other => panic!("{other} inside a streamed group delta"),
        }
    }
    assert_eq!(inserted, [1, 4_000]);

    // A small change afterwards is one ordinary frame with the next seq.
    server.write("+n(4000)").await;
    let (_, delta) = client.recv().await;
    assert_eq!(delta["type"], "subscription_group_delta", "{delta}");
    assert_eq!(delta["seq"], 2);
    assert_eq!(delta["members"][0]["unchanged"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_group_with_a_row_no_frame_can_carry_is_reset_whole() {
    let server = start_server(|config| {
        config.storage.performance.max_string_value_bytes = 2 * MAX_FRAME;
        config.storage.performance.max_query_size_bytes = 2 * MAX_FRAME;
    })
    .await;
    server.install_big(1, MAX_FRAME + 1).await;
    let mut client = Client::connect(&server).await;
    let queries = json!([
        {"name": "n", "query": "?n(X)"},
        {"name": "big", "query": "?big(X, P)"},
    ]);
    let reply = client
        .snapshot_request(json!({"type": "subscribe", "subscription": "w", "queries": queries}))
        .await;
    assert_eq!(reply[0]["type"], "snapshot", "{:?}", reply[0]);

    server.write("+switch(1)").await;
    let (_, reset) = client.recv().await;
    assert_eq!(reset["type"], "subscription_reset", "{reset}");
    assert_eq!(reset["subscription"], "w");
    assert_eq!(server.active(), 0, "the group is gone");

    // Its snapshot cannot be delivered whole either: nothing registers.
    let reply = client
        .snapshot_request(json!({"type": "subscribe", "subscription": "w", "queries": queries}))
        .await;
    assert_eq!(reply.len(), 1, "no partial stream: {reply:?}");
    assert_eq!(reply[0]["type"], "error", "{:?}", reply[0]);
    assert_eq!(server.active(), 0);
}
