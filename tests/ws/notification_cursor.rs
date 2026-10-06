//! Reconnect cursors over a real `/ws` connection: a client presenting
//! `last_seq` and `epoch` gets exactly the retained notifications after its
//! cursor, then live ones; any cursor the server cannot honour gets one
//! `replay_gap` notice and the connection carries on live.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use crate::harness::{config, serve};
use futures_util::{SinkExt, StreamExt};
use inputlayer::protocol::Handler;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const KG: &str = "cursor";
const PASSWORD: &str = "notification-cursor-test-pw";
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

async fn start_server(notification_buffer_size: usize) -> Server {
    let (mut config, tmp) = config(PASSWORD);
    config.http.rate_limit.notification_buffer_size = notification_buffer_size;
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth().unwrap();
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    let (addr, task) = serve(&handler).await;
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
            .unwrap_or_else(|e| panic!("write {program:?} failed: {e}"));
    }

    fn epoch(&self) -> String {
        self.handler.notifications().epoch().to_string()
    }
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    epoch: String,
}

impl Client {
    async fn connect(server: &Server, cursor: &str) -> Self {
        let url = format!("ws://{}/ws?kg={KG}{cursor}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self {
            ws,
            epoch: String::new(),
        };
        let login = json!({"type": "login", "username": "admin", "password": PASSWORD});
        client
            .ws
            .send(Message::Text(login.to_string()))
            .await
            .unwrap();
        let reply = client.recv().await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        client.epoch = reply["stream_epoch"].as_str().unwrap().to_string();
        client
    }

    async fn recv(&mut self) -> Value {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for a frame")
                .expect("connection closed")
                .unwrap();
            if let Message::Text(text) = msg {
                return serde_json::from_str(&text).unwrap();
            }
        }
    }

    /// The next `persistent_update`; fails on any other frame.
    async fn update(&mut self) -> Value {
        let frame = self.recv().await;
        assert_eq!(frame["type"], "persistent_update", "{frame}");
        frame
    }
}

fn relation_and_seq(frame: &Value) -> (String, u64) {
    (
        frame["relation"].as_str().unwrap().to_string(),
        frame["seq"].as_u64().unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_names_the_engine_runs_stream_epoch() {
    let server = start_server(64).await;
    let client = Client::connect(&server, "").await;
    assert_eq!(client.epoch, server.epoch());
    assert_eq!(client.epoch.len(), 16);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_current_cursor_replays_what_was_missed_then_continues_live() {
    let server = start_server(64).await;
    let mut client = Client::connect(&server, "").await;
    server.write("+a(1)").await;
    let (_, last_seen) = relation_and_seq(&client.update().await);
    drop(client);

    server.write("+b(1)").await;
    server.write("+c(1)").await;
    let cursor = format!("&last_seq={last_seen}&epoch={}", server.epoch());
    let mut client = Client::connect(&server, &cursor).await;
    server.write("+d(1)").await;

    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(relation_and_seq(&client.update().await));
    }
    let expected: Vec<(String, u64)> = ["b", "c", "d"]
        .iter()
        .zip(last_seen + 1..)
        .map(|(relation, seq)| ((*relation).to_string(), seq))
        .collect();
    assert_eq!(seen, expected, "replayed, then live, nothing repeated");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_from_another_run_or_without_epoch_gets_a_replay_gap() {
    let server = start_server(64).await;
    server.write("+a(1)").await;
    server.write("+a(2)").await;
    for (i, cursor) in ["&last_seq=1&epoch=0000000000000000", "&last_seq=1"]
        .into_iter()
        .enumerate()
    {
        let mut client = Client::connect(&server, cursor).await;
        let notice = client.recv().await;
        assert_eq!(notice["type"], "notice", "{cursor}: {notice}");
        assert_eq!(notice["code"], "replay_gap", "{cursor}: {notice}");
        // Nothing is replayed, and the connection carries on live.
        server.write(&format!("+live({i})")).await;
        let frame = client.update().await;
        assert_eq!(frame["relation"], "live", "{cursor}: {frame}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_evicted_cursor_gets_a_replay_gap_and_a_retained_one_replays() {
    let server = start_server(4).await;
    let mut published = server.handler.subscribe_notifications();
    for i in 0..10 {
        server.write(&format!("+r{i}(1)")).await;
    }
    // The test's own receiver has the same capacity: it lags to the newest 4.
    let mut retained_seqs = Vec::new();
    loop {
        match published.try_recv() {
            Ok(notification) => retained_seqs.push(notification.seq()),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {}
            Err(_) => break,
        }
    }
    assert_eq!(retained_seqs.len(), 4);
    let last = *retained_seqs.last().unwrap();
    // The ring keeps the newest 4: history is complete after `last - 4` only.
    let epoch = server.epoch();
    let evicted = last - 5;
    let mut client = Client::connect(&server, &format!("&last_seq={evicted}&epoch={epoch}")).await;
    let notice = client.recv().await;
    assert_eq!(notice["code"], "replay_gap", "{notice}");
    assert!(
        notice["message"]
            .as_str()
            .unwrap()
            .contains("no longer retained"),
        "{notice}"
    );

    let retained = last - 4;
    let mut client = Client::connect(&server, &format!("&last_seq={retained}&epoch={epoch}")).await;
    let mut replayed = Vec::new();
    for _ in 0..4 {
        replayed.push(relation_and_seq(&client.update().await).1);
    }
    assert_eq!(replayed, retained_seqs);
}
