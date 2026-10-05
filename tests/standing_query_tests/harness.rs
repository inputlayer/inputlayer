//! A real server on a loopback port and a raw `/ws` client.

use std::collections::VecDeque;
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

pub const KG: &str = "subs";
pub const PASSWORD: &str = "standing-query-test-pw";
pub const TIMEOUT: Duration = Duration::from_secs(20);

pub struct Server {
    pub handler: Arc<Handler>,
    pub addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
    _tmp: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn start_server(max_subscriptions: usize) -> Server {
    start_server_with(max_subscriptions, |_| {}).await
}

pub async fn start_server_with(
    max_subscriptions: usize,
    configure: impl FnOnce(&mut Config),
) -> Server {
    start_server_adjusted(max_subscriptions, configure, |handler| handler).await
}

/// A server of `configure`d config whose handler `adjust` builds on.
pub async fn start_server_adjusted(
    max_subscriptions: usize,
    configure: impl FnOnce(&mut Config),
    adjust: impl FnOnce(Handler) -> Handler,
) -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_subscriptions = max_subscriptions;
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.gui.enabled = false;
    configure(&mut config);
    let handler = Handler::from_config(config).unwrap();
    let permits = handler.compute_permits().max(4);
    let handler = Arc::new(adjust(handler.with_compute_permits(permits)));
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
    pub async fn write(&self, program: &str) {
        self.handler
            .execute_program(None, Some(KG.to_string()), program.to_string(), None)
            .await
            .unwrap_or_else(|e| panic!("write {program:?} failed: {e}"));
    }

    pub fn evaluations(&self) -> u64 {
        self.handler.subscription_metrics().evaluations()
    }

    pub async fn wait_for_active(&self, expected: u64) {
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

pub struct Client {
    pub ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// Subscription pushes received while waiting for a reply.
    pub pushes: VecDeque<Value>,
}

impl Client {
    /// Connect to the test knowledge graph as the admin.
    pub async fn connect(server: &Server) -> Self {
        Self::connect_as(server, KG, "admin", PASSWORD).await
    }

    /// Connect to `kg` as `username`.
    pub async fn connect_as(server: &Server, kg: &str, username: &str, password: &str) -> Self {
        let url = format!("ws://{}/ws?kg={kg}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self {
            ws,
            pushes: VecDeque::new(),
        };
        client
            .send(json!({"type": "login", "username": username, "password": password}))
            .await;
        let reply = client.recv().await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        client
    }

    pub async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    pub async fn recv(&mut self) -> Value {
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
    pub async fn execute(&mut self, program: &str) -> Value {
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

    pub async fn next_push(&mut self) -> Value {
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
    pub async fn next_push_for(&mut self, subscription: &str) -> Value {
        let push = self.next_push().await;
        assert_eq!(push["subscription"], subscription, "unexpected push {push}");
        push
    }

    pub async fn subscribe(&mut self, id: &str, query: &str) -> Value {
        let reply = self.execute(&format!(".subscribe {id} {query}")).await;
        assert_eq!(reply["type"], "result", "subscribe failed: {reply}");
        reply
    }
}

pub fn rows(value: &Value) -> Vec<Value> {
    value.as_array().unwrap().clone()
}

pub async fn admin(server: &Server, program: &str) {
    server
        .handler
        .execute_program(None, None, program.to_string(), None)
        .await
        .unwrap_or_else(|e| panic!("{program:?} failed: {e}"));
}
