//! Credential revocation over a real `/ws` connection: revoking an API key,
//! changing a password or dropping a user ends exactly the sessions bound to
//! that credential, and nothing computed for them leaves after the fence.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use inputlayer::auth::INTERNAL_KG;
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};

const KG: &str = "shared";
const ADMIN_PASSWORD: &str = "revocation-admin-pw";
const BOB_PASSWORD: &str = "bob-password";
const TIMEOUT: Duration = Duration::from_secs(20);
const REVOKED: &str = "Credential revoked; reconnect with valid credentials";

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

/// A server with KG `shared` and user `bob` (global editor, viewer on `shared`).
async fn start_server() -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(ADMIN_PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.ws_auth_timeout_ms = 60_000;
    config.http.gui.enabled = false;
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth();
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    handler
        .handle_user_create("bob", BOB_PASSWORD, "editor")
        .unwrap();
    handler.handle_kg_acl_grant(KG, "bob", "viewer").unwrap();
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
    server.write("+d[(0,)]").await;
    server
}

impl Server {
    /// Commit a change to `shared` as another client would.
    async fn write(&self, program: &str) {
        self.handler
            .execute_program(None, Some(KG.to_string()), program.to_string(), None)
            .await
            .unwrap_or_else(|e| panic!("write {program:?} failed: {e}"));
    }

    fn key(&self, label: &str, owner: &str) -> String {
        self.handler.create_api_key(label, owner).unwrap()
    }

    async fn wait_for_active(&self, expected: u64) {
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        while self.handler.subscription_metrics().active() != expected {
            assert!(tokio::time::Instant::now() < deadline, "subscriptions");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

enum Login<'a> {
    Key(&'a str),
    Password(&'a str, &'a str),
}

struct Client {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    async fn try_connect(server: &Server, kg: &str, login: Login<'_>) -> (Self, Value) {
        Self::try_connect_at(server, &format!("kg={kg}"), login).await
    }

    async fn try_connect_at(server: &Server, query: &str, login: Login<'_>) -> (Self, Value) {
        let url = format!("ws://{}/ws?{query}", server.addr);
        let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let mut client = Self { ws };
        client
            .send(match login {
                Login::Key(key) => json!({"type": "authenticate", "api_key": key}),
                Login::Password(user, password) => {
                    json!({"type": "login", "username": user, "password": password})
                }
            })
            .await;
        let reply = client.recv().await.expect("closed during auth");
        (client, reply)
    }

    async fn connect(server: &Server, login: Login<'_>) -> Self {
        let (client, reply) = Self::try_connect(server, KG, login).await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        client
    }

    async fn send(&mut self, value: Value) {
        self.ws
            .send(Message::Text(value.to_string()))
            .await
            .unwrap();
    }

    /// Next text frame, or `None` once the server closed the connection.
    async fn recv(&mut self) -> Option<Value> {
        loop {
            match tokio::time::timeout(TIMEOUT, self.ws.next())
                .await
                .expect("timed out waiting for a frame")
            {
                Some(Ok(Message::Text(text))) => return Some(serde_json::from_str(&text).unwrap()),
                Some(Ok(Message::Close(_)) | Err(_)) | None => return None,
                Some(Ok(_)) => {}
            }
        }
    }

    /// Every frame until the server closes the connection.
    async fn drain(&mut self) -> Vec<Value> {
        let mut frames = Vec::new();
        while let Some(frame) = self.recv().await {
            frames.push(frame);
        }
        frames
    }

    /// Run a program; returns its `result` or `error` reply.
    async fn execute(&mut self, program: &str) -> Value {
        self.send(json!({"type": "execute", "program": program}))
            .await;
        loop {
            let frame = self.recv().await.expect("closed before reply");
            if matches!(frame["type"].as_str(), Some("result" | "error")) {
                return frame;
            }
        }
    }

    async fn assert_live(&mut self) {
        let reply = self.execute("?d(X)").await;
        assert_eq!(reply["type"], "result", "{reply}");
    }

    /// Frames before the persistent update of `relation` in `shared`.
    async fn frames_before_update_of(&mut self, relation: &str) -> Vec<Value> {
        let mut frames = Vec::new();
        loop {
            let frame = self.recv().await.expect("closed");
            if frame["type"] == "persistent_update"
                && frame["knowledge_graph"] == KG
                && frame["relation"] == relation
            {
                return frames;
            }
            frames.push(frame);
        }
    }
}

/// The frames a revoked connection receives: only the notice, then close.
fn assert_revoked(frames: &[Value]) {
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0]["type"], "error");
    assert_eq!(frames[0]["message"], REVOKED);
}

#[tokio::test(flavor = "multi_thread")]
async fn revoking_a_key_ends_only_its_sessions() {
    let server = start_server().await;
    let (k1, k2) = (server.key("bob-1", "bob"), server.key("bob-2", "bob"));
    let mut via_k1 = Client::connect(&server, Login::Key(&k1)).await;
    let mut also_k1 = Client::connect(&server, Login::Key(&k1)).await;
    let mut via_k2 = Client::connect(&server, Login::Key(&k2)).await;
    let mut via_password = Client::connect(&server, Login::Password("bob", BOB_PASSWORD)).await;

    server.handler.handle_apikey_revoke("bob-1").unwrap();

    assert_revoked(&via_k1.drain().await);
    assert_revoked(&also_k1.drain().await);
    via_k2.assert_live().await;
    via_password.assert_live().await;
    let (_, reply) = Client::try_connect(&server, KG, Login::Key(&k1)).await;
    assert_eq!(reply["type"], "auth_error", "{reply}");
}

#[tokio::test(flavor = "multi_thread")]
async fn password_change_ends_sessions_on_the_old_password_only() {
    let server = start_server().await;
    let key = server.key("bob-key", "bob");
    let mut old_password = Client::connect(&server, Login::Password("bob", BOB_PASSWORD)).await;
    let mut via_key = Client::connect(&server, Login::Key(&key)).await;
    let mut other_user = Client::connect(&server, Login::Password("admin", ADMIN_PASSWORD)).await;

    server
        .handler
        .handle_user_password("bob", "rotated")
        .unwrap();

    assert_revoked(&old_password.drain().await);
    via_key.assert_live().await;
    other_user.assert_live().await;
    let (_, reply) = Client::try_connect(&server, KG, Login::Password("bob", BOB_PASSWORD)).await;
    assert_eq!(reply["type"], "auth_error", "{reply}");
    Client::connect(&server, Login::Password("bob", "rotated"))
        .await
        .assert_live()
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn revoking_your_own_credential_withholds_the_reply() {
    let server = start_server().await;
    let admin_key = server.key("self", "admin");
    let mut client = Client::connect(&server, Login::Key(&admin_key)).await;
    client
        .send(json!({"type": "execute", "program": ".apikey revoke self"}))
        .await;
    assert_revoked(&client.drain().await);
    assert!(server.handler.authenticate_api_key(&admin_key).is_err());

    let mut client = Client::connect(&server, Login::Password("admin", ADMIN_PASSWORD)).await;
    client
        .send(json!({"type": "execute", "program": ".user password admin changed-pw"}))
        .await;
    assert_revoked(&client.drain().await);
    Client::connect(&server, Login::Password("admin", "changed-pw")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn revocation_during_evaluation_withholds_its_rows() {
    let server = start_server().await;
    let edges: Vec<String> = (0..400).map(|i| format!("({i}, {})", i + 1)).collect();
    server
        .write(&format!(
            "+edge[{}]\n+reach(X, Y) <- edge(X, Y)\n+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
            edges.join(", ")
        ))
        .await;
    let query = "?reach(X, Y)";

    let key = server.key("bob-slow", "bob");
    let principal = server.handler.authenticate_api_key(&key).unwrap();
    let started = Instant::now();
    server
        .handler
        .execute_program(
            None,
            Some(KG.to_string()),
            query.to_string(),
            Some(&principal),
        )
        .await
        .unwrap();
    let evaluation = started.elapsed();
    assert!(
        evaluation >= Duration::from_millis(100),
        "the query must be slow enough to revoke during it ({evaluation:?})"
    );

    let mut client = Client::connect(&server, Login::Key(&key)).await;
    client
        .send(json!({"type": "execute", "program": query}))
        .await;
    tokio::time::sleep(evaluation / 4).await;
    server.handler.handle_apikey_revoke("bob-slow").unwrap();
    assert_revoked(&client.drain().await);
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_committed_after_the_fence_reaches_the_revoked_session() {
    let server = start_server().await;
    let key = server.key("bob-sub", "bob");
    let mut client = Client::connect(&server, Login::Key(&key)).await;
    let reply = client.execute(".subscribe all ?d(X)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(1).await;

    server.handler.handle_apikey_revoke("bob-sub").unwrap();
    server.write("+d[(1,)]").await;

    assert_revoked(&client.drain().await);
    server.wait_for_active(0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_user_stops_its_standing_queries() {
    let server = start_server().await;
    let key = server.key("bob-drop", "bob");
    let mut via_key = Client::connect(&server, Login::Key(&key)).await;
    let mut via_password = Client::connect(&server, Login::Password("bob", BOB_PASSWORD)).await;
    let mut admin = Client::connect(&server, Login::Password("admin", ADMIN_PASSWORD)).await;
    for client in [&mut via_key, &mut via_password, &mut admin] {
        let reply = client.execute(".subscribe all ?d(X)").await;
        assert_eq!(reply["type"], "result", "{reply}");
    }
    server.wait_for_active(3).await;

    server.handler.handle_user_drop("bob").unwrap();

    assert_revoked(&via_key.drain().await);
    assert_revoked(&via_password.drain().await);
    server.wait_for_active(1).await;
    server.write("+d[(2,)]").await;
    let mut delta = admin.recv().await.unwrap();
    while delta["type"] != "subscription_delta" {
        delta = admin.recv().await.unwrap();
    }
    assert_eq!(delta["inserted"], json!([[2]]), "{delta}");
}

#[tokio::test(flavor = "multi_thread")]
async fn kg_and_internal_notices_reach_only_authorized_sessions() {
    let server = start_server().await;
    let key = server.key("bob-notices", "bob");
    let mut bob = Client::connect(&server, Login::Key(&key)).await;
    let mut admin = Client::connect(&server, Login::Password("admin", ADMIN_PASSWORD)).await;

    // An admin creates a KG bob has no access to; internal changes are announced.
    let admin_key = server.key("admin-notices", "admin");
    let admin_principal = server.handler.authenticate_api_key(&admin_key).unwrap();
    server
        .handler
        .execute_program(
            None,
            Some(KG.to_string()),
            ".kg create private_kg".to_string(),
            Some(&admin_principal),
        )
        .await
        .unwrap();
    server
        .handler
        .notify_persistent_update(INTERNAL_KG, "users", "insert", 1);
    server.handler.notify_kg_change(INTERNAL_KG, "created");
    server.write("+marker[(1,)]").await;

    let seen_by_bob = bob.frames_before_update_of("marker").await;
    assert!(seen_by_bob.is_empty(), "bob saw {seen_by_bob:?}");
    let seen_by_admin = admin.frames_before_update_of("marker").await;
    assert_eq!(seen_by_admin.len(), 1, "{seen_by_admin:?}");
    assert_eq!(seen_by_admin[0]["type"], "kg_change");
    assert_eq!(seen_by_admin[0]["knowledge_graph"], "private_kg");

    // Replay on reconnect applies the same visibility.
    for (login, expected_kg_changes) in [
        (Login::Key(&key), 0),
        (Login::Password("admin", ADMIN_PASSWORD), 1),
    ] {
        let (mut client, reply) =
            Client::try_connect_at(&server, &format!("kg={KG}&last_seq=0"), login).await;
        assert_eq!(reply["type"], "authenticated", "{reply}");
        let replayed = client.frames_before_update_of("marker").await;
        assert!(
            replayed
                .iter()
                .all(|frame| frame["knowledge_graph"] != INTERNAL_KG),
            "{replayed:?}"
        );
        let kg_changes = replayed
            .iter()
            .filter(|frame| frame["type"] == "kg_change")
            .count();
        assert_eq!(kg_changes, expected_kg_changes, "{replayed:?}");
    }
}
