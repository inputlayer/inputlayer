//! Credential revocation and expiry over a real `/ws` connection: revoking an
//! API key, its expiry passing, changing a password or dropping a user ends
//! exactly the sessions bound to that credential, and nothing computed for
//! them leaves after the fence.

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
const EXPIRED: &str = "Credential expired; reconnect with valid credentials";

struct Server {
    handler: Arc<Handler>,
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
    /// Credential upkeep, as the server binary runs it.
    upkeep: tokio::task::JoinHandle<()>,
    _tmp: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
        self.upkeep.abort();
    }
}

/// A server with KG `shared` and user `bob` (global editor, viewer on `shared`).
async fn start_server() -> Server {
    start_server_with(|_| {}).await
}

/// [`start_server`] with `configure` applied to its config.
async fn start_server_with(configure: impl FnOnce(&mut Config)) -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(ADMIN_PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.ws_auth_timeout_ms = 60_000;
    config.http.gui.enabled = false;
    configure(&mut config);
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
    let upkeep = tokio::spawn(Arc::clone(&handler).credential_upkeep());
    let server = Server {
        handler,
        addr,
        task,
        upkeep,
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
        self.handler.create_api_key(label, owner, None).unwrap()
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
    /// Frames until the first of type `kind`; returns those before it and it.
    async fn frames_until(&mut self, kind: &str) -> (Vec<Value>, Value) {
        let mut frames = Vec::new();
        loop {
            let frame = self.recv().await.expect("closed");
            if frame["type"] == kind {
                return (frames, frame);
            }
            frames.push(frame);
        }
    }

    /// The next frame of type `kind`, skipping others.
    async fn next_push_of(&mut self, kind: &str) -> Value {
        self.frames_until(kind).await.1
    }

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
    assert_ended(frames, REVOKED);
}

/// The frames an ended connection receives: only `notice`, then close.
fn assert_ended(frames: &[Value], notice: &str) {
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0]["type"], "notice");
    let code = if notice == EXPIRED {
        "credential_expired"
    } else {
        "credential_revoked"
    };
    assert_eq!(frames[0]["code"], code);
    assert_eq!(frames[0]["message"], notice);
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
    let epoch = server.handler.notifications().epoch().to_string();
    for (login, expected_kg_changes) in [
        (Login::Key(&key), 0),
        (Login::Password("admin", ADMIN_PASSWORD), 1),
    ] {
        let cursor = format!("kg={KG}&last_seq=0&epoch={epoch}");
        let (mut client, reply) = Client::try_connect_at(&server, &cursor, login).await;
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

/// The credential fence and the result cap compose: a subscriber held at its
/// last complete result by the cap is closed by revocation, and the recovery
/// delta that a live session would get never reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn revocation_fences_a_subscription_held_at_the_result_cap() {
    let server = start_server_with(|config| {
        config.storage.performance.max_result_rows = 3;
    })
    .await;
    let key = server.key("bob-capped", "bob");
    let mut client = Client::connect(&server, Login::Key(&key)).await;
    let reply = client.execute(".subscribe all ?d(X)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(1).await;

    server.write("+d[(1,), (2,), (3,), (4,)]").await;
    let mut push = client.recv().await.unwrap();
    while push["type"] != "subscription_error" {
        assert_ne!(push["type"], "subscription_delta", "capped delta: {push}");
        push = client.recv().await.unwrap();
    }
    assert!(
        push["message"]
            .as_str()
            .unwrap()
            .contains("max_result_rows (3)"),
        "{push}"
    );

    server.handler.handle_apikey_revoke("bob-capped").unwrap();
    server.write("-d[(3,), (4,)]").await;
    let frames: Vec<Value> = client
        .drain()
        .await
        .into_iter()
        .filter(|frame| frame["type"] != "persistent_update")
        .collect();
    assert_revoked(&frames);
    server.wait_for_active(0).await;
}

/// A completed `.why` proof must still pass the credential fence before output.
/// Use the current-thread runtime so the thread-local trace subscriber also
/// observes the server task, and revoke synchronously at its output boundary.
#[tokio::test]
async fn revocation_during_a_proof_withholds_it() {
    use tracing::field::{Field, Visit};
    use tracing_subscriber::prelude::*;

    struct RevokeBeforeOutput(Arc<Handler>);

    #[derive(Default)]
    struct ExecutionEnd {
        matched: bool,
        succeeded: bool,
    }

    impl Visit for ExecutionEnd {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.matched = format!("{value:?}") == "ws_execute_end";
            }
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            if field.name() == "ok" {
                self.succeeded = value;
            }
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RevokeBeforeOutput {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut end = ExecutionEnd::default();
            event.record(&mut end);
            if end.matched {
                assert!(end.succeeded, "proof evaluation failed before the fence");
                self.0.handle_apikey_revoke("bob-why").unwrap();
            }
        }
    }

    let server = start_server().await;
    server.write("+edge[(1, 2)]").await;
    server.write("+path(X, Y) <- edge(X, Y)").await;
    let proof = ".why full ?path(X, Y)";
    let key = server.key("bob-why", "bob");
    let principal = server.handler.authenticate_api_key(&key).unwrap();
    let mut client = Client::connect(&server, Login::Key(&key)).await;

    // Establish that this request produces a proof while the credential is live.
    let result = client.execute(proof).await;
    assert_eq!(result["type"], "result", "{result}");
    assert_eq!(result["proof_trees"].as_array().unwrap().len(), 1);

    // The event runs after evaluation but before any response frame is enqueued.
    // Blocking there until revocation completes removes assumptions about proof
    // duration, scheduling, and the time needed to persist the credential change.
    let subscriber =
        tracing_subscriber::registry().with(RevokeBeforeOutput(Arc::clone(&server.handler)));
    let _guard = tracing::subscriber::set_default(subscriber);
    client
        .send(json!({"type": "execute", "program": proof}))
        .await;
    assert_revoked(&client.drain().await);
    assert!(
        principal.is_revoked(),
        "output-boundary hook did not revoke"
    );
}

/// Revoking a user's access to a knowledge graph stops its change
/// notifications and subscription rows at once, although the connection's
/// credential stays valid; a new grant lets them through again.
#[tokio::test(flavor = "multi_thread")]
async fn kg_access_revocation_stops_pushes_on_a_live_connection() {
    let server = start_server().await;
    let key = server.key("bob-acl", "bob");
    let mut bob = Client::connect(&server, Login::Key(&key)).await;
    let reply = bob.execute(".subscribe d ?d(X)").await;
    assert_eq!(reply["type"], "result", "{reply}");

    server.write("+d[(1,)]").await;
    let delta = bob.next_push_of("subscription_delta").await;
    assert_eq!(delta["inserted"], json!([[1]]), "{delta}");

    // Revoked: the write's notification is withheld, and the re-evaluation
    // it triggers fails; regranting only after that keeps the order exact.
    server.handler.handle_kg_acl_revoke(KG, "bob").unwrap();
    server.write("+d[(2,)]").await;
    let (withheld, error) = bob.frames_until("subscription_error").await;
    assert!(
        error["message"].as_str().unwrap().contains("Access denied"),
        "{error}"
    );
    server
        .handler
        .handle_kg_acl_grant(KG, "bob", "viewer")
        .unwrap();
    server.write("+marker[(1,)]").await;
    let mut frames = withheld;
    frames.extend(bob.frames_before_update_of("marker").await);
    let leaked: Vec<_> = frames
        .iter()
        .filter(|f| f["type"] == "persistent_update" || f["type"] == "subscription_delta")
        .collect();
    assert!(
        leaked.is_empty(),
        "pushed while access was revoked: {leaked:?}"
    );
}

/// Run `program` over an admin `/ws` session; its `result` frame.
async fn admin_execute(server: &Server, program: &str) -> Value {
    let mut admin = Client::connect(server, Login::Password("admin", ADMIN_PASSWORD)).await;
    let reply = admin.execute(program).await;
    assert_eq!(reply["type"], "result", "{program}: {reply}");
    reply
}

/// The `.apikey list` row of `label`, by column name.
async fn listed_key(server: &Server, label: &str) -> serde_json::Map<String, Value> {
    let list = admin_execute(server, ".apikey list").await;
    let columns = list["columns"].as_array().unwrap();
    let row = list["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row[0] == label)
        .unwrap_or_else(|| panic!("{label} not in {list}"));
    columns
        .iter()
        .zip(row.as_array().unwrap())
        .map(|(column, value)| (column.as_str().unwrap().to_string(), value.clone()))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_expiring_key_ends_its_live_sessions_and_standing_queries() {
    let server = start_server().await;
    let key = server
        .handler
        .create_api_key("bob-ttl", "bob", Some(Duration::from_secs(3)))
        .unwrap();
    let mut unrelated = Client::connect(&server, Login::Password("bob", BOB_PASSWORD)).await;
    let mut session = Client::connect(&server, Login::Key(&key)).await;
    let reply = session.execute(".subscribe all ?d(X)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(1).await;

    // No request and no write: the expiry alone ends the session.
    assert_ended(&session.drain().await, EXPIRED);
    server.wait_for_active(0).await;
    unrelated.assert_live().await;
    let (_, reply) = Client::try_connect(&server, KG, Login::Key(&key)).await;
    assert_eq!(reply["type"], "auth_error", "{reply}");
    assert_eq!(reply["message"], "API key expired", "{reply}");
    assert_eq!(listed_key(&server, "bob-ttl").await["status"], "expired");
}

/// The fence is exact at the expiry instant, with or without the upkeep sweep:
/// the first frame after it is withheld and closes the connection instead.
#[tokio::test(flavor = "multi_thread")]
async fn the_fence_alone_closes_an_expired_session_at_its_next_frame() {
    let server = start_server().await;
    server.upkeep.abort();
    let key = server
        .handler
        .create_api_key("bob-unswept", "bob", Some(Duration::from_millis(500)))
        .unwrap();
    let mut session = Client::connect(&server, Login::Key(&key)).await;
    let reply = session.execute(".subscribe all ?d(X)").await;
    assert_eq!(reply["type"], "result", "{reply}");
    server.wait_for_active(1).await;

    tokio::time::sleep(Duration::from_millis(600)).await;
    // The delta for this write is the first frame after the expiry.
    server.write("+d[(7,)]").await;
    assert_ended(&session.drain().await, EXPIRED);
    server.wait_for_active(0).await;
    assert_eq!(
        listed_key(&server, "bob-unswept").await["status"],
        "expired"
    );
}

/// Rotation as documented: create the replacement, bring the old key's
/// expiry forward to a grace period, and both work until it passes.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_grace_period_overlaps_old_and_new_keys() {
    let server = start_server().await;
    let old = admin_execute(&server, ".apikey create svc-v1").await["rows"][0][1]
        .as_str()
        .unwrap()
        .to_string();
    let mut on_old = Client::connect(&server, Login::Key(&old)).await;
    let created = admin_execute(&server, ".apikey create svc-v2 30d").await;
    assert_eq!(
        created["columns"],
        json!(["label", "api_key", "expires_at"])
    );
    assert!(created["rows"][0][2].is_i64(), "{created}");
    let new = created["rows"][0][1].as_str().unwrap().to_string();

    let reply = admin_execute(&server, ".apikey expire svc-v1 4s").await;
    assert_eq!(reply["rows"][0][0], "API key 'svc-v1' expires in 4s.");

    // Grace period: sessions on the old key continue, new sessions on either
    // key are accepted.
    let mut on_new = Client::connect(&server, Login::Key(&new)).await;
    on_old.assert_live().await;
    Client::connect(&server, Login::Key(&old))
        .await
        .assert_live()
        .await;
    on_new.assert_live().await;
    let listed = listed_key(&server, "svc-v1").await;
    assert_eq!(listed["status"], "active");
    assert!(listed["expires_at"].is_i64(), "{listed:?}");
    assert!(listed["last_used_at"].is_i64(), "{listed:?}");

    // After it: only the new key works.
    assert_ended(&on_old.drain().await, EXPIRED);
    on_new.assert_live().await;
    let (_, reply) = Client::try_connect(&server, KG, Login::Key(&old)).await;
    assert_eq!(reply["message"], "API key expired", "{reply}");
    assert_eq!(listed_key(&server, "svc-v1").await["status"], "expired");
    assert_eq!(listed_key(&server, "svc-v2").await["status"], "active");
}
