//! `/ws` surface hygiene: no session-id WebSocket route, no secrets or
//! session ids in INFO logs.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const KG: &str = "hygiene";
const PASSWORD: &str = "ws-hygiene-admin-pw";
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

async fn start_server() -> Server {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    config.http.rate_limit.ws_max_messages_per_sec = 0;
    config.http.gui.enabled = false;
    config.storage.performance.slow_query_log_ms = 1;
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

/// Next `authenticated`, `result` or `error` reply.
async fn recv(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> Value {
    loop {
        let msg = tokio::time::timeout(TIMEOUT, ws.next())
            .await
            .expect("timed out")
            .expect("closed")
            .unwrap();
        if let Message::Text(text) = msg {
            let v: Value = serde_json::from_str(&text).unwrap();
            if matches!(
                v["type"].as_str(),
                Some("authenticated" | "result" | "error")
            ) {
                return v;
            }
        }
    }
}

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl LogBuffer {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for LogBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::test]
async fn session_scoped_ws_route_is_gone() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let server = start_server().await;
    let key = server
        .handler
        .handle_apikey_create("probe", "admin", None, None)
        .unwrap();
    let api_key = key.rows[0].values[1].as_str().unwrap().to_string();
    let session_id = server.handler.create_session(KG).unwrap();
    for prefix in ["", "/v1"] {
        let url = format!("ws://{}{prefix}/sessions/{session_id}/ws", server.addr);
        for (bearer, expected) in [(None, 401), (Some(&api_key), 404)] {
            let mut req = url.as_str().into_client_request().unwrap();
            if let Some(key) = bearer {
                req.headers_mut()
                    .insert("authorization", format!("Bearer {key}").parse().unwrap());
            }
            match tokio_tungstenite::connect_async(req).await {
                Err(tungstenite::Error::Http(resp)) => assert_eq!(resp.status(), expected, "{url}"),
                other => panic!("expected {expected} for {url}, got {other:?}"),
            }
        }
    }
    assert!(server.handler.session_manager().has_session(&session_id));
}

/// Runs on the current-thread runtime, so the thread-local subscriber also
/// captures the server task's events.
#[tokio::test]
async fn info_logs_carry_no_credentials_or_session_ids() {
    let logs = LogBuffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let server = start_server().await;
    let url = format!("ws://{}/ws?kg={KG}", server.addr);
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let login = json!({"type": "login", "username": "admin", "password": PASSWORD});
    ws.send(Message::Text(login.to_string())).await.unwrap();
    let auth = recv(&mut ws).await;
    assert_eq!(auth["type"], "authenticated", "{auth}");
    let session_id = auth["session_id"].as_str().unwrap().to_string();

    for program in [
        ".user create bob bobs-pw-123 viewer",
        ".user password bob bobs-new-pw-456",
        ".USER create carol carols-pw-789 viewer",
        ".User Password carol carols-new-pw-012",
    ] {
        let exec = json!({"type": "execute", "program": program});
        ws.send(Message::Text(exec.to_string())).await.unwrap();
        let reply = recv(&mut ws).await;
        assert_eq!(reply["type"], "result", "{program}: {reply}");
    }
    ws.close(None).await.unwrap();
    drop(ws);
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while server.handler.session_manager().has_session(&session_id) {
        assert!(tokio::time::Instant::now() < deadline, "session not closed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let out = logs.contents();
    assert!(out.contains("ws_execute_start"), "{out}");
    assert!(out.contains(".user create <redacted>"), "{out}");
    assert!(out.contains(".user password <redacted>"), "{out}");
    for secret in [
        "bobs-pw-123",
        "bobs-new-pw-456",
        "carols-pw-789",
        "carols-new-pw-012",
        PASSWORD,
    ] {
        assert!(!out.contains(secret), "logged {secret}: {out}");
    }
    assert!(!out.contains("session_id"), "{out}");
}
