//! Operational metrics over a real server: WebSocket connections, failed
//! authentications, rejections, error replies and notices are counted where
//! they happen (#300).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use inputlayer::protocol::metrics::{AuthMethod, Rejection};
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use inputlayer_ws_protocol::ErrorCode;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const KG: &str = "metrics";
const PASSWORD: &str = "metrics-admin-password";
const TIMEOUT: Duration = Duration::from_secs(20);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
    config.http.gui.enabled = false;
    configure(&mut config);
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth().unwrap();
    handler.get_storage().create_knowledge_graph(KG).unwrap();
    let app = create_router(Arc::clone(&handler), &handler.config().http);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server {
        handler,
        addr,
        task,
        _tmp: tmp,
    }
}

async fn connect(server: &Server) -> Ws {
    let url = format!("ws://{}/ws?kg={KG}", server.addr);
    tokio_tungstenite::connect_async(url).await.unwrap().0
}

/// Next frame of one of `types`.
async fn recv(ws: &mut Ws, types: &[&str]) -> Value {
    loop {
        let msg = tokio::time::timeout(TIMEOUT, ws.next())
            .await
            .expect("timed out")
            .expect("closed")
            .unwrap();
        if let Message::Text(text) = msg {
            let v: Value = serde_json::from_str(&text).unwrap();
            if types.contains(&v["type"].as_str().unwrap_or_default()) {
                return v;
            }
        }
    }
}

async fn login(ws: &mut Ws) {
    let login = json!({"type": "login", "username": "admin", "password": PASSWORD});
    ws.send(Message::Text(login.to_string())).await.unwrap();
    let auth = recv(ws, &["authenticated", "auth_error"]).await;
    assert_eq!(auth["type"], "authenticated", "{auth}");
}

/// Wait until `condition` holds.
async fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !condition() {
        assert!(tokio::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn connections_and_failed_authentication_are_counted() {
    let server = start_server(|_| {}).await;
    let metrics = Arc::clone(server.handler.server_metrics());

    let mut ws = connect(&server).await;
    eventually("unauthenticated connection counted", || {
        metrics.ws_connections() == 1 && metrics.ws_unauthenticated_connections() == 1
    })
    .await;

    let bad_key = json!({"type": "authenticate", "api_key": "not-a-key"});
    ws.send(Message::Text(bad_key.to_string())).await.unwrap();
    assert_eq!(recv(&mut ws, &["auth_error"]).await["type"], "auth_error");
    let bad_password = json!({"type": "login", "username": "admin", "password": "wrong-password"});
    ws.send(Message::Text(bad_password.to_string()))
        .await
        .unwrap();
    assert_eq!(recv(&mut ws, &["auth_error"]).await["type"], "auth_error");
    assert_eq!(metrics.auth_failures(AuthMethod::ApiKey), 1);
    assert_eq!(metrics.auth_failures(AuthMethod::Password), 1);

    login(&mut ws).await;
    assert_eq!(metrics.ws_unauthenticated_connections(), 0);
    assert_eq!(metrics.ws_connections(), 1);

    ws.close(None).await.unwrap();
    drop(ws);
    eventually("closed connection uncounted", || {
        metrics.ws_connections() == 0
    })
    .await;
}

#[tokio::test]
async fn error_replies_and_rate_limits_are_counted() {
    let server = start_server(|config| {
        config.http.rate_limit.ws_max_messages_per_sec = 3;
    })
    .await;
    let metrics = Arc::clone(server.handler.server_metrics());
    let mut ws = connect(&server).await;
    login(&mut ws).await;

    let missing = json!({"type": "execute", "id": "q", "program": "?no_such_relation(X)"});
    ws.send(Message::Text(missing.to_string())).await.unwrap();
    let reply = recv(&mut ws, &["result", "error"]).await;
    // Two more messages in the same second: the second is over the limit
    for id in ["p1", "p2"] {
        let ping = json!({"type": "ping", "id": id});
        ws.send(Message::Text(ping.to_string())).await.unwrap();
    }
    let limited = recv(&mut ws, &["error"]).await;
    assert_eq!(limited["code"], "rate_limited", "{limited}");
    eventually("rate limit counted", || {
        metrics.rejections(Rejection::WsRateLimit) == 1
            && metrics.ws_errors(Some(ErrorCode::RateLimited)) == 1
    })
    .await;
    if reply["type"] == "error" {
        let code: Option<ErrorCode> = serde_json::from_value(reply["code"].clone()).unwrap();
        assert_eq!(metrics.ws_errors(code), 1, "{reply}");
    }
}

#[tokio::test]
async fn refused_connections_are_counted() {
    let server = start_server(|config| {
        config.http.rate_limit.max_ws_connections = 1;
    })
    .await;
    let metrics = Arc::clone(server.handler.server_metrics());
    let _first = connect(&server).await;
    let url = format!("ws://{}/ws?kg={KG}", server.addr);
    match tokio_tungstenite::connect_async(url).await {
        Err(tungstenite::Error::Http(resp)) => assert_eq!(resp.status(), 503),
        other => panic!("expected 503, got {other:?}"),
    }
    assert_eq!(metrics.rejections(Rejection::WsConnectionLimit), 1);
}
