//! Login hardening: per-socket failure limit, pre-auth limits, throttling by
//! real peer IP, constant-cost password checks, argon2 off the async workers.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use inputlayer::protocol::rest::create_router;
use inputlayer::protocol::Handler;
use inputlayer::Config;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const PASSWORD: &str = "auth-hardening-admin-pw";
const TIMEOUT: Duration = Duration::from_secs(20);

struct Server {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
    _tmp: TempDir,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn handler(configure: impl FnOnce(&mut Config)) -> (Arc<Handler>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.http.auth.bootstrap_admin_password = Some(PASSWORD.to_string());
    config.http.gui.enabled = false;
    // Several argon2 checks must fit in the auth window on a loaded machine.
    config.http.ws_auth_timeout_ms = 60_000;
    configure(&mut config);
    let handler = Arc::new(Handler::from_config(config).unwrap());
    handler.bootstrap_auth();
    (handler, tmp)
}

async fn start_server(configure: impl FnOnce(&mut Config)) -> Server {
    let (handler, tmp) = handler(configure);
    let app = create_router(Arc::clone(&handler), &handler.config().http);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    Server {
        addr,
        task,
        _tmp: tmp,
    }
}

async fn connect(server: &Server, forwarded_for: Option<&str>) -> Result<Ws, tungstenite::Error> {
    let mut req = format!("ws://{}/ws", server.addr)
        .into_client_request()
        .unwrap();
    if let Some(xff) = forwarded_for {
        req.headers_mut()
            .insert("x-forwarded-for", xff.parse().unwrap());
    }
    tokio_tungstenite::connect_async(req)
        .await
        .map(|(ws, _)| ws)
}

async fn send(ws: &mut Ws, msg: Value) {
    ws.send(Message::Text(msg.to_string())).await.unwrap();
}

async fn login(ws: &mut Ws, username: &str, password: &str) {
    send(
        ws,
        json!({"type": "login", "username": username, "password": password}),
    )
    .await;
}

/// Next text frame as JSON, or `None` once the server closed the socket.
async fn recv(ws: &mut Ws) -> Option<Value> {
    loop {
        match tokio::time::timeout(TIMEOUT, ws.next())
            .await
            .expect("timed out")
        {
            Some(Ok(Message::Text(text))) => return Some(serde_json::from_str(&text).unwrap()),
            Some(Ok(Message::Close(_)) | Err(_)) | None => return None,
            Some(Ok(_)) => {}
        }
    }
}

fn auth_error(reply: Option<Value>) -> String {
    let reply = reply.expect("socket closed");
    assert_eq!(reply["type"], "auth_error", "{reply}");
    reply["message"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn fourth_bad_login_closes_socket() {
    let server = start_server(|_| {}).await;
    let mut ws = connect(&server, None).await.unwrap();
    for _ in 0..3 {
        login(&mut ws, "admin", "wrong").await;
        assert_eq!(auth_error(recv(&mut ws).await), "Invalid credentials");
    }
    let _ = ws
        .send(Message::Text(
            json!({"type": "login", "username": "admin", "password": PASSWORD}).to_string(),
        ))
        .await;
    assert_eq!(recv(&mut ws).await, None, "socket must be closed");
}

#[tokio::test]
async fn unauthenticated_socket_times_out() {
    let server = start_server(|c| c.http.ws_auth_timeout_ms = 300).await;
    let mut ws = connect(&server, None).await.unwrap();
    let started = Instant::now();
    // Nothing was asked, so the timeout is a notice, not a reply.
    let notice = recv(&mut ws).await.expect("socket closed");
    assert_eq!(notice["type"], "notice", "{notice}");
    assert_eq!(notice["code"], "auth_timeout", "{notice}");
    assert!(notice.get("id").is_none(), "{notice}");
    assert_eq!(recv(&mut ws).await, None);
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[test]
fn default_auth_timeout_is_five_seconds() {
    assert_eq!(Config::default().http.ws_auth_timeout_ms, 5_000);
}

#[tokio::test]
async fn preauth_connections_are_capped_per_ip() {
    let server = start_server(|c| c.http.rate_limit.ws_max_preauth_per_ip = 2).await;
    let mut first = connect(&server, None).await.unwrap();
    let _second = connect(&server, None).await.unwrap();
    match connect(&server, None).await {
        Err(tungstenite::Error::Http(resp)) => assert_eq!(resp.status(), 429),
        other => panic!("expected 429, got {other:?}"),
    }
    // Authenticating frees the slot.
    login(&mut first, "admin", PASSWORD).await;
    assert_eq!(recv(&mut first).await.unwrap()["type"], "authenticated");
    connect(&server, None).await.unwrap();
}

#[tokio::test]
async fn auth_phase_messages_are_rate_limited() {
    let server = start_server(|c| c.http.rate_limit.ws_max_messages_per_sec = 3).await;
    let mut ws = connect(&server, None).await.unwrap();
    for _ in 0..4 {
        send(&mut ws, json!({"type": "ping"})).await;
    }
    for _ in 0..3 {
        assert!(auth_error(recv(&mut ws).await).contains("Authentication required"));
    }
    assert!(auth_error(recv(&mut ws).await).contains("Rate limit exceeded"));
    assert_eq!(recv(&mut ws).await, None);
}

/// Five failed logins from one IP, spread over two sockets.
async fn fail_five_logins(server: &Server, forwarded_for: [&str; 2]) {
    let mut users = (0..5).map(|i| format!("nobody{i}"));
    for (xff, attempts) in forwarded_for.into_iter().zip([3, 2]) {
        let mut ws = connect(server, Some(xff)).await.unwrap();
        for user in users.by_ref().take(attempts) {
            login(&mut ws, &user, "wrong").await;
            assert_eq!(auth_error(recv(&mut ws).await), "Invalid credentials");
        }
    }
}

#[tokio::test]
async fn rotating_forwarded_for_does_not_bypass_login_throttle() {
    let server = start_server(|_| {}).await;
    fail_five_logins(&server, ["203.0.113.1", "203.0.113.2"]).await;
    let mut ws = connect(&server, Some("203.0.113.3")).await.unwrap();
    login(&mut ws, "admin", PASSWORD).await;
    assert!(auth_error(recv(&mut ws).await).starts_with("Too many failed login attempts"));
}

#[tokio::test]
async fn trusted_proxy_forwarded_for_is_honoured() {
    let server =
        start_server(|c| c.http.trusted_proxies = vec!["127.0.0.1".parse().unwrap()]).await;
    fail_five_logins(&server, ["203.0.113.1", "203.0.113.1"]).await;

    let mut throttled = connect(&server, Some("203.0.113.1")).await.unwrap();
    login(&mut throttled, "admin", PASSWORD).await;
    assert!(auth_error(recv(&mut throttled).await).starts_with("Too many failed login attempts"));

    let mut other = connect(&server, Some("203.0.113.2")).await.unwrap();
    login(&mut other, "admin", PASSWORD).await;
    assert_eq!(recv(&mut other).await.unwrap()["type"], "authenticated");
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

#[test]
fn unknown_user_costs_as_much_as_known_user() {
    let (handler, _tmp) = handler(|_| {});
    let time = |user: &str| {
        median(
            (0..5)
                .map(|_| {
                    let started = Instant::now();
                    assert!(handler.authenticate_user(user, "wrong").is_err());
                    started.elapsed()
                })
                .collect(),
        )
    };
    let (known, unknown) = (time("admin"), time("no-such-user"));
    let ratio = known.as_secs_f64() / unknown.as_secs_f64();
    assert!(
        (0.5..2.0).contains(&ratio),
        "known {known:?} vs unknown {unknown:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_bad_logins_do_not_stall_queries() {
    let (handler, _tmp) = handler(|c| c.storage.auto_create_knowledge_graphs = true);
    let key = handler.create_api_key("latency", "admin").unwrap();
    let admin = handler.authenticate_api_key(&key).unwrap();
    let query = || {
        let handler = Arc::clone(&handler);
        let admin = admin.clone();
        async move {
            let started = Instant::now();
            tokio::spawn(async move {
                handler
                    .execute_program_status(
                        None,
                        Some("latency".to_string()),
                        "?edge(X, Y)".to_string(),
                        Some(&admin),
                    )
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
            started.elapsed()
        }
    };
    handler
        .execute_program_status(
            None,
            Some("latency".to_string()),
            "+edge(1, 2)".to_string(),
            Some(&admin),
        )
        .await
        .unwrap();
    query().await;
    let mut baseline = Vec::new();
    for _ in 0..3 {
        baseline.push(query().await);
    }
    let baseline = median(baseline);

    let logins: Vec<_> = (0..50u8)
        .map(|i| {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let peer = IpAddr::from([198, 51, 100, i]);
                handler.login(&format!("user{i}"), "wrong", peer).await
            })
        })
        .collect();
    // Blocked workers would also stall tokio timers, so wait on the OS clock.
    std::thread::sleep(Duration::from_millis(20));
    let during = query().await;
    assert!(
        logins.iter().any(|l| !l.is_finished()),
        "logins must still be running"
    );
    for login in logins {
        assert!(login.await.unwrap().is_err());
    }
    assert!(
        during < baseline + Duration::from_millis(50),
        "query took {during:?} during login storm vs {baseline:?} baseline"
    );
}

fn read_credentials(path: &std::path::Path) -> toml::Table {
    toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

#[test]
fn generated_credentials_default_to_data_dir_owner_only() {
    let (handler, _tmp) = handler(|c| c.http.auth.bootstrap_admin_password = None);
    let path = handler.config().storage.data_dir.join("credentials.toml");
    let creds = read_credentials(&path);
    let password = creds["admin_password"].as_str().unwrap();
    assert!(handler.authenticate_user("admin", password).is_ok());
    let api_key = creds["api_key"].as_str().unwrap();
    assert!(handler.authenticate_api_key(api_key).is_ok());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}

#[test]
fn supplied_password_is_not_persisted() {
    let (handler, _tmp) = handler(|_| {});
    let path = handler.config().storage.data_dir.join("credentials.toml");
    let creds = read_credentials(&path);
    assert!(!creds.contains_key("admin_password"), "{creds:?}");
    assert!(creds.contains_key("api_key"));
    assert!(handler.authenticate_user("admin", PASSWORD).is_ok());
}

#[test]
fn persisted_credentials_are_reused_after_data_wipe() {
    let tmp = TempDir::new().unwrap();
    let creds_path = tmp.path().join("creds").join("credentials.toml");
    let boot = |data: &str| {
        let mut config = Config::default();
        config.storage.data_dir = tmp.path().join(data);
        config.http.auth.credentials_file = Some(creds_path.clone());
        let handler = Handler::from_config(config).unwrap();
        handler.bootstrap_auth();
        handler
    };
    boot("first");
    let creds = read_credentials(&creds_path);
    let second = boot("second");
    assert_eq!(read_credentials(&creds_path), creds);
    let password = creds["admin_password"].as_str().unwrap();
    assert!(second.authenticate_user("admin", password).is_ok());
}

#[test]
fn unsaved_generated_api_key_still_creates_admin() {
    let (handler, _tmp) = handler(|c| {
        let blocker = c.storage.data_dir.with_file_name("not-a-dir");
        std::fs::create_dir_all(blocker.parent().unwrap()).unwrap();
        std::fs::write(&blocker, "").unwrap();
        c.http.auth.credentials_file = Some(blocker.join("credentials.toml"));
    });
    assert!(handler.authenticate_user("admin", PASSWORD).is_ok());
}
