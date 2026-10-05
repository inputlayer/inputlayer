//! Warm-standby replication between real server processes: a follower
//! applies the primary's stream and holds its state; it survives the primary
//! being killed (keeping a consistent prefix, then resyncing from the
//! restarted primary), a network partition (detected by silence, healed by
//! catching up), its own restart, and falling behind the retained log.

#![allow(clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use inputlayer_testkit::{Agent, Engine, EngineBuilder, Replication, Violation, WsClient};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const KG: &str = "default";
const TOKEN: &str = "replication-test-token-0123456789";
const HEARTBEAT_MS: u64 = 100;
const TIMEOUT_MS: u64 = 800;
const SYNC_TIMEOUT_MS: u64 = 1000;
/// Longest wait for a follower to catch up.
const CONVERGE: Duration = Duration::from_secs(30);

fn server() -> EngineBuilder {
    EngineBuilder::new(env!("CARGO_BIN_EXE_inputlayer-server"))
}

async fn primary(retain_bytes: Option<usize>) -> Engine {
    start_primary(retain_bytes, None).await
}

/// A primary shipping synchronously: replies wait up to `SYNC_TIMEOUT_MS`
/// for a follower, then `on_follower_loss` applies.
async fn sync_primary(on_follower_loss: &'static str) -> Engine {
    start_primary(None, Some(on_follower_loss)).await
}

async fn start_primary(retain_bytes: Option<usize>, sync: Option<&'static str>) -> Engine {
    server()
        .replication(Replication {
            role: "primary",
            token: TOKEN.into(),
            primary_url: None,
            retain_bytes,
            heartbeat_ms: Some(HEARTBEAT_MS),
            timeout_ms: Some(TIMEOUT_MS),
            mode: sync.map(|_| "sync"),
            sync_timeout_ms: sync.map(|_| SYNC_TIMEOUT_MS),
            on_follower_loss: sync,
        })
        .start()
        .await
        .expect("start primary")
}

async fn follower(primary_url: String, token: &str, api_key: &str) -> Engine {
    let mut engine = server()
        .replication(Replication {
            role: "follower",
            token: token.into(),
            primary_url: Some(primary_url),
            retain_bytes: None,
            heartbeat_ms: Some(HEARTBEAT_MS),
            timeout_ms: Some(TIMEOUT_MS),
            mode: None,
            sync_timeout_ms: None,
            on_follower_loss: None,
        })
        .start()
        .await
        .expect("start follower");
    engine.set_api_key(api_key);
    engine
}

/// `GET /v1/replication/status`, or `None` while it cannot be read (the
/// follower has not received its primary's keys yet).
async fn status(engine: &Engine) -> Option<Value> {
    let response = reqwest::Client::new()
        .get(format!("{}/v1/replication/status", engine.http_url()))
        .bearer_auth(engine.api_key())
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response.json().await.ok()
}

/// Wait until the follower's status satisfies `ok`; returns that status.
async fn wait_status(follower: &Engine, what: &str, ok: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    let mut last = None;
    loop {
        if let Some(status) = status(follower).await {
            if ok(&status["follower"]) {
                return status;
            }
            last = Some(status);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "follower never reached {what}; last status {last:?}; log {}",
            std::fs::read_to_string(follower.log_path()).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The rows of `query` on `engine`, as a set of JSON strings.
async fn rows(engine: &Engine, query: &str) -> BTreeSet<String> {
    let mut client = WsClient::connect(engine, KG).await.expect("connect");
    let result = client.query(query).await.expect("query");
    client.close().await;
    result.rows.iter().map(Value::to_string).collect()
}

/// Wait until `query` answers the same on the follower as on the primary.
async fn converge(primary: &Engine, follower: &Engine, query: &str) -> BTreeSet<String> {
    let expected = rows(primary, query).await;
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let found = rows(follower, query).await;
        if found == expected {
            return found;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{query}: follower has {} rows, primary {}",
            found.len(),
            expected.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the follower is streaming and has applied everything.
async fn caught_up(follower: &Engine) -> Value {
    wait_status(follower, "streaming with no lag", |f| {
        f["state"] == "streaming" && f["lag_events"] == 0
    })
    .await
}

async fn commit(engine: &Engine, program: &str) {
    let mut client = WsClient::connect(engine, KG).await.expect("connect");
    client.commit(program).await.expect(program);
    client.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_holds_the_primary_state_serves_reads_and_refuses_writes() {
    let primary = primary(None).await;
    commit(&primary, "+edge[(1, 2), (2, 3)]").await;
    commit(&primary, "+reach(X, Y) <- edge(X, Y)").await;
    commit(&primary, "+reach(X, Z) <- reach(X, Y), edge(Y, Z)").await;
    commit(&primary, ".kg create other").await;

    // A fresh follower resyncs from a checkpoint, then streams.
    let follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    let first = caught_up(&follower).await;
    assert_eq!(first["role"], "follower");
    assert_eq!(first["follower"]["resyncs"], 1);
    converge(&primary, &follower, "?edge(X, Y)").await;
    assert_eq!(converge(&primary, &follower, "?reach(1, Y)").await.len(), 2);

    // A subscription on the follower sees the primary's later writes.
    let mut agent = Agent::connect(&follower, KG).await.expect("agent");
    agent
        .subscribe("s", "?edge(X, Y)")
        .await
        .expect("subscribe");
    commit(&primary, "+edge(3, 4)").await;
    agent
        .converge("s", &[json!([1, 2]), json!([2, 3]), json!([3, 4])])
        .await
        .expect("replicated delta");
    assert_eq!(converge(&primary, &follower, "?reach(1, Y)").await.len(), 3);

    // Writes go to the primary only.
    let mut client = WsClient::connect(&follower, KG).await.expect("connect");
    match client.execute("+edge(9, 9)").await {
        Err(Violation::Rejected(message)) => {
            assert!(message.contains("read-only replica"), "{message}");
        }
        other => panic!("a follower took a write: {other:?}"),
    }
    assert_eq!(rows(&follower, "?edge(X, Y)").await.len(), 3);

    let primary_status = status_of(&primary).await;
    let followers = primary_status["primary"]["followers"].as_array().unwrap();
    assert_eq!(followers.len(), 1, "{primary_status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn killing_the_primary_leaves_a_consistent_prefix_and_its_restart_resyncs_the_follower() {
    let mut primary = primary(None).await;
    let follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;

    // One writer commits seq(1), seq(2), ... in order; kill the primary mid-stream.
    let acked = Arc::new(AtomicU64::new(0));
    let mut writer = WsClient::connect(&primary, KG).await.expect("connect");
    let writer_acked = Arc::clone(&acked);
    let writes = tokio::spawn(async move {
        for i in 1..=100_000u64 {
            if writer.commit(&format!("+seq({i})")).await.is_err() {
                break;
            }
            writer_acked.store(i, Ordering::SeqCst);
        }
    });
    while acked.load(Ordering::SeqCst) < 300 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    primary.stop().await.expect("kill primary");
    writes.await.expect("writer task");
    let acked = acked.load(Ordering::SeqCst);

    // The follower notices and keeps what it applied: exactly seq(1..=k).
    wait_status(&follower, "a lost primary", |f| f["state"] == "connecting").await;
    let held = rows(&follower, "?seq(X)").await;
    let k = held.len() as u64;
    let prefix: BTreeSet<String> = (1..=k).map(|i| json!([i]).to_string()).collect();
    assert_eq!(held, prefix, "the follower holds a prefix of the commits");
    assert!(k <= acked + 1, "follower has {k}, primary acked {acked}");
    assert!(k > 0, "the follower applied nothing before the kill");

    // The restarted primary is a new stream: the follower resyncs to it and
    // ends with every write the primary acknowledged.
    primary.restart().await.expect("restart primary");
    caught_up(&follower).await;
    let all = converge(&primary, &follower, "?seq(X)").await;
    assert!(all.len() as u64 >= acked, "{} < {acked}", all.len());
    let status = wait_status(&follower, "a second resync", |f| f["resyncs"] == 2).await;
    assert!(status["follower"]["reconnects"].as_u64().unwrap() >= 1);
}

/// A TCP proxy that can drop everything in both directions without closing
/// connections: a network partition, not a refused connection.
struct Partition {
    port: u16,
    cut: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
}

impl Partition {
    async fn to(upstream: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let cut = Arc::new(AtomicBool::new(false));
        let generation = Arc::new(AtomicU64::new(0));
        let (accept_cut, accept_generation) = (Arc::clone(&cut), Arc::clone(&generation));
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(("127.0.0.1", upstream)).await else {
                    continue;
                };
                let born = accept_generation.load(Ordering::SeqCst);
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                for (from, to) in [
                    (
                        Box::new(client_read) as Box<dyn tokio::io::AsyncRead + Send + Unpin>,
                        Box::new(server_write) as Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
                    ),
                    (Box::new(server_read), Box::new(client_write)),
                ] {
                    let (cut, generation) =
                        (Arc::clone(&accept_cut), Arc::clone(&accept_generation));
                    tokio::spawn(pump(from, to, cut, generation, born));
                }
            }
        });
        Self {
            port,
            cut,
            generation,
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
    }

    /// Restore the network; connections from before or during the cut die.
    fn heal(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.cut.store(false, Ordering::SeqCst);
    }
}

async fn pump(
    mut from: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    mut to: Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    cut: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    born: u64,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let read = tokio::time::timeout(Duration::from_millis(20), from.read(&mut buf)).await;
        if generation.load(Ordering::SeqCst) != born {
            return;
        }
        match read {
            Err(_) => continue,
            Ok(Ok(0) | Err(_)) => return,
            Ok(Ok(n)) => {
                if cut.load(Ordering::SeqCst) {
                    continue;
                }
                if to.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_follower_notices_the_silence_and_catches_up_after_the_heal() {
    let primary = primary(None).await;
    let partition = Partition::to(primary.port()).await;
    let follower = follower(partition.url(), TOKEN, primary.api_key()).await;
    commit(&primary, "+edge(1, 2)").await;
    caught_up(&follower).await;
    converge(&primary, &follower, "?edge(X, Y)").await;

    partition.cut();
    for i in 0..50 {
        commit(&primary, &format!("+edge({i}, 100)")).await;
    }
    // Silence ends the stream; reconnects then time out until the heal.
    let status = wait_status(&follower, "a detected partition", |f| {
        f["state"] == "connecting"
            && f["last_contact_ms"].as_u64().unwrap_or(0) > TIMEOUT_MS
            && f["last_error"]
                .as_str()
                .is_some_and(|e| e.contains("no word from the primary") || e.contains("timed out"))
    })
    .await;
    let log = std::fs::read_to_string(follower.log_path()).unwrap_or_default();
    assert!(log.contains("no word from the primary"), "{log}");
    assert_eq!(status["follower"]["resyncs"], 1, "{status}");
    assert_eq!(rows(&follower, "?edge(X, Y)").await.len(), 1);

    // After the heal the follower tails from its position: no resync.
    partition.heal();
    caught_up(&follower).await;
    assert_eq!(converge(&primary, &follower, "?edge(X, Y)").await.len(), 51);
    let status = status_of(&follower).await;
    assert_eq!(status["follower"]["resyncs"], 1, "{status}");
}

async fn status_of(engine: &Engine) -> Value {
    status(engine).await.expect("status")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_follower_resumes_from_its_saved_position() {
    let mut primary = primary(None).await;
    let mut follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    commit(&primary, "+edge(1, 2)").await;
    caught_up(&follower).await;

    follower.stop().await.expect("kill follower");
    commit(&primary, "+edge(2, 3)").await;
    commit(&primary, "+edge(3, 4)").await;
    follower.restart().await.expect("restart follower");
    caught_up(&follower).await;
    assert_eq!(converge(&primary, &follower, "?edge(X, Y)").await.len(), 3);
    assert_eq!(status_of(&follower).await["follower"]["resyncs"], 0);

    // Restarted against an idle primary, both sides report the saved
    // position as caught up without waiting for another commit.
    follower.stop().await.expect("kill follower");
    follower.restart().await.expect("restart follower");
    let head = status_of(&primary).await["primary"]["head_lsn"].clone();
    let status = caught_up(&follower).await;
    assert_eq!(status["follower"]["applied_lsn"], head, "{status}");
    assert_eq!(status["follower"]["resyncs"], 0, "{status}");
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let status = status_of(&primary).await;
        let followers = status["primary"]["followers"].as_array().unwrap();
        if followers.len() == 1 && followers[0]["lag_events"] == 0 {
            assert_eq!(followers[0]["acked_lsn"], head, "{status}");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the primary never saw the restarted follower caught up: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Restarted while the primary is down, it reports its saved position
    // but no contact with the primary.
    primary.stop().await.expect("kill primary");
    follower.stop().await.expect("kill follower");
    follower.restart().await.expect("restart follower");
    let status = wait_status(&follower, "the saved position", |f| {
        f["applied_lsn"] == head
    })
    .await;
    assert!(status["follower"]["last_contact_ms"].is_null(), "{status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_behind_the_retained_log_resyncs() {
    let primary = primary(Some(4096)).await;
    let mut follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;

    follower.stop().await.expect("kill follower");
    let mut client = WsClient::connect(&primary, KG).await.expect("connect");
    for i in 0..200 {
        client
            .commit(&format!("+filler({i}, \"{}\")", "x".repeat(64)))
            .await
            .expect("filler");
    }
    follower.restart().await.expect("restart follower");
    caught_up(&follower).await;
    assert_eq!(
        converge(&primary, &follower, "?filler(X, Y)").await.len(),
        200
    );
    assert_eq!(status_of(&follower).await["follower"]["resyncs"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_with_the_wrong_token_is_refused() {
    let primary = primary(None).await;
    let follower = follower(
        primary.http_url(),
        "not-the-replication-token-at-all",
        primary.api_key(),
    )
    .await;
    // It has none of the primary's keys, so its status is unreadable; the
    // primary never lists it.
    tokio::time::sleep(Duration::from_millis(TIMEOUT_MS * 2)).await;
    assert!(status(&follower).await.is_none());
    let primary_status = status_of(&primary).await;
    assert_eq!(primary_status["primary"]["followers"], json!([]));
    let log = std::fs::read_to_string(follower.log_path()).unwrap_or_default();
    assert!(log.contains("401"), "{log}");
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_on_many_graphs_converge_on_the_follower() {
    let primary = primary(None).await;
    let follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;
    for kg in ["g0", "g1", "g2"] {
        commit(&primary, &format!(".kg create {kg}")).await;
    }
    // Writers on different graphs commit concurrently, so their revisions
    // and WAL order interleave; one writer also creates and drops graphs.
    let mut tasks = Vec::new();
    for w in 0..3usize {
        let mut client = WsClient::connect(&primary, &format!("g{w}"))
            .await
            .expect("connect");
        tasks.push(tokio::spawn(async move {
            for i in 0..60 {
                client
                    .commit(&format!("+fact({w}, {i})"))
                    .await
                    .expect("insert");
                if i % 3 == 0 {
                    client
                        .commit(&format!("-fact({w}, {})", i / 2))
                        .await
                        .expect("delete");
                }
            }
        }));
    }
    let mut churn = WsClient::connect(&primary, KG).await.expect("connect");
    for i in 0..10 {
        churn
            .commit(&format!(".kg create tmp{i}"))
            .await
            .expect("create");
        churn.commit(&format!("+t({i})")).await.expect("insert");
        churn.commit(&format!(".kg use {KG}")).await.expect("use");
        if i % 2 == 0 {
            churn
                .commit(&format!(".kg drop tmp{i}"))
                .await
                .expect("drop");
        }
    }
    for task in tasks {
        task.await.expect("writer");
    }
    caught_up(&follower).await;
    for kg in ["g0", "g1", "g2"] {
        let query = "?fact(X, Y)";
        let expected = {
            let mut c = WsClient::connect(&primary, kg).await.expect("connect");
            c.query(query).await.expect("query").rows
        };
        let found = {
            let mut c = WsClient::connect(&follower, kg).await.expect("connect");
            c.query(query).await.expect("query").rows
        };
        let set = |rows: Vec<Value>| rows.iter().map(Value::to_string).collect::<BTreeSet<_>>();
        assert_eq!(set(found), set(expected), "graph {kg}");
    }
    assert_eq!(
        converge(&primary, &follower, ".kg list").await.len(),
        rows(&primary, ".kg list").await.len()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn api_keys_issued_and_revoked_on_the_primary_apply_on_the_follower() {
    let primary = primary(None).await;
    let follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;
    // A session open on the follower survives the credential changes.
    let mut session = WsClient::connect(&follower, KG).await.expect("connect");

    let mut admin = WsClient::connect(&primary, KG).await.expect("connect");
    let created = admin.execute(".apikey create svc").await.expect("create");
    let key = created.rows[0][1].as_str().expect("key").to_string();
    caught_up(&follower).await;
    let mut with_key = WsClient::connect_url(&follower.ws_url(KG), &key)
        .await
        .expect("the new key works on the follower");
    with_key.query("?x(X)").await.expect("query");

    admin.commit(".apikey revoke svc").await.expect("revoke");
    caught_up(&follower).await;
    assert!(
        WsClient::connect_url(&follower.ws_url(KG), &key)
            .await
            .is_err(),
        "a revoked key still works on the follower"
    );
    session
        .query("?x(X)")
        .await
        .expect("the admin session on the follower is still open");
}

// Synchronous shipping (`mode = "sync"`): a write is acknowledged only once
// the follower applied it durably.

/// Commit `program` on `engine`; the error message when it fails.
async fn try_commit(engine: &Engine, program: &str) -> Result<(), String> {
    let mut client = WsClient::connect(engine, KG)
        .await
        .map_err(|e| format!("connect: {e:?}"))?;
    let result = client.commit(program).await.map(drop);
    client.close().await;
    result.map_err(|e| format!("{e:?}"))
}

/// Wait until the primary's synchronous shipping is in `state`.
async fn wait_sync_state(primary: &Engine, state: &str) -> Value {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let status = status_of(primary).await;
        if status["primary"]["sync"]["state"] == state {
            return status;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "primary never reached sync state {state}: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn in_sync_mode_killing_the_primary_loses_no_acknowledged_write() {
    const WRITERS: u64 = 4;
    const TRIALS: u64 = 3;
    let mut primary = sync_primary("block").await;
    let follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;
    wait_sync_state(&primary, "sync").await;

    let mut acknowledged = BTreeSet::new();
    for trial in 0..TRIALS {
        // Concurrent writers, each committing w(trial, writer, i) in order;
        // kill the primary while they run.
        let acked = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut writers = Vec::new();
        for writer in 0..WRITERS {
            let mut client = WsClient::connect(&primary, KG).await.expect("connect");
            let acked = Arc::clone(&acked);
            writers.push(tokio::spawn(async move {
                for i in 0.. {
                    let program = format!("+w({trial}, {writer}, {i})");
                    if client.commit(&program).await.is_err() {
                        break;
                    }
                    acked.lock().push(json!([trial, writer, i]).to_string());
                }
            }));
        }
        while acked.lock().len() < 200 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        primary.stop().await.expect("kill primary");
        for writer in writers {
            writer.await.expect("writer task");
        }
        acknowledged.extend(acked.lock().drain(..));

        // Everything any writer was told is committed is on the follower.
        wait_status(&follower, "a lost primary", |f| f["state"] == "connecting").await;
        let held = rows(&follower, "?w(T, W, I)").await;
        let lost: Vec<_> = acknowledged.difference(&held).collect();
        assert!(
            lost.is_empty(),
            "trial {trial}: {} acknowledged writes missing on the follower, e.g. {:?}",
            lost.len(),
            lost.iter().take(5).collect::<Vec<_>>()
        );

        primary.restart().await.expect("restart primary");
        caught_up(&follower).await;
        wait_sync_state(&primary, "sync").await;
    }
    // The restarted primary kept every acknowledged write too.
    let on_primary = rows(&primary, "?w(T, W, I)").await;
    assert!(acknowledged.is_subset(&on_primary));
}

#[tokio::test(flavor = "multi_thread")]
async fn block_fails_writes_no_follower_confirms_and_recovers_when_one_returns() {
    let primary = sync_primary("block").await;
    let mut follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;
    try_commit(&primary, "+edge(1, 2)")
        .await
        .expect("confirmed write");
    let status = wait_sync_state(&primary, "sync").await;
    assert!(status["primary"]["sync"]["waits"].as_u64().unwrap() >= 1);

    // No follower: the write waits, then is reported as not confirmed on a
    // replica. It is committed on the primary all the same.
    follower.stop().await.expect("kill follower");
    let started = std::time::Instant::now();
    let error = try_commit(&primary, "+edge(2, 3)")
        .await
        .expect_err("acknowledged without a follower");
    assert!(error.contains("no replica confirmed"), "{error}");
    assert!(started.elapsed() >= Duration::from_millis(SYNC_TIMEOUT_MS));
    assert_eq!(rows(&primary, "?edge(X, Y)").await.len(), 2);
    let status = wait_sync_state(&primary, "stalled").await;
    assert_eq!(status["primary"]["sync"]["unconfirmed"], 1);
    assert!(status["primary"]["lag_events"].as_u64().unwrap() >= 1);

    let metrics = reqwest::Client::new()
        .get(format!("{}/metrics/prometheus", primary.http_url()))
        .bearer_auth(primary.api_key())
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        metrics.contains("inputlayer_replication_sync_state{state=\"stalled\"} 1"),
        "{metrics}"
    );
    assert!(metrics.contains("inputlayer_replication_sync_unconfirmed_total 1"));

    // The follower returns, catches up (the unconfirmed write included), and
    // writes are confirmed again.
    follower.restart().await.expect("restart follower");
    caught_up(&follower).await;
    converge(&primary, &follower, "?edge(X, Y)").await;
    try_commit(&primary, "+edge(3, 4)")
        .await
        .expect("confirmed write");
    wait_sync_state(&primary, "sync").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn degrade_falls_back_to_async_and_rearms_once_the_follower_catches_up() {
    let primary = sync_primary("degrade").await;
    let mut follower = follower(primary.http_url(), TOKEN, primary.api_key()).await;
    caught_up(&follower).await;
    try_commit(&primary, "+edge(1, 2)")
        .await
        .expect("confirmed write");

    // The first write without a follower waits out the timeout, then
    // succeeds; shipping falls back to async and later writes do not wait.
    follower.stop().await.expect("kill follower");
    let started = std::time::Instant::now();
    try_commit(&primary, "+edge(2, 3)")
        .await
        .expect("degraded write");
    assert!(started.elapsed() >= Duration::from_millis(SYNC_TIMEOUT_MS));
    let status = wait_sync_state(&primary, "degraded").await;
    assert_eq!(status["primary"]["sync"]["degrades"], 1);
    let started = std::time::Instant::now();
    for i in 0..10 {
        try_commit(&primary, &format!("+edge(9, {i})"))
            .await
            .expect("async write");
    }
    assert!(started.elapsed() < Duration::from_millis(SYNC_TIMEOUT_MS));

    // The follower catches up and shipping is synchronous again.
    follower.restart().await.expect("restart follower");
    caught_up(&follower).await;
    let status = wait_sync_state(&primary, "sync").await;
    assert_eq!(status["primary"]["sync"]["rearms"], 1);
    try_commit(&primary, "+edge(3, 4)")
        .await
        .expect("confirmed write");
    converge(&primary, &follower, "?edge(X, Y)").await;
}
