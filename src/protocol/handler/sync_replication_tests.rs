//! Synchronous shipping on a primary: every kind of write waits for a
//! follower before its reply, reads never do, and what happens when no
//! follower confirms in time follows `on_follower_loss`.

use super::*;
use crate::config::{FollowerLoss, ReplicationMode, ReplicationRole};
use crate::protocol::replication::SyncState;
use std::time::Duration;

const KG: &str = "default";
const SYNC_TIMEOUT_MS: u64 = 200;

fn primary(on_loss: FollowerLoss) -> (Arc<Handler>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.storage.auto_create_knowledge_graphs = true;
    config.replication.role = ReplicationRole::Primary;
    config.replication.token = Some("sync-test-token-0123456789".into());
    config.replication.mode = ReplicationMode::Sync;
    config.replication.sync_timeout_ms = SYNC_TIMEOUT_MS;
    config.replication.on_follower_loss = on_loss;
    config.http.auth.bootstrap_admin_password = Some("admin-password".to_string());
    config.http.auth.credentials_file = Some(tmp.path().join("credentials.toml"));
    let handler = Arc::new(Handler::from_config(config).unwrap());
    // Startup writes are no request's: they never wait.
    handler.bootstrap_auth().unwrap();
    (handler, tmp)
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    handler
        .execute_program(None, Some(KG.to_string()), program.to_string(), None)
        .await
}

fn head(handler: &Handler) -> u64 {
    handler.get_storage().replication_log().unwrap().head()
}

/// Every kind of client write, each appending replication events.
const WRITES: [&str; 12] = [
    "+edge(1, 2)",
    "+edge[(2, 3), (3, 4)]",
    "-edge(3, 4)",
    "+path(X, Y) <- edge(X, Y)",
    "+person(name: string, age: int)",
    "+docs(id: int, emb: vector)\n+docs[(1, [1.0, 0.0]), (2, [0.0, 1.0])]",
    ".index create doc_idx on docs(emb) metric cosine",
    ".kg create other",
    ".user create carol carol-password-1 editor",
    ".kg acl grant other carol viewer",
    ".apikey create sync-key",
    ".kg drop other",
];

#[tokio::test(flavor = "multi_thread")]
async fn block_fails_every_unconfirmed_write_as_replica_unconfirmed_and_keeps_it() {
    let (handler, _tmp) = primary(FollowerLoss::Block);
    for program in WRITES {
        let before = head(&handler);
        let started = Instant::now();
        let error = run(&handler, program)
            .await
            .expect_err(&format!("{program}: acknowledged with no follower"));
        assert_eq!(
            error.code,
            Some(ErrorCode::ReplicaUnconfirmed),
            "{program}: {}",
            error.message
        );
        assert!(
            started.elapsed() >= Duration::from_millis(SYNC_TIMEOUT_MS),
            "{program} did not wait"
        );
        assert!(head(&handler) > before, "{program} appended no event");
    }
    // The commits stand on the primary.
    let rows = run(&handler, "?edge(X, Y)").await.unwrap();
    assert_eq!(rows.rows.len(), 2);
    let status = handler.replication_status().sync().report();
    assert_eq!(status.state, SyncState::Stalled);
    assert_eq!(status.unconfirmed, WRITES.len() as u64);
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_and_failed_writes_never_wait() {
    let (handler, _tmp) = primary(FollowerLoss::Block);
    for program in [
        "?edge(X, Y)",
        ".kg list",
        ".rel",
        "?edge(X, Y)\n?edge(Y, X)",
        // Refused before committing anything.
        "-nosuch(1)",
        "+edge(1, \"too\", \"wide\")\n+edge(1, 2)\n+edge(1, 2, 3)",
    ] {
        let started = Instant::now();
        let _ = run(&handler, program).await;
        assert!(
            started.elapsed() < Duration::from_millis(SYNC_TIMEOUT_MS),
            "{program} waited for a follower"
        );
    }
    assert_eq!(handler.replication_status().sync().report().waits, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_is_acknowledged_once_a_follower_confirms_its_events() {
    let (handler, _tmp) = primary(FollowerLoss::Block);
    let confirmer = {
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            // A follower acking whatever the log holds, a little late.
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let head = head(&handler);
                handler.replication_status().sync().acked(head, true);
            }
        })
    };
    let started = Instant::now();
    run(&handler, "+edge(1, 2)").await.unwrap();
    run(&handler, ".kg create other").await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(2 * SYNC_TIMEOUT_MS));
    confirmer.abort();
    let report = handler.replication_status().sync().report();
    assert_eq!((report.state, report.waits), (SyncState::Sync, 2));
    assert!(report.confirmed_lsn >= head(&handler));
}

#[tokio::test(flavor = "multi_thread")]
async fn degrade_acknowledges_after_the_timeout_then_stops_waiting_until_rearmed() {
    let (handler, _tmp) = primary(FollowerLoss::Degrade);
    let started = Instant::now();
    run(&handler, "+edge(1, 2)").await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(SYNC_TIMEOUT_MS));
    let sync = handler.replication_status().sync();
    assert_eq!(sync.state(), SyncState::Degraded);

    let started = Instant::now();
    run(&handler, "+edge(2, 3)").await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(SYNC_TIMEOUT_MS));

    // A follower that applied everything re-arms synchronous shipping.
    sync.acked(head(&handler), true);
    assert_eq!(sync.state(), SyncState::Sync);
    let report = sync.report();
    assert_eq!((report.degrades, report.rearms), (1, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn async_mode_never_tracks_or_waits() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().join("data");
    config.replication.role = ReplicationRole::Primary;
    config.replication.token = Some("sync-test-token-0123456789".into());
    let handler = Handler::from_config(config).unwrap();
    let started = Instant::now();
    run(&handler, "+edge(1, 2)").await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(SYNC_TIMEOUT_MS));
    let report = handler.replication_status().sync().report();
    assert_eq!((report.state, report.waits), (SyncState::Async, 0));
}

#[test]
fn metrics_report_the_sync_state_and_lag() {
    let (handler, _tmp) = primary(FollowerLoss::Block);
    let report = handler.replication_report(&handler.get_storage()).unwrap();
    let text = report.format_prometheus();
    assert!(
        text.contains("inputlayer_replication_sync_state{state=\"sync\"} 1\n"),
        "{text}"
    );
    assert!(text.contains("inputlayer_replication_sync_state{state=\"degraded\"} 0\n"));
    assert!(text.contains("inputlayer_replication_lag_events "));
    assert!(text.contains("# TYPE inputlayer_replication_sync_waits_total counter\n"));
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["primary"]["sync"]["mode"], "sync");
    assert_eq!(json["primary"]["sync"]["on_follower_loss"], "block");
}
