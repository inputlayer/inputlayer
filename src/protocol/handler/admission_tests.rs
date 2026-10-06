//! Admission through the handler: which lane each kind of request takes,
//! and how a request the pool cannot admit is answered.

use super::admission::Lane;
use super::*;
use inputlayer_ws_protocol::NamedQuery;
use std::time::Duration;

const KG: &str = "admission";

fn handler_with(
    adjust: impl FnOnce(&mut crate::config::AdmissionConfig),
) -> (Arc<Handler>, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    adjust(&mut config.storage.performance.admission);
    let handler = Handler::from_config(config).expect("handler creation failed");
    handler
        .storage
        .read()
        .create_knowledge_graph(KG)
        .expect("knowledge graph creation failed");
    (Arc::new(handler), tmp)
}

fn handler() -> (Arc<Handler>, tempfile::TempDir) {
    handler_with(|_| {})
}

async fn run(handler: &Handler, program: &str) -> Result<QueryResult, ProgramError> {
    let control = handler.request_control(None);
    handler
        .execute_program_status(
            None,
            Some(KG.to_string()),
            program.to_string(),
            None,
            &control,
        )
        .await
}

fn admitted(handler: &Handler, lane: Lane) -> u64 {
    handler.admission().stats(lane).admitted
}

#[tokio::test]
async fn each_kind_of_request_takes_its_lane() {
    let (handler, _tmp) = handler();
    run(&handler, "+edge[(1, 2), (2, 3)]").await.expect("write");
    assert_eq!(admitted(&handler, Lane::Write), 1);
    assert_eq!(admitted(&handler, Lane::Interactive), 0);

    run(&handler, "?edge(X, Y)").await.expect("query");
    assert_eq!(admitted(&handler, Lane::Interactive), 1);

    // A persistent rule and a query of it in one program: the rule commits,
    // so the program writes. (A bare `head <- body` is a session rule, which
    // commits nothing: interactive.)
    run(&handler, "+path(X, Y) <- edge(X, Y)\n?path(X, Y)")
        .await
        .expect("rule");
    assert_eq!(admitted(&handler, Lane::Write), 2);

    // A proof is interactive; a command that changes durable state writes.
    run(&handler, ".why ?path(1, 2)").await.expect("proof");
    assert_eq!(admitted(&handler, Lane::Interactive), 2);
    run(&handler, ".index list")
        .await
        .expect("read-only command");
    assert_eq!(admitted(&handler, Lane::Interactive), 3);
    run(&handler, ".rule drop path").await.expect("rule drop");
    assert_eq!(admitted(&handler, Lane::Write), 3);
    assert_eq!(admitted(&handler, Lane::Background), 0);

    // A read of several queries is interactive, once per query.
    let control = handler.request_control(None);
    let queries = [
        NamedQuery {
            name: "a".to_string(),
            query: "?edge(1, Y)".to_string(),
        },
        NamedQuery {
            name: "b".to_string(),
            query: "?edge(2, Y)".to_string(),
        },
    ];
    handler
        .read_snapshot(KG, &queries, None, &control)
        .await
        .expect("read");
    assert_eq!(admitted(&handler, Lane::Interactive), 5);

    // A subscription refresh is background; a sharing probe is a probe.
    let snapshot = handler
        .get_storage()
        .get_snapshot_for(KG)
        .expect("snapshot");
    handler
        .query_snapshot(KG, Arc::clone(&snapshot), "?edge(X, Y)", None, false)
        .await
        .expect("refresh");
    assert_eq!(admitted(&handler, Lane::Background), 1);
    handler
        .query_snapshot(KG, snapshot, "?edge(X, Y)", None, true)
        .await
        .expect("probe");
    assert_eq!(admitted(&handler, Lane::Probe), 1);
    assert_eq!(admitted(&handler, Lane::Background), 1);
    assert_eq!(handler.compute_permits_in_use(), 0);
}

#[tokio::test]
async fn a_request_the_pool_cannot_admit_in_time_is_refused_as_overloaded() {
    let (handler, _tmp) = handler_with(|admission| admission.max_wait_ms = 50);
    let held = handler.hold_compute_permits();
    let error = run(&handler, "?edge(X, Y)").await.expect_err("no permit");
    assert_eq!(error.code, Some(ErrorCode::Overloaded));
    assert!(
        error.message.contains("interactive lane"),
        "names the lane: {}",
        error.message
    );
    assert_eq!(handler.admission().stats(Lane::Interactive).timed_out, 1);
    // A refresh is refused the same way, as the subscription's error.
    let snapshot = handler
        .get_storage()
        .get_snapshot_for(KG)
        .expect("snapshot");
    let error = handler
        .query_snapshot(KG, snapshot, "?edge(X, Y)", None, false)
        .await
        .expect_err("no permit");
    assert!(error.contains("Server overloaded"), "{error}");
    drop(held);
    run(&handler, "?edge(X, Y)")
        .await
        .expect("admitted once a permit is free");
}

#[tokio::test]
async fn a_full_lane_queue_refuses_at_once_and_the_others_still_run() {
    let (handler, _tmp) = handler_with(|admission| {
        admission.max_wait_ms = 10_000;
        admission.interactive.max_queued = 1;
    });
    let held = handler.hold_compute_permits();
    let queued = {
        let handler = Arc::clone(&handler);
        tokio::spawn(async move { run(&handler, "?edge(X, Y)").await })
    };
    tokio::task::yield_now().await;
    assert_eq!(handler.admission().stats(Lane::Interactive).queued, 1);
    let started = Instant::now();
    let error = run(&handler, "?edge(X, Y)").await.expect_err("queue full");
    assert_eq!(error.code, Some(ErrorCode::Overloaded));
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "refused at once"
    );
    assert_eq!(
        handler.admission().stats(Lane::Interactive).rejected_full,
        1
    );
    // The write lane's queue is its own.
    let write = {
        let handler = Arc::clone(&handler);
        tokio::spawn(async move { run(&handler, "+edge[(1, 2)]").await })
    };
    tokio::task::yield_now().await;
    assert_eq!(handler.admission().stats(Lane::Write).queued, 1);
    drop(held);
    queued.await.expect("task").expect("the queued query ran");
    write.await.expect("task").expect("the write ran");
}

#[tokio::test]
async fn a_request_cancelled_while_it_waits_never_runs_and_leaves_the_queue() {
    let (handler, _tmp) = handler();
    let held = handler.hold_compute_permits();
    let control = handler.request_control(None);
    let waiting = {
        let handler = Arc::clone(&handler);
        let control = Arc::clone(&control);
        tokio::spawn(async move {
            handler
                .execute_program_status(
                    None,
                    Some(KG.to_string()),
                    "+edge[(1, 2)]".to_string(),
                    None,
                    &control,
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(handler.admission().stats(Lane::Write).queued, 1);
    control.cancel();
    let error = waiting.await.expect("task").expect_err("cancelled");
    assert_eq!(error.code, Some(ErrorCode::Cancelled));
    assert_eq!(handler.admission().stats(Lane::Write).queued, 0);
    drop(held);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handler.admission().stats(Lane::Write).admitted, 0);
    let rows = run(&handler, "?edge(X, Y)").await.expect("query").rows;
    assert!(rows.is_empty(), "the cancelled write never ran: {rows:?}");
}

#[tokio::test]
async fn the_configured_pool_size_and_lane_bounds_are_in_effect() {
    let (handler, _tmp) = handler_with(|admission| {
        admission.compute_permits = 3;
        admission.write.max_permits = 1;
    });
    assert_eq!(handler.compute_permits(), 3);
    let held = handler.hold_compute_permits();
    assert_eq!(held.len(), 3);
    assert_eq!(handler.compute_permits_in_use(), 3);
    // No lane holds more than its cap.
    assert!(handler.admission().stats(Lane::Write).running <= 1);
    drop(held);
    assert_eq!(handler.compute_permits_in_use(), 0);
}
