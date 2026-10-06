//! WI-03: Session cleanup tests.
//! WI-10: Broadcast notification tests.

use crate::harness::engine_config;
use inputlayer::protocol::notification_log::Cursor;
use inputlayer::protocol::Handler;
use inputlayer::StorageEngine;
use tempfile::TempDir;

fn create_test_handler() -> (Handler, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let config = engine_config(temp.path());
    let storage = StorageEngine::new(config).expect("create storage engine");
    let handler = Handler::new(storage);
    (handler, temp)
}

// === WI-03: Session Cleanup ===

#[tokio::test]
async fn test_session_close_is_idempotent() {
    let (handler, _tmp) = create_test_handler();
    let sid = handler.create_session("default").unwrap();
    // First close succeeds
    assert!(handler.close_session(&sid).is_ok());
    // Second close should not panic (may return error, that's fine)
    let _ = handler.close_session(&sid);
    // We reach here without panicking
}

#[tokio::test]
async fn test_session_count_after_close() {
    let (handler, _tmp) = create_test_handler();
    let before = handler.session_manager().session_count();
    let sid = handler.create_session("default").unwrap();
    assert_eq!(handler.session_manager().session_count(), before + 1);
    handler.close_session(&sid).unwrap();
    assert_eq!(
        handler.session_manager().session_count(),
        before,
        "Session count should return to original after close"
    );
}

#[tokio::test]
async fn test_multiple_sessions_independent_close() {
    let (handler, _tmp) = create_test_handler();
    let sid1 = handler.create_session("default").unwrap();
    let sid2 = handler.create_session("default").unwrap();
    let sid3 = handler.create_session("default").unwrap();

    handler.close_session(&sid1).unwrap();
    // Other sessions should still be accessible
    assert!(handler.session_manager().has_session(&sid2));
    assert!(handler.session_manager().has_session(&sid3));

    handler.close_session(&sid2).unwrap();
    assert!(handler.session_manager().has_session(&sid3));

    handler.close_session(&sid3).unwrap();
    assert!(!handler.session_manager().has_session(&sid3));
}

// === WI-10: Broadcast Notification Tests ===

#[tokio::test]
async fn test_notify_without_subscriber_does_not_panic() {
    let (handler, _tmp) = create_test_handler();
    // No subscriber - send goes to empty channel - must not panic
    handler.notify_persistent_update("default", "edge", "insert", 5);
    // Reaching here means no panic
}

#[tokio::test]
async fn test_notify_after_subscriber_dropped_does_not_panic() {
    let (handler, _tmp) = create_test_handler();
    let rx = handler.subscribe_notifications();
    drop(rx);
    // Receiver dropped - must not panic on send
    handler.notify_persistent_update("default", "edge", "insert", 5);
    // Reaching here means no panic
}

#[tokio::test]
async fn test_notify_with_active_subscriber_delivers_message() {
    let (handler, _tmp) = create_test_handler();
    let mut rx = handler.subscribe_notifications();
    handler.notify_persistent_update("default", "edge", "insert", 5);
    let msg = rx.try_recv();
    assert!(
        msg.is_ok(),
        "Should receive notification with active subscriber"
    );
}

#[tokio::test]
async fn test_notify_multiple_subscribers() {
    let (handler, _tmp) = create_test_handler();
    let mut rx1 = handler.subscribe_notifications();
    let mut rx2 = handler.subscribe_notifications();
    handler.notify_persistent_update("mygraph", "node", "delete", 3);
    assert!(rx1.try_recv().is_ok());
    assert!(rx2.try_recv().is_ok());
}

// === WI-01: Query Timeout ===

#[tokio::test]
async fn test_query_within_timeout_succeeds() {
    let temp = TempDir::new().unwrap();
    let mut config = engine_config(temp.path());
    config.storage.performance.query_timeout_ms = 5_000; // 5 seconds
    let handler = Handler::from_config(config).unwrap();
    // Simple insert + query should complete well within 5 seconds
    handler
        .query_program(None, "+edge[(1, 2)]".to_string())
        .await
        .unwrap();
    let result = handler.query_program(None, "?edge(X, Y)".to_string()).await;
    assert!(
        result.is_ok(),
        "Query within timeout should succeed, got: {result:?}"
    );
}

// === #39: Notification Dedup Tests ===

#[tokio::test]
async fn test_notification_seq_monotonic() {
    let (handler, _tmp) = create_test_handler();
    let mut rx = handler.subscribe_notifications();
    handler.notify_persistent_update("default", "a", "insert", 1);
    handler.notify_persistent_update("default", "b", "insert", 2);
    let n1 = rx.try_recv().unwrap();
    let n2 = rx.try_recv().unwrap();
    assert!(
        n1.seq() < n2.seq(),
        "Sequence numbers should be monotonically increasing"
    );
}

/// A cursor in `handler`'s current stream epoch.
fn cursor(handler: &Handler, last_seq: u64) -> Cursor {
    Cursor {
        epoch: Some(handler.notifications().epoch().to_string()),
        last_seq,
    }
}

#[tokio::test]
async fn test_resume_replays_notifications_after_the_cursor() {
    let (handler, _tmp) = create_test_handler();
    handler.notify_persistent_update("default", "a", "insert", 1);
    handler.notify_persistent_update("default", "b", "insert", 2);
    handler.notify_persistent_update("default", "c", "insert", 3);

    let missed = handler
        .notifications()
        .resume(Some(&cursor(&handler, 1)))
        .replay
        .unwrap();
    assert_eq!(missed.len(), 2, "Should return 2 notifications after seq 1");
    assert_eq!(missed[0].seq(), 2);
    assert_eq!(missed[1].seq(), 3);
}

#[tokio::test]
async fn test_resume_replays_nothing_when_caught_up() {
    let (handler, _tmp) = create_test_handler();
    handler.notify_persistent_update("default", "a", "insert", 1);
    let missed = handler
        .notifications()
        .resume(Some(&cursor(&handler, 1)))
        .replay
        .unwrap();
    assert!(
        missed.is_empty(),
        "Should be empty when caught up to latest seq"
    );
}

// === #14: Session Exclusivity Tests ===

#[tokio::test]
async fn test_session_ws_attach_detach() {
    let (handler, _tmp) = create_test_handler();
    let sid = handler.create_session("default").unwrap();

    // Attach should succeed
    assert!(handler.session_manager().attach_ws(&sid).is_ok());

    // Second attach should fail (exclusive)
    assert!(handler.session_manager().attach_ws(&sid).is_err());

    // Detach then re-attach should succeed
    handler.session_manager().detach_ws(&sid);
    assert!(handler.session_manager().attach_ws(&sid).is_ok());
}
