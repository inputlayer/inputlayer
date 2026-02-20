//! WI-02: WebSocket idle timeout configuration tests.
//! WI-03: Session cleanup tests.
//! WI-10: Broadcast notification tests.

use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

fn create_test_handler() -> (Handler, TempDir) {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let storage = StorageEngine::new(config).unwrap();
    let handler = Handler::new(storage);
    (handler, temp)
}

// === WI-02: WS Idle Timeout Config ===

#[test]
fn test_ws_idle_timeout_has_sane_default() {
    let config = Config::default();
    assert_eq!(
        config.http.ws_idle_timeout_ms, 300_000,
        "Default WS idle timeout should be 5 minutes (300,000 ms)"
    );
}

#[test]
fn test_ws_idle_timeout_zero_disabled_is_valid() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.http.ws_idle_timeout_ms = 0;
    // Should be able to create handler with disabled idle timeout
    assert!(
        Handler::from_config(config).is_ok(),
        "Handler::from_config with ws_idle_timeout_ms=0 should succeed"
    );
}

#[test]
fn test_ws_idle_timeout_custom_value_is_stored() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.http.ws_idle_timeout_ms = 60_000;
    let handler = Handler::from_config(config).unwrap();
    assert_eq!(handler.config().http.ws_idle_timeout_ms, 60_000);
}

// === WI-03: Session Cleanup ===

#[tokio::test]
async fn test_session_close_is_idempotent() {
    let (handler, _tmp) = create_test_handler();
    let sid = handler.create_session("default").unwrap();
    // First close succeeds
    assert!(handler.close_session(sid).is_ok());
    // Second close should not panic (may return error, that's fine)
    let _ = handler.close_session(sid);
    // We reach here without panicking
}

#[tokio::test]
async fn test_session_count_after_close() {
    let (handler, _tmp) = create_test_handler();
    let before = handler.session_manager().session_count();
    let sid = handler.create_session("default").unwrap();
    assert_eq!(handler.session_manager().session_count(), before + 1);
    handler.close_session(sid).unwrap();
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

    handler.close_session(sid1).unwrap();
    // Other sessions should still be accessible
    assert!(handler.session_manager().has_session(sid2));
    assert!(handler.session_manager().has_session(sid3));

    handler.close_session(sid2).unwrap();
    assert!(handler.session_manager().has_session(sid3));

    handler.close_session(sid3).unwrap();
    assert!(!handler.session_manager().has_session(sid3));
}

// === WI-10: Broadcast Notification Tests ===

#[tokio::test]
async fn test_notify_without_subscriber_does_not_panic() {
    let (handler, _tmp) = create_test_handler();
    // No subscriber — send goes to empty channel — must not panic
    handler.notify_persistent_update("default", "edge", "insert", 5);
    // Reaching here means no panic
}

#[tokio::test]
async fn test_notify_after_subscriber_dropped_does_not_panic() {
    let (handler, _tmp) = create_test_handler();
    let rx = handler.subscribe_notifications();
    drop(rx);
    // Receiver dropped — must not panic on send
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

// === WI-01: Query Timeout Config Tests ===

#[test]
fn test_query_timeout_config_default() {
    let config = Config::default();
    assert_eq!(
        config.storage.performance.query_timeout_ms, 30_000,
        "Default query timeout should be 30 seconds (30,000 ms)"
    );
}

#[test]
fn test_query_timeout_zero_means_disabled() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 0;
    let handler = Handler::from_config(config).unwrap();
    assert_eq!(handler.config().storage.performance.query_timeout_ms, 0);
}

#[tokio::test]
async fn test_query_within_timeout_succeeds() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
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

#[tokio::test]
async fn test_query_timeout_config_is_accessible_via_handler() {
    let temp = TempDir::new().unwrap();
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.query_timeout_ms = 12_345;
    let handler = Handler::from_config(config).unwrap();
    assert_eq!(
        handler.config().storage.performance.query_timeout_ms,
        12_345
    );
}
