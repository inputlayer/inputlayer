//! `max_result_rows` caps only the final result, flagged `truncated`.

use inputlayer::protocol::Handler;
use inputlayer::{Config, StorageEngine};
use tempfile::TempDir;

async fn handler_with_edges(max_result_rows: usize) -> (Handler, TempDir) {
    let temp = TempDir::new().expect("create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    config.storage.performance.max_result_rows = max_result_rows;
    let handler = Handler::new(StorageEngine::new(config).expect("create storage engine"));
    let facts: Vec<String> = (0..10).map(|i| format!("({i}, {})", 100 + i)).collect();
    handler
        .query_program(None, format!("+e[{}]", facts.join(", ")))
        .await
        .expect("insert edges");
    (handler, temp)
}

#[test]
fn test_max_result_rows_defaults_to_100k() {
    assert_eq!(
        Config::default().storage.performance.max_result_rows,
        100_000
    );
}

#[tokio::test]
async fn test_intermediate_over_limit_keeps_small_answer() {
    let (handler, _tmp) = handler_with_edges(3).await;
    let result = handler
        .query_program(None, "big(X, Y) <- e(X, Y)\n?big(7, Y)".to_string())
        .await
        .expect("query must not time out");
    assert_eq!(result.rows.len(), 1);
    assert!(!result.truncated);

    let result = handler
        .query_program(
            None,
            "big2(X, Y) <- e(X, Y)\nn(count<X>) <- big2(X, Y)\n?n(N)".to_string(),
        )
        .await
        .expect("query must not time out");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].values[0],
        inputlayer::protocol::wire::WireValue::Int64(10)
    );
}

#[tokio::test]
async fn test_final_result_over_limit_is_truncated() {
    let (handler, _tmp) = handler_with_edges(3).await;
    let result = handler
        .query_program(None, "?e(X, Y)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert!(result.truncated);

    // The next request on the same handler is unaffected.
    let result = handler
        .query_program(None, "?e(1, Y)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(!result.truncated);
}

#[tokio::test]
async fn test_conditional_delete_over_limit_is_rejected() {
    let (handler, _tmp) = handler_with_edges(3).await;
    let err = handler
        .query_program(None, "-e(X, Y) <- e(X, Y)".to_string())
        .await
        .unwrap_err();
    assert!(err.contains("max_result_rows"), "{err}");

    let remaining = handler
        .query_program(None, "cnt(count<X>) <- e(X, Y)\n?cnt(N)".to_string())
        .await
        .unwrap();
    assert_eq!(
        remaining.rows[0].values[0],
        inputlayer::protocol::wire::WireValue::Int64(10),
        "nothing may be deleted"
    );
}

#[tokio::test]
async fn test_session_query_over_limit_is_truncated() {
    let (handler, _tmp) = handler_with_edges(3).await;
    let session_id = handler.create_session("default").unwrap();
    let extra = vec![inputlayer::Tuple::new(vec![
        inputlayer::Value::Int64(50),
        inputlayer::Value::Int64(150),
    ])];
    handler
        .session_insert_ephemeral(&session_id, "e", extra)
        .unwrap();

    let result = handler
        .query_program_with_session(&session_id, "?e(X, Y)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert!(result.truncated);

    let result = handler
        .query_program_with_session(&session_id, "?e(50, Y)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(!result.truncated);
}
