//! `max_result_rows` caps only the final result, flagged `truncated`.

use inputlayer::protocol::wire::WireValue;
use inputlayer::protocol::Handler;
use inputlayer::session::Provenance;
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
    assert_eq!(result.rows[0].values[0], WireValue::Int64(10));
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

async fn count_e(handler: &Handler) -> WireValue {
    let result = handler
        .query_program(None, "cnt(count<X>) <- e(X, Y)\n?cnt(N)".to_string())
        .await
        .expect("count e");
    result.rows[0].values[0].clone()
}

/// Mutation match queries are internal and never capped.
#[tokio::test]
async fn test_conditional_delete_over_limit_applies_to_all_matches() {
    let (handler, _tmp) = handler_with_edges(3).await;
    handler
        .query_program(None, "-e(X, Y) <- e(X, Y), X > 1".to_string())
        .await
        .unwrap();
    assert_eq!(count_e(&handler).await, WireValue::Int64(2));
}

#[tokio::test]
async fn test_conditional_update_over_limit_applies_to_all_matches() {
    let (handler, _tmp) = handler_with_edges(3).await;
    handler
        .query_program(None, "-e(X, Y), +e(X, 0) <- e(X, Y), Y > 100".to_string())
        .await
        .unwrap();
    let result = handler
        .query_program(None, "z(count<X>) <- e(X, 0)\n?z(N)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows[0].values[0], WireValue::Int64(9));
}

/// Sorting sees every row; the cap applies after.
#[tokio::test]
async fn test_order_by_over_limit_returns_true_top_rows() {
    let (handler, _tmp) = handler_with_edges(3).await;
    let result = handler
        .query_program(None, "?e(X:desc, Y)".to_string())
        .await
        .unwrap();
    let firsts: Vec<_> = result.rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(firsts, [9, 8, 7].map(WireValue::Int64));
    assert!(result.truncated);
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
    for row in &result.rows {
        let persistent = row.values[0] != WireValue::Int64(50);
        assert_eq!(
            row.provenance == Some(Provenance::Persistent),
            persistent,
            "{row:?}"
        );
    }

    let result = handler
        .query_program_with_session(&session_id, "?e(X:desc, Y)".to_string())
        .await
        .unwrap();
    let tagged: Vec<_> = result
        .rows
        .iter()
        .map(|r| (r.values[0].clone(), r.provenance))
        .collect();
    assert_eq!(
        tagged,
        [
            (50, Provenance::Ephemeral),
            (9, Provenance::Persistent),
            (8, Provenance::Persistent)
        ]
        .map(|(x, p)| (WireValue::Int64(x), Some(p)))
    );

    let result = handler
        .query_program_with_session(&session_id, "?e(50, Y)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(!result.truncated);
}

#[tokio::test]
async fn test_why_over_limit_is_flagged_truncated() {
    let (handler, _tmp) = handler_with_edges(3).await;
    let result = handler
        .query_program(None, ".why ?e(X, Y)".to_string())
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert!(result.truncated);
}
