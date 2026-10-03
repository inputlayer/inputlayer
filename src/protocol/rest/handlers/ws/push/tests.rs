use futures_util::FutureExt;
use serde_json::json;

use super::*;

fn delta(rows: Vec<Row>) -> SubscriptionPush {
    SubscriptionPush::SubscriptionDelta {
        subscription: "s".into(),
        generation: 1,
        knowledge_graph: "kg".into(),
        seq: 1,
        revision: 1,
        columns: vec!["n".into()],
        inserted: rows,
        retracted: Vec::new(),
    }
}

#[test]
fn a_small_delta_of_many_rows_is_encoded_without_the_blocking_pool() {
    let push = delta((0..300).map(|i| vec![json!(i)]).collect());
    // No Tokio runtime is running: reaching the blocking pool would panic,
    // and an inline encode completes on its first poll.
    let frames = encode(push).now_or_never().unwrap().unwrap();
    assert_eq!(frames.len(), 1);
}

#[test]
fn a_wide_delta_behind_a_short_first_row_goes_to_the_blocking_pool() {
    let rows: Vec<Row> = std::iter::once(vec![json!("")])
        .chain((0..10_000).map(|_| vec![json!("x".repeat(1_000))]))
        .collect();
    let push = delta(rows);
    // No Tokio runtime is running, so reaching the blocking pool panics.
    let offloaded =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| encode(push).now_or_never()));
    assert!(offloaded.is_err());
}

#[test]
fn a_delta_too_large_for_one_frame_is_estimated_over_the_budget() {
    let rows: Vec<Row> = (0..2_000)
        .map(|i| vec![json!(i), json!("x".repeat(1_000))])
        .collect();
    let push = delta(rows);
    assert!(estimated_bytes(&push) > FRAME_BUDGET);
    assert!(stream::push_frames(push).unwrap().len() > 1);
}

#[test]
fn the_estimate_covers_the_serialized_rows() {
    let row = vec![
        json!(i64::MIN),
        json!("héllo"),
        json!(null),
        json!([1, {"k": true}]),
    ];
    assert!(row_width(&row) >= serde_json::to_string(&row).unwrap().len());
}
