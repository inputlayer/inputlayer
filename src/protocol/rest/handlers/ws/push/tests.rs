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
fn a_wide_small_integer_delta_that_fits_one_frame_stays_inline() {
    let push = delta(
        (0..1_500)
            .map(|i| (0..30).map(|c| json!((i + c) % 100)).collect())
            .collect(),
    );
    assert!(estimated_bytes(&push) <= FRAME_BUDGET);
    let frames = encode(push).now_or_never().unwrap().unwrap();
    assert_eq!(frames.len(), 1);
}

#[test]
fn integers_are_estimated_exactly() {
    for n in [
        json!(0),
        json!(9),
        json!(10),
        json!(-1),
        json!(i64::MIN),
        json!(u64::MAX),
    ] {
        assert_eq!(value_width(&n), n.to_string().len(), "{n}");
    }
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
        json!(-1.234_567_890_123_456_7e-300),
        json!("héllo"),
        json!(null),
        json!([1, {"k": true}]),
    ];
    assert!(row_width(&row) >= serde_json::to_string(&row).unwrap().len());
}
