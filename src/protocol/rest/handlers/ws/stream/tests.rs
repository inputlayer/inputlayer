use inputlayer_ws_protocol::{RequestId, StatementCounts, StatementKind, Subscribed};
use serde_json::{json, Value};

use super::*;

fn parsed(frames: &[String]) -> Vec<(usize, ServerFrame)> {
    frames
        .iter()
        .map(|text| (text.len(), serde_json::from_str(text).unwrap()))
        .collect()
}

/// Row `i` of about `pad` bytes.
fn row(i: usize, pad: usize) -> Row {
    vec![json!(i), json!("x".repeat(pad))]
}

fn result(rows: Vec<Row>) -> ResultFrame {
    ResultFrame {
        id: RequestId::new("q").ok(),
        columns: vec!["i".into(), "pad".into()],
        row_count: rows.len(),
        total_count: rows.len(),
        row_provenance: vec!["persistent".to_string(); rows.len()],
        rows,
        truncated: false,
        execution_time_ms: 1,
        metadata: None,
        switched_kg: None,
        proof_trees: None,
        timing_breakdown: None,
        errors: Vec::new(),
        statements: Vec::new(),
        revision: None,
        subscribed: Some(Subscribed {
            subscription: "s".into(),
            generation: 2,
            revision: 7,
        }),
    }
}

fn delta(inserted: Vec<Row>, retracted: Vec<Row>) -> SubscriptionPush {
    SubscriptionPush::SubscriptionDelta {
        subscription: "s".into(),
        generation: 2,
        knowledge_graph: "kg".into(),
        seq: 4,
        revision: 9,
        columns: vec!["i".into(), "pad".into()],
        inserted,
        retracted,
    }
}

#[test]
fn a_small_result_is_one_frame_serialized_as_before() {
    let reply = result(vec![row(1, 10), row(2, 10)]);
    let expected = serde_json::to_string(&ServerFrame::Result(reply.clone())).unwrap();
    assert_eq!(result_frames(reply).unwrap(), [expected]);
}

#[test]
fn a_large_result_streams_as_one_bounded_ordered_result() {
    let rows: Vec<Row> = (0..3_000).map(|i| row(i, 1_000)).collect();
    let counts = vec![StatementCounts {
        index: 0,
        kind: StatementKind::Insert,
        inserted: 3,
        deleted: 0,
    }];
    let frames = parsed(
        &result_frames(ResultFrame {
            statements: counts.clone(),
            ..result(rows.clone())
        })
        .unwrap(),
    );

    let (_, ServerFrame::ResultStart(start)) = &frames[0] else {
        panic!("{:?}", frames[0]);
    };
    assert_eq!(start.id.as_ref().map(RequestId::as_str), Some("q"));
    assert_eq!(start.statements, counts, "counts ride on the header");
    assert_eq!(start.subscribed.as_ref().map(|s| s.generation), Some(2));
    let (mut got, mut provenance) = (Vec::new(), Vec::new());
    for (index, (bytes, frame)) in frames[1..frames.len() - 1].iter().enumerate() {
        let ServerFrame::ResultChunk {
            rows,
            row_provenance,
            chunk_index,
            ..
        } = frame
        else {
            panic!("{frame:?}");
        };
        assert_eq!(*chunk_index, index);
        assert!(*bytes <= FRAME_BUDGET, "chunk {index} is {bytes} bytes");
        got.extend(rows.iter().cloned());
        provenance.extend(row_provenance.iter().cloned());
    }
    assert_eq!(got, rows, "every row, in order, once");
    assert_eq!(provenance.len(), rows.len());
    let (
        _,
        ServerFrame::ResultEnd {
            row_count,
            chunk_count,
            ..
        },
    ) = frames.last().unwrap()
    else {
        panic!("{:?}", frames.last());
    };
    assert_eq!((*row_count, *chunk_count), (3_000, frames.len() - 2));
}

#[test]
fn a_result_with_a_row_no_frame_can_carry_is_undeliverable() {
    let rows = vec![row(0, 10), row(1, MAX_MESSAGE_SIZE), row(2, 10)];
    let reason = result_frames(result(rows)).expect_err("an unframable row must be refused");
    assert!(reason.contains("message limit"), "{reason}");
}

#[test]
fn a_small_delta_is_one_frame_serialized_as_before() {
    let push = delta(vec![row(1, 10)], vec![row(2, 10)]);
    let expected = serde_json::to_string(&ServerFrame::Subscription(push.clone())).unwrap();
    assert_eq!(push_frames(push).unwrap(), [expected]);
}

/// Reassemble a streamed delta as a client must: in order, checked at the end.
fn reassemble(frames: &[(usize, ServerFrame)]) -> (Vec<Row>, Vec<Row>) {
    let pushes: Vec<&SubscriptionPush> = frames
        .iter()
        .map(|(bytes, frame)| {
            assert!(*bytes <= FRAME_BUDGET, "frame of {bytes} bytes");
            let ServerFrame::Subscription(push) = frame else {
                panic!("{frame:?}");
            };
            assert_eq!(push.subscription(), ("s", 2));
            push
        })
        .collect();
    let SubscriptionPush::SubscriptionDeltaStart {
        seq: 4,
        revision: 9,
        ..
    } = pushes[0]
    else {
        panic!("{:?}", pushes[0]);
    };
    let (mut inserted, mut retracted) = (Vec::new(), Vec::new());
    for (index, push) in pushes[1..pushes.len() - 1].iter().enumerate() {
        let SubscriptionPush::SubscriptionDeltaChunk {
            seq: 4,
            chunk_index,
            inserted: ins,
            retracted: ret,
            ..
        } = push
        else {
            panic!("{push:?}");
        };
        assert_eq!(*chunk_index, index);
        inserted.extend(ins.iter().cloned());
        retracted.extend(ret.iter().cloned());
    }
    let SubscriptionPush::SubscriptionDeltaEnd {
        seq: 4,
        chunk_count,
        inserted_count,
        retracted_count,
        ..
    } = pushes.last().unwrap()
    else {
        panic!("{:?}", pushes.last());
    };
    assert_eq!(*chunk_count, pushes.len() - 2);
    assert_eq!(
        (*inserted_count, *retracted_count),
        (inserted.len(), retracted.len())
    );
    (inserted, retracted)
}

#[test]
fn a_large_delta_streams_as_one_logical_delta() {
    let inserted: Vec<Row> = (0..2_000).map(|i| row(i, 900)).collect();
    let retracted: Vec<Row> = (2_000..3_100).map(|i| row(i, 900)).collect();
    let frames = parsed(&push_frames(delta(inserted.clone(), retracted.clone())).unwrap());
    assert!(frames.len() > 4, "streamed: {} frames", frames.len());
    assert_eq!(reassemble(&frames), (inserted, retracted));
}

#[test]
fn a_delta_with_a_row_no_frame_can_carry_is_undeliverable() {
    let push = delta(vec![row(0, 10)], vec![row(1, MAX_MESSAGE_SIZE)]);
    let reason = push_frames(push).expect_err("an unframable row must be refused");
    assert!(reason.starts_with("Delta 4 has a row of"), "{reason}");
}

#[test]
fn errors_and_resets_pass_through_whole() {
    let reset = SubscriptionPush::SubscriptionReset {
        subscription: "s".into(),
        generation: 2,
        message: "gone".into(),
    };
    let frames = push_frames(reset).unwrap();
    let frames: Vec<Value> = frames
        .iter()
        .map(|f| serde_json::from_str(f).unwrap())
        .collect();
    assert_eq!(
        frames,
        [
            json!({"type": "subscription_reset", "subscription": "s", "generation": 2,
                "message": "gone"})
        ]
    );
}
