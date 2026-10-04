use super::*;

#[allow(clippy::unnecessary_wraps)] // reads as the `id` field it fills
fn id(s: &str) -> Option<RequestId> {
    Some(RequestId::new(s).unwrap())
}

fn result(id: Option<RequestId>) -> ServerFrame {
    ServerFrame::Result(ResultFrame {
        id,
        columns: vec!["x".into()],
        rows: vec![vec![serde_json::json!(1)]],
        row_count: 1,
        total_count: 1,
        truncated: false,
        execution_time_ms: 0,
        row_provenance: Vec::new(),
        metadata: None,
        switched_kg: None,
        proof_trees: None,
        timing_breakdown: None,
        errors: Vec::new(),
        statements: Vec::new(),
        revision: None,
        subscribed: None,
    })
}

fn round_trip(frame: &ServerFrame) -> ServerFrame {
    let json = serde_json::to_string(frame).unwrap();
    serde_json::from_str(&json).unwrap_or_else(|e| panic!("{json}: {e}"))
}

#[test]
fn every_reply_echoes_its_id() {
    let replies = [
        ServerFrame::Authenticated {
            id: id("a"),
            session_id: "s".into(),
            knowledge_graph: "default".into(),
            version: "0".into(),
            role: "admin".into(),
            protocol_version: crate::PROTOCOL_VERSION,
            stream_epoch: "0123456789abcdef".into(),
        },
        ServerFrame::AuthError {
            id: id("a"),
            message: "no".into(),
        },
        result(id("a")),
        ServerFrame::ResultStart(ResultStartFrame {
            id: id("a"),
            columns: Vec::new(),
            total_count: 0,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
            switched_kg: None,
            proof_trees: None,
            timing_breakdown: None,
            errors: Vec::new(),
            statements: Vec::new(),
            revision: None,
            subscribed: None,
        }),
        ServerFrame::ResultChunk {
            id: id("a"),
            rows: Vec::new(),
            row_provenance: Vec::new(),
            chunk_index: 0,
        },
        ServerFrame::ResultEnd {
            id: id("a"),
            row_count: 0,
            chunk_count: 0,
        },
        ServerFrame::error(id("a"), Some(ErrorCode::InvalidRequest), "bad".into()),
        ServerFrame::Pong { id: id("a") },
    ];
    for frame in replies {
        let json = serde_json::to_value(&frame).unwrap();
        assert_eq!(json["id"], "a", "{json}");
        let parsed = round_trip(&frame);
        assert_eq!(parsed, frame);
        assert_eq!(parsed.class(), FrameClass::Reply);
        assert_eq!(parsed.request_id(), id("a").as_ref());
    }
}

#[test]
fn replies_without_id_omit_it() {
    assert_eq!(
        serde_json::to_string(&ServerFrame::Pong { id: None }).unwrap(),
        r#"{"type":"pong"}"#
    );
    let json = serde_json::to_value(result(None)).unwrap();
    assert!(json.get("id").is_none(), "{json}");
}

#[test]
fn notices_and_pushes_never_carry_an_id() {
    let frames = [
        ServerFrame::Notice {
            code: NoticeCode::IdleTimeout,
            message: "Idle timeout".into(),
        },
        ServerFrame::Subscription(SubscriptionPush::SubscriptionDelta {
            subscription: "s".into(),
            generation: 2,
            knowledge_graph: "default".into(),
            seq: 1,
            revision: 12,
            columns: vec!["x".into()],
            inserted: vec![vec![serde_json::json!(1)]],
            retracted: Vec::new(),
        }),
        ServerFrame::Subscription(SubscriptionPush::SubscriptionError {
            subscription: "s".into(),
            generation: 2,
            message: "boom".into(),
        }),
        ServerFrame::Subscription(SubscriptionPush::SubscriptionDeltaStart {
            subscription: "s".into(),
            generation: 2,
            knowledge_graph: "default".into(),
            seq: 2,
            revision: 13,
            columns: vec!["x".into()],
        }),
        ServerFrame::Subscription(SubscriptionPush::SubscriptionDeltaChunk {
            subscription: "s".into(),
            generation: 2,
            seq: 2,
            chunk_index: 0,
            inserted: vec![vec![serde_json::json!(2)]],
            retracted: vec![vec![serde_json::json!(1)]],
        }),
        ServerFrame::Subscription(SubscriptionPush::SubscriptionDeltaEnd {
            subscription: "s".into(),
            generation: 2,
            seq: 2,
            chunk_count: 1,
            inserted_count: 1,
            retracted_count: 1,
        }),
        ServerFrame::Subscription(SubscriptionPush::SubscriptionReset {
            subscription: "s".into(),
            generation: 2,
            message: "gone".into(),
        }),
        ServerFrame::Notification(Notification::KgChange {
            knowledge_graph: "kg".into(),
            operation: "created".into(),
            timestamp_ms: 1,
            seq: 9,
        }),
    ];
    for frame in frames {
        let parsed = round_trip(&frame);
        assert_eq!(parsed, frame);
        assert_ne!(parsed.class(), FrameClass::Reply);
        assert_eq!(parsed.request_id(), None);
    }
}

#[test]
fn notice_wire_shape() {
    let frame = ServerFrame::Notice {
        code: NoticeCode::NotificationsMissed,
        message: "Missed 3 notification(s)".into(),
    };
    assert_eq!(
        serde_json::to_string(&frame).unwrap(),
        r#"{"type":"notice","code":"notifications_missed","message":"Missed 3 notification(s)"}"#
    );
    assert!(!NoticeCode::NotificationsMissed.closes_connection());
    assert!(!NoticeCode::ReplayGap.closes_connection());
    assert!(NoticeCode::IdleTimeout.closes_connection());
    assert_eq!(
        serde_json::to_value(NoticeCode::ReplayGap).unwrap(),
        serde_json::json!("replay_gap")
    );
}

#[test]
fn push_wire_shape() {
    let json = r#"{"type":"subscription_delta","subscription":"s","generation":4,
        "knowledge_graph":"default","seq":1,"revision":9,"columns":["X"],"inserted":[[3]],
        "retracted":[]}"#;
    let frame: ServerFrame = serde_json::from_str(json).unwrap();
    let ServerFrame::Subscription(push) = &frame else {
        panic!("{frame:?}");
    };
    assert_eq!(push.subscription(), ("s", 4));

    let json = r#"{"type":"persistent_update","knowledge_graph":"default","relation":"edge",
        "operation":"insert","count":5,"timestamp_ms":1,"seq":42}"#;
    let ServerFrame::Notification(notification) = serde_json::from_str(json).unwrap() else {
        panic!("not a notification");
    };
    assert_eq!(notification.knowledge_graph(), "default");
    assert_eq!(notification.seq(), 42);
}

#[test]
fn streamed_delta_wire_shape() {
    let frames = [
        r#"{"type":"subscription_delta_start","subscription":"s","generation":4,
            "knowledge_graph":"default","seq":3,"revision":9,"columns":["X"]}"#,
        r#"{"type":"subscription_delta_chunk","subscription":"s","generation":4,"seq":3,
            "chunk_index":0,"inserted":[[3]],"retracted":[]}"#,
        r#"{"type":"subscription_delta_end","subscription":"s","generation":4,"seq":3,
            "chunk_count":1,"inserted_count":1,"retracted_count":0}"#,
        r#"{"type":"subscription_reset","subscription":"s","generation":4,"message":"m"}"#,
    ];
    for json in frames {
        let frame: ServerFrame =
            serde_json::from_str(json).unwrap_or_else(|e| panic!("{json}: {e}"));
        let ServerFrame::Subscription(push) = &frame else {
            panic!("{frame:?}");
        };
        assert_eq!(push.subscription(), ("s", 4));
        assert_eq!(frame.class(), FrameClass::Push);
    }
}

#[test]
fn streamed_subscribe_reply_names_the_subscription() {
    let json = r#"{"type":"result_start","id":"r","columns":["X"],"total_count":2,
        "truncated":false,"execution_time_ms":1,
        "subscribed":{"subscription":"s","generation":7,"revision":3}}"#;
    let ServerFrame::ResultStart(start) = serde_json::from_str(json).unwrap() else {
        panic!("not a result_start");
    };
    assert_eq!(start.subscribed.map(|s| s.generation), Some(7));
}

#[test]
fn subscribe_reply_names_the_subscription() {
    let ServerFrame::Result(mut frame) = result(id("r")) else {
        unreachable!()
    };
    frame.subscribed = Some(Subscribed {
        subscription: "s".into(),
        generation: 7,
        revision: 3,
    });
    let json = serde_json::to_value(ServerFrame::Result(frame)).unwrap();
    assert_eq!(
        json["subscribed"],
        serde_json::json!({"subscription": "s", "generation": 7, "revision": 3})
    );
}

#[test]
fn unknown_frame_types_are_rejected() {
    assert!(serde_json::from_str::<ServerFrame>(r#"{"type":"bogus"}"#).is_err());
}

#[test]
fn cancel_ack_is_a_reply_to_the_cancel() {
    let ack = ServerFrame::CancelAck {
        id: id("c"),
        target: RequestId::new("q").unwrap(),
        outcome: CancelOutcome::TooLate,
    };
    let json = serde_json::to_value(&ack).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"type": "cancel_ack", "id": "c", "target": "q", "outcome": "too_late"})
    );
    assert_eq!(round_trip(&ack), ack);
    assert_eq!(ack.class(), FrameClass::Reply);
    assert_eq!(ack.request_id(), id("c").as_ref());
}

#[test]
fn stop_codes_serialize_in_snake_case() {
    for (code, name) in [
        (ErrorCode::DeadlineExceeded, "deadline_exceeded"),
        (ErrorCode::Cancelled, "cancelled"),
        (ErrorCode::OutcomeUnknown, "outcome_unknown"),
    ] {
        assert_eq!(serde_json::to_value(code).unwrap(), name);
    }
}

#[test]
fn statement_counts_are_listed_only_when_a_fact_statement_committed() {
    let ServerFrame::Result(mut frame) = result(None) else {
        unreachable!()
    };
    let json = serde_json::to_value(ServerFrame::Result(frame.clone())).unwrap();
    assert!(json.get("statements").is_none(), "{json}");

    frame.statements = vec![StatementCounts {
        index: 1,
        kind: StatementKind::Update,
        inserted: 1,
        deleted: 0,
    }];
    let reply = ServerFrame::Result(frame);
    let json = serde_json::to_value(&reply).unwrap();
    assert_eq!(
        json["statements"],
        serde_json::json!([{"index": 1, "kind": "update", "inserted": 1, "deleted": 0}])
    );
    assert_eq!(round_trip(&reply), reply);
}

#[test]
fn a_result_names_its_commit_revision_only_when_it_has_one() {
    let json = serde_json::to_value(result(None)).unwrap();
    assert!(json.get("revision").is_none(), "{json}");

    let ServerFrame::Result(mut frame) = result(None) else {
        unreachable!()
    };
    frame.revision = Some(42);
    let frame = ServerFrame::Result(frame);
    let json = serde_json::to_value(&frame).unwrap();
    assert_eq!(json["revision"], 42, "{json}");
    assert_eq!(round_trip(&frame), frame);
}
