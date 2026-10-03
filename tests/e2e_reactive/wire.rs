//! Required passes: request correlation on the `/ws` wire.
//!
//! A raw socket pipelines requests, malformed ones included, while its own
//! writes make the engine push notifications, a subscription delta and a
//! `notifications_missed` notice between the replies. Every reply must echo
//! its request's `id`, in request order; pushes and notices never carry one.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use inputlayer_testkit::{Checked, Fixture, Violation};
use inputlayer_ws_protocol::{ErrorCode, FrameClass, NoticeCode, ServerFrame, SubscriptionPush};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

use crate::engine;

const KG: &str = "wire";
/// Rows of `big`; each row's JSON is ~400 bytes, so `?big` streams (> 1 MB).
const BIG_ROWS: i64 = 3_000;
/// Per-connection notification buffer: a program touching more relations
/// than this lags the connection while it runs.
const BUFFER: usize = 8;
/// Relations one program inserts into: lags by `RELATIONS - BUFFER`, within
/// the disconnect limit of `BUFFER`.
const RELATIONS: usize = 12;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A received frame, typed and raw.
struct Received {
    frame: ServerFrame,
    raw: Value,
}

impl Received {
    fn id(&self) -> Option<&str> {
        self.frame.request_id().map(|id| id.as_str())
    }
}

async fn recv(socket: &mut Socket) -> Checked<Received> {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(60), socket.next())
            .await
            .map_err(|_| Violation::Timeout("a frame".into()))?
            .ok_or_else(|| Violation::Transport("closed".into()))?
            .map_err(|e| Violation::Transport(e.to_string()))?;
        let Message::Text(text) = message else {
            continue;
        };
        let frame = serde_json::from_str(&text)
            .map_err(|e| Violation::Transport(format!("not a protocol frame ({e}): {text}")))?;
        let raw = serde_json::from_str(&text).expect("parsed as a frame");
        return Ok(Received { frame, raw });
    }
}

async fn send(socket: &mut Socket, text: String) {
    socket.send(Message::Text(text)).await.expect("send");
}

fn padding(n: i64) -> String {
    format!("{n:0>390}")
}

#[tokio::test(flavor = "multi_thread")]
async fn replies_correlate_through_pushes_notices_and_bad_requests() -> Checked<()> {
    let engine = engine()
        .notification_buffer_size(BUFFER)
        .start()
        .await
        .expect("start engine");
    let fixture = (0..RELATIONS).fold(
        Fixture::new("wire_big", KG).facts(
            "big",
            (0..BIG_ROWS).map(|n| format!("({n}, \"{}\")", padding(n))),
        ),
        |fixture, i| fixture.facts(&format!("r{i}"), ["(0)".to_string()]),
    );
    fixture.install(&engine).await?;

    let (mut socket, _) = tokio_tungstenite::connect_async(engine.ws_url(KG))
        .await
        .expect("connect");
    let auth = json!({"type": "authenticate", "id": "auth", "api_key": engine.api_key()});
    send(&mut socket, auth.to_string()).await;
    let reply = recv(&mut socket).await?;
    assert!(
        matches!(reply.frame, ServerFrame::Authenticated { protocol_version, .. }
            if protocol_version == inputlayer_ws_protocol::PROTOCOL_VERSION),
        "{}",
        reply.raw
    );
    assert_eq!(reply.id(), Some("auth"));

    send(
        &mut socket,
        json!({"type": "execute", "id": "sub", "program": ".subscribe live ?r0(X)"}).to_string(),
    )
    .await;
    let reply = recv(&mut socket).await?;
    assert_eq!(reply.id(), Some("sub"), "{}", reply.raw);
    let ServerFrame::Result(snapshot) = &reply.frame else {
        panic!("subscribe reply: {}", reply.raw);
    };
    let generation = snapshot
        .subscribed
        .as_ref()
        .expect("names its subscription")
        .generation;

    // Pipelined: nothing is read until every request is sent.
    let writes: Vec<String> = (0..RELATIONS).map(|i| format!("+r{i}(1)")).collect();
    let requests = [
        json!({"type": "execute", "id": "w", "program": writes.join("\n")}).to_string(),
        json!({"type": "execute", "id": "q1", "program": "?big(X, Y)"}).to_string(),
        json!({"type": "ping", "id": "p"}).to_string(),
        r#"{"type":"ping","id":7}"#.to_string(),
        r#"{"type":"ping","id":"d","id":"e"}"#.to_string(),
        "not json".to_string(),
        json!({"type": "bogus", "id": "x"}).to_string(),
        json!({"type": "execute", "id": "q2", "program": "?r0(X)"}).to_string(),
        // An id may be reused once its request is answered.
        json!({"type": "execute", "id": "q2", "program": "?r1(X)"}).to_string(),
        json!({"type": "ping", "id": "last"}).to_string(),
    ];
    for request in requests {
        send(&mut socket, request).await;
    }

    let mut replies = Vec::new();
    let mut notices = Vec::new();
    let mut deltas = Vec::new();
    loop {
        let received = recv(&mut socket).await?;
        match received.frame.class() {
            FrameClass::Reply => {
                let last = received.id() == Some("last");
                replies.push(received);
                if last {
                    break;
                }
            }
            FrameClass::Notice => notices.push(received),
            FrameClass::Push => {
                assert!(received.raw.get("id").is_none(), "{}", received.raw);
                if let ServerFrame::Subscription(push) = &received.frame {
                    deltas.push(push.clone());
                }
            }
        }
    }
    // The delta for `r0(1)` may still be on its way.
    while deltas.is_empty() {
        let received = recv(&mut socket).await?;
        if let ServerFrame::Subscription(push) = received.frame {
            deltas.push(push);
        }
    }

    // Replies, in request order, each echoing its request's id.
    let ids: Vec<Option<&str>> = replies.iter().map(Received::id).collect();
    let chunks = ids.iter().filter(|id| **id == Some("q1")).count() - 2;
    let mut expected = vec![Some("w")];
    expected.extend(std::iter::repeat_n(Some("q1"), chunks + 2));
    expected.extend([
        Some("p"),
        None,
        None,
        None,
        Some("x"),
        Some("q2"),
        Some("q2"),
    ]);
    expected.push(Some("last"));
    assert_eq!(ids, expected);
    assert!(
        chunks >= 2,
        "?big must stream in several chunks, got {chunks}"
    );

    let kinds: Vec<&str> = replies
        .iter()
        .map(|r| r.raw["type"].as_str().unwrap())
        .collect();
    assert_eq!(kinds[0], "result");
    assert_eq!(kinds[1], "result_start");
    assert!(
        kinds[2..2 + chunks].iter().all(|k| *k == "result_chunk"),
        "{kinds:?}"
    );
    assert_eq!(kinds[2 + chunks], "result_end");
    let streamed_rows: usize = replies[2..2 + chunks]
        .iter()
        .map(|r| r.raw["rows"].as_array().unwrap().len())
        .sum();
    assert_eq!(streamed_rows, BIG_ROWS as usize);

    let after_stream = &replies[3 + chunks..];
    assert_eq!(after_stream[0].raw["type"], "pong");
    for bad in &after_stream[1..5] {
        assert!(
            matches!(
                bad.frame,
                ServerFrame::Error {
                    code: Some(ErrorCode::InvalidRequest),
                    ..
                }
            ),
            "{}",
            bad.raw
        );
    }
    for reply in &after_stream[5..7] {
        let mut rows = reply.raw["rows"].as_array().expect("a result").clone();
        rows.sort_by_key(Value::to_string);
        assert_eq!(rows, [json!([0]), json!([1])], "{}", reply.raw);
    }

    // The lag while `w` ran is announced, not passed off as a reply.
    assert!(
        notices.iter().any(|n| matches!(
            n.frame,
            ServerFrame::Notice {
                code: NoticeCode::NotificationsMissed,
                ..
            }
        )),
        "no notifications_missed notice: {:?}",
        notices.iter().map(|n| &n.raw).collect::<Vec<_>>()
    );
    for notice in &notices {
        assert!(notice.raw.get("id").is_none(), "{}", notice.raw);
    }

    // The subscription's delta names the generation `.subscribe` returned.
    let SubscriptionPush::SubscriptionDelta {
        subscription,
        generation: delta_generation,
        inserted,
        ..
    } = &deltas[0]
    else {
        panic!("{:?}", deltas[0]);
    };
    assert_eq!(subscription, "live");
    assert_eq!(*delta_generation, generation);
    assert_eq!(inserted, &vec![vec![json!(1)]]);
    Ok(())
}
