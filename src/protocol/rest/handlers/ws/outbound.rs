//! The write half of a `/ws` connection, fenced by its credential and bounded
//! in time.
//!
//! Once a connection is authenticated, every data frame passes the credential
//! fence: the principal is checked after the socket is ready for the frame
//! and before the frame is handed to it, with no await in between. A frame is
//! therefore authorized at the instant it is committed to the socket, so no
//! result, notification or subscription push leaves after the credential is
//! revoked or expires. The one exception is the closing notice saying so.
//!
//! Each frame is written and flushed before the next, so a connection holds
//! at most one outbound frame. A client that stops reading cannot hold the
//! connection forever: a frame the socket does not take within the send
//! timeout (`http.ws_send_timeout_ms`) fails the connection, and nothing is
//! written to it after that.
//!
//! The check and the hand-off hold the credential's end lock, so revoking it
//! or bringing its expiry forward waits for a frame already admitted.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{Sink, SinkExt};
use inputlayer_ws_protocol::{ErrorCode, NoticeCode, ServerFrame};
use tokio::time::Sleep;
use tracing::warn;

use crate::auth::{CredentialEnded, Principal};
use crate::protocol::MAX_MESSAGE_SIZE;

/// Why a frame was not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SendError {
    /// The socket is closed or failed.
    Closed,
    /// The connection's credential was revoked or expired.
    CredentialEnded,
    /// The client stopped reading: a frame waited past the send timeout.
    TimedOut,
}

pub(super) struct Outbound<S = SplitSink<WebSocket, Message>> {
    sink: S,
    /// Unset during the auth phase.
    principal: Option<Principal>,
    /// Longest wait for the socket to take one frame; `None` waits forever.
    send_timeout: Option<Duration>,
    /// Set once the socket failed or stalled: nothing more is written.
    failed: Option<SendError>,
}

impl<S: Sink<Message> + Unpin> Outbound<S> {
    pub(super) fn new(sink: S) -> Self {
        Self {
            sink,
            principal: None,
            send_timeout: None,
            failed: None,
        }
    }

    /// Fail a frame the socket has not taken within `timeout` (zero: never).
    pub(super) fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = (!timeout.is_zero()).then_some(timeout);
        self
    }

    /// Fence every later data frame with `principal`.
    pub(super) fn bind(&mut self, principal: Principal) {
        self.principal = Some(principal);
    }

    /// Send `message`; data frames only while the credential is live.
    pub(super) async fn send(&mut self, message: Message) -> Result<(), SendError> {
        self.transmit(message, true).await
    }

    async fn transmit(&mut self, message: Message, fenced: bool) -> Result<(), SendError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        let data = matches!(message, Message::Text(_) | Message::Binary(_));
        let mut deadline = Deadline::new(self.send_timeout);
        let result = async {
            std::future::poll_fn(|cx| {
                let step = self.sink.poll_ready_unpin(cx);
                deadline.poll(cx, step)
            })
            .await?;
            let sink = &mut self.sink;
            let enqueue = || {
                sink.start_send_unpin(message)
                    .map_err(|_| SendError::Closed)
            };
            match self.principal.as_ref().filter(|_| fenced && data) {
                Some(principal) => principal
                    .admit(enqueue)
                    .map_err(|_| SendError::CredentialEnded)??,
                None => enqueue()?,
            }
            std::future::poll_fn(|cx| {
                let step = self.sink.poll_flush_unpin(cx);
                deadline.poll(cx, step)
            })
            .await
        }
        .await;
        match result {
            Err(error @ (SendError::Closed | SendError::TimedOut)) => {
                if error == SendError::TimedOut {
                    warn!(timeout = ?self.send_timeout, "ws_send_timeout");
                }
                self.failed = Some(error);
                Err(error)
            }
            other => other,
        }
    }

    /// Serialize and send `frame`; `false` if the connection is dead. A frame
    /// over [`MAX_MESSAGE_SIZE`] is replaced by an `error` for the same request.
    pub(super) async fn send_frame(&mut self, frame: &ServerFrame) -> bool {
        let json = encode(frame);
        self.send(Message::Text(json)).await.is_ok()
    }

    /// Send a `notice`; `false` if the connection is dead.
    pub(super) async fn send_notice(&mut self, code: NoticeCode, message: String) -> bool {
        self.send_frame(&ServerFrame::Notice { code, message })
            .await
    }

    /// Tell the client its credential was revoked or expired: the one data
    /// frame sent past the fence.
    pub(super) async fn send_credential_notice(&mut self, ended: CredentialEnded) {
        let (code, message) = match ended {
            CredentialEnded::Revoked => (
                NoticeCode::CredentialRevoked,
                "Credential revoked; reconnect with valid credentials",
            ),
            CredentialEnded::Expired => (
                NoticeCode::CredentialExpired,
                "Credential expired; reconnect with valid credentials",
            ),
        };
        let notice = ServerFrame::Notice {
            code,
            message: message.to_string(),
        };
        let _ = self.transmit(Message::Text(encode(&notice)), false).await;
    }

    pub(super) async fn close(&mut self) {
        if self.failed.is_some() {
            return;
        }
        let mut deadline = Deadline::new(self.send_timeout);
        let _ = std::future::poll_fn(|cx| {
            let step = self.sink.poll_close_unpin(cx);
            deadline.poll(cx, step)
        })
        .await;
    }

    #[cfg(test)]
    pub(super) fn sink(&self) -> &S {
        &self.sink
    }
}

/// The time limit for the socket to take one frame. The timer is armed only
/// once the socket makes the frame wait, so a writable socket costs nothing.
struct Deadline {
    limit: Option<Duration>,
    timer: Option<Pin<Box<Sleep>>>,
}

impl Deadline {
    fn new(limit: Option<Duration>) -> Self {
        Self { limit, timer: None }
    }

    /// `step`'s outcome, or [`SendError::TimedOut`] once the frame has waited
    /// past the limit.
    fn poll<E>(
        &mut self,
        cx: &mut Context<'_>,
        step: Poll<Result<(), E>>,
    ) -> Poll<Result<(), SendError>> {
        match step {
            Poll::Ready(result) => Poll::Ready(result.map_err(|_| SendError::Closed)),
            Poll::Pending => {
                let Some(limit) = self.limit else {
                    return Poll::Pending;
                };
                let timer = self
                    .timer
                    .get_or_insert_with(|| Box::pin(tokio::time::sleep(limit)));
                timer.as_mut().poll(cx).map(|()| Err(SendError::TimedOut))
            }
        }
    }
}

/// JSON of `frame`, or of an `error` answering the same request when it
/// cannot be sent.
pub(super) fn encode(frame: &ServerFrame) -> String {
    let failure = match serde_json::to_string(frame) {
        Ok(json) if json.len() <= MAX_MESSAGE_SIZE => return json,
        Ok(json) => {
            warn!(
                size = json.len(),
                max = MAX_MESSAGE_SIZE,
                "ws_frame_too_large"
            );
            format!(
                "Result too large ({} bytes, max {MAX_MESSAGE_SIZE})",
                json.len()
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "ws_frame_serialize_failed");
            "Internal server error".to_string()
        }
    };
    let error = ServerFrame::error(
        frame.request_id().cloned(),
        Some(ErrorCode::Internal),
        failure,
    );
    serde_json::to_string(&error)
        .unwrap_or_else(|_| r#"{"type":"error","message":"Internal serialization error"}"#.into())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::auth::{ApiKeyRecord, ApiKeyTimes, CredentialRegistry, Role, UserRecord};
    use inputlayer_ws_protocol::RequestId;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    fn principal(registry: &CredentialRegistry) -> Principal {
        registry.load(
            vec![UserRecord {
                username: "bob".to_string(),
                password_hash: String::new(),
                role: Role::Viewer,
            }],
            vec![ApiKeyRecord {
                label: "k".to_string(),
                key_hash: "h".to_string(),
                username: "bob".to_string(),
                times: ApiKeyTimes::default(),
                scope: None,
            }],
        );
        registry.authenticate_key("h").unwrap()
    }

    #[tokio::test]
    async fn revoked_credential_stops_data_frames_but_not_control_frames() {
        let registry = CredentialRegistry::default();
        let mut outbound = Outbound::new(Vec::new());
        outbound.bind(principal(&registry));

        outbound.send(Message::Text("before".into())).await.unwrap();
        registry.revoke_key("k");
        assert_eq!(
            outbound.send(Message::Text("after".into())).await,
            Err(SendError::CredentialEnded)
        );
        assert_eq!(
            outbound.send(Message::Binary(vec![1])).await,
            Err(SendError::CredentialEnded)
        );
        outbound.send(Message::Ping(Vec::new())).await.unwrap();
        assert!(!outbound.send_frame(&ServerFrame::Pong { id: None }).await);
        outbound
            .send_credential_notice(CredentialEnded::Revoked)
            .await;
        outbound.close().await;

        assert_eq!(outbound.sink.len(), 3, "{:?}", outbound.sink);
        assert_eq!(
            outbound.sink[..2],
            [Message::Text("before".into()), Message::Ping(Vec::new())]
        );
        let Message::Text(notice) = &outbound.sink[2] else {
            panic!("{:?}", outbound.sink[2]);
        };
        let notice: ServerFrame = serde_json::from_str(notice).unwrap();
        assert!(matches!(
            notice,
            ServerFrame::Notice {
                code: NoticeCode::CredentialRevoked,
                ..
            }
        ));
    }

    #[test]
    fn revocation_waits_for_final_frame_enqueue() {
        use std::pin::Pin;
        use std::sync::{mpsc, Arc};
        use std::task::{Context, Poll};
        use std::time::Duration;

        struct PausedSink {
            entered: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            frames: Vec<Message>,
        }

        impl Sink<Message> for PausedSink {
            type Error = std::convert::Infallible;

            fn poll_ready(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
                self.entered.send(()).unwrap();
                self.release.recv_timeout(Duration::from_secs(10)).unwrap();
                self.frames.push(message);
                Ok(())
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn poll_close(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
        }

        let ends: [fn(&CredentialRegistry); 2] = [
            |registry| {
                registry.revoke_key("k");
            },
            |registry| registry.expire_key("h", 0),
        ];
        let cases = ends.into_iter().flat_map(|end| {
            [Message::Text("protected".into()), Message::Binary(vec![1])]
                .map(|message| (end, message))
        });
        for (end, message) in cases {
            let registry = Arc::new(CredentialRegistry::default());
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let mut outbound = Outbound::new(PausedSink {
                entered: entered_tx,
                release: release_rx,
                frames: Vec::new(),
            });
            outbound.bind(principal(&registry));
            let expected = message.clone();
            let sender = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap()
                    .block_on(outbound.send(message))
                    .unwrap();
                outbound
            });
            entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let (started_tx, started_rx) = mpsc::channel();
            let (revoked_tx, revoked_rx) = mpsc::channel();
            let revoker = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                end(&registry);
                revoked_tx.send(()).unwrap();
            });
            started_rx.recv().unwrap();
            let early = revoked_rx.recv_timeout(Duration::from_millis(200));
            release_tx.send(()).unwrap();
            let mut outbound = sender.join().unwrap();
            revoker.join().unwrap();
            assert!(early.is_err(), "credential ended before final enqueue");
            assert_eq!(outbound.sink.frames, vec![expected]);
            let result = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(outbound.send(Message::Text("after".into())));
            assert_eq!(result, Err(SendError::CredentialEnded));
        }
    }

    #[tokio::test]
    async fn unbound_connection_is_not_fenced() {
        let mut outbound = Outbound::new(Vec::new());
        outbound
            .send(Message::Text("auth_error".into()))
            .await
            .unwrap();
        assert_eq!(outbound.sink, vec![Message::Text("auth_error".into())]);
    }

    /// A socket whose client never reads: it never takes another frame.
    struct Stalled;

    impl Sink<Message> for Stalled {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), ()> {
            unreachable!("never ready")
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Pending
        }
    }

    #[tokio::test]
    async fn a_stalled_reader_fails_the_connection_after_the_send_timeout() {
        let timeout = Duration::from_millis(50);
        let mut outbound = Outbound::new(Stalled).with_send_timeout(timeout);
        let started = std::time::Instant::now();
        assert_eq!(
            outbound.send(Message::Text("x".into())).await,
            Err(SendError::TimedOut)
        );
        let waited = started.elapsed();
        assert!(waited >= timeout, "{waited:?}");

        // Nothing more is written, and nothing waits again.
        let again = std::time::Instant::now();
        assert_eq!(
            outbound.send(Message::Ping(Vec::new())).await,
            Err(SendError::TimedOut)
        );
        outbound
            .send_credential_notice(CredentialEnded::Revoked)
            .await;
        outbound.close().await;
        assert!(again.elapsed() < timeout, "{:?}", again.elapsed());
    }

    #[tokio::test]
    async fn without_a_send_timeout_a_stalled_reader_is_waited_for() {
        let mut outbound = Outbound::new(Stalled).with_send_timeout(Duration::ZERO);
        let send = outbound.send(Message::Text("x".into()));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), send)
                .await
                .is_err(),
            "zero disables the timeout"
        );
    }

    #[test]
    fn oversized_frame_becomes_an_error_for_the_same_request() {
        let frame = ServerFrame::error(
            Some(RequestId::new("big").unwrap()),
            None,
            "x".repeat(MAX_MESSAGE_SIZE),
        );
        let reply: ServerFrame = serde_json::from_str(&encode(&frame)).unwrap();
        let ServerFrame::Error {
            id, code, message, ..
        } = reply
        else {
            panic!("{reply:?}");
        };
        assert_eq!(id.unwrap().as_str(), "big");
        assert_eq!(code, Some(ErrorCode::Internal));
        assert!(message.starts_with("Result too large"), "{message}");
    }
}
