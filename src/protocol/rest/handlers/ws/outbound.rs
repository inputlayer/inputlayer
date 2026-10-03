//! The write half of a `/ws` connection, fenced by its credential.
//!
//! Once a connection is authenticated, every data frame passes the credential
//! fence: the principal is checked after the socket is ready for the frame
//! and before the frame is handed to it, with no await in between. A frame is
//! therefore authorized at the instant it is committed to the socket, so no
//! result, notification or subscription push leaves after the credential is
//! revoked or expires. The one exception is the closing notice saying so.

use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{Sink, SinkExt};
use inputlayer_ws_protocol::{ErrorCode, NoticeCode, ServerFrame};
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
}

pub(super) struct Outbound<S = SplitSink<WebSocket, Message>> {
    sink: S,
    /// Unset during the auth phase.
    principal: Option<Principal>,
}

impl<S: Sink<Message> + Unpin> Outbound<S> {
    pub(super) fn new(sink: S) -> Self {
        Self {
            sink,
            principal: None,
        }
    }

    /// Fence every later data frame with `principal`.
    pub(super) fn bind(&mut self, principal: Principal) {
        self.principal = Some(principal);
    }

    /// Send `message`; data frames only while the credential is live.
    pub(super) async fn send(&mut self, message: Message) -> Result<(), SendError> {
        let data = matches!(message, Message::Text(_) | Message::Binary(_));
        std::future::poll_fn(|cx| self.sink.poll_ready_unpin(cx))
            .await
            .map_err(|_| SendError::Closed)?;
        if data && self.principal.as_ref().and_then(Principal::ended).is_some() {
            return Err(SendError::CredentialEnded);
        }
        self.sink
            .start_send_unpin(message)
            .map_err(|_| SendError::Closed)?;
        self.sink.flush().await.map_err(|_| SendError::Closed)
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
        let _ = self.sink.send(Message::Text(encode(&notice))).await;
    }

    pub(super) async fn close(&mut self) {
        let _ = self.sink.close().await;
    }

    #[cfg(test)]
    pub(super) fn sink(&self) -> &S {
        &self.sink
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

    #[tokio::test]
    async fn unbound_connection_is_not_fenced() {
        let mut outbound = Outbound::new(Vec::new());
        outbound
            .send(Message::Text("auth_error".into()))
            .await
            .unwrap();
        assert_eq!(outbound.sink, vec![Message::Text("auth_error".into())]);
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
