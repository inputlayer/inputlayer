//! The write half of a `/ws` connection, fenced by its credential.
//!
//! Once a connection is authenticated, every data frame passes the credential
//! fence: the principal is checked after the socket is ready for the frame
//! and before the frame is handed to it, with no await in between. A frame is
//! therefore authorized at the instant it is committed to the socket, so no
//! result, notification or subscription push leaves after the credential is
//! revoked. The one exception is the closing revocation notice.

use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{Sink, SinkExt};

use crate::auth::Principal;

/// Why a frame was not sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SendError {
    /// The socket is closed or failed.
    Closed,
    /// The connection's credential was revoked.
    Revoked,
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
        if data && self.principal.as_ref().is_some_and(Principal::is_revoked) {
            return Err(SendError::Revoked);
        }
        self.sink
            .start_send_unpin(message)
            .map_err(|_| SendError::Closed)?;
        self.sink.flush().await.map_err(|_| SendError::Closed)
    }

    /// Send `json` past the fence: only for telling the client its
    /// credential was revoked.
    pub(super) async fn send_revocation_notice(&mut self, json: String) {
        let _ = self.sink.send(Message::Text(json)).await;
    }

    pub(super) async fn close(&mut self) {
        let _ = self.sink.close().await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::auth::{ApiKeyRecord, CredentialRegistry, Role, UserRecord};

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
            Err(SendError::Revoked)
        );
        assert_eq!(
            outbound.send(Message::Binary(vec![1])).await,
            Err(SendError::Revoked)
        );
        outbound.send(Message::Ping(Vec::new())).await.unwrap();
        outbound.send_revocation_notice("revoked".into()).await;
        outbound.close().await;

        assert_eq!(
            outbound.sink,
            vec![
                Message::Text("before".into()),
                Message::Ping(Vec::new()),
                Message::Text("revoked".into()),
            ]
        );
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
}
