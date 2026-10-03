//! The authentication phase of a `/ws` connection.

use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitStream;
use futures_util::StreamExt;
use inputlayer_ws_protocol::{probe_request_id, ClientFrame, NoticeCode, RequestId, ServerFrame};
use tracing::warn;

use super::{MessageRate, Outbound};
use crate::auth::Principal;
use crate::protocol::Handler;

/// Failed authentications allowed per connection before it is closed.
const MAX_AUTH_FAILURES: u32 = 3;

/// The credential a connection authenticated with, and the `id` of the
/// request that did it.
pub(super) struct Authenticated {
    pub principal: Principal,
    pub request: Option<RequestId>,
}

/// Wait for `login` or `authenticate`. `None` once the connection must close:
/// the client left, timed out, exceeded the rate or failed too often. Every
/// request is answered with an `auth_error` carrying its `id`, except the
/// successful one, which the caller answers with `authenticated`.
pub(super) async fn authenticate(
    handler: &Arc<Handler>,
    receiver: &mut SplitStream<WebSocket>,
    sender: &mut Outbound,
    rate: &mut MessageRate,
    peer: IpAddr,
) -> Option<Authenticated> {
    let auth_timeout = std::time::Duration::from_millis(handler.config().http.ws_auth_timeout_ms);
    let deadline = tokio::time::Instant::now() + auth_timeout;
    let timeout_message = format!("Authentication timeout ({}s)", auth_timeout.as_secs_f32());
    let mut failures = 0;

    loop {
        let msg = tokio::select! {
            () = tokio::time::sleep_until(deadline) => {
                sender.send_notice(NoticeCode::AuthTimeout, timeout_message).await;
                return None;
            }
            msg = receiver.next() => msg,
        };
        let text = match msg {
            Some(Ok(Message::Text(text))) => text,
            Some(Ok(Message::Close(_))) | None => return None,
            Some(Err(e)) => {
                warn!(error = %e, "ws_auth_protocol_error");
                return None;
            }
            _ => continue,
        };

        if !rate.allow() {
            warn!(%peer, "ws_auth_rate_limited");
            let message = format!("Rate limit exceeded ({} msgs/sec)", rate.max_per_sec());
            auth_error(sender, probe_request_id(&text), message).await;
            return None;
        }

        let (id, outcome) = match serde_json::from_str::<ClientFrame>(&text) {
            Ok(ClientFrame::Login {
                id,
                username,
                password,
            }) => {
                let login = handler.login(&username, &password, peer);
                match tokio::time::timeout_at(deadline, login).await {
                    Ok(outcome) => (id, outcome),
                    Err(_) => {
                        auth_error(sender, id, timeout_message).await;
                        return None;
                    }
                }
            }
            Ok(ClientFrame::Authenticate { id, api_key }) => (
                id,
                handler
                    .authenticate_api_key(&api_key)
                    .inspect_err(|_| warn!(%peer, "ws_apikey_auth_failed")),
            ),
            Ok(
                frame @ (ClientFrame::Execute { .. }
                | ClientFrame::Cancel { .. }
                | ClientFrame::Ping { .. }),
            ) => {
                let message = "Authentication required. Send login or authenticate first.";
                auth_error(sender, frame.id().cloned(), message.to_string()).await;
                continue;
            }
            Err(e) => {
                let message = format!("Invalid message format ({e}). Send login or authenticate.");
                auth_error(sender, probe_request_id(&text), message).await;
                continue;
            }
        };

        match outcome {
            Ok(principal) => {
                return Some(Authenticated {
                    principal,
                    request: id,
                })
            }
            Err(message) => {
                failures += 1;
                auth_error(sender, id, message).await;
                if failures >= MAX_AUTH_FAILURES {
                    warn!(%peer, failures, "ws_auth_failures_exceeded");
                    return None;
                }
            }
        }
    }
}

/// Send `auth_error`; `false` if the connection is dead.
pub(super) async fn auth_error(
    sender: &mut Outbound,
    id: Option<RequestId>,
    message: String,
) -> bool {
    sender
        .send_frame(&ServerFrame::AuthError { id, message })
        .await
}
