//! Delivering standing-query pushes whole, or ending the subscription.
//!
//! When a delta is sent, the server's view of its subscription has already
//! moved to the delta's revision. A delta that cannot reach the client
//! completely (one of its rows fits no frame, or the client may no longer
//! read its knowledge graph) would therefore leave the client silently
//! behind. Instead the subscription is removed and the client gets a
//! `subscription_reset`: the rows it holds for the subscription are no longer
//! maintained, and it must subscribe again for a fresh snapshot. Nothing more
//! is pushed for that registration.

use axum::extract::ws::Message;
use futures_util::Sink;
use inputlayer_ws_protocol::{ServerFrame, SubscriptionPush};
use tracing::warn;

use super::outbound::Outbound;
use super::stream::{self, Undeliverable};
use crate::protocol::subscription::ConnectionSubscriptions;

/// Deltas with more rows than this are encoded on the blocking pool, so a
/// large delta never stalls a runtime worker.
const INLINE_PUSH_ROWS: usize = 256;

/// Deliver `push`, which `access` allows or withholds for this reason.
/// Returns `false` if the connection is dead.
pub(super) async fn deliver<S: Sink<Message> + Unpin>(
    sender: &mut Outbound<S>,
    subscriptions: &mut ConnectionSubscriptions,
    push: SubscriptionPush,
    access: Result<(), Undeliverable>,
) -> bool {
    let (subscription, generation) = {
        let (subscription, generation) = push.subscription();
        (subscription.to_string(), generation)
    };
    let frames = match access {
        Ok(()) => encode(push).await,
        Err(withheld) => Err(withheld),
    };
    let reason = match frames {
        Ok(frames) => {
            for frame in frames {
                if sender.send(Message::Text(frame)).await.is_err() {
                    return false;
                }
            }
            return true;
        }
        Err(reason) => reason,
    };
    warn!(%subscription, generation, %reason, "ws_subscription_reset");
    subscriptions.reset(&subscription, generation);
    let reset = SubscriptionPush::SubscriptionReset {
        subscription,
        generation,
        message: format!("{reason}. The subscription was removed; subscribe again."),
    };
    sender.send_frame(&ServerFrame::Subscription(reset)).await
}

/// The frames of `push`, encoded off the runtime workers when large.
async fn encode(push: SubscriptionPush) -> Result<Vec<String>, Undeliverable> {
    let rows = match &push {
        SubscriptionPush::SubscriptionDelta {
            inserted,
            retracted,
            ..
        } => inserted.len() + retracted.len(),
        _ => 0,
    };
    if rows <= INLINE_PUSH_ROWS {
        return stream::push_frames(push);
    }
    tokio::task::spawn_blocking(move || stream::push_frames(push))
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "ws_push_serialization_failed");
            Err("Internal server error".to_string())
        })
}
