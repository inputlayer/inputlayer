//! Delivering standing-query pushes whole, or ending the subscription.
//!
//! When a delta is sent, the server's view of its subscription has already
//! moved to the delta's revision. A delta that cannot reach the client
//! completely (one of its rows fits no frame) would therefore leave the
//! client silently behind. Instead the subscription is removed and the client gets a
//! `subscription_reset`: the rows it holds for the subscription are no longer
//! maintained, and it must subscribe again for a fresh snapshot. Nothing more
//! is pushed for that registration.

use axum::extract::ws::Message;
use futures_util::Sink;
use inputlayer_ws_protocol::{Row, ServerFrame, SubscriptionPush};
use serde_json::Value;
use tracing::warn;

use super::framing::FRAME_BUDGET;
use super::outbound::Outbound;
use super::stream::{self, Undeliverable};
use crate::protocol::subscription::ConnectionSubscriptions;

/// Bytes estimated for a JSON float: the longest an `f64` serializes to.
const FLOAT_BYTES: usize = 24;

/// Deliver `push`. Returns `false` if the connection is dead.
pub(super) async fn deliver<S: Sink<Message> + Unpin>(
    sender: &mut Outbound<S>,
    subscriptions: &mut ConnectionSubscriptions,
    push: SubscriptionPush,
) -> bool {
    let (subscription, generation) = {
        let (subscription, generation) = push.subscription();
        (subscription.to_string(), generation)
    };
    let reason = match encode(push).await {
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

/// The frames of `push`. A push estimated to fit one frame is encoded
/// right here, so small deltas keep their latency; a larger one is streamed
/// from the blocking pool, so it never stalls a runtime worker.
async fn encode(push: SubscriptionPush) -> Result<Vec<String>, Undeliverable> {
    if estimated_bytes(&push) <= FRAME_BUDGET {
        return stream::push_frames(push);
    }
    tokio::task::spawn_blocking(move || stream::push_frames(push))
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "ws_push_serialization_failed");
            Err("Internal server error".to_string())
        })
}

/// About how many bytes the rows of `push` serialize to, judged without
/// serializing anything. Counting stops once past [`FRAME_BUDGET`], so the
/// walk covers at most about one frame's worth of rows.
fn estimated_bytes(push: &SubscriptionPush) -> usize {
    let SubscriptionPush::SubscriptionDelta {
        inserted,
        retracted,
        ..
    } = push
    else {
        return 0;
    };
    let mut total = 0;
    for row in inserted.iter().chain(retracted) {
        total += row_width(row);
        if total > FRAME_BUDGET {
            break;
        }
    }
    total
}

fn row_width(row: &Row) -> usize {
    row.iter()
        .map(|value| value_width(value) + 1)
        .sum::<usize>()
        + 2
}

fn value_width(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) => 5,
        Value::Number(n) => number_width(n),
        Value::String(s) => s.len() + 2,
        Value::Array(items) => items.iter().map(|v| value_width(v) + 1).sum::<usize>() + 2,
        Value::Object(fields) => {
            fields
                .iter()
                .map(|(k, v)| k.len() + 4 + value_width(v))
                .sum::<usize>()
                + 2
        }
    }
}

/// Exact for integers, counted digit by digit; [`FLOAT_BYTES`] for floats.
fn number_width(n: &serde_json::Number) -> usize {
    let digits = |u: u64| u.checked_ilog10().map_or(1, |d| d as usize + 1);
    match (n.as_u64(), n.as_i64()) {
        (Some(u), _) => digits(u),
        (None, Some(i)) => 1 + digits(i.unsigned_abs()),
        (None, None) => FLOAT_BYTES,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
