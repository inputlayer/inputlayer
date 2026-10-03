//! Frame types of the engine's `/ws` protocol.
//!
//! The single definition of every message on the global WebSocket, shared by
//! the engine (which serializes [`ServerFrame`] and parses [`ClientFrame`])
//! and its Rust clients (which do the reverse). Python, JS and the AsyncAPI
//! spec (`docs/spec/asyncapi.yaml`) mirror these types.
//!
//! # Correlation
//!
//! A client may tag any request with an [`RequestId`]. Every frame that
//! answers it — `authenticated`, `auth_error`, `result`, `result_start`,
//! `result_chunk`, `result_end`, `error`, `pong` — echoes that `id`. A request
//! without an `id` gets replies without one. Frames the client did not ask
//! for never carry an `id` and have their own types, so they cannot be read as
//! a reply (see [`FrameClass`]):
//!
//! - **pushes**: data changes (`persistent_update`, `rule_change`,
//!   `kg_change`, `schema_change`) and standing-query results
//!   (`subscription_delta`, `subscription_error`), which name their
//!   subscription and its [generation](Subscribed::generation);
//! - **notices**: connection events such as an idle timeout ([`NoticeCode`]).
//!
//! A request whose frame or `id` is malformed is answered by one `error` with
//! code [`ErrorCode::InvalidRequest`], carrying the `id` when it could be read.
//!
//! # Streams
//!
//! Notifications and subscription results are separate streams. Notification
//! [`seq`](Notification::seq) numbers increase in delivery order within one
//! stream epoch (`authenticated.stream_epoch`); a reconnect cursor (`last_seq`
//! with `epoch`) is replayed or answered with a [`NoticeCode::ReplayGap`]. A
//! subscription's pushes have their own gapless `seq` and name the knowledge
//! graph revision they reach, above the snapshot's
//! [`Subscribed::revision`]; subscriptions never outlive their connection.
//!
//! # Deadlines and cancellation
//!
//! An `execute` runs under one deadline covering its queueing, admission and
//! computation (`timeout_ms`, capped by the engine's query timeout). A
//! [`ClientFrame::Cancel`] names an unanswered request by its `id` and is
//! answered by [`ServerFrame::CancelAck`]. A request stopped by either before
//! it began committing applied nothing and fails with
//! [`ErrorCode::DeadlineExceeded`] or [`ErrorCode::Cancelled`]; once it began
//! committing it is not interrupted, and its reply reports the committed
//! result, or [`ErrorCode::OutcomeUnknown`] if the commit itself failed in a
//! way that leaves the outcome open.
//!
//! # Large payloads
//!
//! No frame exceeds the engine's message size limit. A result or a
//! `.subscribe` snapshot too large for one frame is streamed as
//! `result_start`, `result_chunk`s and `result_end`; a subscription delta as
//! `subscription_delta_start`, `subscription_delta_chunk`s and
//! `subscription_delta_end` (see [`SubscriptionPush`]). Each stream is one
//! logical result or delta, complete only at its end frame, whose counts the
//! chunks must add up to. What the server cannot deliver whole it reports
//! instead: an `error` for a reply, a `subscription_reset` for a delta.

mod client;
mod error;
mod notice;
mod push;
mod request_id;
mod server;
mod timing;

pub use client::ClientFrame;
pub use error::{ErrorCode, StatementError, ValidationError};
pub use notice::NoticeCode;
pub use push::{Notification, Row, SubscriptionPush};
pub use request_id::{probe_request_id, InvalidRequestId, RequestId, MAX_REQUEST_ID_LEN};
pub use server::{
    CancelOutcome, FrameClass, ResultFrame, ResultStartFrame, ServerFrame, SessionMetadata,
    Subscribed,
};
pub use timing::{IrBuilderTiming, OptimizerTiming, RuleTiming, TimingBreakdown};

/// Version of this protocol, sent in `authenticated`. Bumped on any change a
/// client must know about.
pub const PROTOCOL_VERSION: u32 = 3;
