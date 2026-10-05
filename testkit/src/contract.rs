//! The reactive delivery contract an agent relies on, as typed violations.
//!
//! Every check in the harness reports a [`Violation`] instead of panicking,
//! so a known defect can assert that *exactly its* violation is observed
//! (see [`crate::KnownDefect`]) while any other failure stays a real failure.

use std::fmt;

/// One way the engine broke the agent-facing contract.
#[derive(Debug, Clone, PartialEq)]
pub enum Violation {
    /// The engine answered a request with an error.
    Rejected(String),
    /// A snapshot or result was capped or partial.
    IncompleteSnapshot { rows: usize, detail: String },
    /// A delta skipped or repeated a sequence number.
    SeqGap {
        subscription: String,
        expected: u64,
        got: u64,
    },
    /// A delta did not name a higher revision than the state it applies to.
    StaleRevision {
        subscription: String,
        previous: u64,
        got: u64,
    },
    /// A delta retracted an absent row or inserted a present one.
    Inconsistent {
        subscription: String,
        detail: String,
    },
    /// The engine pushed `subscription_error`.
    SubscriptionError {
        subscription: String,
        message: String,
    },
    /// The engine ended the subscription with `subscription_reset`.
    SubscriptionReset {
        subscription: String,
        message: String,
    },
    /// A streamed delta's frames were out of order, missing, duplicated or
    /// did not add up to its end frame.
    BrokenStream {
        subscription: String,
        detail: String,
    },
    /// The agent's maintained result differs from a fresh full query.
    Diverged {
        subscription: String,
        missing: Vec<String>,
        unexpected: Vec<String>,
    },
    /// A delta's rows differ from the rows the change must produce.
    WrongDelta {
        subscription: String,
        detail: String,
    },
    /// A push arrived that the change must not produce.
    UnexpectedPush(String),
    /// Change notifications arrived out of sequence order.
    NotificationOrder { previous: u64, got: u64 },
    /// A reconnect cursor silently missed committed changes.
    MissedChanges(String),
    /// Nothing arrived in time.
    Timeout(String),
    /// A reply did not echo the id of the request it answered.
    Uncorrelated { expected: String, frame: String },
    /// Connection closed, malformed frame or an unknown protocol message.
    Transport(String),
    /// The engine does not export a counter the assertion needs.
    NotMeasurable(String),
    /// The engine's counters show work the step must not cause, such as a
    /// read of a deployed rule evaluating the rule instead of reading its view.
    UnexpectedWork(String),
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(message) => write!(f, "request rejected: {message}"),
            Self::IncompleteSnapshot { rows, detail } => {
                write!(f, "incomplete result with {rows} row(s): {detail}")
            }
            Self::SeqGap {
                subscription,
                expected,
                got,
            } => write!(
                f,
                "subscription '{subscription}': expected delta seq {expected}, got {got}"
            ),
            Self::StaleRevision {
                subscription,
                previous,
                got,
            } => write!(
                f,
                "subscription '{subscription}': delta at revision {got} applied to revision \
                 {previous}"
            ),
            Self::Inconsistent {
                subscription,
                detail,
            } => write!(
                f,
                "subscription '{subscription}': inconsistent delta: {detail}"
            ),
            Self::SubscriptionError {
                subscription,
                message,
            } => write!(f, "subscription '{subscription}' error: {message}"),
            Self::SubscriptionReset {
                subscription,
                message,
            } => write!(f, "subscription '{subscription}' reset: {message}"),
            Self::BrokenStream {
                subscription,
                detail,
            } => write!(
                f,
                "subscription '{subscription}': broken streamed delta: {detail}"
            ),
            Self::Diverged {
                subscription,
                missing,
                unexpected,
            } => write!(
                f,
                "subscription '{subscription}' diverged from a fresh query: \
                 missing {missing:?}, unexpected {unexpected:?}"
            ),
            Self::WrongDelta {
                subscription,
                detail,
            } => write!(f, "subscription '{subscription}': wrong delta: {detail}"),
            Self::UnexpectedPush(push) => write!(f, "unexpected push: {push}"),
            Self::NotificationOrder { previous, got } => {
                write!(f, "notification seq {got} delivered after {previous}")
            }
            Self::MissedChanges(detail) => write!(f, "missed committed changes: {detail}"),
            Self::Timeout(what) => write!(f, "timed out waiting for {what}"),
            Self::Uncorrelated { expected, frame } => {
                write!(f, "reply does not answer request {expected}: {frame}")
            }
            Self::Transport(detail) => write!(f, "transport: {detail}"),
            Self::NotMeasurable(counter) => write!(f, "counter {counter} is not exported"),
            Self::UnexpectedWork(detail) => write!(f, "unexpected work: {detail}"),
        }
    }
}

impl std::error::Error for Violation {}

/// Result of a contract check.
pub type Checked<T> = Result<T, Violation>;
