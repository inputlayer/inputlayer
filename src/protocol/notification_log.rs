//! The engine's change-notification stream.
//!
//! Every committed change is published here once: it gets the next sequence
//! number, joins a bounded ring of recent notifications and is broadcast to
//! every live connection. The three happen under one lock, so sequence order,
//! ring order and broadcast order are the same order: a connection never sees
//! a lower `seq` after a higher one.
//!
//! Sequence numbers are scoped by the stream epoch, a random identifier of
//! this engine run; a restarted engine numbers from 1 again under a new epoch.
//! A reconnecting client presents its cursor (epoch and last seen `seq`).
//! [`NotificationLog::resume`] replays what the ring still holds after it, or
//! reports a [`ReplayGap`] when that is not the complete history since the
//! cursor. The log never claims durable history: a gap means "re-read state".
//!
//! Notifications are hints that something committed. Standing-query results
//! are a separate domain with their own per-subscription sequence (see
//! [`crate::protocol::subscription`]).

use std::collections::VecDeque;

use parking_lot::Mutex;
use tokio::sync::broadcast;

use super::handler::Notification;

/// A position in the stream presented by a reconnecting client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// The epoch `last_seq` belongs to; `None` when the client did not say.
    pub epoch: Option<String>,
    /// The last `seq` the client saw.
    pub last_seq: u64,
}

/// Why a cursor cannot be resumed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayGap {
    /// The cursor names no epoch or another one (the engine restarted).
    OtherEpoch,
    /// Notifications after the cursor were dropped from the ring.
    Evicted { oldest_retained: u64 },
    /// The cursor is past the last published notification.
    Ahead { last_published: u64 },
}

impl ReplayGap {
    /// Explanation for the client.
    pub fn message(&self, last_seq: u64) -> String {
        let reason = match self {
            Self::OtherEpoch => {
                "it is from another engine run (stream epoch missing or different)".to_string()
            }
            Self::Evicted { oldest_retained } => format!(
                "notifications after it are no longer retained (oldest retained seq is \
                 {oldest_retained})"
            ),
            Self::Ahead { last_published } => {
                format!("it is past the last published seq {last_published}")
            }
        };
        format!(
            "Cannot replay notifications after seq {last_seq}: {reason}. Nothing was \
             replayed; re-read the state you track."
        )
    }
}

/// What a reconnecting client receives before live notifications.
#[derive(Debug)]
pub struct Resumed {
    /// Retained notifications after the cursor, in order, or why they are not
    /// the complete history since it.
    pub replay: Result<Vec<Notification>, ReplayGap>,
    /// Live notifications published after the replayed ones (or after the
    /// moment of resuming), with nothing skipped or repeated in between.
    pub live: broadcast::Receiver<Notification>,
}

struct Ring {
    /// `seq` of the last published notification (0 before the first).
    last_seq: u64,
    retained: VecDeque<Notification>,
}

/// The ordered notification stream of one engine run.
pub struct NotificationLog {
    epoch: String,
    capacity: usize,
    ring: Mutex<Ring>,
    sender: broadcast::Sender<Notification>,
}

impl NotificationLog {
    /// A new stream under a fresh random epoch, retaining and buffering up to
    /// `capacity` notifications (at least 1).
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (sender, _) = broadcast::channel(capacity);
        Self {
            epoch: format!("{:016x}", rand::random::<u64>()),
            capacity,
            ring: Mutex::new(Ring {
                last_seq: 0,
                retained: VecDeque::with_capacity(capacity),
            }),
            sender,
        }
    }

    /// This engine run's stream epoch.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// Number `notification`, retain it and broadcast it, in that order.
    pub fn publish(&self, mut notification: Notification) {
        let mut ring = self.ring.lock();
        ring.last_seq += 1;
        set_seq(&mut notification, ring.last_seq);
        if ring.retained.len() == self.capacity {
            ring.retained.pop_front();
        }
        ring.retained.push_back(notification.clone());
        // Sent under the lock: a later publisher cannot overtake this one.
        if self.sender.send(notification).is_err() {
            tracing::trace!("notification_published_without_receivers");
        }
    }

    /// Live notifications from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Notification> {
        self.sender.subscribe()
    }

    /// Resume from `cursor`: the retained notifications after it and a
    /// receiver for everything published later. With no cursor, nothing is
    /// replayed.
    pub fn resume(&self, cursor: Option<&Cursor>) -> Resumed {
        let ring = self.ring.lock();
        // Subscribed under the lock: exactly the notifications published
        // after the ring was read reach `live`.
        let live = self.sender.subscribe();
        let replay = match cursor {
            None => Ok(Vec::new()),
            Some(cursor) => self.replay_after(&ring, cursor),
        };
        Resumed { replay, live }
    }

    fn replay_after(&self, ring: &Ring, cursor: &Cursor) -> Result<Vec<Notification>, ReplayGap> {
        if cursor.epoch.as_deref() != Some(self.epoch.as_str()) {
            return Err(ReplayGap::OtherEpoch);
        }
        if cursor.last_seq > ring.last_seq {
            return Err(ReplayGap::Ahead {
                last_published: ring.last_seq,
            });
        }
        let oldest_retained = ring.last_seq + 1 - ring.retained.len() as u64;
        if cursor.last_seq + 1 < oldest_retained {
            return Err(ReplayGap::Evicted { oldest_retained });
        }
        let skip = (cursor.last_seq + 1 - oldest_retained) as usize;
        Ok(ring.retained.iter().skip(skip).cloned().collect())
    }
}

fn set_seq(notification: &mut Notification, value: u64) {
    match notification {
        Notification::PersistentUpdate { seq, .. }
        | Notification::RuleChange { seq, .. }
        | Notification::KgChange { seq, .. }
        | Notification::SchemaChange { seq, .. } => *seq = value,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
