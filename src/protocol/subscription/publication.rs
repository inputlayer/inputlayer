//! How a shared view hands its results to its subscribers without a lock.
//!
//! The view's worker stores each [`Publication`] in the view's [`ViewCell`],
//! replacing the previous one, then rings every subscriber's [`Doorbell`]. A
//! doorbell queues at most one wake-up in its connection's mailbox, however
//! many publications arrive before the connection answers it, and answering
//! reads the cell's *latest* publication. So a slow connection coalesces
//! publications instead of queueing them, its mailbox never holds more than
//! one entry per subscription, and the worker never waits for a connection.
//!
//! Each subscriber keeps the last result it delivered. When a publication's
//! delta starts from that result, the subscriber forwards the shared delta;
//! otherwise (it skipped publications, or was denied one) it diffs its own
//! result against the publication's complete result.
//!
//! The store, the ring, the answer and the load are sequentially consistent:
//! when a ring finds a wake-up still queued, the answer that dequeues it comes
//! later and so loads the newer publication.

use std::sync::atomic::{fence, AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::mpsc::UnboundedSender;

use super::{ResultSet, Row};

/// Identifies one subscriber across the whole server; never reused.
pub type SubscriberId = u64;

/// One state of a shared view.
#[derive(Debug)]
pub struct Publication {
    /// Position in the view's publications, from 1.
    pub number: u64,
    /// The knowledge graph revision `result` is the exact answer at.
    pub revision: u64,
    /// Number of the publication that produced `result`.
    pub result_number: u64,
    pub columns: Vec<String>,
    /// The view's last complete result.
    pub result: Arc<ResultSet>,
    pub outcome: Outcome,
}

/// What a publication reports.
#[derive(Debug)]
pub enum Outcome {
    /// The view's first result.
    Snapshot,
    /// `result` is the result of publication `base` with these rows changed.
    Delta {
        base: u64,
        inserted: Vec<Row>,
        retracted: Vec<Row>,
    },
    /// A refresh failed; `result` is still the last complete result.
    Failed(String),
}

/// A view's latest publication, read without a lock.
#[derive(Debug)]
pub struct ViewCell(ArcSwap<Publication>);

impl ViewCell {
    pub fn new(publication: Arc<Publication>) -> Self {
        Self(ArcSwap::new(publication))
    }

    /// The latest publication.
    pub fn latest(&self) -> Arc<Publication> {
        self.0.load_full()
    }

    pub(super) fn publish(&self, publication: Arc<Publication>) {
        self.0.store(publication);
        fence(Ordering::SeqCst);
    }
}

/// Wakes one subscriber's connection, at most one queued wake-up at a time.
#[derive(Debug)]
pub struct Doorbell {
    id: SubscriberId,
    queued: AtomicBool,
    mailbox: UnboundedSender<SubscriberId>,
}

impl Doorbell {
    pub fn new(id: SubscriberId, mailbox: UnboundedSender<SubscriberId>) -> Arc<Self> {
        Arc::new(Self {
            id,
            queued: AtomicBool::new(false),
            mailbox,
        })
    }

    pub fn id(&self) -> SubscriberId {
        self.id
    }

    /// Queue a wake-up unless one is pending. `false` once the connection is
    /// gone.
    pub(super) fn ring(&self) -> bool {
        if self.queued.swap(true, Ordering::SeqCst) {
            return !self.mailbox.is_closed();
        }
        self.mailbox.send(self.id).is_ok()
    }

    /// Take the queued wake-up; ring again for anything published after.
    /// Call before reading the cell.
    pub fn answer(&self) {
        self.queued.store(false, Ordering::SeqCst);
        fence(Ordering::SeqCst);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;

    fn publication(number: u64) -> Arc<Publication> {
        Arc::new(Publication {
            number,
            revision: number,
            result_number: number,
            columns: Vec::new(),
            result: Arc::default(),
            outcome: Outcome::Snapshot,
        })
    }

    #[test]
    fn rings_queue_one_wake_up_until_answered() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let doorbell = Doorbell::new(7, tx);
        let cell = ViewCell::new(publication(1));
        for number in 2..=5 {
            cell.publish(publication(number));
            assert!(doorbell.ring());
        }
        assert_eq!(rx.try_recv().unwrap(), 7);
        assert!(rx.try_recv().is_err(), "one wake-up for four publications");
        doorbell.answer();
        assert_eq!(cell.latest().number, 5, "the answer reads the latest");

        cell.publish(publication(6));
        assert!(doorbell.ring());
        assert_eq!(rx.try_recv().unwrap(), 7, "answered: rings again");
    }

    #[test]
    fn ring_reports_a_closed_mailbox() {
        let (tx, rx) = mpsc::unbounded_channel();
        let doorbell = Doorbell::new(1, tx);
        drop(rx);
        assert!(!doorbell.ring());
        assert!(!doorbell.ring(), "also while a wake-up is marked queued");
    }
}
