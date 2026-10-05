//! One subscription as its connection sees it: name, generation, numbering,
//! and the last result it delivered.
//!
//! A shared view's publications are the same for every subscriber; what each
//! subscriber sends is its own. Before anything leaves, the subscriber's read
//! access to the knowledge graph is checked; a denied publication becomes a
//! `subscription_error` and the subscriber keeps its last delivered result,
//! so once access is restored its next delta is relative to that result and
//! its `seq` has no gap. A failed refresh's error follows the view's last
//! complete result: a subscriber that has not delivered that result yet gets
//! it as a delta first, and the error on its next wake-up.

use std::sync::Arc;

use inputlayer_ws_protocol::SubscriptionPush;

use super::publication::{Doorbell, Outcome, Publication, SubscriberId, ViewCell};
use super::views::Attachment;
use super::{ResultSet, Row};

/// The initial result a subscription starts from.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub columns: Vec<String>,
    /// Every row, sorted.
    pub rows: Vec<Row>,
    /// The knowledge graph revision `rows` is the exact answer at.
    pub revision: u64,
}

impl Snapshot {
    pub fn of(attachment: &Attachment) -> Self {
        let publication = &attachment.publication;
        Self {
            columns: publication.columns.clone(),
            rows: attachment.initial_rows.as_ref().map_or_else(
                || publication.result.sorted_rows(),
                |rows| rows.as_ref().clone(),
            ),
            revision: publication.revision,
        }
    }
}

/// One subscription of one connection.
pub struct Subscriber {
    name: String,
    generation: u64,
    knowledge_graph: String,
    doorbell: Arc<Doorbell>,
    cell: Arc<ViewCell>,
    /// Number of the last delta sent.
    seq: u64,
    /// Number of the last publication handled.
    seen: u64,
    /// The last result delivered, and the publication that produced it.
    base: Arc<ResultSet>,
    base_number: u64,
}

impl Subscriber {
    /// Subscriber `name` (generation `generation`) starting from `attachment`.
    pub fn new(
        name: &str,
        generation: u64,
        knowledge_graph: &str,
        doorbell: Arc<Doorbell>,
        attachment: &Attachment,
    ) -> Self {
        let publication = &attachment.publication;
        Self {
            name: name.to_string(),
            generation,
            knowledge_graph: knowledge_graph.to_string(),
            doorbell,
            cell: Arc::clone(&attachment.cell),
            seq: 0,
            seen: publication.number,
            base: Arc::clone(&publication.result),
            base_number: publication.result_number,
        }
    }

    pub fn id(&self) -> SubscriberId {
        self.doorbell.id()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn knowledge_graph(&self) -> &str {
        &self.knowledge_graph
    }

    /// Answer the doorbell: the push for the view's latest publication, if
    /// it is news to this subscriber. `readable` tells whether the subscriber
    /// may currently read the knowledge graph.
    pub fn deliver(&mut self, readable: impl FnOnce(&str) -> bool) -> Option<SubscriptionPush> {
        self.doorbell.answer();
        let publication = self.cell.latest();
        if publication.number == self.seen {
            return None;
        }
        if !readable(&self.knowledge_graph) {
            self.seen = publication.number;
            return Some(self.error(format!(
                "Access denied to knowledge graph '{}'. The subscription keeps its last \
                 delivered result; once access is restored, its next delta is relative to it.",
                self.knowledge_graph
            )));
        }
        let (inserted, retracted) = match &publication.outcome {
            Outcome::Failed(message) if publication.result_number == self.base_number => {
                self.seen = publication.number;
                return Some(self.error(message.clone()));
            }
            Outcome::Delta {
                base,
                inserted,
                retracted,
            } if *base == self.base_number => (inserted.clone(), retracted.clone()),
            Outcome::Delta { .. } | Outcome::Snapshot | Outcome::Failed(_) => (
                publication.result.difference(&self.base),
                self.base.difference(&publication.result),
            ),
        };
        if matches!(publication.outcome, Outcome::Failed(_)) {
            self.doorbell.ring();
        } else {
            self.seen = publication.number;
        }
        self.adopt(&publication);
        if inserted.is_empty() && retracted.is_empty() {
            return None;
        }
        self.seq += 1;
        Some(SubscriptionPush::SubscriptionDelta {
            subscription: self.name.clone(),
            generation: self.generation,
            knowledge_graph: self.knowledge_graph.clone(),
            seq: self.seq,
            revision: publication.revision,
            columns: publication.columns.clone(),
            inserted,
            retracted,
        })
    }

    /// Take up wake-ups once registered. The hub may ring the doorbell
    /// before the connection registers this subscriber; the connection drops
    /// that wake-up as stale without answering it, so the doorbell stays
    /// marked queued and would swallow every later ring. Answer it, then ring
    /// again if the view published since this subscriber's starting point.
    /// Answering before loading the cell means a publication either is seen
    /// here or rings an answered doorbell.
    pub fn resume(&self) {
        self.doorbell.answer();
        if self.cell.latest().number != self.seen {
            self.doorbell.ring();
        }
    }

    fn adopt(&mut self, publication: &Publication) {
        self.base = Arc::clone(&publication.result);
        self.base_number = publication.result_number;
    }

    fn error(&self, message: String) -> SubscriptionPush {
        SubscriptionPush::SubscriptionError {
            subscription: self.name.clone(),
            generation: self.generation,
            message,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
