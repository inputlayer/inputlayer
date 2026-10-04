//! One subscription as its connection sees it: name, generation, numbering,
//! and the last results it delivered.
//!
//! A shared view's publications are the same for every subscriber; what each
//! subscriber sends is its own. A plain subscription sends one query's
//! changes as `subscription_delta`s; a subscription group sends every
//! member's in one `subscription_group_delta`, marking the members whose
//! result did not change, so each push leaves all members exact at its
//! revision. Before anything leaves, the subscriber's read access to the
//! knowledge graph is checked; a denied publication becomes a
//! `subscription_error` and the subscriber keeps its last delivered results,
//! so once access is restored its next delta is relative to them and its
//! `seq` has no gap. A failed refresh's error follows the view's last
//! complete results: a subscriber that has not delivered them yet gets them
//! as a delta first, and the error on its next wake-up.

use std::sync::Arc;

use inputlayer_ws_protocol::{GroupMemberDelta, SubscriptionPush};

use super::publication::{
    Doorbell, Outcome, Publication, RowChange, SubscriberId, ViewCell, ViewResult,
};
use super::views::Attachment;
use super::Row;

/// The initial results a subscription starts from.
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// One per query of the subscription, in order.
    pub results: Vec<SnapshotResult>,
    /// The knowledge graph revision every result is the exact answer at.
    pub revision: u64,
}

/// One query's initial result.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SnapshotResult {
    pub columns: Vec<String>,
    /// Every row, sorted.
    pub rows: Vec<Row>,
}

impl Snapshot {
    pub fn of(attachment: &Attachment) -> Self {
        let publication = &attachment.publication;
        let initial = attachment.initial_rows.as_deref();
        let results = publication
            .results
            .iter()
            .enumerate()
            .map(|(i, result)| SnapshotResult {
                columns: result.columns.clone(),
                rows: initial
                    .and_then(|rows| rows.get(i))
                    .map_or_else(|| result.rows.sorted_rows(), Clone::clone),
            })
            .collect();
        Self {
            results,
            revision: publication.revision,
        }
    }
}

/// One subscription of one connection.
pub struct Subscriber {
    name: String,
    generation: u64,
    knowledge_graph: String,
    /// Member names of a subscription group, in order; `None` for a plain
    /// subscription.
    members: Option<Arc<[String]>>,
    doorbell: Arc<Doorbell>,
    cell: Arc<ViewCell>,
    /// Number of the last delta sent.
    seq: u64,
    /// Number of the last publication handled.
    seen: u64,
    /// The last results delivered, and the publication that produced them.
    base: Arc<[ViewResult]>,
    base_number: u64,
}

impl Subscriber {
    /// Plain subscriber `name` (generation `generation`) starting from `attachment`.
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
            members: None,
            doorbell,
            cell: Arc::clone(&attachment.cell),
            seq: 0,
            seen: publication.number,
            base: Arc::clone(&publication.results),
            base_number: publication.result_number,
        }
    }

    /// This subscriber as a subscription group whose members are named
    /// `members`, one per query of its view, in order.
    pub fn grouped(mut self, members: Arc<[String]>) -> Self {
        self.members = Some(members);
        self
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

    /// Queries this subscription evaluates: what it counts against the
    /// connection's subscription limit.
    pub fn weight(&self) -> usize {
        self.members.as_ref().map_or(1, |members| members.len())
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
        let changes = match &publication.outcome {
            Outcome::Failed(message) if publication.result_number == self.base_number => {
                self.seen = publication.number;
                return Some(self.error(message.clone()));
            }
            Outcome::Delta { base, changes } if *base == self.base_number => changes.clone(),
            Outcome::Delta { .. } | Outcome::Snapshot | Outcome::Failed(_) => {
                changes_between(&self.base, &publication.results)
            }
        };
        if matches!(publication.outcome, Outcome::Failed(_)) {
            self.doorbell.ring();
        } else {
            self.seen = publication.number;
        }
        self.adopt(&publication);
        if changes.iter().all(RowChange::is_empty) {
            return None;
        }
        self.seq += 1;
        Some(self.delta(&publication, changes))
    }

    /// The push carrying `changes`, the rows `publication` changed.
    fn delta(&self, publication: &Publication, changes: Vec<RowChange>) -> SubscriptionPush {
        let Some(members) = &self.members else {
            let change = changes.into_iter().next().unwrap_or_default();
            return SubscriptionPush::SubscriptionDelta {
                subscription: self.name.clone(),
                generation: self.generation,
                knowledge_graph: self.knowledge_graph.clone(),
                seq: self.seq,
                revision: publication.revision,
                columns: publication
                    .results
                    .first()
                    .map(|result| result.columns.clone())
                    .unwrap_or_default(),
                inserted: change.inserted,
                retracted: change.retracted,
            };
        };
        let members = members
            .iter()
            .zip(publication.results.iter())
            .zip(changes)
            .map(|((name, result), change)| GroupMemberDelta {
                name: name.clone(),
                unchanged: change.is_empty(),
                columns: result.columns.clone(),
                inserted: change.inserted,
                retracted: change.retracted,
            })
            .collect();
        SubscriptionPush::SubscriptionGroupDelta {
            subscription: self.name.clone(),
            generation: self.generation,
            knowledge_graph: self.knowledge_graph.clone(),
            seq: self.seq,
            revision: publication.revision,
            members,
        }
    }

    fn adopt(&mut self, publication: &Publication) {
        self.base = Arc::clone(&publication.results);
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

/// The change from each result of `base` to the same query's in `next`; a
/// result both share did not change.
fn changes_between(base: &[ViewResult], next: &[ViewResult]) -> Vec<RowChange> {
    base.iter()
        .zip(next)
        .map(|(base, next)| {
            if Arc::ptr_eq(&base.rows, &next.rows) {
                RowChange::default()
            } else {
                RowChange::between(&base.rows, &next.rows)
            }
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
