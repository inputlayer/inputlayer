//! Shared views: one evaluation per distinct standing query, however many subscribers it has.
//!
//! A view is keyed by its knowledge graph and query text ([`ViewKey`]). Keys are compared in
//! full: a hash collision never hands one query's rows to another.
//!
//! Each view is either *idle* (its [`StandingQuery`] is parked here) or *in flight* (handed
//! out in a [`Dispatch`], back in a [`Completion`]): at most one evaluation per view. Changes
//! that arrive while in flight are kept and checked against the *new* dependency set on
//! completion, so a burst of commits costs one follow-up evaluation and the final state is
//! never lost. With a coalescing window, an idle view waits up to that window after the first
//! relevant change before evaluating; the window never restarts, so a change is evaluated at
//! most one window (plus an evaluation in flight) after it is seen.
//!
//! A completed refresh that changed, failed or recovered the result is the view's next
//! [`Publication`], and every subscriber's doorbell rings; one that left it unchanged only
//! advances the revision the result is exact at. A subscriber joins at once only a clean view:
//! one with a current result, no refresh in flight or due, and a revision no older than the
//! knowledge graph's when it subscribed. Otherwise it waits for an evaluation that has seen
//! every change seen before it joined, started at once if idle, so its snapshot includes every
//! write acknowledged before it subscribed and its revision is as recent as the state it read.
//! A retry of a failed view failing as before is no news to the others.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::publication::{Doorbell, Outcome, Publication, SubscriberId, ViewCell};
use super::{ChangeSet, Dependencies, Row, StandingQuery};

mod evaluation;
mod publish;

pub use evaluation::{Completion, Dispatch};
use publish::{publish, start};

/// Identity of a shared view: what its result is a function of.
///
/// Rules: a refresh reads the knowledge graph's current persistent rules; a rule change
/// that may affect a view retires its key ([`ViewRegistry::on_rule_change`]): later
/// subscribers start a new view.
///
/// Visibility: authorization is per knowledge graph and checked for every subscriber when it
/// subscribes and before every push, so all readers of a view may see all its rows. Row- or
/// relation-level authorization would make rows depend on who asks: it must join this key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewKey {
    pub knowledge_graph: String,
    /// The trimmed query text; spellings differ in column names, so in views.
    pub query: String,
}

/// Unique per registry; never reused.
pub type ViewId = u64;

/// A subscriber's start: the view's latest publication is its snapshot.
#[derive(Clone)]
pub struct Attachment {
    pub cell: Arc<ViewCell>,
    pub publication: Arc<Publication>,
    /// The snapshot's rows, sorted, when the view was created for it.
    pub initial_rows: Option<Arc<Vec<Row>>>,
}

/// Result of [`ViewRegistry::attach`].
pub enum Attach {
    /// Attached to a live view.
    Attached(Attachment),
    /// Waiting for the view's next result; start the dispatch if one is given.
    Waiting(Option<Dispatch>),
}

/// Result of [`ViewRegistry::on_complete`].
#[derive(Default)]
pub struct Completed {
    /// Answers for subscribers that waited for the evaluation.
    pub replies: Vec<(SubscriberId, Result<Attachment, String>)>,
    /// Evaluation to start for changes seen meanwhile.
    pub follow_up: Option<Dispatch>,
}

struct Live {
    cell: Arc<ViewCell>,
    latest: Arc<Publication>,
}

impl Live {
    fn attachment(&self, initial_rows: Option<Arc<Vec<Row>>>) -> Attachment {
        Attachment {
            cell: Arc::clone(&self.cell),
            publication: Arc::clone(&self.latest),
            initial_rows,
        }
    }
}

struct Pending {
    change: ChangeSet,
    /// When the first of these changes was seen.
    since: Instant,
}

struct View {
    key: ViewKey,
    /// `None` while an evaluation is in flight.
    query: Option<Box<dyn StandingQuery>>,
    dependencies: Dependencies,
    /// Changes seen while in flight.
    pending: Option<Pending>,
    /// When an idle view's coalesced refresh starts.
    due: Option<Instant>,
    /// The evaluation in flight only retries a failed view for new subscribers.
    retry: bool,
    /// `None` until the first result.
    live: Option<Live>,
    /// Waiting for the result of the evaluation in flight, or of the next one if idle.
    waiting: Vec<Arc<Doorbell>>,
    /// Waiting for the evaluation after the one in flight, which predates changes they must see.
    waiting_next: Vec<Arc<Doorbell>>,
    subscribers: BTreeMap<SubscriberId, Arc<Doorbell>>,
}

impl View {
    fn is_unused(&self) -> bool {
        self.waiting.is_empty() && self.waiting_next.is_empty() && self.subscribers.is_empty()
    }

    /// Answer every waiting subscriber: the latest publication, or the evaluation's error.
    fn release_waiting(
        &mut self,
        failure: Option<String>,
        initial_rows: Option<Arc<Vec<Row>>>,
    ) -> Vec<(SubscriberId, Result<Attachment, String>)> {
        let answer = match (failure, &self.live) {
            (None, Some(live)) => Ok(live.attachment(initial_rows)),
            (failure, _) => Err(failure.unwrap_or_default()),
        };
        std::mem::take(&mut self.waiting)
            .into_iter()
            .map(|doorbell| {
                let subscriber = doorbell.id();
                if answer.is_ok() {
                    self.subscribers.insert(subscriber, doorbell);
                }
                (subscriber, answer.clone())
            })
            .collect()
    }
}

/// All shared views of one server.
pub struct ViewRegistry<S = RandomState> {
    window: Duration,
    next_view: ViewId,
    keys: HashMap<ViewKey, ViewId, S>,
    views: BTreeMap<ViewId, View>,
    /// Views of each knowledge graph, so a commit visits only its own.
    graphs: HashMap<String, BTreeSet<ViewId>>,
    subscribers: HashMap<SubscriberId, ViewId>,
    schedule: BTreeSet<(Instant, ViewId)>,
}

impl ViewRegistry {
    /// Empty registry coalescing changes over `window` (zero: evaluate at once).
    pub fn new(window: Duration) -> Self {
        Self::with_hasher(window)
    }
}

impl<S: BuildHasher + Default> ViewRegistry<S> {
    /// [`ViewRegistry::new`] hashing keys with `S`.
    pub fn with_hasher(window: Duration) -> Self {
        Self {
            window,
            next_view: 0,
            keys: HashMap::default(),
            views: BTreeMap::new(),
            graphs: HashMap::new(),
            subscribers: HashMap::new(),
            schedule: BTreeSet::new(),
        }
    }

    /// Number of views.
    pub fn len(&self) -> usize {
        self.views.len()
    }

    /// True when there are no views.
    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// Attach `doorbell`'s subscriber to the view of `key`, creating it (with the query
    /// `create` builds) if there is none. `revision` is the knowledge graph's when it
    /// subscribed: its snapshot is exact at that revision or a later one.
    pub fn attach(
        &mut self,
        key: ViewKey,
        doorbell: Arc<Doorbell>,
        revision: u64,
        create: impl FnOnce() -> Box<dyn StandingQuery>,
    ) -> Attach {
        let subscriber = doorbell.id();
        if let Some(id) = self.keys.get(&key).copied() {
            if let Some(view) = self.views.get_mut(&id) {
                self.subscribers.insert(subscriber, id);
                let clean = view.query.is_some() && view.due.is_none();
                return match &view.live {
                    Some(live)
                        if clean
                            && live.latest.revision >= revision
                            && !matches!(live.latest.outcome, Outcome::Failed(_)) =>
                    {
                        let attachment = live.attachment(None);
                        view.subscribers.insert(subscriber, doorbell);
                        Attach::Attached(attachment)
                    }
                    _ if view.query.is_none() && view.pending.is_some() => {
                        view.waiting_next.push(doorbell);
                        Attach::Waiting(None)
                    }
                    _ => {
                        view.waiting.push(doorbell);
                        if view.query.is_some() {
                            view.retry = view.due.is_none();
                        }
                        if let Some(due) = view.due.take() {
                            self.schedule.remove(&(due, id));
                        }
                        Attach::Waiting(take_dispatch(id, view))
                    }
                };
            }
        }
        self.next_view += 1;
        let id = self.next_view;
        self.keys.insert(key.clone(), id);
        self.graphs
            .entry(key.knowledge_graph.clone())
            .or_default()
            .insert(id);
        self.views.insert(
            id,
            View {
                key,
                query: None,
                dependencies: Dependencies::default(),
                pending: None,
                due: None,
                retry: false,
                live: None,
                waiting: vec![doorbell],
                waiting_next: Vec::new(),
                subscribers: BTreeMap::new(),
            },
        );
        self.subscribers.insert(subscriber, id);
        Attach::Waiting(Some(Dispatch {
            view: id,
            query: create(),
        }))
    }

    /// Detach a subscriber. Returns the view it left when that view is now gone: an
    /// evaluation still running for it is wasted work.
    pub fn detach(&mut self, subscriber: SubscriberId) -> Option<ViewId> {
        let id = self.subscribers.remove(&subscriber)?;
        let view = self.views.get_mut(&id)?;
        view.subscribers.remove(&subscriber);
        view.waiting.retain(|doorbell| doorbell.id() != subscriber);
        view.waiting_next
            .retain(|doorbell| doorbell.id() != subscriber);
        if !view.is_unused() {
            return None;
        }
        self.remove(id);
        Some(id)
    }

    fn remove(&mut self, id: ViewId) {
        if let Some(view) = self.views.remove(&id) {
            if self.keys.get(&view.key) == Some(&id) {
                self.keys.remove(&view.key);
            }
            if let Some(views) = self.graphs.get_mut(&view.key.knowledge_graph) {
                views.remove(&id);
                if views.is_empty() {
                    self.graphs.remove(&view.key.knowledge_graph);
                }
            }
            if let Some(due) = view.due {
                self.schedule.remove(&(due, id));
            }
            for subscriber in view.subscribers.keys() {
                self.subscribers.remove(subscriber);
            }
            for doorbell in view.waiting.iter().chain(&view.waiting_next) {
                self.subscribers.remove(&doorbell.id());
            }
        }
    }

    /// React to committed changes in `knowledge_graph`: returns evaluations to start.
    pub fn on_change(
        &mut self,
        knowledge_graph: &str,
        change: &ChangeSet,
        now: Instant,
    ) -> Vec<Dispatch> {
        let window = self.window;
        let mut dispatches = Vec::new();
        let Some(ids) = self.graphs.get(knowledge_graph) else {
            return dispatches;
        };
        for &id in ids {
            let Some(view) = self.views.get_mut(&id) else {
                continue;
            };
            if view.query.is_none() {
                match &mut view.pending {
                    Some(pending) => pending.change.merge(change),
                    None => {
                        view.pending = Some(Pending {
                            change: change.clone(),
                            since: now,
                        });
                    }
                }
            } else if view.due.is_none() && view.dependencies.is_affected_by(change) {
                if window.is_zero() {
                    dispatches.extend(take_dispatch(id, view));
                } else {
                    view.due = Some(now + window);
                    self.schedule.insert((now + window, id));
                }
            }
        }
        dispatches
    }

    /// React to committed changes in `knowledge_graph` that may include rule changes. A view
    /// they may affect keeps its subscribers, which get its refresh, but takes no new ones:
    /// they start a view of the new rules.
    pub fn on_rule_change(
        &mut self,
        knowledge_graph: &str,
        change: &ChangeSet,
        now: Instant,
    ) -> Vec<Dispatch> {
        for id in self.graphs.get(knowledge_graph).into_iter().flatten() {
            let Some(view) = self.views.get(id) else {
                continue;
            };
            let affected = view.query.is_none() || view.dependencies.is_affected_by(change);
            if affected && self.keys.get(&view.key) == Some(id) {
                self.keys.remove(&view.key);
            }
        }
        self.on_change(knowledge_graph, change, now)
    }

    /// React to changes of unknown scope in every knowledge graph.
    pub fn on_unknown_changes(&mut self, now: Instant) -> Vec<Dispatch> {
        let graphs: Vec<String> = self.graphs.keys().cloned().collect();
        graphs
            .iter()
            .flat_map(|kg| self.on_rule_change(kg, &ChangeSet::Everything, now))
            .collect()
    }

    /// When the earliest coalesced refresh is due.
    pub fn next_due(&self) -> Option<Instant> {
        self.schedule.first().map(|(due, _)| *due)
    }

    /// Start the coalesced refreshes due by `now`.
    pub fn take_due(&mut self, now: Instant) -> Vec<Dispatch> {
        let mut dispatches = Vec::new();
        while let Some(&(due, id)) = self.schedule.first() {
            if due > now {
                break;
            }
            self.schedule.pop_first();
            if let Some(view) = self.views.get_mut(&id) {
                view.due = None;
                dispatches.extend(take_dispatch(id, view));
            }
        }
        dispatches
    }

    /// Accept a finished evaluation: publish its outcome to the view's subscribers, answer
    /// those waiting for it, and return a follow-up evaluation for changes seen meanwhile.
    pub fn on_complete(&mut self, completion: Completion, now: Instant) -> Completed {
        let Completion {
            view: id,
            query,
            result,
        } = completion;
        let Some(view) = self.views.get_mut(&id) else {
            // Every subscriber left while it ran.
            return Completed::default();
        };
        view.query = Some(query);
        let retry = std::mem::take(&mut view.retry);
        let failure = result.as_ref().err().cloned();
        let mut initial_rows = None;
        let gone = match result {
            Ok(refresh) if view.live.is_none() => {
                initial_rows = Some(start(view, refresh));
                Vec::new()
            }
            result => publish(view, result, retry),
        };
        let pending = view.pending.as_ref();
        if !pending.is_some_and(|pending| view.dependencies.is_affected_by(&pending.change)) {
            view.waiting.append(&mut view.waiting_next);
        }
        let replies = view.release_waiting(failure, initial_rows);
        view.waiting = std::mem::take(&mut view.waiting_next);
        for (subscriber, reply) in &replies {
            if reply.is_err() {
                self.subscribers.remove(subscriber);
            }
        }
        for subscriber in gone {
            self.detach(subscriber);
        }
        if self.views.get(&id).is_some_and(View::is_unused) {
            self.remove(id);
        }
        Completed {
            replies,
            follow_up: self.follow_up(id, now),
        }
    }

    /// The evaluation for `id`'s pending changes, scheduled or started now.
    fn follow_up(&mut self, id: ViewId, now: Instant) -> Option<Dispatch> {
        let view = self.views.get_mut(&id)?;
        let pending = view.pending.take()?;
        if !view.dependencies.is_affected_by(&pending.change) {
            return None;
        }
        let due = pending.since + self.window;
        if due <= now {
            return take_dispatch(id, view);
        }
        view.due = Some(due);
        self.schedule.insert((due, id));
        None
    }
}

fn take_dispatch(id: ViewId, view: &mut View) -> Option<Dispatch> {
    view.query.take().map(|query| Dispatch { view: id, query })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
