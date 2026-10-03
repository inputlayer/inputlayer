//! Shared views: one evaluation per distinct standing query, however many
//! subscribers it has.
//!
//! A view is keyed by its knowledge graph and query text ([`ViewKey`]); a
//! result depends on nothing else (see [`StandingQuery`]). Keys are compared
//! in full, so a hash collision can never hand one query's rows to another.
//!
//! Each view is either *idle* (its [`StandingQuery`] is parked here) or *in
//! flight* (handed out in a [`Dispatch`], back in a [`Completion`]): at most
//! one evaluation per view. Changes that arrive while in flight are kept and
//! checked against the *new* dependency set on completion, so a burst of
//! commits costs one follow-up evaluation and the final state is never lost.
//! With a coalescing window, an idle view waits up to that window after the
//! first relevant change before evaluating; the window never restarts, so a
//! change is evaluated at most one window (plus an evaluation in flight)
//! after it is seen.
//!
//! A completed refresh becomes the view's next [`Publication`] when its result
//! changed or failed, and every subscriber's doorbell rings. Subscribers that
//! join while the first evaluation runs wait for its result; later ones attach
//! to the latest publication without any evaluation.

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::publication::{Doorbell, Outcome, Publication, SubscriberId, ViewCell};
use super::{ChangeSet, Dependencies, Refresh, Row, StandingQuery};

/// Identity of a shared view: what its result is a function of.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ViewKey {
    pub knowledge_graph: String,
    /// The trimmed query text. Spelling variants are distinct views: column
    /// names come from the text.
    pub query: String,
}

/// Unique per registry; never reused.
pub type ViewId = u64;

/// An evaluation to run: the caller drives it and hands back the [`Completion`].
pub struct Dispatch {
    pub view: ViewId,
    query: Box<dyn StandingQuery>,
}

/// Outcome of a [`Dispatch`].
pub struct Completion {
    pub view: ViewId,
    query: Box<dyn StandingQuery>,
    result: Result<Refresh, String>,
}

impl Dispatch {
    /// Run the evaluation, turning a panic into an error so the view survives.
    pub async fn run(self) -> Completion {
        use futures_util::FutureExt;
        let Dispatch { view, mut query } = self;
        let result = std::panic::AssertUnwindSafe(query.refresh())
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("Internal error while evaluating subscription".to_string()));
        Completion {
            view,
            query,
            result,
        }
    }
}

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
    /// Waiting for the view's first result; start the dispatch if one is given.
    Waiting(Option<Dispatch>),
    /// The view's latest refresh failed: no complete current result to give.
    Rejected(String),
}

/// Result of [`ViewRegistry::on_complete`].
#[derive(Default)]
pub struct Completed {
    /// Answers for subscribers that waited for the view's first result.
    pub replies: Vec<(SubscriberId, Result<Attachment, String>)>,
    /// Evaluation to start for changes seen meanwhile.
    pub follow_up: Option<Dispatch>,
}

struct Live {
    cell: Arc<ViewCell>,
    latest: Arc<Publication>,
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
    /// `None` until the first result.
    live: Option<Live>,
    /// Waiting for the first result.
    waiting: Vec<Arc<Doorbell>>,
    subscribers: BTreeMap<SubscriberId, Arc<Doorbell>>,
}

impl View {
    fn is_unused(&self) -> bool {
        self.waiting.is_empty() && self.subscribers.is_empty()
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

    /// Attach `doorbell`'s subscriber to the view of `key`, creating it (with
    /// the query `create` builds) if there is none.
    pub fn attach(
        &mut self,
        key: ViewKey,
        doorbell: Arc<Doorbell>,
        create: impl FnOnce() -> Box<dyn StandingQuery>,
    ) -> Attach {
        let subscriber = doorbell.id();
        if let Some(id) = self.keys.get(&key).copied() {
            if let Some(view) = self.views.get_mut(&id) {
                let attach = match &view.live {
                    None => {
                        view.waiting.push(doorbell);
                        Attach::Waiting(None)
                    }
                    Some(live) => {
                        if let Outcome::Failed(message) = &live.latest.outcome {
                            return Attach::Rejected(message.clone());
                        }
                        let attachment = Attachment {
                            cell: Arc::clone(&live.cell),
                            publication: Arc::clone(&live.latest),
                            initial_rows: None,
                        };
                        view.subscribers.insert(subscriber, doorbell);
                        Attach::Attached(attachment)
                    }
                };
                self.subscribers.insert(subscriber, id);
                return attach;
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
                live: None,
                waiting: vec![doorbell],
                subscribers: BTreeMap::new(),
            },
        );
        self.subscribers.insert(subscriber, id);
        Attach::Waiting(Some(Dispatch {
            view: id,
            query: create(),
        }))
    }

    /// Detach a subscriber. Returns the view it left when that view is now
    /// gone: an evaluation still running for it is wasted work.
    pub fn detach(&mut self, subscriber: SubscriberId) -> Option<ViewId> {
        let id = self.subscribers.remove(&subscriber)?;
        let view = self.views.get_mut(&id)?;
        view.subscribers.remove(&subscriber);
        view.waiting.retain(|doorbell| doorbell.id() != subscriber);
        if !view.is_unused() {
            return None;
        }
        self.remove(id);
        Some(id)
    }

    fn remove(&mut self, id: ViewId) {
        if let Some(view) = self.views.remove(&id) {
            self.keys.remove(&view.key);
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
            for doorbell in &view.waiting {
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

    /// React to changes of unknown scope in every knowledge graph.
    pub fn on_unknown_changes(&mut self, now: Instant) -> Vec<Dispatch> {
        let graphs: Vec<String> = self.graphs.keys().cloned().collect();
        graphs
            .iter()
            .flat_map(|kg| self.on_change(kg, &ChangeSet::Everything, now))
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

    /// Accept a finished evaluation: publish its outcome to the view's
    /// subscribers, answer those waiting for the first result, and return a
    /// follow-up evaluation for changes seen meanwhile.
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
        let mut completed = Completed::default();
        if view.live.is_none() {
            match result {
                Ok(refresh) => completed.replies = start(view, refresh),
                Err(message) => {
                    completed.replies = view
                        .waiting
                        .iter()
                        .map(|doorbell| (doorbell.id(), Err(message.clone())))
                        .collect();
                    self.remove(id);
                    return completed;
                }
            }
        } else {
            let gone = publish(view, result);
            for subscriber in gone {
                self.detach(subscriber);
            }
        }
        completed.follow_up = self.follow_up(id, now);
        completed
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

/// Make `refresh` the view's first publication; every waiting subscriber
/// attaches to it.
fn start(view: &mut View, refresh: Refresh) -> Vec<(SubscriberId, Result<Attachment, String>)> {
    view.dependencies = refresh.dependencies;
    let publication = Arc::new(Publication {
        number: 1,
        revision: refresh.revision,
        result_number: 1,
        columns: refresh.columns,
        result: refresh.result,
        outcome: Outcome::Snapshot,
    });
    let attachment = Attachment {
        cell: Arc::new(ViewCell::new(Arc::clone(&publication))),
        publication: Arc::clone(&publication),
        initial_rows: Some(Arc::new(refresh.inserted)),
    };
    view.live = Some(Live {
        cell: Arc::clone(&attachment.cell),
        latest: publication,
    });
    view.waiting
        .drain(..)
        .map(|doorbell| {
            let subscriber = doorbell.id();
            view.subscribers.insert(subscriber, doorbell);
            (subscriber, Ok(attachment.clone()))
        })
        .collect()
}

/// Publish a later refresh's outcome when there is news, ringing every
/// subscriber. Returns the subscribers whose connections are gone.
fn publish(view: &mut View, result: Result<Refresh, String>) -> Vec<SubscriberId> {
    let Some(live) = &mut view.live else {
        return Vec::new();
    };
    let latest = &live.latest;
    let number = latest.number + 1;
    let publication = match result {
        Ok(refresh) => {
            view.dependencies = refresh.dependencies;
            if refresh.inserted.is_empty() && refresh.retracted.is_empty() {
                return Vec::new();
            }
            Publication {
                number,
                revision: refresh.revision,
                result_number: number,
                columns: refresh.columns,
                result: refresh.result,
                outcome: Outcome::Delta {
                    base: latest.result_number,
                    inserted: refresh.inserted,
                    retracted: refresh.retracted,
                },
            }
        }
        Err(message) => Publication {
            number,
            revision: latest.revision,
            result_number: latest.result_number,
            columns: latest.columns.clone(),
            result: Arc::clone(&latest.result),
            outcome: Outcome::Failed(message),
        },
    };
    let publication = Arc::new(publication);
    live.cell.publish(Arc::clone(&publication));
    live.latest = publication;
    view.subscribers
        .values()
        .filter(|doorbell| !doorbell.ring())
        .map(|doorbell| doorbell.id())
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
