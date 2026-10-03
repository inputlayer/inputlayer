//! Incremental adapter: today's standing-query path.
//!
//! Every query is subscribed before the first step, and its observed result is
//! assembled purely from the pushed `inserted`/`retracted` deltas, exactly as a
//! subscribed agent would assemble it. Commits reach the subscriptions through
//! the real notification broadcast, notification-to-change mapping,
//! dependency filtering and coalescing registry; only the WebSocket framing is
//! left out. Evaluations run one at a time, so a run is deterministic.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use inputlayer::protocol::handler::PersistentNotification;
use inputlayer::protocol::subscription::{
    change_of, Dispatch, Push, ReevaluatingQuery, Row as WireRow, StandingQuery,
    SubscriptionRegistry,
};
use tokio::sync::broadcast::{self, error::TryRecvError};

use crate::adapter::Adapter;
use crate::engine::{EngineHost, KG};
use crate::model::{show_row, AdapterError, Cell, Observation, Outcome, Revision, Row};

/// A deliberate defect, used to prove the oracle catches broken maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    None,
    /// Ignore every retraction: consequences of deletes are never removed.
    DropRetractions,
}

/// Client-side state of one subscribed query.
struct View {
    query: String,
    /// Accumulated deltas, or why this query has no trustworthy result.
    result: Result<Observation, AdapterError>,
}

pub struct SubscriptionAdapter {
    host: EngineHost,
    notifications: broadcast::Receiver<PersistentNotification>,
    registry: SubscriptionRegistry,
    /// Keyed by subscription id.
    views: BTreeMap<String, View>,
    fault: Fault,
}

impl SubscriptionAdapter {
    pub fn open(queries: &[String], fault: Fault) -> Result<Self, AdapterError> {
        let host = EngineHost::open()?;
        let notifications = host.handler().subscribe_notifications();
        let mut adapter = Self {
            host,
            notifications,
            registry: SubscriptionRegistry::new(0),
            views: BTreeMap::new(),
            fault,
        };
        for (index, query) in queries.iter().enumerate() {
            let id = format!("q{index}");
            let result = adapter.subscribe(&id, query);
            adapter.views.insert(
                id,
                View {
                    query: query.clone(),
                    result,
                },
            );
        }
        Ok(adapter)
    }

    /// Register `query` and return its initial snapshot.
    fn subscribe(&mut self, id: &str, query: &str) -> Result<Observation, AdapterError> {
        let mut view = ReevaluatingQuery::new(Arc::clone(self.host.handler()), KG, query, None)
            .map_err(|e| AdapterError::Unsupported(format!("not subscribable: {e}")))?;
        let snapshot = self
            .host
            .block_on(view.refresh())
            .map_err(|e| AdapterError::Failed(format!("initial snapshot: {e}")))?;
        self.registry
            .add(id, KG, Box::new(view), snapshot.dependencies)
            .map_err(AdapterError::Failed)?;
        Ok(Observation::from_rows(snapshot.inserted.iter().map(cells)))
    }

    /// Feed every pending notification to the registry and run evaluations
    /// (including coalesced follow-ups) until none remain.
    fn settle(&mut self) {
        let mut queue: VecDeque<Dispatch> = VecDeque::new();
        loop {
            match self.notifications.try_recv() {
                Ok(notification) => {
                    let (knowledge_graph, change) = change_of(&notification);
                    queue.extend(self.registry.on_change(knowledge_graph, &change));
                }
                Err(TryRecvError::Lagged(_)) => queue.extend(self.registry.on_unknown_changes()),
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
            }
        }
        while let Some(dispatch) = queue.pop_front() {
            let completion = self.host.block_on(dispatch.run());
            let (push, follow_up) = self.registry.on_complete(completion);
            if let Some(push) = push {
                self.apply(push);
            }
            queue.extend(follow_up);
        }
    }

    fn apply(&mut self, push: Push) {
        match push {
            Push::SubscriptionDelta {
                subscription,
                inserted,
                retracted,
                ..
            } => {
                let Some(view) = self.views.get_mut(&subscription) else {
                    return;
                };
                if let Ok(rows) = &mut view.result {
                    for row in &inserted {
                        rows.add(cells(row), 1);
                    }
                    if self.fault != Fault::DropRetractions {
                        for row in &retracted {
                            rows.add(cells(row), -1);
                        }
                    }
                }
            }
            Push::SubscriptionError {
                subscription,
                message,
            } => {
                if let Some(view) = self.views.get_mut(&subscription) {
                    if view.result.is_ok() {
                        view.result =
                            Err(AdapterError::Failed(format!("refresh failed: {message}")));
                    }
                }
            }
        }
    }
}

fn cells(row: &WireRow) -> Row {
    row.iter().map(Cell::from_json).collect()
}

impl Adapter for SubscriptionAdapter {
    fn name(&self) -> &'static str {
        match self.fault {
            Fault::None => "subscription",
            Fault::DropRetractions => "subscription[drop-retractions]",
        }
    }

    fn execute(&mut self, statement: &str, _revision: Revision) -> Result<Outcome, AdapterError> {
        let outcome = self.host.execute(statement);
        self.settle();
        Ok(outcome)
    }

    /// Reconnect after the restart: each query is subscribed again, and the
    /// fresh snapshot must equal what the deltas had built before it.
    fn restart(&mut self, _revision: Revision) -> Result<(), AdapterError> {
        self.settle();
        self.registry = SubscriptionRegistry::new(0);
        self.host.restart()?;
        self.notifications = self.host.handler().subscribe_notifications();
        let ids: Vec<String> = self.views.keys().cloned().collect();
        for id in ids {
            let query = self.views[&id].query.clone();
            let fresh = self.subscribe(&id, &query);
            let view = self.views.get_mut(&id).expect("id taken from views");
            view.result = match (&view.result, fresh) {
                (Ok(before), Ok(after)) if before != &after => Err(AdapterError::Failed(format!(
                    "restart changed the result: before {}, after {}",
                    rows_text(before),
                    rows_text(&after)
                ))),
                (Ok(_), fresh) => fresh,
                (Err(e), _) => Err(e.clone()),
            };
        }
        Ok(())
    }

    fn observe(&mut self, query: &str, _revision: Revision) -> Result<Observation, AdapterError> {
        // `execute` settles every evaluation, so deltas are complete up to `revision`.
        self.views
            .values()
            .find(|view| view.query == query)
            .map_or_else(
                || {
                    Err(AdapterError::Failed(format!(
                        "query was not subscribed: {query}"
                    )))
                },
                |view| view.result.clone(),
            )
    }
}

fn rows_text(observation: &Observation) -> String {
    let rows: Vec<String> = observation.support().map(show_row).collect();
    format!("{{{}}}", rows.join(", "))
}
