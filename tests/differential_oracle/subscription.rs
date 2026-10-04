//! Incremental adapter: today's standing-query path.
//!
//! Every query is subscribed before the first step, twice, by two subscribers
//! sharing one view, and each subscriber's observed result is assembled purely
//! from its pushed `inserted`/`retracted` deltas, exactly as a subscribed agent
//! would assemble it. Commits reach the views through the real notification
//! broadcast, notification-to-change mapping, dependency filtering and
//! coalescing view registry; the first subscriber takes every publication as it
//! comes (the shared delta), the second only once evaluations settle (its own
//! combined delta). The two must agree. Only the hub task and the WebSocket
//! framing are left out. Evaluations run one at a time, so a run is
//! deterministic.

use std::sync::Arc;
use std::time::{Duration, Instant};

use inputlayer::protocol::handler::Notification;
use inputlayer::protocol::subscription::{
    change_of, Attach, Dispatch, Doorbell, ReevaluatingQuery, Row as WireRow, Subscriber, ViewKey,
    ViewRegistry,
};
use inputlayer_ws_protocol::SubscriptionPush;
use tokio::sync::broadcast::{self, error::TryRecvError};
use tokio::sync::mpsc;

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

/// What one subscriber has assembled, or why it has no trustworthy result.
type Assembled = Result<Observation, AdapterError>;

/// Client-side state of one subscribed query.
struct View {
    query: String,
    /// Delivers each publication as it comes.
    eager: Option<(Subscriber, Assembled)>,
    /// Delivers once evaluations settle.
    lazy: Option<(Subscriber, Assembled)>,
    /// Why the query could not be subscribed.
    failed: Option<AdapterError>,
}

pub struct SubscriptionAdapter {
    host: EngineHost,
    notifications: broadcast::Receiver<Notification>,
    registry: ViewRegistry,
    mailbox: mpsc::UnboundedSender<u64>,
    wake_ups: mpsc::UnboundedReceiver<u64>,
    next_subscriber: u64,
    views: Vec<View>,
    fault: Fault,
}

impl SubscriptionAdapter {
    pub fn open(queries: &[String], fault: Fault) -> Result<Self, AdapterError> {
        let host = EngineHost::open()?;
        let notifications = host.handler().subscribe_notifications();
        let (mailbox, wake_ups) = mpsc::unbounded_channel();
        let mut adapter = Self {
            host,
            notifications,
            registry: ViewRegistry::new(Duration::ZERO),
            mailbox,
            wake_ups,
            next_subscriber: 0,
            views: Vec::new(),
            fault,
        };
        for query in queries {
            let view = adapter.subscribe(query);
            adapter.views.push(view);
        }
        Ok(adapter)
    }

    fn doorbell(&mut self) -> Arc<Doorbell> {
        self.next_subscriber += 1;
        Doorbell::new(self.next_subscriber, self.mailbox.clone())
    }

    /// Subscribe `query` twice on one shared view.
    fn subscribe(&mut self, query: &str) -> View {
        let mut view = View {
            query: query.to_string(),
            eager: None,
            lazy: None,
            failed: None,
        };
        match self.attach_pair(query) {
            Ok((eager, lazy)) => {
                view.eager = Some(eager);
                view.lazy = Some(lazy);
            }
            Err(e) => view.failed = Some(e),
        }
        view
    }

    fn attach_pair(
        &mut self,
        query: &str,
    ) -> Result<((Subscriber, Assembled), (Subscriber, Assembled)), AdapterError> {
        let fresh = ReevaluatingQuery::new(Arc::clone(self.host.handler()), KG, query)
            .map_err(|e| AdapterError::Unsupported(format!("not subscribable: {e}")))?;
        let key = ViewKey {
            knowledge_graph: KG.to_string(),
            queries: vec![query.trim().to_string()],
        };
        let first = self.doorbell();
        let Attach::Waiting(Some(dispatch)) =
            self.registry
                .attach(key.clone(), Arc::clone(&first), || Box::new(fresh))
        else {
            return Err(AdapterError::Failed(
                "a new query did not get its own view".to_string(),
            ));
        };
        let completion = self.host.block_on(dispatch.run());
        let mut completed = self.registry.on_complete(completion, Instant::now());
        let (_, reply) = completed.replies.remove(0);
        let attachment =
            reply.map_err(|e| AdapterError::Failed(format!("initial snapshot: {e}")))?;
        let initial = attachment
            .initial_rows
            .as_ref()
            .and_then(|rows| rows.first())
            .map(|rows| Observation::from_rows(rows.iter().map(cells)));
        let eager = Subscriber::new("eager", 1, KG, first, &attachment);

        let second = self.doorbell();
        let Attach::Attached(shared) = self
            .registry
            .attach(key, Arc::clone(&second), || unreachable!("the view exists"))
        else {
            return Err(AdapterError::Failed(
                "the second subscriber did not share the view".to_string(),
            ));
        };
        let joined = Observation::from_rows(
            shared.publication.results[0]
                .rows
                .sorted_rows()
                .iter()
                .map(cells),
        );
        let lazy = Subscriber::new("lazy", 1, KG, second, &shared);
        let initial = initial.ok_or_else(|| AdapterError::Failed("no initial rows".to_string()))?;
        if initial != joined {
            return Err(AdapterError::Failed(format!(
                "a joining subscriber's snapshot {} differs from the first {}",
                rows_text(&joined),
                rows_text(&initial)
            )));
        }
        Ok(((eager, Ok(initial.clone())), (lazy, Ok(initial))))
    }

    /// Feed every pending notification to the views and run evaluations
    /// (including coalesced follow-ups) until none remain, delivering to the
    /// eager subscribers after each one and to the lazy ones at the end.
    fn settle(&mut self) {
        let mut queue: Vec<Dispatch> = Vec::new();
        loop {
            let now = Instant::now();
            match self.notifications.try_recv() {
                Ok(notification) => {
                    let (knowledge_graph, change) = change_of(&notification);
                    queue.extend(self.registry.on_change(knowledge_graph, &change, now));
                }
                Err(TryRecvError::Lagged(_)) => queue.extend(self.registry.on_unknown_changes(now)),
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
            }
        }
        while let Some(dispatch) = queue.pop() {
            let completion = self.host.block_on(dispatch.run());
            let completed = self.registry.on_complete(completion, Instant::now());
            queue.extend(completed.follow_up);
            self.deliver(|view| view.eager.as_mut());
        }
        self.deliver(|view| view.lazy.as_mut());
        while self.wake_ups.try_recv().is_ok() {}
    }

    /// Answer the doorbells of the subscribers `pick` selects.
    fn deliver(&mut self, pick: impl Fn(&mut View) -> Option<&mut (Subscriber, Assembled)>) {
        let fault = self.fault;
        for view in &mut self.views {
            if let Some((subscriber, assembled)) = pick(view) {
                if let Some(push) = subscriber.deliver(|_| true) {
                    apply(assembled, push, fault);
                }
            }
        }
    }
}

/// Apply `push` to an assembled result.
fn apply(assembled: &mut Assembled, push: SubscriptionPush, fault: Fault) {
    match push {
        SubscriptionPush::SubscriptionDelta {
            inserted,
            retracted,
            ..
        } => {
            if let Ok(rows) = assembled {
                for row in &inserted {
                    rows.add(cells(row), 1);
                }
                if fault != Fault::DropRetractions {
                    for row in &retracted {
                        rows.add(cells(row), -1);
                    }
                }
            }
        }
        SubscriptionPush::SubscriptionError { message, .. }
        | SubscriptionPush::SubscriptionReset { message, .. } => {
            if assembled.is_ok() {
                *assembled = Err(AdapterError::Failed(format!("refresh failed: {message}")));
            }
        }
        SubscriptionPush::SubscriptionDeltaStart { .. }
        | SubscriptionPush::SubscriptionDeltaChunk { .. }
        | SubscriptionPush::SubscriptionDeltaEnd { .. }
        | SubscriptionPush::SubscriptionGroupDeltaStart { .. }
        | SubscriptionPush::SubscriptionGroupDeltaChunk { .. }
        | SubscriptionPush::SubscriptionGroupDeltaEnd { .. } => {
            unreachable!("streaming is the WebSocket's framing; subscribers push whole deltas")
        }
        SubscriptionPush::SubscriptionGroupDelta { .. } => {
            unreachable!("plain subscribers push plain deltas")
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
        self.registry = ViewRegistry::new(Duration::ZERO);
        self.host.restart()?;
        self.notifications = self.host.handler().subscribe_notifications();
        let before = std::mem::take(&mut self.views);
        for old in before {
            let mut fresh = self.subscribe(&old.query);
            let previous = observed(&old);
            if let (Ok(before), Ok(after)) = (&previous, observed(&fresh)) {
                if before != &after {
                    fresh.failed = Some(AdapterError::Failed(format!(
                        "restart changed the result: before {}, after {}",
                        rows_text(before),
                        rows_text(&after)
                    )));
                }
            } else if let Err(e) = previous {
                fresh.failed = Some(e);
            }
            self.views.push(fresh);
        }
        Ok(())
    }

    fn observe(&mut self, query: &str, _revision: Revision) -> Result<Observation, AdapterError> {
        // `execute` settles every evaluation, so deltas are complete up to `revision`.
        self.views
            .iter()
            .find(|view| view.query == query)
            .map_or_else(
                || {
                    Err(AdapterError::Failed(format!(
                        "query was not subscribed: {query}"
                    )))
                },
                observed,
            )
    }
}

/// The result both subscribers of `view` agree on.
fn observed(view: &View) -> Assembled {
    if let Some(e) = &view.failed {
        return Err(e.clone());
    }
    match (&view.eager, &view.lazy) {
        (Some((_, eager)), Some((_, lazy))) => match (eager, lazy) {
            (Ok(a), Ok(b)) if a != b => Err(AdapterError::Failed(format!(
                "shared subscribers diverged: per publication {}, settled {}",
                rows_text(a),
                rows_text(b)
            ))),
            _ => eager.clone(),
        },
        _ => Err(AdapterError::Failed("not subscribed".to_string())),
    }
}

fn rows_text(observation: &Observation) -> String {
    let rows: Vec<String> = observation.support().map(show_row).collect();
    format!("{{{}}}", rows.join(", "))
}
