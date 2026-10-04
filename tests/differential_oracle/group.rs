//! Group adapter: every query of a history subscribed as one subscription group.
//!
//! The history's subscribable queries form one group view, so each refresh
//! evaluates them on one snapshot and re-runs only those whose inputs changed.
//! Two subscribers share the view, and each assembles every member purely
//! from its pushed group deltas, as a subscribed agent rendering one window
//! would: the first takes every publication as it comes (the shared delta),
//! the second only once evaluations settle (its own combined delta). Both
//! must agree with each other and with the oracle's other adapters at every
//! checkpoint. Each push must also list every member, mark exactly the
//! unchanged ones, and name a higher revision than the last.

use std::sync::Arc;
use std::time::{Duration, Instant};

use inputlayer::protocol::subscription::{
    change_of, changes_rules, Attach, Dispatch, Doorbell, GroupQuery, ReevaluatingQuery,
    Row as WireRow, Subscriber, ViewKey, ViewRegistry,
};
use inputlayer_ws_protocol::SubscriptionPush;
use tokio::sync::broadcast::{self, error::TryRecvError};
use tokio::sync::mpsc;

use crate::adapter::Adapter;
use crate::engine::{EngineHost, KG};
use crate::model::{show_row, AdapterError, Cell, Observation, Outcome, Revision, Row};

/// What one subscriber has assembled for every member, or why it has no
/// trustworthy result.
type Assembled = Result<Vec<Observation>, AdapterError>;

/// One subscriber of the group and what it assembled.
struct Member {
    subscriber: Subscriber,
    assembled: Assembled,
    /// The revision of the last push it applied.
    revision: u64,
}

pub struct GroupAdapter {
    host: EngineHost,
    notifications: broadcast::Receiver<inputlayer::protocol::handler::Notification>,
    registry: ViewRegistry,
    mailbox: mpsc::UnboundedSender<u64>,
    wake_ups: mpsc::UnboundedReceiver<u64>,
    next_subscriber: u64,
    /// The group's queries, in order.
    queries: Vec<String>,
    /// Queries that cannot be subscribed on their own, and why.
    refused: Vec<(String, AdapterError)>,
    /// Delivers each publication as it comes.
    eager: Option<Member>,
    /// Delivers once evaluations settle.
    lazy: Option<Member>,
    /// Why the group could not be subscribed.
    failed: Option<AdapterError>,
}

impl GroupAdapter {
    pub fn open(queries: &[String]) -> Result<Self, AdapterError> {
        let host = EngineHost::open()?;
        let notifications = host.handler().subscribe_notifications();
        let (mailbox, wake_ups) = mpsc::unbounded_channel();
        let mut grouped = Vec::new();
        let mut refused = Vec::new();
        for query in queries {
            match ReevaluatingQuery::new(Arc::clone(host.handler()), KG, query) {
                Ok(_) if !grouped.contains(query) => grouped.push(query.clone()),
                Ok(_) => {}
                Err(e) => refused.push((
                    query.clone(),
                    AdapterError::Unsupported(format!("not subscribable: {e}")),
                )),
            }
        }
        let mut adapter = Self {
            host,
            notifications,
            registry: ViewRegistry::new(Duration::ZERO),
            mailbox,
            wake_ups,
            next_subscriber: 0,
            queries: grouped,
            refused,
            eager: None,
            lazy: None,
            failed: None,
        };
        adapter.subscribe();
        Ok(adapter)
    }

    fn doorbell(&mut self) -> Arc<Doorbell> {
        self.next_subscriber += 1;
        Doorbell::new(self.next_subscriber, self.mailbox.clone())
    }

    /// Subscribe the group twice on one shared view.
    fn subscribe(&mut self) {
        (self.eager, self.lazy, self.failed) = (None, None, None);
        if self.queries.is_empty() {
            return;
        }
        match self.attach_pair() {
            Ok((eager, lazy)) => {
                self.eager = Some(eager);
                self.lazy = Some(lazy);
            }
            Err(e) => self.failed = Some(e),
        }
    }

    fn attach_pair(&mut self) -> Result<(Member, Member), AdapterError> {
        let texts: Vec<&str> = self.queries.iter().map(String::as_str).collect();
        let fresh = GroupQuery::new(Arc::clone(self.host.handler()), KG, &texts)
            .map_err(|e| AdapterError::Failed(format!("group not subscribable: {e}")))?;
        let key = ViewKey {
            knowledge_graph: KG.to_string(),
            queries: self.queries.iter().map(|q| q.trim().to_string()).collect(),
        };
        let names: Arc<[String]> = (0..self.queries.len()).map(|i| format!("q{i}")).collect();
        let first = self.doorbell();
        let Attach::Waiting(Some(dispatch)) =
            self.registry
                .attach(key.clone(), Arc::clone(&first), || Box::new(fresh))
        else {
            return Err(AdapterError::Failed(
                "a new group did not get its own view".to_string(),
            ));
        };
        let completion = self.host.block_on(dispatch.run());
        let mut completed = self.registry.on_complete(completion, Instant::now());
        let (_, reply) = completed.replies.remove(0);
        let attachment =
            reply.map_err(|e| AdapterError::Failed(format!("initial snapshot: {e}")))?;
        let initial: Vec<Observation> = attachment
            .initial_rows
            .as_ref()
            .ok_or_else(|| AdapterError::Failed("no initial rows".to_string()))?
            .iter()
            .map(|rows| Observation::from_rows(rows.iter().map(cells)))
            .collect();
        let revision = attachment.publication.revision;
        let eager = Subscriber::new("eager", 1, KG, first, &attachment).grouped(Arc::clone(&names));

        let second = self.doorbell();
        let Attach::Attached(shared) = self
            .registry
            .attach(key, Arc::clone(&second), || unreachable!("the view exists"))
        else {
            return Err(AdapterError::Failed(
                "the second subscriber did not share the group".to_string(),
            ));
        };
        let joined: Vec<Observation> = shared
            .publication
            .results
            .iter()
            .map(|result| Observation::from_rows(result.rows.sorted_rows().iter().map(cells)))
            .collect();
        if initial != joined {
            return Err(AdapterError::Failed(
                "a joining subscriber's group snapshot differs from the first".to_string(),
            ));
        }
        let lazy = Subscriber::new("lazy", 1, KG, second, &shared).grouped(names);
        let member = |subscriber| Member {
            subscriber,
            assembled: Ok(initial.clone()),
            revision,
        };
        Ok((member(eager), member(lazy)))
    }

    /// Feed every pending notification to the view and run evaluations
    /// (including coalesced follow-ups) until none remain, delivering to the
    /// eager subscriber after each one and to the lazy one at the end.
    fn settle(&mut self) {
        let mut queue: Vec<Dispatch> = Vec::new();
        loop {
            let now = Instant::now();
            match self.notifications.try_recv() {
                Ok(notification) => {
                    let (knowledge_graph, change) = change_of(&notification);
                    queue.extend(if changes_rules(&notification) {
                        self.registry.on_rule_change(knowledge_graph, &change, now)
                    } else {
                        self.registry.on_change(knowledge_graph, &change, now)
                    });
                }
                Err(TryRecvError::Lagged(_)) => queue.extend(self.registry.on_unknown_changes(now)),
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
            }
        }
        let width = self.queries.len();
        while let Some(dispatch) = queue.pop() {
            let completion = self.host.block_on(dispatch.run());
            let completed = self.registry.on_complete(completion, Instant::now());
            queue.extend(completed.follow_up);
            deliver(self.eager.as_mut(), width);
        }
        deliver(self.lazy.as_mut(), width);
        while self.wake_ups.try_recv().is_ok() {}
    }
}

/// Answer `member`'s doorbell and apply what it pushes.
fn deliver(member: Option<&mut Member>, width: usize) {
    let Some(member) = member else {
        return;
    };
    if let Some(push) = member.subscriber.deliver(|_| true) {
        apply(member, push, width);
    }
}

/// Apply `push` to what `member` assembled, checking the group contract.
fn apply(member: &mut Member, push: SubscriptionPush, width: usize) {
    let fail = |member: &mut Member, why: String| {
        if member.assembled.is_ok() {
            member.assembled = Err(AdapterError::Failed(why));
        }
    };
    match push {
        SubscriptionPush::SubscriptionGroupDelta {
            revision, members, ..
        } => {
            if revision <= member.revision {
                return fail(
                    member,
                    format!("push at revision {revision} after {}", member.revision),
                );
            }
            member.revision = revision;
            if members.len() != width {
                return fail(
                    member,
                    format!("push lists {} of {width} members", members.len()),
                );
            }
            let Ok(observations) = &mut member.assembled else {
                return;
            };
            for (i, (delta, observation)) in members.iter().zip(observations.iter_mut()).enumerate()
            {
                let empty = delta.inserted.is_empty() && delta.retracted.is_empty();
                if delta.name != format!("q{i}") || delta.unchanged != empty {
                    let why = format!("member {i} mislabelled: {delta:?}");
                    return fail(member, why);
                }
                for row in &delta.inserted {
                    observation.add(cells(row), 1);
                }
                for row in &delta.retracted {
                    observation.add(cells(row), -1);
                }
            }
        }
        SubscriptionPush::SubscriptionError { message, .. }
        | SubscriptionPush::SubscriptionReset { message, .. } => {
            fail(member, format!("group refresh failed: {message}"));
        }
        other => fail(member, format!("a group subscriber pushed {other:?}")),
    }
}

fn cells(row: &WireRow) -> Row {
    row.iter().map(Cell::from_json).collect()
}

impl Adapter for GroupAdapter {
    fn name(&self) -> &'static str {
        "subscription[group]"
    }

    fn execute(&mut self, statement: &str, _revision: Revision) -> Result<Outcome, AdapterError> {
        let outcome = self.host.execute(statement);
        self.settle();
        Ok(outcome)
    }

    /// Reconnect after the restart: the group is subscribed again, and the
    /// fresh snapshot must equal what the deltas had built before it.
    fn restart(&mut self, _revision: Revision) -> Result<(), AdapterError> {
        self.settle();
        let before = self.observed_all();
        self.registry = ViewRegistry::new(Duration::ZERO);
        self.host.restart()?;
        self.notifications = self.host.handler().subscribe_notifications();
        self.subscribe();
        if let (Ok(before), Ok(after)) = (&before, self.observed_all()) {
            if before != &after {
                self.failed = Some(AdapterError::Failed(
                    "restart changed the group's results".to_string(),
                ));
            }
        } else if let Err(e) = before {
            self.failed = Some(e);
        }
        Ok(())
    }

    fn observe(&mut self, query: &str, _revision: Revision) -> Result<Observation, AdapterError> {
        if let Some((_, error)) = self.refused.iter().find(|(q, _)| q == query) {
            return Err(error.clone());
        }
        let index = self
            .queries
            .iter()
            .position(|q| q == query)
            .ok_or_else(|| AdapterError::Failed(format!("query was not subscribed: {query}")))?;
        Ok(self.observed_all()?.swap_remove(index))
    }
}

impl GroupAdapter {
    /// Every member's result both subscribers agree on.
    fn observed_all(&self) -> Assembled {
        if let Some(e) = &self.failed {
            return Err(e.clone());
        }
        match (&self.eager, &self.lazy) {
            (Some(eager), Some(lazy)) => match (&eager.assembled, &lazy.assembled) {
                (Ok(a), Ok(b)) if a != b => {
                    let differing = a
                        .iter()
                        .zip(b)
                        .position(|(a, b)| a != b)
                        .unwrap_or_default();
                    Err(AdapterError::Failed(format!(
                        "group subscribers diverged on {}: per publication {}, settled {}",
                        self.queries[differing],
                        rows_text(&a[differing]),
                        rows_text(&b[differing])
                    )))
                }
                _ => eager.assembled.clone(),
            },
            _ => Err(AdapterError::Failed("not subscribed".to_string())),
        }
    }
}

fn rows_text(observation: &Observation) -> String {
    let rows: Vec<String> = observation.support().map(show_row).collect();
    format!("{{{}}}", rows.join(", "))
}
