//! Drives a connection's [`SubscriptionRegistry`] from the WebSocket loop.
//!
//! Evaluations run as tasks in a [`JoinSet`], so neither the read loop nor the
//! write path waits on them. Dropping this value (disconnect) aborts them and
//! releases the subscriptions.
//!
//! A `.subscribe` evaluates its initial snapshot off the loop too: between
//! [`ConnectionSubscriptions::begin_subscribe`] and
//! [`ConnectionSubscriptions::finish_subscribe`] the loop keeps feeding
//! notifications, which are recorded against the opening subscription so a
//! commit landing during the snapshot still produces its delta.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use inputlayer_ws_protocol::SubscriptionPush;

use crate::auth::Principal;
use crate::protocol::handler::Notification;
use crate::protocol::Handler;

use super::{
    ChangeSet, Completion, Dispatch, ReevaluatingQuery, Refresh, StandingQuery,
    SubscriptionRegistry,
};

/// Subscriptions owned by one WebSocket connection.
pub struct ConnectionSubscriptions {
    handler: Arc<Handler>,
    auth: Option<Principal>,
    registry: SubscriptionRegistry,
    in_flight: JoinSet<Completion>,
    /// Subscriptions whose initial snapshot is being evaluated: their KG and
    /// the changes committed there meanwhile.
    opening: BTreeMap<String, (String, Option<ChangeSet>)>,
}

/// A subscription's initial snapshot, evaluated off the connection loop.
pub struct Opening {
    id: String,
    knowledge_graph: String,
    view: Box<dyn StandingQuery>,
}

/// An evaluated [`Opening`], handed back to
/// [`ConnectionSubscriptions::finish_subscribe`].
pub struct Opened {
    id: String,
    knowledge_graph: String,
    view: Box<dyn StandingQuery>,
    snapshot: Result<Refresh, String>,
}

impl Opening {
    /// Evaluate the initial snapshot, turning a panic into an error.
    pub async fn run(self) -> Opened {
        let Opening {
            id,
            knowledge_graph,
            mut view,
        } = self;
        let snapshot = std::panic::AssertUnwindSafe(view.refresh())
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("Internal error while evaluating subscription".to_string()));
        Opened {
            id,
            knowledge_graph,
            view,
            snapshot,
        }
    }
}

impl ConnectionSubscriptions {
    /// Empty set, limited by `http.rate_limit.ws_max_subscriptions`.
    pub fn new(handler: Arc<Handler>, auth: Option<Principal>) -> Self {
        let limit = handler.config().http.rate_limit.ws_max_subscriptions;
        Self {
            handler,
            auth,
            registry: SubscriptionRegistry::new(limit),
            in_flight: JoinSet::new(),
            opening: BTreeMap::new(),
        }
    }

    /// Start registering `id` for `query` on `knowledge_graph`. Run the
    /// returned [`Opening`] anywhere, then pass its result to
    /// [`Self::finish_subscribe`]; commits in between are not lost.
    pub fn begin_subscribe(
        &mut self,
        knowledge_graph: &str,
        id: &str,
        query: &str,
    ) -> Result<Opening, String> {
        self.registry.check_can_add(id)?;
        if self.opening.contains_key(id) {
            return Err(format!(
                "Subscription '{id}' is already being registered on this connection."
            ));
        }
        let view = ReevaluatingQuery::new(
            Arc::clone(&self.handler),
            knowledge_graph,
            query,
            self.auth.clone(),
        )?;
        Ok(self.open(knowledge_graph, id, Box::new(view)))
    }

    /// Start taking `view`'s initial snapshot as subscription `id`.
    fn open(&mut self, knowledge_graph: &str, id: &str, view: Box<dyn StandingQuery>) -> Opening {
        self.handler.subscription_metrics().record_evaluation();
        self.opening
            .insert(id.to_string(), (knowledge_graph.to_string(), None));
        Opening {
            id: id.to_string(),
            knowledge_graph: knowledge_graph.to_string(),
            view,
        }
    }

    /// Register an evaluated [`Opening`]; returns its initial snapshot and the
    /// subscription's generation. If a relevant change was committed while the
    /// snapshot was evaluated, a re-evaluation starts at once and its delta
    /// follows the snapshot.
    ///
    /// The snapshot is the query's answer at its revision. A commit published
    /// after that revision but announced before the subscription existed would
    /// reach no one, so registration also compares the knowledge graph's
    /// current revision and re-evaluates at once when it moved on.
    pub fn finish_subscribe(&mut self, opened: Opened) -> Result<(Refresh, u64), String> {
        let Opened {
            id,
            knowledge_graph,
            view,
            snapshot,
        } = opened;
        let missed = self.opening.remove(&id).and_then(|(_, missed)| missed);
        let snapshot = snapshot?;
        let (generation, follow_up) = self.registry.add(
            &id,
            &knowledge_graph,
            view,
            snapshot.dependencies.clone(),
            missed.as_ref(),
        )?;
        self.handler.subscription_metrics().add_active(1);
        debug!(
            subscription = id,
            kg = knowledge_graph,
            generation,
            revision = snapshot.revision,
            "subscription_added"
        );
        let follow_up = follow_up.or_else(|| {
            let moved_on = self
                .handler
                .get_storage()
                .get_snapshot_for(&knowledge_graph)
                .map_or(true, |current| current.revision > snapshot.revision);
            moved_on.then(|| self.registry.invalidate(&id)).flatten()
        });
        if let Some(dispatch) = follow_up {
            self.start(dispatch);
        }
        Ok((snapshot, generation))
    }

    /// Remove `id`; errors if it is not registered.
    pub fn unsubscribe(&mut self, id: &str) -> Result<(), String> {
        if !self.registry.remove(id) {
            return Err(format!("No subscription '{id}' on this connection."));
        }
        self.handler.subscription_metrics().remove_active(1);
        Ok(())
    }

    /// Remove every subscription.
    pub fn clear(&mut self) {
        let removed = self.registry.clear() as u64;
        self.handler.subscription_metrics().remove_active(removed);
    }

    /// Remove every subscription not on `knowledge_graph`: subscriptions are
    /// scoped to the connection's KG, so switching drops them.
    pub fn retain_knowledge_graph(&mut self, knowledge_graph: &str) {
        let removed = self.registry.retain_knowledge_graph(knowledge_graph) as u64;
        self.handler.subscription_metrics().remove_active(removed);
    }

    /// Feed a persistent-change notification.
    pub fn on_notification(&mut self, notification: &Notification) {
        if self.registry.is_empty() && self.opening.is_empty() {
            return;
        }
        let (knowledge_graph, change) = change_of(notification);
        for (opening_kg, missed) in self.opening.values_mut() {
            if opening_kg == knowledge_graph {
                merge_into(missed, &change);
            }
        }
        for dispatch in self.registry.on_change(knowledge_graph, &change) {
            self.start(dispatch);
        }
    }

    /// Notifications were lost (broadcast lag): re-check everything.
    pub fn on_missed_notifications(&mut self) {
        for (_, missed) in self.opening.values_mut() {
            *missed = Some(ChangeSet::Everything);
        }
        for dispatch in self.registry.on_unknown_changes() {
            self.start(dispatch);
        }
    }

    /// Wait for the next finished evaluation. Pending forever when none run.
    pub async fn next_completion(&mut self) -> Completion {
        loop {
            match self.in_flight.join_next().await {
                Some(Ok(completion)) => return completion,
                Some(Err(e)) => warn!(error = %e, "subscription_task_failed"),
                None => std::future::pending::<()>().await,
            }
        }
    }

    /// Accept a finished evaluation; returns the message to push, if any.
    pub fn on_completion(&mut self, completion: Completion) -> Option<SubscriptionPush> {
        let (push, follow_up) = self.registry.on_complete(completion);
        if let Some(dispatch) = follow_up {
            self.start(dispatch);
        }
        push
    }

    fn start(&mut self, dispatch: Dispatch) {
        // Counted here, synchronously, so callers observe it in order with pushes.
        self.handler.subscription_metrics().record_evaluation();
        self.in_flight.spawn(dispatch.run());
    }
}

fn merge_into(missed: &mut Option<ChangeSet>, change: &ChangeSet) {
    match missed {
        Some(missed) => missed.merge(change),
        None => *missed = Some(change.clone()),
    }
}

/// The knowledge graph a notification is about and what it changed there.
pub fn change_of(notification: &Notification) -> (&str, ChangeSet) {
    match notification {
        Notification::PersistentUpdate {
            knowledge_graph,
            relation,
            ..
        } => (knowledge_graph, ChangeSet::relation(relation)),
        Notification::RuleChange {
            knowledge_graph,
            rule_name,
            ..
        } => (knowledge_graph, ChangeSet::relation(rule_name)),
        Notification::SchemaChange {
            knowledge_graph,
            entity,
            ..
        } => (knowledge_graph, ChangeSet::relation(entity)),
        Notification::KgChange {
            knowledge_graph, ..
        } => (knowledge_graph, ChangeSet::Everything),
    }
}

impl Drop for ConnectionSubscriptions {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
