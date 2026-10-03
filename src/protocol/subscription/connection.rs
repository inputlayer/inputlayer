//! Drives a connection's [`SubscriptionRegistry`] from the WebSocket loop.
//!
//! Evaluations run as tasks in a [`JoinSet`], so neither the read loop nor the
//! write path waits on them. Dropping this value (disconnect) aborts them and
//! releases the subscriptions.

use std::sync::Arc;

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
        }
    }

    /// Register `id` for `query` on `knowledge_graph`; returns the initial
    /// snapshot and the subscription's generation.
    pub async fn subscribe(
        &mut self,
        knowledge_graph: &str,
        id: &str,
        query: &str,
    ) -> Result<(Refresh, u64), String> {
        self.registry.check_can_add(id)?;
        let mut view = ReevaluatingQuery::new(
            Arc::clone(&self.handler),
            knowledge_graph,
            query,
            self.auth.clone(),
        )?;
        self.handler.subscription_metrics().record_evaluation();
        let snapshot = view.refresh().await?;
        let generation = self.registry.add(
            id,
            knowledge_graph,
            Box::new(view),
            snapshot.dependencies.clone(),
        )?;
        self.handler.subscription_metrics().add_active(1);
        debug!(
            subscription = id,
            kg = knowledge_graph,
            generation,
            "subscription_added"
        );
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

    /// Feed a persistent-change notification.
    pub fn on_notification(&mut self, notification: &Notification) {
        if self.registry.is_empty() {
            return;
        }
        let (knowledge_graph, change) = change_of(notification);
        for dispatch in self.registry.on_change(knowledge_graph, &change) {
            self.start(dispatch);
        }
    }

    /// Notifications were lost (broadcast lag): re-check everything.
    pub fn on_missed_notifications(&mut self) {
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
