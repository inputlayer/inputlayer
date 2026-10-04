//! A WebSocket connection's subscriptions.
//!
//! Each `.subscribe` is authorized for the connection's principal, then
//! attaches to the shared view of its query through the server's
//! [`SubscriptionHub`]; identical queries on one knowledge graph share one
//! evaluation, whoever subscribes. The connection keeps what is its own: the
//! subscription names and generations, the delta numbering, and each
//! subscriber's last delivered result. Its WS loop awaits
//! [`ConnectionSubscriptions::next_delivery`] and turns each wake-up into a
//! push with [`ConnectionSubscriptions::deliver`]. Dropping this value
//! (disconnect) detaches every subscriber.
//!
//! Attaching waits for the view's first evaluation when the view is new, so
//! it runs off the loop: [`ConnectionSubscriptions::begin_subscribe`] returns
//! an [`Opening`] to run anywhere, and
//! [`ConnectionSubscriptions::finish_subscribe`] registers its result, after
//! checking read access again since it may have been revoked meanwhile. A
//! subscriber attached but never registered (the opening was dropped, or
//! registration failed) is detached again.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::debug;

use inputlayer_ws_protocol::SubscriptionPush;

use crate::auth::Principal;
use crate::protocol::Handler;

use super::publication::{Doorbell, SubscriberId};
use super::views::{Attachment, ViewKey};
use super::{ReevaluatingQuery, Snapshot, StandingQuery, Subscriber, SubscriptionHub};

/// Subscriptions owned by one WebSocket connection.
pub struct ConnectionSubscriptions {
    handler: Arc<Handler>,
    auth: Option<Principal>,
    /// Maximum subscriptions (0 = unlimited).
    limit: usize,
    next_generation: u64,
    names: BTreeMap<String, SubscriberId>,
    subscribers: HashMap<SubscriberId, Subscriber>,
    mailbox: mpsc::UnboundedSender<SubscriberId>,
    wake_ups: mpsc::UnboundedReceiver<SubscriberId>,
}

impl ConnectionSubscriptions {
    /// Empty set, limited by `http.rate_limit.ws_max_subscriptions`.
    pub fn new(handler: Arc<Handler>, auth: Option<Principal>) -> Self {
        let limit = handler.config().http.rate_limit.ws_max_subscriptions;
        let (mailbox, wake_ups) = mpsc::unbounded_channel();
        Self {
            handler,
            auth,
            limit,
            next_generation: 0,
            names: BTreeMap::new(),
            subscribers: HashMap::new(),
            mailbox,
            wake_ups,
        }
    }

    /// Number of subscriptions.
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// True when nothing is subscribed.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Start registering `id` for `query` on `knowledge_graph`, as the
    /// connection's principal. Run the returned [`Opening`] anywhere, then pass
    /// its result to [`Self::finish_subscribe`].
    pub fn begin_subscribe(
        &mut self,
        knowledge_graph: &str,
        id: &str,
        query: &str,
    ) -> Result<Opening, String> {
        self.check_can_add(id)?;
        let view = ReevaluatingQuery::new(Arc::clone(&self.handler), knowledge_graph, query)?;
        self.handler
            .authorize_query(self.auth.as_ref(), knowledge_graph, view.goal())?;
        // A view not refreshed since is still exact now, but must say so: a
        // client may expect the revision its snapshot reports.
        let revision = view.current_snapshot().map_or(0, |snapshot| snapshot.revision);
        let key = ViewKey {
            knowledge_graph: knowledge_graph.to_string(),
            query: query.trim().to_string(),
        };
        let share = self.handler.config().subscriptions.share_parameterized;
        let view = self.hub().standing_query(view, share);
        Ok(self.opening(key, id, revision, view))
    }

    /// An [`Opening`] attaching `id` to the view of `key`, created from `view`
    /// if there is none, with a snapshot exact at `revision` or later.
    fn opening(
        &self,
        key: ViewKey,
        id: &str,
        revision: u64,
        view: Box<dyn StandingQuery>,
    ) -> Opening {
        let hub = self.hub().clone();
        let doorbell = Doorbell::new(hub.next_subscriber_id(), self.mailbox.clone());
        Opening {
            id: id.to_string(),
            key,
            revision,
            view,
            attached: Attached {
                hub,
                doorbell,
                kept: false,
            },
        }
    }

    /// Register an [`Opened`] subscription; returns its initial snapshot and
    /// generation. `readable` tells whether this connection may currently
    /// read a knowledge graph: access may have been revoked while it opened.
    pub fn finish_subscribe(
        &mut self,
        opened: Opened,
        readable: impl FnOnce(&str) -> bool,
    ) -> Result<(Snapshot, u64), String> {
        let Opened {
            id,
            key,
            attached,
            attachment,
        } = opened;
        let attachment = attachment?;
        if !readable(&key.knowledge_graph) {
            return Err(format!(
                "Access denied to knowledge graph '{}'.",
                key.knowledge_graph
            ));
        }
        // Checked again: another `.subscribe` may have taken the name meanwhile.
        self.check_can_add(&id)?;
        self.next_generation += 1;
        let generation = self.next_generation;
        let snapshot = Snapshot::of(&attachment);
        let doorbell = attached.keep();
        let subscriber = doorbell.id();
        self.names.insert(id.clone(), subscriber);
        self.subscribers.insert(
            subscriber,
            Subscriber::new(&id, generation, &key.knowledge_graph, doorbell, &attachment),
        );
        self.handler.subscription_metrics().add_active(1);
        debug!(
            subscription = id,
            kg = key.knowledge_graph,
            generation,
            revision = snapshot.revision,
            "subscription_added"
        );
        Ok((snapshot, generation))
    }

    /// Fail if `id` is taken or the limit is reached.
    fn check_can_add(&self, id: &str) -> Result<(), String> {
        if self.names.contains_key(id) {
            return Err(format!(
                "Subscription '{id}' already exists on this connection. \
                 Use .unsubscribe {id} first or pick another id."
            ));
        }
        if self.limit > 0 && self.names.len() >= self.limit {
            return Err(format!(
                "Subscription limit reached ({} per connection, see \
                 http.rate_limit.ws_max_subscriptions). Unsubscribe from one first.",
                self.limit
            ));
        }
        Ok(())
    }

    /// Remove `id`; errors if it is not registered.
    pub fn unsubscribe(&mut self, id: &str) -> Result<(), String> {
        let subscriber = self
            .names
            .remove(id)
            .ok_or_else(|| format!("No subscription '{id}' on this connection."))?;
        self.subscribers.remove(&subscriber);
        self.hub().detach(subscriber);
        self.handler.subscription_metrics().remove_active(1);
        Ok(())
    }

    /// End registration `generation` of `id`, whose change could not be
    /// delivered; a newer registration under the same id stays. Returns
    /// whether it was registered.
    pub fn reset(&mut self, id: &str, generation: u64) -> bool {
        let current = self
            .names
            .get(id)
            .and_then(|subscriber| self.subscribers.get(subscriber))
            .is_some_and(|subscriber| subscriber.generation() == generation);
        current && self.unsubscribe(id).is_ok()
    }

    /// Remove every subscription.
    pub fn clear(&mut self) {
        self.remove_where(|_| true);
    }

    /// Remove every subscription not on `knowledge_graph`: subscriptions are
    /// scoped to the connection's KG, so switching drops them.
    pub fn retain_knowledge_graph(&mut self, knowledge_graph: &str) {
        self.remove_where(|subscriber| subscriber.knowledge_graph() != knowledge_graph);
    }

    fn remove_where(&mut self, remove: impl Fn(&Subscriber) -> bool) {
        let gone: Vec<SubscriberId> = self
            .subscribers
            .values()
            .filter(|subscriber| remove(subscriber))
            .map(Subscriber::id)
            .collect();
        if gone.is_empty() {
            return;
        }
        let hub = self.hub().clone();
        for subscriber in &gone {
            if let Some(removed) = self.subscribers.remove(subscriber) {
                self.names.remove(removed.name());
            }
            hub.detach(*subscriber);
        }
        self.handler
            .subscription_metrics()
            .remove_active(gone.len() as u64);
    }

    /// Wait until a subscriber has news. Pending forever while none does.
    pub async fn next_delivery(&mut self) -> SubscriberId {
        match self.wake_ups.recv().await {
            Some(subscriber) => subscriber,
            // Unreachable: `self.mailbox` keeps the channel open.
            None => std::future::pending().await,
        }
    }

    /// The push for `subscriber`'s wake-up, if any. `readable` tells whether
    /// this connection may currently read a knowledge graph.
    pub fn deliver(
        &mut self,
        subscriber: SubscriberId,
        readable: impl FnOnce(&str) -> bool,
    ) -> Option<SubscriptionPush> {
        // A wake-up for a subscriber already removed is stale.
        self.subscribers.get_mut(&subscriber)?.deliver(readable)
    }

    fn hub(&self) -> &SubscriptionHub {
        self.handler.subscription_hub()
    }
}

impl Drop for ConnectionSubscriptions {
    fn drop(&mut self) {
        self.clear();
    }
}

/// A subscription's attachment to its shared view, evaluated off the
/// connection loop when the view is new.
pub struct Opening {
    id: String,
    key: ViewKey,
    revision: u64,
    view: Box<dyn StandingQuery>,
    attached: Attached,
}

/// A run [`Opening`], handed back to [`ConnectionSubscriptions::finish_subscribe`].
pub struct Opened {
    id: String,
    key: ViewKey,
    attached: Attached,
    attachment: Result<Attachment, String>,
}

impl Opening {
    /// Attach to the view, waiting for its first result if it is new.
    pub async fn run(self) -> Opened {
        let Opening {
            id,
            key,
            revision,
            view,
            attached,
        } = self;
        let attachment = attached
            .hub
            .attach(key.clone(), Arc::clone(&attached.doorbell), revision, view)
            .await;
        Opened {
            id,
            key,
            attached,
            attachment,
        }
    }
}

/// A subscriber the hub may hold: detached when dropped unless kept.
struct Attached {
    hub: SubscriptionHub,
    doorbell: Arc<Doorbell>,
    kept: bool,
}

impl Attached {
    fn keep(mut self) -> Arc<Doorbell> {
        self.kept = true;
        Arc::clone(&self.doorbell)
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        if !self.kept {
            self.hub.detach(self.doorbell.id());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests;
