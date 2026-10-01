//! Per-connection subscription state machine.
//!
//! Each subscription is either *idle* (its [`StandingQuery`] is parked here) or
//! *in flight* (the view was handed out in a [`Dispatch`] and comes back in a
//! [`Completion`]). That ownership hand-off is what guarantees at most one
//! evaluation per subscription. Changes that arrive while in flight are
//! accumulated and checked against the *new* dependency set on completion, so
//! a burst of commits costs one follow-up evaluation and the final state is
//! never dropped.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use super::{Dependencies, Refresh, Row, StandingQuery};

/// Committed persistent changes in one knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeSet {
    /// These relations (base data, rules defining them, or the relation itself).
    Relations(BTreeSet<String>),
    /// Unknown scope (missed notifications, KG dropped): treat as relevant.
    Everything,
}

impl ChangeSet {
    /// Change to a single relation.
    pub fn relation(name: &str) -> Self {
        Self::Relations(BTreeSet::from([name.to_string()]))
    }

    fn merge(&mut self, other: &ChangeSet) {
        match (&mut *self, other) {
            (Self::Everything, _) => {}
            (_, Self::Everything) => *self = Self::Everything,
            (Self::Relations(mine), Self::Relations(theirs)) => {
                mine.extend(theirs.iter().cloned());
            }
        }
    }
}

/// Unsolicited message pushed to the subscriber.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Push {
    /// The result set changed.
    SubscriptionDelta {
        subscription: String,
        knowledge_graph: String,
        seq: u64,
        columns: Vec<String>,
        inserted: Vec<Row>,
        retracted: Vec<Row>,
    },
    /// Re-evaluation failed; the subscription stays registered.
    SubscriptionError {
        subscription: String,
        message: String,
    },
}

/// An evaluation to run: the caller drives `view.refresh()` and hands the view
/// back in a [`Completion`].
pub struct Dispatch {
    pub id: String,
    pub token: u64,
    pub view: Box<dyn StandingQuery>,
}

/// Outcome of a [`Dispatch`].
pub struct Completion {
    pub id: String,
    pub token: u64,
    pub view: Box<dyn StandingQuery>,
    pub result: Result<Refresh, String>,
}

impl Dispatch {
    /// Run the evaluation, turning a panic into an error so the view survives.
    pub async fn run(self) -> Completion {
        use futures_util::FutureExt;
        let Dispatch {
            id,
            token,
            mut view,
        } = self;
        let result = std::panic::AssertUnwindSafe(view.refresh())
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err("Internal error while evaluating subscription".to_string()));
        Completion {
            id,
            token,
            view,
            result,
        }
    }
}

struct Entry {
    /// Distinguishes this registration from an earlier one with the same id.
    token: u64,
    knowledge_graph: String,
    seq: u64,
    dependencies: Dependencies,
    /// `None` while an evaluation is in flight.
    view: Option<Box<dyn StandingQuery>>,
    /// Changes seen while in flight.
    pending: Option<ChangeSet>,
}

/// All subscriptions of one connection.
pub struct SubscriptionRegistry {
    /// Maximum subscriptions (0 = unlimited).
    limit: usize,
    next_token: u64,
    entries: BTreeMap<String, Entry>,
}

impl SubscriptionRegistry {
    /// Create an empty registry allowing `limit` subscriptions (0 = unlimited).
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            next_token: 0,
            entries: BTreeMap::new(),
        }
    }

    /// Number of registered subscriptions.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when nothing is registered.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Fail if `id` is taken or the limit is reached.
    pub fn check_can_add(&self, id: &str) -> Result<(), String> {
        if self.entries.contains_key(id) {
            return Err(format!(
                "Subscription '{id}' already exists on this connection. \
                 Use .unsubscribe {id} first or pick another id."
            ));
        }
        if self.limit > 0 && self.entries.len() >= self.limit {
            return Err(format!(
                "Subscription limit reached ({} per connection, see \
                 http.rate_limit.ws_max_subscriptions). Unsubscribe from one first.",
                self.limit
            ));
        }
        Ok(())
    }

    /// Register a subscription whose initial refresh already ran.
    pub fn add(
        &mut self,
        id: &str,
        knowledge_graph: &str,
        view: Box<dyn StandingQuery>,
        dependencies: Dependencies,
    ) -> Result<(), String> {
        self.check_can_add(id)?;
        self.next_token += 1;
        self.entries.insert(
            id.to_string(),
            Entry {
                token: self.next_token,
                knowledge_graph: knowledge_graph.to_string(),
                seq: 0,
                dependencies,
                view: Some(view),
                pending: None,
            },
        );
        Ok(())
    }

    /// Remove a subscription. Returns false if it did not exist.
    pub fn remove(&mut self, id: &str) -> bool {
        self.entries.remove(id).is_some()
    }

    /// Remove every subscription; returns how many there were.
    pub fn clear(&mut self) -> usize {
        let count = self.entries.len();
        self.entries.clear();
        count
    }

    /// React to committed changes in `knowledge_graph`: returns evaluations to start.
    pub fn on_change(&mut self, knowledge_graph: &str, change: &ChangeSet) -> Vec<Dispatch> {
        let mut dispatches = Vec::new();
        for (id, entry) in &mut self.entries {
            if entry.knowledge_graph != knowledge_graph {
                continue;
            }
            match entry.view.take() {
                Some(view) if entry.dependencies.is_affected_by(change) => {
                    dispatches.push(Dispatch {
                        id: id.clone(),
                        token: entry.token,
                        view,
                    });
                }
                Some(view) => entry.view = Some(view),
                None => match &mut entry.pending {
                    Some(pending) => pending.merge(change),
                    None => entry.pending = Some(change.clone()),
                },
            }
        }
        dispatches
    }

    /// React to changes of unknown scope in every knowledge graph.
    pub fn on_unknown_changes(&mut self) -> Vec<Dispatch> {
        let graphs: BTreeSet<String> = self
            .entries
            .values()
            .map(|e| e.knowledge_graph.clone())
            .collect();
        graphs
            .iter()
            .flat_map(|kg| self.on_change(kg, &ChangeSet::Everything))
            .collect()
    }

    /// Accept a finished evaluation: returns the message to push (if the result
    /// changed or failed) and a follow-up evaluation (if changes arrived meanwhile).
    pub fn on_complete(&mut self, completion: Completion) -> (Option<Push>, Option<Dispatch>) {
        let Completion {
            id,
            token,
            view,
            result,
        } = completion;
        let Some(entry) = self.entries.get_mut(&id).filter(|e| e.token == token) else {
            // Unsubscribed (or replaced) while in flight.
            return (None, None);
        };
        let push = match result {
            Ok(refresh) => {
                let changed = !refresh.is_unchanged();
                entry.dependencies = refresh.dependencies;
                changed.then(|| {
                    entry.seq += 1;
                    Push::SubscriptionDelta {
                        subscription: id.clone(),
                        knowledge_graph: entry.knowledge_graph.clone(),
                        seq: entry.seq,
                        columns: refresh.columns,
                        inserted: refresh.inserted,
                        retracted: refresh.retracted,
                    }
                })
            }
            Err(message) => Some(Push::SubscriptionError {
                subscription: id.clone(),
                message,
            }),
        };
        let rerun = entry
            .pending
            .take()
            .is_some_and(|pending| entry.dependencies.is_affected_by(&pending));
        let follow_up = if rerun {
            Some(Dispatch { id, token, view })
        } else {
            entry.view = Some(view);
            None
        };
        (push, follow_up)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::statement::parse_query;
    use futures_util::future::BoxFuture;

    /// Test view: returns queued refreshes in order.
    struct Scripted(Vec<Result<Refresh, String>>);

    impl StandingQuery for Scripted {
        fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
            let next = self.0.remove(0);
            Box::pin(async move { next })
        }
    }

    fn deps_on(relation: &str) -> Dependencies {
        Dependencies::for_query(&parse_query(&format!("{relation}(X)")).unwrap(), &[])
    }

    fn refresh(inserted: &[i64], retracted: &[i64], relation: &str) -> Refresh {
        let rows = |v: &[i64]| v.iter().map(|n| vec![serde_json::json!(n)]).collect();
        Refresh {
            columns: vec!["x".to_string()],
            inserted: rows(inserted),
            retracted: rows(retracted),
            dependencies: deps_on(relation),
        }
    }

    fn registry_with(script: Vec<Result<Refresh, String>>) -> SubscriptionRegistry {
        let mut registry = SubscriptionRegistry::new(4);
        registry
            .add("s", "kg", Box::new(Scripted(script)), deps_on("a"))
            .unwrap();
        registry
    }

    async fn complete(
        registry: &mut SubscriptionRegistry,
        d: Dispatch,
    ) -> (Option<Push>, Option<Dispatch>) {
        registry.on_complete(d.run().await)
    }

    #[tokio::test]
    async fn test_registry_change_dispatches_and_numbers_deltas() {
        let mut registry = registry_with(vec![
            Ok(refresh(&[1], &[], "a")),
            Ok(refresh(&[], &[1], "a")),
        ]);
        for expected_seq in 1..=2 {
            let mut dispatches = registry.on_change("kg", &ChangeSet::relation("a"));
            assert_eq!(dispatches.len(), 1);
            let (push, follow_up) = complete(&mut registry, dispatches.remove(0)).await;
            assert!(
                matches!(push, Some(Push::SubscriptionDelta { seq, .. }) if seq == expected_seq)
            );
            assert!(follow_up.is_none());
        }
    }

    #[test]
    fn test_registry_unrelated_change_or_other_kg_does_not_dispatch() {
        let mut registry = registry_with(vec![]);
        assert!(registry
            .on_change("kg", &ChangeSet::relation("b"))
            .is_empty());
        assert!(registry
            .on_change("other", &ChangeSet::relation("a"))
            .is_empty());
    }

    #[tokio::test]
    async fn test_registry_coalesces_changes_while_in_flight() {
        let mut registry = registry_with(vec![
            Ok(refresh(&[1], &[], "a")),
            Ok(refresh(&[2, 3], &[], "a")),
        ]);
        let first = registry
            .on_change("kg", &ChangeSet::relation("a"))
            .remove(0);
        for _ in 0..10 {
            assert!(registry
                .on_change("kg", &ChangeSet::relation("a"))
                .is_empty());
        }
        let (push, follow_up) = complete(&mut registry, first).await;
        assert!(matches!(push, Some(Push::SubscriptionDelta { seq: 1, .. })));
        let (push, follow_up) = complete(&mut registry, follow_up.unwrap()).await;
        assert!(matches!(push, Some(Push::SubscriptionDelta { seq: 2, .. })));
        assert!(follow_up.is_none());
    }

    #[tokio::test]
    async fn test_registry_pending_change_checked_against_new_dependencies() {
        // The in-flight evaluation discovers a new dependency `b` (rule added);
        // a write to `b` seen meanwhile must trigger a follow-up.
        let mut registry = registry_with(vec![
            Ok(refresh(&[], &[], "b")),
            Ok(refresh(&[7], &[], "b")),
        ]);
        let first = registry
            .on_change("kg", &ChangeSet::relation("a"))
            .remove(0);
        assert!(registry
            .on_change("kg", &ChangeSet::relation("b"))
            .is_empty());
        let (push, follow_up) = complete(&mut registry, first).await;
        assert!(push.is_none(), "unchanged result pushes nothing");
        let (push, _) = complete(&mut registry, follow_up.unwrap()).await;
        assert!(matches!(push, Some(Push::SubscriptionDelta { seq: 1, .. })));
    }

    #[tokio::test]
    async fn test_registry_error_keeps_subscription() {
        let mut registry =
            registry_with(vec![Err("boom".to_string()), Ok(refresh(&[1], &[], "a"))]);
        let d = registry
            .on_change("kg", &ChangeSet::relation("a"))
            .remove(0);
        let (push, _) = complete(&mut registry, d).await;
        assert_eq!(
            push,
            Some(Push::SubscriptionError {
                subscription: "s".to_string(),
                message: "boom".to_string()
            })
        );
        assert_eq!(registry.len(), 1);
        let d = registry
            .on_change("kg", &ChangeSet::relation("a"))
            .remove(0);
        let (push, _) = complete(&mut registry, d).await;
        assert!(matches!(push, Some(Push::SubscriptionDelta { seq: 1, .. })));
    }

    #[tokio::test]
    async fn test_registry_completion_after_resubscribe_is_ignored() {
        let mut registry = registry_with(vec![Ok(refresh(&[1], &[], "a"))]);
        let stale = registry
            .on_change("kg", &ChangeSet::relation("a"))
            .remove(0);
        assert!(registry.remove("s"));
        registry
            .add("s", "kg", Box::new(Scripted(vec![])), deps_on("a"))
            .unwrap();
        let (push, follow_up) = complete(&mut registry, stale).await;
        assert!(push.is_none() && follow_up.is_none());
    }

    #[test]
    fn test_registry_rejects_duplicates_and_enforces_limit() {
        let mut registry = SubscriptionRegistry::new(2);
        let view = || Box::new(Scripted(vec![])) as Box<dyn StandingQuery>;
        registry.add("a", "kg", view(), deps_on("r")).unwrap();
        assert!(registry.add("a", "kg", view(), deps_on("r")).is_err());
        registry.add("b", "kg", view(), deps_on("r")).unwrap();
        let err = registry.add("c", "kg", view(), deps_on("r")).unwrap_err();
        assert!(err.contains("limit"), "{err}");
        assert_eq!(registry.clear(), 2);
        assert!(registry.is_empty());
    }

    #[test]
    fn test_registry_unknown_changes_dispatch_everything() {
        let mut registry = registry_with(vec![]);
        assert_eq!(registry.on_unknown_changes().len(), 1);
    }

    #[test]
    fn test_push_serializes_with_protocol_type_tags() {
        let delta = Push::SubscriptionDelta {
            subscription: "s".to_string(),
            knowledge_graph: "kg".to_string(),
            seq: 1,
            columns: vec!["x".to_string()],
            inserted: vec![vec![serde_json::json!(1)]],
            retracted: vec![],
        };
        assert_eq!(
            serde_json::to_value(&delta).unwrap(),
            serde_json::json!({"type": "subscription_delta", "subscription": "s",
                "knowledge_graph": "kg", "seq": 1, "columns": ["x"],
                "inserted": [[1]], "retracted": []})
        );
        let error = Push::SubscriptionError {
            subscription: "s".to_string(),
            message: "m".to_string(),
        };
        assert_eq!(
            serde_json::to_value(&error).unwrap(),
            serde_json::json!({"type": "subscription_error", "subscription": "s", "message": "m"})
        );
    }
}
