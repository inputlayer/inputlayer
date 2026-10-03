//! Test doubles for the subscription state machines.

use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_json::json;
use tokio::sync::mpsc;

use super::publication::{Doorbell, SubscriberId};
use super::{Dependencies, Refresh, ResultSet, Row, StandingQuery};
use crate::statement::parse_query;

/// One scripted refresh: the complete result (single-column integers) and
/// the relation it depends on, or an error.
pub(super) type Step = Result<(Vec<i64>, &'static str), String>;

/// A view that returns scripted results, diffing them like a real strategy.
/// Each refresh is at the next revision.
pub(super) struct Scripted {
    steps: VecDeque<Step>,
    current: Arc<ResultSet>,
    revision: u64,
}

impl Scripted {
    pub(super) fn boxed(steps: impl IntoIterator<Item = Step>) -> Box<dyn StandingQuery> {
        Box::new(Self {
            steps: steps.into_iter().collect(),
            current: Arc::default(),
            revision: 0,
        })
    }
}

impl StandingQuery for Scripted {
    fn refresh(&mut self) -> BoxFuture<'_, Result<Refresh, String>> {
        self.revision += 1;
        let step = self.steps.pop_front().expect("a scripted step per refresh");
        let refresh = step.map(|(values, relation)| {
            let next: Arc<ResultSet> = Arc::new(rows(&values).into_iter().collect());
            let refresh = Refresh {
                columns: vec!["x".to_string()],
                inserted: next.difference(&self.current),
                retracted: self.current.difference(&next),
                dependencies: deps_on(relation),
                revision: self.revision,
                result: Arc::clone(&next),
            };
            self.current = next;
            refresh
        });
        Box::pin(async move { refresh })
    }
}

/// Dependencies of `?relation(X)`.
pub(super) fn deps_on(relation: &str) -> Dependencies {
    Dependencies::for_query(&parse_query(&format!("{relation}(X)")).unwrap(), &[])
}

/// Single-column rows.
pub(super) fn rows(values: &[i64]) -> Vec<Row> {
    values.iter().map(|n| vec![json!(n)]).collect()
}

/// A subscriber's doorbell and the connection mailbox it rings.
pub(super) fn doorbell(id: SubscriberId) -> (Arc<Doorbell>, mpsc::UnboundedReceiver<SubscriberId>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (Doorbell::new(id, tx), rx)
}
