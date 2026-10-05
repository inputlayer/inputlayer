//! The subscribing agent: one `/ws` connection holding a scenario's business
//! questions as standing queries and maintaining each answer from the
//! snapshot plus pushed deltas, exactly as a reactive agent does.
//!
//! A reader task stamps every push the moment it is read and hands it to the
//! scenario driver over the agent's own channel; nothing is shared between
//! agents or with the writer.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::{bail, Result};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::client::{Client, Frame, Stamped};
use crate::fixtures::elapsed_us;

/// A deliberately broken delta path, to prove the checks catch one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Fault {
    /// The agent ignores retracted rows.
    DropRetractions,
    /// The agent ignores inserted rows.
    DropInserts,
}

/// A result set: full engine rows, keyed by their JSON spelling.
pub type RowSet = BTreeMap<String, Vec<Value>>;

pub fn row_set(rows: Vec<Vec<Value>>) -> RowSet {
    rows.into_iter()
        .map(|row| (Value::Array(row.clone()).to_string(), row))
        .collect()
}

/// What one subscription saw since the last [`Agent::take_activity`].
#[derive(Debug, Clone, Copy, Default)]
pub struct Activity {
    pub frames: usize,
    pub inserted: usize,
    pub retracted: usize,
    pub last: Option<Instant>,
}

/// One standing query and its maintained answer.
pub struct Subscription {
    pub id: String,
    pub state: RowSet,
    pub activity: Activity,
    /// Retractions of rows the agent did not hold, and duplicate inserts.
    pub anomalies: usize,
    pub error: Option<String>,
}

impl Subscription {
    pub fn new(id: &str, snapshot: Vec<Vec<Value>>) -> Self {
        Self {
            id: id.to_string(),
            state: row_set(snapshot),
            activity: Activity::default(),
            anomalies: 0,
            error: None,
        }
    }

    /// Apply one delta frame read at `at`: retractions, then insertions.
    pub fn apply(
        &mut self,
        at: Instant,
        inserted: Vec<Vec<Value>>,
        retracted: Vec<Vec<Value>>,
        fault: Option<Fault>,
    ) {
        let activity = &mut self.activity;
        activity.frames += 1;
        activity.inserted += inserted.len();
        activity.retracted += retracted.len();
        activity.last = Some(at);
        if fault != Some(Fault::DropRetractions) {
            for row in retracted {
                if self.state.remove(&Value::Array(row).to_string()).is_none() {
                    self.anomalies += 1;
                }
            }
        }
        if fault != Some(Fault::DropInserts) {
            for row in inserted {
                let key = Value::Array(row.clone()).to_string();
                if self.state.insert(key, row).is_some() {
                    self.anomalies += 1;
                }
            }
        }
    }
}

/// A connected agent whose subscriptions are being maintained.
pub struct Agent {
    pub subscriptions: BTreeMap<String, Subscription>,
    pushes: mpsc::UnboundedReceiver<Result<Stamped>>,
    reader: JoinHandle<()>,
    fault: Option<Fault>,
}

impl Agent {
    /// Subscribe `client` to every `(id, query)` and start maintaining them.
    /// Returns the agent and each subscription's snapshot latency (µs).
    pub async fn start(
        mut client: Client,
        queries: &[(String, String)],
        fault: Option<Fault>,
    ) -> Result<(Self, Vec<u64>)> {
        let mut subscriptions = BTreeMap::new();
        let mut latencies = Vec::new();
        for (id, query) in queries {
            let (start, answer) = client.query(&format!(".subscribe {id} {query}")).await?;
            if let Some(error) = answer.errors.first() {
                bail!("subscribe {id}: {error}");
            }
            if answer.truncated {
                bail!("subscribe {id}: truncated snapshot");
            }
            latencies.push(elapsed_us(start, answer.at));
            subscriptions.insert(id.clone(), Subscription::new(id, answer.rows));
        }
        let (tx, pushes) = mpsc::unbounded_channel();
        let reader = tokio::spawn(async move {
            loop {
                let push = client.next_push().await;
                let failed = push.is_err();
                if tx.send(push).is_err() || failed {
                    return;
                }
            }
        });
        Ok((
            Self {
                subscriptions,
                pushes,
                reader,
                fault,
            },
            latencies,
        ))
    }

    /// Apply every push that arrives before `deadline`; returns early once
    /// `done` holds. Fails only if the connection breaks.
    pub async fn drain_until(
        &mut self,
        deadline: tokio::time::Instant,
        done: impl Fn(&Self) -> bool,
    ) -> Result<()> {
        while !done(self) {
            match tokio::time::timeout_at(deadline, self.pushes.recv()).await {
                Err(_) => return Ok(()),
                Ok(None) => bail!("agent connection closed"),
                Ok(Some(push)) => self.apply(push?),
            }
        }
        Ok(())
    }

    /// Apply pushes until none arrives for `quiet` (or `deadline` passes).
    pub async fn drain_quiet(
        &mut self,
        quiet: std::time::Duration,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        loop {
            let until = (tokio::time::Instant::now() + quiet).min(deadline);
            match tokio::time::timeout_at(until, self.pushes.recv()).await {
                Err(_) => return Ok(()),
                Ok(None) => bail!("agent connection closed"),
                Ok(Some(push)) => self.apply(push?),
            }
        }
    }

    /// Reset and return per-subscription activity.
    pub fn take_activity(&mut self) -> BTreeMap<String, Activity> {
        self.subscriptions
            .iter_mut()
            .map(|(id, s)| (id.clone(), std::mem::take(&mut s.activity)))
            .collect()
    }

    fn apply(&mut self, Stamped { at, frame }: Stamped) {
        match frame {
            Frame::SubscriptionDelta {
                subscription,
                inserted,
                retracted,
                ..
            } => {
                if let Some(sub) = self.subscriptions.get_mut(&subscription) {
                    sub.apply(at, inserted, retracted, self.fault);
                }
            }
            Frame::SubscriptionError {
                subscription,
                message,
            } => {
                if let Some(sub) = self.subscriptions.get_mut(&subscription) {
                    sub.error.get_or_insert(message);
                    sub.activity.last = Some(at);
                }
            }
            _ => {}
        }
    }

    /// Ids whose maintained state differs from `truth`, or that errored or
    /// have no truth, apart from the `retired` questions, which no longer
    /// count.
    pub fn diverged(
        &self,
        truth: &BTreeMap<String, RowSet>,
        retired: &BTreeMap<String, String>,
    ) -> BTreeSet<String> {
        self.subscriptions
            .values()
            .filter(|s| !retired.contains_key(&s.id))
            .filter(|s| {
                s.error.is_some() || truth.get(&s.id).is_none_or(|t| keys_differ(&s.state, t))
            })
            .map(|s| s.id.clone())
            .collect()
    }

    /// Forget the subscription errors of every question in `evaluated`: a
    /// successful re-query since.
    pub fn clear_errors(&mut self, evaluated: &BTreeMap<String, RowSet>) {
        for sub in self.subscriptions.values_mut() {
            if evaluated.contains_key(&sub.id) {
                sub.error = None;
            }
        }
    }

    #[cfg(test)]
    pub(super) fn holding(subscriptions: Vec<Subscription>) -> Self {
        let (_tx, pushes) = mpsc::unbounded_channel();
        Self {
            subscriptions: subscriptions
                .into_iter()
                .map(|s| (s.id.clone(), s))
                .collect(),
            pushes,
            reader: tokio::spawn(async {}),
            fault: None,
        }
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

fn keys_differ(a: &RowSet, b: &RowSet) -> bool {
    a.len() != b.len() || a.keys().zip(b.keys()).any(|(x, y)| x != y)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows(v: Value) -> Vec<Vec<Value>> {
        serde_json::from_value(v).unwrap()
    }

    fn truth(id: &str, v: Value) -> BTreeMap<String, RowSet> {
        BTreeMap::from([(id.to_string(), row_set(rows(v)))])
    }

    #[tokio::test]
    async fn convergence_requires_truth_for_every_live_subscription() {
        let mut agent = Agent::holding(vec![
            Subscription::new("q0", rows(json!([[1]]))),
            Subscription::new("q1", vec![]),
        ]);
        let live = BTreeMap::new();
        let mut expected = truth("q0", json!([[1]]));
        assert_eq!(
            agent.diverged(&expected, &live),
            BTreeSet::from(["q1".into()])
        );
        assert_eq!(agent.diverged(&BTreeMap::new(), &live).len(), 2);
        expected.insert("q1".into(), row_set(vec![]));
        assert!(agent.diverged(&expected, &live).is_empty());
        agent.subscriptions.get_mut("q0").unwrap().state.clear();
        assert_eq!(
            agent.diverged(&expected, &live),
            BTreeSet::from(["q0".into()])
        );
        agent.subscriptions.get_mut("q1").unwrap().error = Some("result truncated".into());
        assert_eq!(agent.diverged(&expected, &live).len(), 2);
    }

    #[tokio::test]
    async fn a_retired_question_no_longer_counts_and_requery_clears_errors() {
        let mut agent = Agent::holding(vec![
            Subscription::new("q0", rows(json!([[1]]))),
            Subscription::new("q3", rows(json!([[7]]))),
        ]);
        agent.subscriptions.get_mut("q3").unwrap().error = Some("result truncated".into());
        let retired = BTreeMap::from([("q3".to_string(), "result truncated".to_string())]);
        assert!(agent
            .diverged(&truth("q0", json!([[1]])), &retired)
            .is_empty());

        agent.subscriptions.get_mut("q0").unwrap().error = Some("denied".into());
        let evaluated = truth("q0", json!([[1]]));
        assert_eq!(
            agent.diverged(&evaluated, &retired),
            BTreeSet::from(["q0".into()])
        );
        agent.clear_errors(&evaluated);
        assert!(agent.diverged(&evaluated, &retired).is_empty());
        assert!(agent.subscriptions["q3"].error.is_some());
    }

    #[test]
    fn deltas_maintain_the_answer() {
        let mut sub = Subscription::new("q0", rows(json!([["a", 1], ["b", 2]])));
        sub.apply(
            Instant::now(),
            rows(json!([["c", 3]])),
            rows(json!([["a", 1]])),
            None,
        );
        assert_eq!(sub.state, row_set(rows(json!([["b", 2], ["c", 3]]))));
        assert_eq!(
            (
                sub.activity.frames,
                sub.activity.inserted,
                sub.activity.retracted
            ),
            (1, 1, 1)
        );
        assert_eq!(sub.anomalies, 0);
    }

    #[test]
    fn a_dropped_retraction_leaves_a_stale_row() {
        let mut sub = Subscription::new("q0", rows(json!([["a", 1]])));
        sub.apply(
            Instant::now(),
            vec![],
            rows(json!([["a", 1]])),
            Some(Fault::DropRetractions),
        );
        assert_eq!(
            sub.state.len(),
            1,
            "the broken path must keep the stale row"
        );
        assert!(keys_differ(&sub.state, &truth("q0", json!([]))["q0"]));
    }

    #[test]
    fn impossible_deltas_are_counted() {
        let mut sub = Subscription::new("q0", rows(json!([["a", 1]])));
        sub.apply(
            Instant::now(),
            rows(json!([["a", 1]])),
            rows(json!([["z", 9]])),
            None,
        );
        assert_eq!(sub.anomalies, 2);
    }
}
