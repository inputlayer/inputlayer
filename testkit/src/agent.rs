//! A subscribing agent: registers standing queries and maintains their result
//! sets purely from pushed deltas, never re-querying.
//!
//! Each delta is checked as it is applied: sequence numbers are contiguous,
//! revisions increase, retracted rows were present and inserted rows were
//! absent. Comparing the
//! maintained [`View`] with a fresh query on *another* connection
//! ([`View::assert_matches`]) proves convergence.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::client::{Frame, QueryResult, WsClient, FRAME_TIMEOUT};
use crate::contract::{Checked, Violation};
use crate::engine::Engine;
use crate::streamed::StreamedDelta;

/// A result row in canonical form (its compact JSON), for set comparisons.
pub type RowKey = String;

/// Canonical keys of `rows`.
pub fn row_keys<'a>(rows: impl IntoIterator<Item = &'a Value>) -> BTreeSet<RowKey> {
    rows.into_iter().map(Value::to_string).collect()
}

/// A subscription's result set as maintained by the agent.
#[derive(Debug, Clone)]
pub struct View {
    pub id: String,
    pub columns: Vec<String>,
    pub rows: BTreeSet<RowKey>,
    /// Sequence number of the last applied delta (0 = snapshot only).
    pub seq: u64,
    /// Knowledge graph revision the maintained rows are the answer at.
    pub revision: u64,
}

impl View {
    /// Fail unless this view holds exactly `expected`, typically the rows of a
    /// fresh full query on another connection.
    pub fn assert_matches(&self, expected: &[Value]) -> Checked<()> {
        let fresh = row_keys(expected);
        if self.rows == fresh {
            return Ok(());
        }
        Err(Violation::Diverged {
            subscription: self.id.clone(),
            missing: fresh.difference(&self.rows).cloned().collect(),
            unexpected: self.rows.difference(&fresh).cloned().collect(),
        })
    }

    fn apply(&mut self, delta: &Delta) -> Checked<()> {
        if delta.seq != self.seq + 1 {
            return Err(Violation::SeqGap {
                subscription: self.id.clone(),
                expected: self.seq + 1,
                got: delta.seq,
            });
        }
        if delta.revision <= self.revision {
            return Err(Violation::StaleRevision {
                subscription: self.id.clone(),
                previous: self.revision,
                got: delta.revision,
            });
        }
        for row in &delta.retracted {
            if !self.rows.remove(row) {
                return Err(self.inconsistent(format!("retracted absent row {row}")));
            }
        }
        for row in &delta.inserted {
            if !self.rows.insert(row.clone()) {
                return Err(self.inconsistent(format!("inserted present row {row}")));
            }
        }
        self.seq = delta.seq;
        self.revision = delta.revision;
        Ok(())
    }

    fn inconsistent(&self, detail: String) -> Violation {
        Violation::Inconsistent {
            subscription: self.id.clone(),
            detail,
        }
    }
}

/// One applied `subscription_delta`.
#[derive(Debug, Clone)]
pub struct Delta {
    pub subscription: String,
    pub seq: u64,
    pub revision: u64,
    pub inserted: BTreeSet<RowKey>,
    pub retracted: BTreeSet<RowKey>,
    /// When the frame arrived at the agent.
    pub at: Instant,
}

impl Delta {
    fn from_frame(frame: &Frame) -> Checked<Self> {
        let rows = |field: &str| {
            frame.value[field].as_array().map(row_keys).ok_or_else(|| {
                Violation::Transport(format!("delta without {field}: {}", frame.value))
            })
        };
        Ok(Self {
            subscription: subscription_of(frame),
            seq: frame.value["seq"].as_u64().ok_or_else(|| {
                Violation::Transport(format!("delta without seq: {}", frame.value))
            })?,
            revision: frame.value["revision"].as_u64().ok_or_else(|| {
                Violation::Transport(format!("delta without revision: {}", frame.value))
            })?,
            inserted: rows("inserted")?,
            retracted: rows("retracted")?,
            at: frame.at,
        })
    }

    /// Fail unless this delta inserts exactly `inserted` and retracts exactly `retracted`.
    pub fn assert_rows(&self, inserted: &[Value], retracted: &[Value]) -> Checked<()> {
        let (want_in, want_out) = (row_keys(inserted), row_keys(retracted));
        if self.inserted == want_in && self.retracted == want_out {
            return Ok(());
        }
        Err(Violation::WrongDelta {
            subscription: self.subscription.clone(),
            detail: format!(
                "seq {}: inserted {:?} retracted {:?}, expected inserted {want_in:?} \
                 retracted {want_out:?}",
                self.seq, self.inserted, self.retracted
            ),
        })
    }
}

fn subscription_of(frame: &Frame) -> String {
    frame.value["subscription"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// A subscribing connection: it listens for deltas and may run requests of
/// its own meanwhile.
pub struct Agent {
    client: WsClient,
    views: BTreeMap<String, View>,
    /// Subscription pushes not yet consumed, per subscription.
    pending: BTreeMap<String, VecDeque<Frame>>,
    /// Change notifications (`persistent_update`, `rule_change`, ...) in arrival order.
    notices: Vec<Frame>,
}

impl Agent {
    /// Connect an agent to `knowledge_graph`.
    pub async fn connect(engine: &Engine, knowledge_graph: &str) -> Checked<Self> {
        Ok(Self::over(
            WsClient::connect(engine, knowledge_graph).await?,
        ))
    }

    /// An agent over an existing connection.
    pub fn over(client: WsClient) -> Self {
        Self {
            client,
            views: BTreeMap::new(),
            pending: BTreeMap::new(),
            notices: Vec::new(),
        }
    }

    /// Register `query` as `id`; the snapshot must be complete.
    pub async fn subscribe(&mut self, id: &str, query: &str) -> Checked<&View> {
        let snapshot = self
            .client
            .execute(&format!(".subscribe {id} {query}"))
            .await?
            .complete()?;
        let revision = snapshot.subscribed_revision.ok_or_else(|| {
            Violation::Transport(format!("'.subscribe {id}' reply names no revision"))
        })?;
        let view = View {
            id: id.to_string(),
            columns: snapshot.columns,
            rows: row_keys(&snapshot.rows),
            seq: 0,
            revision,
        };
        self.views.insert(id.to_string(), view);
        Ok(&self.views[id])
    }

    /// The maintained result of subscription `id`.
    pub fn view(&self, id: &str) -> &View {
        &self.views[id]
    }

    /// Change notifications received so far.
    pub fn notices(&self) -> &[Frame] {
        &self.notices
    }

    /// The connection underneath, e.g. for its connection notices or epoch.
    pub fn client(&self) -> &WsClient {
        &self.client
    }

    /// The connection underneath, for requests of the agent's own.
    pub fn client_mut(&mut self) -> &mut WsClient {
        &mut self.client
    }

    /// Forget the change notifications received so far, e.g. to bound memory
    /// while writers commit for a long time.
    pub fn clear_notices(&mut self) {
        self.notices.clear();
    }

    /// The next delta of any subscription if its first frame arrives within
    /// `within`, applied as [`Self::next_delta`] applies it; `None` otherwise.
    pub async fn next_any_delta(&mut self, within: Duration) -> Checked<Option<Delta>> {
        let waiting = self
            .pending
            .iter()
            .find(|(_, frames)| !frames.is_empty())
            .map(|(id, _)| id.clone());
        if let Some(id) = waiting {
            return self.next_delta(&id).await.map(Some);
        }
        let deadline = tokio::time::Instant::now() + within;
        let id = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Some(frame) = self.client.poll_push(remaining).await? else {
                return Ok(None);
            };
            if frame.kind().starts_with("subscription_") {
                let id = subscription_of(&frame);
                self.pending.entry(id.clone()).or_default().push_back(frame);
                break id;
            }
            self.route(frame);
        };
        self.next_delta(&id).await.map(Some)
    }

    /// Wait for the next delta of `id`, whole, and apply it to its view. A
    /// streamed delta applies only once its end frame confirms it complete.
    pub async fn next_delta(&mut self, id: &str) -> Checked<Delta> {
        let frame = self.next_frame_of(id).await?;
        if frame.kind() != "subscription_delta_start" {
            return self.apply(id, &frame);
        }
        let mut stream = StreamedDelta::start(&frame.value)?;
        loop {
            let frame = self.next_frame_of(id).await?;
            if frame.kind() == "subscription_reset" {
                return self.apply(id, &frame);
            }
            if let Some(delta) = stream.next(&frame.value, frame.at)? {
                self.apply_delta(id, &frame, &delta)?;
                return Ok(delta);
            }
        }
    }

    async fn next_frame_of(&mut self, id: &str) -> Checked<Frame> {
        let frame = self.next_push_for(id, FRAME_TIMEOUT).await?;
        frame.ok_or_else(|| Violation::Timeout(format!("a delta for '{id}'")))
    }

    /// Apply deltas of `id` until its view holds exactly `expected` (coalescing
    /// may merge several commits into one delta).
    pub async fn converge(&mut self, id: &str, expected: &[Value]) -> Checked<()> {
        while self.views[id].assert_matches(expected).is_err() {
            self.next_delta(id).await?;
        }
        Ok(())
    }

    /// Fail if subscription `id` receives any push within `within`.
    pub async fn expect_quiet(&mut self, id: &str, within: Duration) -> Checked<()> {
        match self.next_push_for(id, within).await? {
            None => Ok(()),
            Some(frame) => Err(Violation::UnexpectedPush(frame.value.to_string())),
        }
    }

    /// Wait until a change notification arrives; returns how many arrived so far.
    pub async fn wait_notices(&mut self, at_least: usize) -> Checked<usize> {
        while self.notices.len() < at_least {
            let frame = self.client.next_push(FRAME_TIMEOUT).await?;
            self.route(frame);
        }
        Ok(self.notices.len())
    }

    /// Send a request of the agent's own without waiting; deltas keep
    /// arriving while it runs. Returns its request id; read its reply with
    /// [`Self::result`].
    pub async fn send_execute(&mut self, program: &str) -> Checked<String> {
        self.client.send_execute(program).await
    }

    /// Cancel the agent's unanswered request `target`; read the outcome with
    /// [`Self::cancel_ack`] after the target's reply.
    pub async fn send_cancel(&mut self, target: &str) -> Checked<()> {
        self.client.send_cancel(target).await
    }

    /// The outcome of the agent's oldest outstanding `cancel`.
    pub async fn cancel_ack(&mut self) -> Checked<String> {
        self.client.cancel_ack().await
    }

    /// The reply to the agent's oldest outstanding request.
    pub async fn result(&mut self) -> Checked<QueryResult> {
        self.client.result().await
    }

    /// Close the connection.
    pub async fn disconnect(self) {
        self.client.close().await;
    }

    fn apply(&mut self, id: &str, frame: &Frame) -> Checked<Delta> {
        let message = || {
            frame.value["message"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        };
        match frame.kind() {
            "subscription_delta" => {
                let delta = Delta::from_frame(frame)?;
                self.apply_delta(id, frame, &delta)?;
                Ok(delta)
            }
            "subscription_error" => Err(Violation::SubscriptionError {
                subscription: id.to_string(),
                message: message(),
            }),
            // The engine dropped the subscription: its rows are no longer
            // maintained, so the view goes too.
            "subscription_reset" => {
                self.views.remove(id);
                Err(Violation::SubscriptionReset {
                    subscription: id.to_string(),
                    message: message(),
                })
            }
            other => Err(Violation::BrokenStream {
                subscription: id.to_string(),
                detail: format!("{other} outside a streamed delta: {}", frame.value),
            }),
        }
    }

    fn apply_delta(&mut self, id: &str, frame: &Frame, delta: &Delta) -> Checked<()> {
        let view = self
            .views
            .get_mut(id)
            .ok_or_else(|| Violation::UnexpectedPush(frame.value.to_string()))?;
        view.apply(delta)
    }

    /// Next subscription push for `id` within `within`, or `None`.
    async fn next_push_for(&mut self, id: &str, within: Duration) -> Checked<Option<Frame>> {
        if let Some(frame) = self.pending.get_mut(id).and_then(VecDeque::pop_front) {
            return Ok(Some(frame));
        }
        let deadline = tokio::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let Some(frame) = self.client.poll_push(remaining).await? else {
                return Ok(None);
            };
            if frame.kind().starts_with("subscription_") && subscription_of(&frame) == id {
                return Ok(Some(frame));
            }
            self.route(frame);
        }
    }

    fn route(&mut self, frame: Frame) {
        if frame.kind().starts_with("subscription_") {
            self.pending
                .entry(subscription_of(&frame))
                .or_default()
                .push_back(frame);
        } else {
            self.notices.push(frame);
        }
    }
}
