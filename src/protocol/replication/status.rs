//! What replication is doing, for `/v1/replication/status`.

use super::sync::{SyncReport, SyncShipping, SyncState};
use crate::config::{ReplicationConfig, ReplicationRole};
use crate::replication::ReplicationLog;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

/// Live replication state of this server.
#[derive(Debug)]
pub struct ReplicationStatus {
    role: ReplicationRole,
    sync: SyncShipping,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    next_follower: u64,
    followers: BTreeMap<u64, FollowerEntry>,
    resync_pin_overflows: u64,
    follower: FollowerSide,
}

#[derive(Debug)]
struct FollowerEntry {
    name: String,
    addr: String,
    since: Instant,
    sent_lsn: u64,
    acked_lsn: u64,
    /// The last LSN sent at a moment the follower had been sent the whole
    /// log (0 before that happened).
    drained_lsn: u64,
    resynced: bool,
}

#[derive(Debug, Default)]
struct FollowerSide {
    state: FollowerState,
    stream_id: u64,
    applied_lsn: u64,
    primary_head: u64,
    primary_revision: u64,
    last_contact: Option<Instant>,
    resyncs: u64,
    resync_failures: u64,
    reconnects: u64,
    last_error: Option<String>,
}

/// Where a follower's stream is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FollowerState {
    /// Not connected; (re)connecting.
    #[default]
    Connecting,
    /// Bringing its state to a checkpoint of the primary.
    Resyncing,
    /// Applying the primary's stream as it arrives.
    Streaming,
}

/// The status report.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct StatusReport {
    pub role: ReplicationRole,
    /// Primary: its stream incarnation and newest LSN.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<PrimaryReport>,
    /// Follower: its position and connection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub follower: Option<FollowerReport>,
}

/// A primary's side of the report.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PrimaryReport {
    pub stream_id: String,
    pub head_lsn: u64,
    /// Events no follower has confirmed applying: what losing this server
    /// now would lose.
    pub lag_events: u64,
    /// How long ago the oldest of those events was committed (0 when none).
    pub lag_ms: u64,
    pub sync: SyncReport,
    pub followers: Vec<ConnectedFollower>,
    /// Resyncs abandoned because the changes made while their checkpoint
    /// was sent outgrew the log's pin cap.
    pub resync_pin_overflows: u64,
}

/// One follower connected to this primary.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConnectedFollower {
    pub name: String,
    pub addr: String,
    pub connected_ms: u64,
    pub sent_lsn: u64,
    pub acked_lsn: u64,
    /// Events the follower has not acknowledged applying.
    pub lag_events: u64,
    /// How long ago the oldest of those events was committed (0 when none).
    pub lag_ms: u64,
    /// Whether this connection started with a checkpoint.
    pub resynced: bool,
}

/// A follower's side of the report.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct FollowerReport {
    pub state: FollowerState,
    pub stream_id: String,
    pub applied_lsn: u64,
    pub primary_head_lsn: u64,
    /// Events the primary has that this follower has not applied.
    pub lag_events: u64,
    /// The newest primary revision this follower's state includes.
    pub primary_revision: u64,
    /// Milliseconds since the primary was last heard from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_contact_ms: Option<u64>,
    /// Resyncs begun.
    pub resyncs: u64,
    /// Resyncs that ended before the follower caught up with the primary's head.
    pub resync_failures: u64,
    pub reconnects: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn millis(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// How long ago the oldest event after `acked` was committed, when `log`
/// holds events after it.
fn lag_ms(log: &ReplicationLog, head: u64, acked: u64) -> u64 {
    if acked >= head {
        return 0;
    }
    log.appended_at(acked + 1).map_or(0, millis)
}

/// One Prometheus metric: its help and type lines, then its samples.
fn metric(out: &mut String, name: &str, kind: &str, help: &str, samples: &[(&str, f64)]) {
    use std::fmt::Write;
    let _ = writeln!(out, "# HELP inputlayer_replication_{name} {help}");
    let _ = writeln!(out, "# TYPE inputlayer_replication_{name} {kind}");
    for (labels, value) in samples {
        let _ = writeln!(out, "inputlayer_replication_{name}{labels} {value}");
    }
}

/// `value` as a sample.
#[allow(clippy::cast_precision_loss)]
fn sample(value: u64) -> [(&'static str, f64); 1] {
    [("", value as f64)]
}

/// `ms` milliseconds as a sample in seconds.
#[allow(clippy::cast_precision_loss)]
fn seconds(ms: u64) -> [(&'static str, f64); 1] {
    [("", ms as f64 / 1000.0)]
}

impl StatusReport {
    /// The report in Prometheus text exposition format.
    pub fn format_prometheus(&self) -> String {
        let mut out = String::new();
        let out = &mut out;
        if let Some(primary) = &self.primary {
            let sync = &primary.sync;
            metric(
                out,
                "head_lsn",
                "gauge",
                "Newest replication event on this primary.",
                &sample(primary.head_lsn),
            );
            metric(
                out,
                "confirmed_lsn",
                "gauge",
                "Newest event a follower confirmed applying durably.",
                &sample(sync.confirmed_lsn),
            );
            metric(
                out,
                "lag_events",
                "gauge",
                "Events committed here that no follower has confirmed.",
                &sample(primary.lag_events),
            );
            metric(
                out,
                "lag_seconds",
                "gauge",
                "Age of the oldest event no follower has confirmed.",
                &seconds(primary.lag_ms),
            );
            metric(
                out,
                "followers",
                "gauge",
                "Followers connected to this primary.",
                &sample(primary.followers.len() as u64),
            );
            let labels: Vec<String> = SyncState::ALL
                .iter()
                .map(|state| format!("{{state=\"{}\"}}", state.name()))
                .collect();
            let states: Vec<(&str, f64)> = SyncState::ALL
                .iter()
                .zip(&labels)
                .map(|(state, label)| (label.as_str(), f64::from(u8::from(*state == sync.state))))
                .collect();
            metric(
                out,
                "sync_state",
                "gauge",
                "Shipping state: async, sync, stalled (block, timing out) or degraded.",
                &states,
            );
            metric(
                out,
                "sync_waits_total",
                "counter",
                "Replies that waited for a follower to confirm their commit.",
                &sample(sync.waits),
            );
            metric(
                out,
                "sync_wait_seconds_total",
                "counter",
                "Total time replies waited for a follower.",
                &seconds(sync.wait_ms_total),
            );
            metric(
                out,
                "sync_unconfirmed_total",
                "counter",
                "Replies failed as not confirmed on a replica.",
                &sample(sync.unconfirmed),
            );
            metric(
                out,
                "sync_degrades_total",
                "counter",
                "Falls back from synchronous to asynchronous shipping.",
                &sample(sync.degrades),
            );
            metric(
                out,
                "sync_rearms_total",
                "counter",
                "Returns to synchronous shipping after a fall back.",
                &sample(sync.rearms),
            );
        }
        if let Some(follower) = &self.follower {
            let streaming = u64::from(follower.state == FollowerState::Streaming);
            metric(
                out,
                "follower_streaming",
                "gauge",
                "1 while this follower applies its primary's stream.",
                &sample(streaming),
            );
            metric(
                out,
                "follower_applied_lsn",
                "gauge",
                "Newest primary event this follower applied.",
                &sample(follower.applied_lsn),
            );
            metric(
                out,
                "follower_lag_events",
                "gauge",
                "Primary events this follower has not applied.",
                &sample(follower.lag_events),
            );
            if let Some(ms) = follower.last_contact_ms {
                metric(
                    out,
                    "follower_last_contact_seconds",
                    "gauge",
                    "Time since the primary was last heard from.",
                    &seconds(ms),
                );
            }
            metric(
                out,
                "follower_resyncs_total",
                "counter",
                "Resyncs from a checkpoint begun.",
                &sample(follower.resyncs),
            );
            metric(
                out,
                "follower_reconnects_total",
                "counter",
                "Reconnections to the primary.",
                &sample(follower.reconnects),
            );
        }
        std::mem::take(out)
    }
}

impl ReplicationStatus {
    /// The state of a server configured with `config`; a primary ships
    /// `log`.
    pub fn new(config: &ReplicationConfig, log: Option<Arc<ReplicationLog>>) -> Self {
        Self {
            role: config.role,
            sync: SyncShipping::new(config, log),
            inner: Mutex::default(),
        }
    }

    /// This server's role.
    pub fn role(&self) -> ReplicationRole {
        self.role
    }

    /// Synchronous shipping (a primary's replies waiting for followers).
    pub fn sync(&self) -> &SyncShipping {
        &self.sync
    }

    // Primary side

    /// A follower connected; returns its id for later updates.
    pub(super) fn follower_connected(&self, name: &str, addr: &str, resynced: bool) -> u64 {
        let mut inner = self.inner.lock();
        inner.next_follower += 1;
        let id = inner.next_follower;
        inner.followers.insert(
            id,
            FollowerEntry {
                name: name.to_string(),
                addr: addr.to_string(),
                since: Instant::now(),
                sent_lsn: 0,
                acked_lsn: 0,
                drained_lsn: 0,
                resynced,
            },
        );
        id
    }

    pub(super) fn follower_sent(&self, id: u64, lsn: u64) {
        if let Some(entry) = self.inner.lock().followers.get_mut(&id) {
            entry.sent_lsn = lsn;
        }
    }

    /// Follower `id` applied every event up to `lsn` durably.
    pub(super) fn follower_acked(&self, id: u64, lsn: u64) {
        let caught_up = {
            let mut inner = self.inner.lock();
            let Some(entry) = inner.followers.get_mut(&id) else {
                return;
            };
            entry.acked_lsn = entry.acked_lsn.max(lsn);
            entry.drained_lsn > 0 && entry.acked_lsn >= entry.drained_lsn
        };
        self.sync.acked(lsn, caught_up);
    }

    /// Follower `id` has been sent every event up to `lsn`, the head.
    pub(super) fn follower_drained(&self, id: u64, lsn: u64) {
        let acked = {
            let mut inner = self.inner.lock();
            let Some(entry) = inner.followers.get_mut(&id) else {
                return;
            };
            entry.drained_lsn = lsn;
            entry.acked_lsn
        };
        if acked >= lsn && lsn > 0 {
            self.sync.acked(acked, true);
        }
    }

    pub(super) fn follower_gone(&self, id: u64) {
        self.inner.lock().followers.remove(&id);
    }

    pub(super) fn resync_pin_overflowed(&self) {
        self.inner.lock().resync_pin_overflows += 1;
    }

    // Follower side

    pub(super) fn set_state(&self, state: FollowerState) {
        let mut inner = self.inner.lock();
        if state == FollowerState::Resyncing {
            inner.follower.resyncs += 1;
        }
        if state == FollowerState::Connecting && inner.follower.state != state {
            inner.follower.reconnects += 1;
        }
        inner.follower.state = state;
    }

    pub(super) fn contact(&self, primary_head: u64) {
        let mut inner = self.inner.lock();
        inner.follower.last_contact = Some(Instant::now());
        inner.follower.primary_head = inner.follower.primary_head.max(primary_head);
    }

    pub(super) fn applied(&self, stream_id: u64, lsn: u64, primary_revision: u64) {
        self.resumed(stream_id, lsn, primary_revision);
        self.inner.lock().follower.last_contact = Some(Instant::now());
    }

    /// Record a position without claiming contact with the primary.
    pub(super) fn resumed(&self, stream_id: u64, lsn: u64, primary_revision: u64) {
        let mut inner = self.inner.lock();
        let side = &mut inner.follower;
        if side.stream_id != stream_id {
            side.primary_head = 0;
        }
        side.stream_id = stream_id;
        side.applied_lsn = lsn;
        side.primary_head = side.primary_head.max(lsn);
        side.primary_revision = primary_revision;
    }

    pub(super) fn resync_failed(&self) {
        self.inner.lock().follower.resync_failures += 1;
    }

    pub(super) fn failed(&self, error: &str) {
        self.inner.lock().follower.last_error = Some(error.to_string());
    }

    /// The report, with the primary's stream id, head and lag when this is
    /// a primary (its replication `log`).
    pub fn report(&self, log: Option<&ReplicationLog>) -> StatusReport {
        let sync = self.sync.report();
        let inner = self.inner.lock();
        let primary = log
            .map(|log| (log, log.head()))
            .map(|(log, head)| PrimaryReport {
                stream_id: format!("{:016x}", log.stream_id()),
                head_lsn: head,
                lag_events: head.saturating_sub(sync.confirmed_lsn),
                lag_ms: lag_ms(log, head, sync.confirmed_lsn),
                sync: sync.clone(),
                followers: inner
                    .followers
                    .values()
                    .map(|f| ConnectedFollower {
                        name: f.name.clone(),
                        addr: f.addr.clone(),
                        connected_ms: millis(f.since),
                        sent_lsn: f.sent_lsn,
                        acked_lsn: f.acked_lsn,
                        lag_events: head.saturating_sub(f.acked_lsn),
                        lag_ms: lag_ms(log, head, f.acked_lsn),
                        resynced: f.resynced,
                    })
                    .collect(),
                resync_pin_overflows: inner.resync_pin_overflows,
            });
        let follower = (self.role == ReplicationRole::Follower).then(|| {
            let side = &inner.follower;
            FollowerReport {
                state: side.state,
                stream_id: format!("{:016x}", side.stream_id),
                applied_lsn: side.applied_lsn,
                primary_head_lsn: side.primary_head,
                lag_events: side.primary_head.saturating_sub(side.applied_lsn),
                primary_revision: side.primary_revision,
                last_contact_ms: side.last_contact.map(millis),
                resyncs: side.resyncs,
                resync_failures: side.resync_failures,
                reconnects: side.reconnects,
                last_error: side.last_error.clone(),
            }
        });
        StatusReport {
            role: self.role,
            primary,
            follower,
        }
    }
}
