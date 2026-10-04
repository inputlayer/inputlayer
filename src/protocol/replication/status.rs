//! What replication is doing, for `/v1/replication/status`.

use crate::config::ReplicationRole;
use parking_lot::Mutex;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Instant;

/// Live replication state of this server.
#[derive(Debug)]
pub struct ReplicationStatus {
    role: ReplicationRole,
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    next_follower: u64,
    followers: BTreeMap<u64, FollowerEntry>,
    follower: FollowerSide,
}

#[derive(Debug)]
struct FollowerEntry {
    name: String,
    addr: String,
    since: Instant,
    sent_lsn: u64,
    acked_lsn: u64,
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
    pub followers: Vec<ConnectedFollower>,
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
    pub resyncs: u64,
    pub reconnects: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

fn millis(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

impl ReplicationStatus {
    /// The state of a server in `role`.
    pub fn new(role: ReplicationRole) -> Self {
        Self {
            role,
            inner: Mutex::default(),
        }
    }

    /// This server's role.
    pub fn role(&self) -> ReplicationRole {
        self.role
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

    pub(super) fn follower_acked(&self, id: u64, lsn: u64) {
        if let Some(entry) = self.inner.lock().followers.get_mut(&id) {
            entry.acked_lsn = entry.acked_lsn.max(lsn);
        }
    }

    pub(super) fn follower_gone(&self, id: u64) {
        self.inner.lock().followers.remove(&id);
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
        let mut inner = self.inner.lock();
        let side = &mut inner.follower;
        if side.stream_id != stream_id {
            side.primary_head = 0;
        }
        side.stream_id = stream_id;
        side.applied_lsn = lsn;
        side.primary_head = side.primary_head.max(lsn);
        side.primary_revision = primary_revision;
        side.last_contact = Some(Instant::now());
    }

    pub(super) fn failed(&self, error: &str) {
        self.inner.lock().follower.last_error = Some(error.to_string());
    }

    /// The report, with the primary's stream id and head when this is a
    /// primary.
    pub fn report(&self, primary_log: Option<(u64, u64)>) -> StatusReport {
        let inner = self.inner.lock();
        let primary = primary_log.map(|(stream_id, head)| PrimaryReport {
            stream_id: format!("{stream_id:016x}"),
            head_lsn: head,
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
                    resynced: f.resynced,
                })
                .collect(),
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
