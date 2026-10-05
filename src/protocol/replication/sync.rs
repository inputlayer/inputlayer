//! Synchronous shipping: a primary's commit is acknowledged only once a
//! follower has applied it durably.
//!
//! Followers ack the newest LSN they applied after their WAL fsync. The
//! highest ack from any follower is the *confirmed* LSN: every event up to it
//! survives the loss of the primary. In `sync` mode a request that appended
//! events waits until the confirmed LSN reaches its newest one (see
//! [`writes`](crate::replication::writes)), for at most `sync_timeout_ms`.
//! Then `on_follower_loss` decides:
//!
//! - `block`: the request fails as not confirmed on a replica. Its commit
//!   stands on the primary; it may be lost with the primary.
//! - `degrade`: the primary falls back to asynchronous shipping with an
//!   alert. It returns to synchronous once a follower has caught up: it acked
//!   everything it was sent at a moment it had been sent the whole log, and
//!   at least everything committed when the primary degraded.

use crate::config::{FollowerLoss, ReplicationConfig, ReplicationMode};
use crate::replication::ReplicationLog;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{info, warn};

/// Whether a request's events are on a follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmation {
    /// A follower applied them, or the primary does not wait for followers
    /// (asynchronous, or degraded to it).
    Confirmed,
    /// No follower confirmed them within the timeout (`block`).
    Unconfirmed {
        /// How long the request waited.
        waited: Duration,
    },
}

/// Where synchronous shipping is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncState {
    /// Asynchronous shipping: replies never wait for followers.
    Async,
    /// Replies wait for a follower.
    Sync,
    /// `block`: the last wait timed out and no follower has confirmed
    /// anything since; replies still wait, then fail.
    Stalled,
    /// `degrade`: fell back to asynchronous until a follower catches up.
    Degraded,
}

impl SyncState {
    /// Every state, for metrics that report each one.
    pub const ALL: [Self; 4] = [Self::Async, Self::Sync, Self::Stalled, Self::Degraded];

    /// The state's name on the wire.
    pub fn name(self) -> &'static str {
        match self {
            Self::Async => "async",
            Self::Sync => "sync",
            Self::Stalled => "stalled",
            Self::Degraded => "degraded",
        }
    }
}

/// What followers have confirmed, watched by waiting requests.
#[derive(Debug, Clone, Copy, Default)]
struct Progress {
    /// Every event up to here is durable on a follower.
    confirmed: u64,
    /// `degrade` fell back to asynchronous shipping.
    degraded: bool,
    /// The primary's head when it degraded: a follower must have applied
    /// at least this much to re-arm.
    degrade_mark: u64,
}

/// Synchronous shipping state of a primary (inert elsewhere).
#[derive(Debug)]
pub struct SyncShipping {
    /// The primary's log; `None` elsewhere.
    log: Option<Arc<ReplicationLog>>,
    /// `sync` mode on a primary.
    enabled: bool,
    timeout: Duration,
    on_loss: FollowerLoss,
    progress: watch::Sender<Progress>,
    stalled: AtomicBool,
    waits: AtomicU64,
    wait_micros: AtomicU64,
    unconfirmed: AtomicU64,
    degrades: AtomicU64,
    rearms: AtomicU64,
}

/// Synchronous shipping in a status report.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SyncReport {
    pub mode: ReplicationMode,
    pub state: SyncState,
    pub on_follower_loss: FollowerLoss,
    pub sync_timeout_ms: u64,
    /// Every event up to this LSN is durable on a follower.
    pub confirmed_lsn: u64,
    /// Replies that waited for a follower.
    pub waits: u64,
    /// Total time replies waited, in milliseconds.
    pub wait_ms_total: u64,
    /// Replies that failed as not confirmed on a replica (`block`).
    pub unconfirmed: u64,
    /// Falls back to asynchronous shipping (`degrade`).
    pub degrades: u64,
    /// Returns to synchronous shipping after a fall back.
    pub rearms: u64,
}

impl SyncShipping {
    /// Synchronous shipping as `config` sets it, for a primary shipping
    /// `log` (`None` on any other server).
    pub fn new(config: &ReplicationConfig, log: Option<Arc<ReplicationLog>>) -> Self {
        Self {
            enabled: log.is_some() && config.mode == ReplicationMode::Sync,
            log,
            timeout: Duration::from_millis(config.sync_timeout_ms),
            on_loss: config.on_follower_loss,
            progress: watch::channel(Progress::default()).0,
            stalled: AtomicBool::new(false),
            waits: AtomicU64::new(0),
            wait_micros: AtomicU64::new(0),
            unconfirmed: AtomicU64::new(0),
            degrades: AtomicU64::new(0),
            rearms: AtomicU64::new(0),
        }
    }

    /// Whether replies wait for followers (`sync` mode on a primary).
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// The newest LSN a follower has confirmed.
    pub fn confirmed(&self) -> u64 {
        self.progress.borrow().confirmed
    }

    /// The current state.
    pub fn state(&self) -> SyncState {
        if !self.enabled {
            SyncState::Async
        } else if self.progress.borrow().degraded {
            SyncState::Degraded
        } else if self.stalled.load(Ordering::Acquire) {
            SyncState::Stalled
        } else {
            SyncState::Sync
        }
    }

    /// Wait until a follower has applied event `lsn`.
    pub async fn confirm(&self, lsn: u64) -> Confirmation {
        if !self.enabled || lsn == 0 {
            return Confirmation::Confirmed;
        }
        let mut progress = self.progress.subscribe();
        if progress.borrow().degraded {
            return Confirmation::Confirmed;
        }
        let started = Instant::now();
        let reached = tokio::time::timeout(
            self.timeout,
            progress.wait_for(|p| p.confirmed >= lsn || p.degraded),
        )
        .await;
        let waited = started.elapsed();
        self.waits.fetch_add(1, Ordering::Relaxed);
        self.wait_micros.fetch_add(
            u64::try_from(waited.as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        // The sender lives in `self`, so the wait only ends by a match or the
        // timeout.
        if matches!(reached, Ok(Ok(_))) {
            return Confirmation::Confirmed;
        }
        match self.on_loss {
            FollowerLoss::Block => {
                self.unconfirmed.fetch_add(1, Ordering::Relaxed);
                if !self.stalled.swap(true, Ordering::AcqRel) {
                    warn!(
                        lsn,
                        confirmed_lsn = self.confirmed(),
                        timeout_ms = self.timeout_ms(),
                        "replication_sync_stalled: no follower confirmed a commit in time; \
                         writes fail as not confirmed on a replica until one does"
                    );
                }
                Confirmation::Unconfirmed { waited }
            }
            FollowerLoss::Degrade => {
                self.degrade(self.log.as_ref().map_or(lsn, |log| log.head()));
                Confirmation::Confirmed
            }
        }
    }

    /// Fall back to asynchronous shipping (once), remembering `head`.
    fn degrade(&self, head: u64) {
        let degraded = self.progress.send_if_modified(|p| {
            if p.degraded {
                return false;
            }
            p.degraded = true;
            p.degrade_mark = head;
            true
        });
        if degraded {
            self.degrades.fetch_add(1, Ordering::Relaxed);
            warn!(
                head,
                confirmed_lsn = self.confirmed(),
                timeout_ms = self.timeout_ms(),
                "replication_sync_degraded: no follower confirmed a commit in time; \
                 shipping asynchronously until a follower catches up"
            );
        }
    }

    /// A follower applied every event up to `lsn` durably. `caught_up` says
    /// it had then applied everything it was sent at a moment it had been
    /// sent the whole log.
    pub(crate) fn acked(&self, lsn: u64, caught_up: bool) {
        let mut rearmed = false;
        let mut advanced = false;
        self.progress.send_if_modified(|p| {
            if lsn > p.confirmed {
                p.confirmed = lsn;
                advanced = true;
            }
            if p.degraded && caught_up && lsn >= p.degrade_mark {
                p.degraded = false;
                rearmed = true;
            }
            advanced || rearmed
        });
        if rearmed {
            self.rearms.fetch_add(1, Ordering::Relaxed);
            info!(
                confirmed_lsn = lsn,
                "replication_sync_rearmed: a follower caught up; shipping synchronously again"
            );
        }
        if advanced && self.stalled.swap(false, Ordering::AcqRel) {
            info!(
                confirmed_lsn = lsn,
                "replication_sync_resumed: a follower confirms commits again"
            );
        }
    }

    fn timeout_ms(&self) -> u64 {
        u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX)
    }

    /// The report for `/v1/replication/status` and `/metrics`.
    pub fn report(&self) -> SyncReport {
        SyncReport {
            mode: if self.enabled {
                ReplicationMode::Sync
            } else {
                ReplicationMode::Async
            },
            state: self.state(),
            on_follower_loss: self.on_loss,
            sync_timeout_ms: self.timeout_ms(),
            confirmed_lsn: self.confirmed(),
            waits: self.waits.load(Ordering::Relaxed),
            wait_ms_total: self.wait_micros.load(Ordering::Relaxed) / 1000,
            unconfirmed: self.unconfirmed.load(Ordering::Relaxed),
            degrades: self.degrades.load(Ordering::Relaxed),
            rearms: self.rearms.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shipping(mode: ReplicationMode, on_loss: FollowerLoss) -> Arc<SyncShipping> {
        let config = ReplicationConfig {
            mode,
            on_follower_loss: on_loss,
            sync_timeout_ms: 100,
            ..ReplicationConfig::default()
        };
        let log = Arc::new(ReplicationLog::new(1024));
        for _ in 0..6 {
            log.append(b"event\n".to_vec());
        }
        Arc::new(SyncShipping::new(&config, Some(log)))
    }

    #[tokio::test]
    async fn async_mode_and_followers_never_wait() {
        let sync = shipping(ReplicationMode::Async, FollowerLoss::Block);
        assert_eq!(sync.confirm(5).await, Confirmation::Confirmed);
        assert_eq!(sync.state(), SyncState::Async);
        assert_eq!(sync.report().waits, 0);

        let config = ReplicationConfig {
            mode: ReplicationMode::Sync,
            ..ReplicationConfig::default()
        };
        let follower = SyncShipping::new(&config, None);
        assert!(!follower.enabled());
        assert_eq!(follower.confirm(5).await, Confirmation::Confirmed);
    }

    #[tokio::test]
    async fn a_reply_waits_for_a_follower_to_apply_its_lsn() {
        let sync = shipping(ReplicationMode::Sync, FollowerLoss::Block);
        assert_eq!(sync.state(), SyncState::Sync);
        let waiter = tokio::spawn({
            let sync = Arc::clone(&sync);
            async move { sync.confirm(3).await }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        sync.acked(2, false);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!waiter.is_finished(), "LSN 2 does not confirm 3");
        sync.acked(4, false);
        assert_eq!(waiter.await.unwrap(), Confirmation::Confirmed);
        assert_eq!(sync.confirmed(), 4);

        // Already confirmed: no wait.
        assert_eq!(sync.confirm(4).await, Confirmation::Confirmed);
        // Nothing written: nothing to wait for.
        assert_eq!(sync.confirm(0).await, Confirmation::Confirmed);
        assert_eq!(sync.report().waits, 2);
    }

    #[tokio::test]
    async fn block_fails_unconfirmed_replies_and_resumes_on_the_next_ack() {
        let sync = shipping(ReplicationMode::Sync, FollowerLoss::Block);
        match sync.confirm(1).await {
            Confirmation::Unconfirmed { waited } => {
                assert!(waited >= Duration::from_millis(100), "{waited:?}");
            }
            other => panic!("expected a timeout, got {other:?}"),
        }
        assert_eq!(sync.state(), SyncState::Stalled);
        assert!(matches!(
            sync.confirm(2).await,
            Confirmation::Unconfirmed { .. }
        ));
        assert_eq!(sync.report().unconfirmed, 2);

        sync.acked(2, true);
        assert_eq!(sync.state(), SyncState::Sync);
        assert_eq!(sync.confirm(2).await, Confirmation::Confirmed);
    }

    #[tokio::test]
    async fn degrade_falls_back_to_async_and_rearms_once_a_follower_caught_up() {
        let sync = shipping(ReplicationMode::Sync, FollowerLoss::Degrade);
        // Two waiters time out together: shipping degrades once.
        let (a, b) = tokio::join!(sync.confirm(5), sync.confirm(6));
        assert_eq!((a, b), (Confirmation::Confirmed, Confirmation::Confirmed));
        assert_eq!(sync.state(), SyncState::Degraded);
        assert_eq!(sync.report().degrades, 1);

        // Degraded: replies do not wait.
        let started = Instant::now();
        assert_eq!(sync.confirm(9).await, Confirmation::Confirmed);
        assert!(started.elapsed() < Duration::from_millis(50));

        // Acks short of the mark, or from a follower not caught up, keep it
        // degraded.
        sync.acked(5, true);
        sync.acked(8, false);
        assert_eq!(sync.state(), SyncState::Degraded);
        sync.acked(8, true);
        assert_eq!(sync.state(), SyncState::Sync);
        assert_eq!(sync.report().rearms, 1);
    }

    #[tokio::test]
    async fn degrading_releases_replies_already_waiting() {
        let sync = shipping(ReplicationMode::Sync, FollowerLoss::Degrade);
        let late = tokio::spawn({
            let sync = Arc::clone(&sync);
            async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                let started = Instant::now();
                (sync.confirm(2).await, started.elapsed())
            }
        });
        assert_eq!(sync.confirm(1).await, Confirmation::Confirmed);
        let (confirmation, waited) = late.await.unwrap();
        assert_eq!(confirmation, Confirmation::Confirmed);
        assert!(waited < Duration::from_millis(90), "{waited:?}");
    }
}
