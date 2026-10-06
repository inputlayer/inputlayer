//! Admitting computations to the compute pool: one policy for every lane.
//!
//! Every computation of the server (a query, a program that writes, a proof,
//! a `read`, a subscription refresh or a shared round) holds one of a fixed
//! number of compute permits while it runs on the blocking pool, so the
//! server never runs more dataflows at once than it has cores for. Before
//! this policy every request waited for a permit in one first-come queue, so
//! a burst of writes (each holding its permit while it waited for its
//! graph's write lock) or a storm of refreshes delayed every cheap query
//! behind it, and a lane could be starved for as long as another kept
//! arriving.
//!
//! Now each request is admitted on a [`Lane`]: interactive work, writes or
//! background work. Lanes share the pool by **measured permit time**: each
//! lane accrues the time its computations held permits, divided by its
//! weight, and when a permit frees the lane that has used the least gets it.
//! So a lane that only ever runs cheap work keeps getting through however
//! much another lane queues, and a lane that monopolized the pool yields
//! until the others catch up. Two bounds sit on top of that share: a lane
//! holding fewer than its `min_permits` is served before any other (so every
//! lane always makes progress while it has work, even on a host with few
//! permits), and no lane holds more than its `max_permits`. A lane that was
//! idle starts level with the busiest active lane rather than with a credit
//! for the time it did not use. The pool is work-conserving: a free permit
//! never idles while any lane can use it.
//!
//! Waits are bounded: each lane's queue holds at most `max_queued` requests
//! (more are refused at once), and no request waits longer than its deadline
//! or [`AdmissionConfig::max_wait_ms`], whichever is sooner. A request
//! refused for either is answered with `overloaded` (or `deadline_exceeded`
//! when its own deadline passed), it never runs later, and nothing it would
//! have changed is applied. A request cancelled or disconnected while it
//! waits leaves the queue at once, so counters stay exact. Standing-query
//! sharing probes hold one permit of their own beside the pool: a probe never
//! takes a permit a request waits for.
//!
//! The policy itself is a few counters under one short lock, taken only when
//! a request is admitted, refused or finishes, never while anything computes.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::config::{AdmissionConfig, LaneConfig};
use crate::execution::{RequestControl, Stop};

/// The lanes requests are admitted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    /// A request someone waits for: a query, a `read`, a proof, a session
    /// query, or a command that changes no durable state.
    Interactive,
    /// A program that commits: facts, schemas, rules, index builds and the
    /// other commands that change durable state.
    Write,
    /// Work no one request waits for: subscription refreshes and the shared
    /// rounds of standing-query families.
    Background,
    /// A standing-query sharing probe, on a permit of its own.
    Probe,
}

impl Lane {
    /// The pool lanes, in the order ties between them break.
    const POOL: [Lane; 3] = [Lane::Interactive, Lane::Write, Lane::Background];

    /// The lane's name, as metrics and messages print it.
    pub fn name(self) -> &'static str {
        match self {
            Lane::Interactive => "interactive",
            Lane::Write => "write",
            Lane::Background => "background",
            Lane::Probe => "probe",
        }
    }

    fn index(self) -> usize {
        match self {
            Lane::Interactive => 0,
            Lane::Write => 1,
            Lane::Background => 2,
            Lane::Probe => unreachable!("probes are not pooled"),
        }
    }
}

/// Why a request was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The request was stopped (its deadline passed, or it was cancelled)
    /// while it waited; nothing started.
    Stopped(Stop),
    /// The lane's queue is full: the request was refused at once.
    QueueFull { lane: Lane, limit: usize },
    /// No permit within the longest admission wait.
    Timeout { lane: Lane, waited: Duration },
}

/// Counts of one lane, for metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LaneStats {
    /// Computations holding a permit now.
    pub running: usize,
    /// Requests waiting for a permit now.
    pub queued: usize,
    /// Requests admitted since the server started.
    pub admitted: u64,
    /// Requests refused because the queue was full.
    pub rejected_full: u64,
    /// Requests refused because no permit came within the longest wait.
    pub timed_out: u64,
    /// Requests that stopped (deadline or cancel) while they waited.
    pub stopped_waiting: u64,
    /// Time requests spent waiting for a permit, summed, in nanoseconds.
    pub wait_ns: u64,
    /// Time computations held permits, summed, in nanoseconds.
    pub held_ns: u64,
}

/// Permits of the `probe` lane, beside the pool.
const PROBE_PERMITS: usize = 1;

/// The first estimate of how long a lane's computations hold a permit,
/// before any has finished.
const FIRST_HOLD_ESTIMATE: Duration = Duration::from_millis(1);

/// The compute pool and its admission policy. Cheap to clone; clones share
/// the pool.
#[derive(Clone)]
pub struct Admission {
    inner: Arc<Inner>,
}

struct Inner {
    capacity: usize,
    max_wait: Duration,
    lanes: [LaneSettings; 3],
    state: Mutex<State>,
    probes: Arc<tokio::sync::Semaphore>,
    probe_stats: Mutex<LaneStats>,
    next_ticket: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct LaneSettings {
    weight: u64,
    min: usize,
    max: usize,
    max_queued: usize,
}

struct State {
    lanes: [LaneState; 3],
}

#[derive(Default)]
struct LaneState {
    running: usize,
    /// Permit time used, divided by the weight, in nanoseconds: the lane
    /// with the least is served first.
    vtime: i128,
    /// Running estimate of how long one computation holds a permit, in
    /// nanoseconds: charged when a permit is granted and corrected when it
    /// is released, so a lane running long computations counts as busy
    /// while they run.
    hold_estimate_ns: u64,
    queue: VecDeque<Waiter>,
    stats: LaneStats,
}

struct Waiter {
    ticket: u64,
    queued_at: Instant,
    grant: oneshot::Sender<Permit>,
}

/// A permit to compute, released on drop.
pub struct Permit {
    inner: Option<Arc<Inner>>,
    lane: Lane,
    granted_at: Instant,
    /// The hold time charged to the lane when the permit was granted.
    charged_ns: u64,
    /// A probe permit, outside the pool: held until the permit drops.
    _probe: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Admission {
    /// The pool of `config` on a host with `cores` cores. With
    /// `compute_permits` unset the pool is the cores not reserved for I/O:
    /// about a quarter (at least two) handle connections and the rest
    /// compute.
    pub fn new(config: &AdmissionConfig, cores: usize) -> Self {
        let capacity = match config.compute_permits {
            0 => {
                let cores = cores.max(2);
                let io_reserve = (cores / 4).max(2).min(cores - 1);
                cores - io_reserve
            }
            permits => permits,
        };
        Self::with_capacity(config, capacity)
    }

    /// The pool of `config` with exactly `capacity` permits.
    pub fn with_capacity(config: &AdmissionConfig, capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let settings = |lane: &LaneConfig| LaneSettings {
            weight: u64::from(lane.weight.max(1)),
            min: lane.min_permits,
            max: match lane.max_permits {
                0 => capacity,
                max => max.min(capacity),
            },
            max_queued: lane.max_queued,
        };
        Self {
            inner: Arc::new(Inner {
                capacity,
                max_wait: Duration::from_millis(config.max_wait_ms.max(1)),
                lanes: [
                    settings(&config.interactive),
                    settings(&config.write),
                    settings(&config.background),
                ],
                state: Mutex::new(State {
                    lanes: std::array::from_fn(|_| LaneState {
                        hold_estimate_ns: FIRST_HOLD_ESTIMATE.as_nanos() as u64,
                        ..LaneState::default()
                    }),
                }),
                probes: Arc::new(tokio::sync::Semaphore::new(PROBE_PERMITS)),
                probe_stats: Mutex::new(LaneStats::default()),
                next_ticket: AtomicU64::new(0),
            }),
        }
    }

    /// Permits the pool lanes share.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Permits held by computations running now.
    pub fn in_use(&self) -> usize {
        let state = self.inner.state.lock();
        state.lanes.iter().map(|lane| lane.running).sum()
    }

    /// Requests waiting for a permit now, over every lane.
    pub fn queued(&self) -> usize {
        let state = self.inner.state.lock();
        state.lanes.iter().map(|lane| lane.queue.len()).sum()
    }

    /// The counts of `lane`.
    pub fn stats(&self, lane: Lane) -> LaneStats {
        match lane {
            Lane::Probe => {
                let mut stats = *self.inner.probe_stats.lock();
                stats.running = PROBE_PERMITS - self.inner.probes.available_permits();
                stats
            }
            pooled => {
                let state = self.inner.state.lock();
                let lane = &state.lanes[pooled.index()];
                LaneStats {
                    running: lane.running,
                    queued: lane.queue.len(),
                    ..lane.stats
                }
            }
        }
    }

    /// The longest a request waits for a permit.
    pub fn max_wait(&self) -> Duration {
        self.inner.max_wait
    }

    /// Wait for a permit on `lane` until the request is admitted, stopped,
    /// or refused; see the module docs.
    pub async fn acquire(&self, lane: Lane, control: &RequestControl) -> Result<Permit, Refusal> {
        if lane == Lane::Probe {
            return self.acquire_probe(control).await;
        }
        let queued_at = Instant::now();
        let mut receiver = {
            let mut state = self.inner.state.lock();
            if let Some(permit) = state.try_grant(&self.inner, lane, queued_at) {
                return Ok(permit);
            }
            let settings = &self.inner.lanes[lane.index()];
            let lane_state = &mut state.lanes[lane.index()];
            if settings.max_queued > 0 && lane_state.queue.len() >= settings.max_queued {
                lane_state.stats.rejected_full += 1;
                return Err(Refusal::QueueFull {
                    lane,
                    limit: settings.max_queued,
                });
            }
            let (grant, receiver) = oneshot::channel();
            let ticket = self.inner.next_ticket.fetch_add(1, Ordering::Relaxed);
            if lane_state.running == 0 && lane_state.queue.is_empty() {
                state.level_idle_lane(lane);
            }
            state.lanes[lane.index()].queue.push_back(Waiter {
                ticket,
                queued_at,
                grant,
            });
            Queued {
                inner: &self.inner,
                lane,
                ticket,
                receiver,
            }
        };
        let wait = self.wait_budget(control.deadline(), queued_at);
        let stop_reason = |stop: Option<Stop>| stop.unwrap_or(Stop::Cancelled);
        let outcome = tokio::select! {
            biased;
            granted = tokio::time::timeout(wait, receiver.recv()) => match granted {
                Ok(Ok(permit)) => Ok(permit),
                // The pool is gone: nothing more is ever granted.
                Ok(Err(_)) => Err(Refusal::Stopped(Stop::Cancelled)),
                Err(_) => {
                    let waited = queued_at.elapsed();
                    // The deadline, not the pool's limit, ended the wait.
                    if control.expire_if_due() {
                        Err(Refusal::Stopped(stop_reason(control.stopped())))
                    } else {
                        Err(Refusal::Timeout { lane, waited })
                    }
                }
            },
            stop = control.interrupted() => Err(Refusal::Stopped(stop_reason(stop))),
        };
        if let Err(refusal) = &outcome {
            // Leave the queue now, so a permit freed later goes to someone
            // waiting and the queue counts stay exact.
            receiver.leave(refusal);
        }
        outcome
    }

    /// How long a request queued at `queued_at` may wait: until `deadline`,
    /// or the longest admission wait when that is sooner.
    fn wait_budget(&self, deadline: Option<Instant>, queued_at: Instant) -> Duration {
        let to_deadline = deadline.map(|deadline| deadline.saturating_duration_since(queued_at));
        match to_deadline {
            Some(to_deadline) => to_deadline.min(self.inner.max_wait),
            None => self.inner.max_wait,
        }
    }

    async fn acquire_probe(&self, control: &RequestControl) -> Result<Permit, Refusal> {
        let queued_at = Instant::now();
        let wait = self.wait_budget(control.deadline(), queued_at);
        let acquire = tokio::time::timeout(wait, Arc::clone(&self.inner.probes).acquire_owned());
        let outcome = tokio::select! {
            biased;
            acquired = acquire => match acquired {
                Ok(Ok(permit)) => Ok(permit),
                Ok(Err(_)) => Err(Refusal::Stopped(Stop::Cancelled)),
                Err(_) => {
                    if control.expire_if_due() {
                        Err(Refusal::Stopped(control.stopped().unwrap_or(Stop::Deadline)))
                    } else {
                        Err(Refusal::Timeout { lane: Lane::Probe, waited: queued_at.elapsed() })
                    }
                }
            },
            stop = control.interrupted() => Err(Refusal::Stopped(stop.unwrap_or(Stop::Cancelled))),
        };
        let mut stats = self.inner.probe_stats.lock();
        match outcome {
            Ok(permit) => {
                stats.admitted += 1;
                stats.wait_ns += queued_at.elapsed().as_nanos() as u64;
                Ok(Permit {
                    inner: Some(Arc::clone(&self.inner)),
                    lane: Lane::Probe,
                    granted_at: Instant::now(),
                    charged_ns: 0,
                    _probe: Some(permit),
                })
            }
            Err(refusal) => {
                match refusal {
                    Refusal::Timeout { .. } => stats.timed_out += 1,
                    Refusal::Stopped(_) => stats.stopped_waiting += 1,
                    Refusal::QueueFull { .. } => {}
                }
                Err(refusal)
            }
        }
    }

    /// Every permit of the pool at once, or `None` when any is held: for
    /// tests that need the pool full.
    #[cfg(test)]
    pub(crate) fn try_hold_all(&self) -> Option<Vec<Permit>> {
        let mut state = self.inner.state.lock();
        if state.lanes.iter().any(|lane| lane.running > 0) {
            return None;
        }
        let now = Instant::now();
        let mut held = Vec::with_capacity(self.inner.capacity);
        // Spread over the lanes within their caps; the pool is full either way.
        for lane in Lane::POOL {
            while held.len() < self.inner.capacity
                && state.lanes[lane.index()].running < self.inner.lanes[lane.index()].max
            {
                held.push(state.grant(&self.inner, lane, now));
            }
        }
        (held.len() == self.inner.capacity).then_some(held)
    }

    /// The lane's virtual time, for tests of the share.
    #[cfg(test)]
    fn vtime(&self, lane: Lane) -> i128 {
        self.inner.state.lock().lanes[lane.index()].vtime
    }
}

/// A request waiting in a lane's queue; leaves it when dropped before a
/// grant arrived.
struct Queued<'a> {
    inner: &'a Arc<Inner>,
    lane: Lane,
    ticket: u64,
    receiver: oneshot::Receiver<Permit>,
}

impl Queued<'_> {
    async fn recv(&mut self) -> Result<Permit, oneshot::error::RecvError> {
        (&mut self.receiver).await
    }

    /// Leave the queue after `refusal`, counting it.
    fn leave(mut self, refusal: &Refusal) {
        let mut state = self.inner.state.lock();
        let lane = &mut state.lanes[self.lane.index()];
        if let Some(at) = lane.queue.iter().position(|w| w.ticket == self.ticket) {
            lane.queue.remove(at);
        }
        match refusal {
            Refusal::Timeout { .. } => lane.stats.timed_out += 1,
            Refusal::Stopped(_) => lane.stats.stopped_waiting += 1,
            Refusal::QueueFull { .. } => {}
        }
        drop(state);
        // A permit granted in the meantime goes back to the pool: the
        // receiver drops it, releasing it under no lock.
        self.receiver.close();
        if let Ok(permit) = self.receiver.try_recv() {
            drop(permit);
        }
    }
}

impl State {
    /// Grant `lane` a permit now if the policy allows it without waiting.
    fn try_grant(&mut self, inner: &Arc<Inner>, lane: Lane, now: Instant) -> Option<Permit> {
        let running: usize = self.lanes.iter().map(|lane| lane.running).sum();
        let settings = &inner.lanes[lane.index()];
        let lane_state = &self.lanes[lane.index()];
        // Someone is already waiting on this lane: queue behind them.
        if !lane_state.queue.is_empty() {
            return None;
        }
        if running >= inner.capacity || lane_state.running >= settings.max {
            return None;
        }
        // A free permit with a lane below its reserve waiting (and able to
        // take it) is theirs.
        let reserved_waiting = Lane::POOL.iter().any(|other| {
            let state = &self.lanes[other.index()];
            let settings = &inner.lanes[other.index()];
            *other != lane
                && !state.queue.is_empty()
                && state.running < settings.min
                && state.running < settings.max
        });
        if reserved_waiting {
            return None;
        }
        if lane_state.running == 0 {
            self.level_idle_lane(lane);
        }
        Some(self.grant(inner, lane, now))
    }

    /// Hand `lane` a permit: counted as running and charged its estimated
    /// hold time.
    fn grant(&mut self, inner: &Arc<Inner>, lane: Lane, queued_at: Instant) -> Permit {
        let settings = &inner.lanes[lane.index()];
        let lane_state = &mut self.lanes[lane.index()];
        let now = Instant::now();
        lane_state.running += 1;
        let charged_ns = lane_state.hold_estimate_ns;
        lane_state.vtime += i128::from(charged_ns / settings.weight);
        lane_state.stats.admitted += 1;
        lane_state.stats.wait_ns += now.saturating_duration_since(queued_at).as_nanos() as u64;
        Permit {
            inner: Some(Arc::clone(inner)),
            lane,
            granted_at: now,
            charged_ns,
            _probe: None,
        }
    }

    /// A lane with nothing running or waiting starts level with the active
    /// lanes rather than with a credit for its idle time.
    fn level_idle_lane(&mut self, lane: Lane) {
        let floor = Lane::POOL
            .iter()
            .filter(|other| **other != lane)
            .map(|other| &self.lanes[other.index()])
            .filter(|other| other.running > 0 || !other.queue.is_empty())
            .map(|other| other.vtime)
            .min();
        if let Some(floor) = floor {
            let lane_state = &mut self.lanes[lane.index()];
            lane_state.vtime = lane_state.vtime.max(floor);
        }
    }

    /// Release a permit of `lane` held for `held`, charged `charged_ns`
    /// when granted, then hand out every permit the policy now allows.
    fn release(&mut self, inner: &Arc<Inner>, lane: Lane, held: Duration, charged_ns: u64) {
        let settings = &inner.lanes[lane.index()];
        let lane_state = &mut self.lanes[lane.index()];
        lane_state.running = lane_state.running.saturating_sub(1);
        let held_ns = held.as_nanos() as u64;
        lane_state.stats.held_ns += held_ns;
        // Correct the estimate charged at the grant to what was held.
        let correction = i128::from(held_ns) - i128::from(charged_ns);
        lane_state.vtime += correction / i128::from(settings.weight);
        // Exponential moving average, an eighth per computation.
        lane_state.hold_estimate_ns = (lane_state.hold_estimate_ns * 7).saturating_add(held_ns) / 8;
        self.dispatch(inner);
    }

    /// Hand out permits while one is free and a lane may take it.
    fn dispatch(&mut self, inner: &Arc<Inner>) {
        loop {
            let running: usize = self.lanes.iter().map(|lane| lane.running).sum();
            if running >= inner.capacity {
                return;
            }
            let Some(lane) = self.next_lane(inner) else {
                return;
            };
            let Some(waiter) = self.lanes[lane.index()].queue.pop_front() else {
                return;
            };
            let permit = self.grant(inner, lane, waiter.queued_at);
            if let Err(mut permit) = waiter.grant.send(permit) {
                // Gone (cancelled, timed out or disconnected) before its
                // turn: the permit was never held.
                permit.inner = None;
                let lane_state = &mut self.lanes[lane.index()];
                lane_state.running -= 1;
                lane_state.stats.admitted -= 1;
                lane_state.vtime -=
                    i128::from(permit.charged_ns / inner.lanes[lane.index()].weight);
            }
        }
    }

    /// The lane to serve next: among lanes with a waiter and room under their
    /// cap, one below its reserve if any, else the one that has used the
    /// least permit time for its weight; ties go to the longest wait.
    fn next_lane(&self, inner: &Inner) -> Option<Lane> {
        let candidates = || {
            Lane::POOL.into_iter().filter(|lane| {
                let state = &self.lanes[lane.index()];
                !state.queue.is_empty() && state.running < inner.lanes[lane.index()].max
            })
        };
        let key = |lane: Lane| {
            let state = &self.lanes[lane.index()];
            let waited_since = state.queue.front().map(|w| w.queued_at);
            (state.vtime, waited_since)
        };
        let reserved = candidates()
            .filter(|lane| self.lanes[lane.index()].running < inner.lanes[lane.index()].min)
            .min_by_key(|lane| key(*lane));
        reserved.or_else(|| candidates().min_by_key(|lane| key(*lane)))
    }
}

impl Permit {
    /// The lane the permit was granted on.
    pub fn lane(&self) -> Lane {
        self.lane
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        let held = self.granted_at.elapsed();
        if self.lane == Lane::Probe {
            inner.probe_stats.lock().held_ns += held.as_nanos() as u64;
            // The semaphore permit releases itself.
            return;
        }
        inner
            .state
            .lock()
            .release(&inner, self.lane, held, self.charged_ns);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::config::AdmissionConfig;

    fn pool(capacity: usize) -> Admission {
        Admission::with_capacity(&AdmissionConfig::default(), capacity)
    }

    fn pool_with(capacity: usize, adjust: impl FnOnce(&mut AdmissionConfig)) -> Admission {
        let mut config = AdmissionConfig::default();
        adjust(&mut config);
        Admission::with_capacity(&config, capacity)
    }

    fn control() -> Arc<RequestControl> {
        RequestControl::new(None)
    }

    async fn admitted(pool: &Admission, lane: Lane) -> Permit {
        tokio::time::timeout(Duration::from_secs(5), pool.acquire(lane, &control()))
            .await
            .expect("admitted in time")
            .expect("admitted")
    }

    /// A waiter on `lane` as a task, reporting its permit over a channel.
    fn waiter(pool: &Admission, lane: Lane) -> oneshot::Receiver<Result<Permit, Refusal>> {
        let pool = pool.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = tx.send(pool.acquire(lane, &control()).await);
        });
        rx
    }

    async fn settled(
        mut rx: oneshot::Receiver<Result<Permit, Refusal>>,
    ) -> Option<Result<Permit, Refusal>> {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        rx.try_recv().ok()
    }

    #[tokio::test]
    async fn permits_are_granted_at_once_while_the_pool_has_room() {
        let pool = pool(2);
        let a = admitted(&pool, Lane::Interactive).await;
        let b = admitted(&pool, Lane::Write).await;
        assert_eq!(pool.in_use(), 2);
        assert_eq!(pool.stats(Lane::Interactive).admitted, 1);
        drop(a);
        assert_eq!(pool.in_use(), 1);
        drop(b);
        assert_eq!(pool.in_use(), 0);
    }

    #[tokio::test]
    async fn a_freed_permit_goes_to_a_lane_below_its_reserve_first() {
        let pool = pool_with(2, |c| {
            c.write.min_permits = 1;
            c.background.min_permits = 1;
        });
        let w1 = admitted(&pool, Lane::Write).await;
        let _w2 = admitted(&pool, Lane::Write).await;
        // Writes queue more; a background refresh and a query arrive after.
        let w3 = waiter(&pool, Lane::Write);
        let w4 = waiter(&pool, Lane::Write);
        tokio::task::yield_now().await;
        let b = waiter(&pool, Lane::Background);
        let q = waiter(&pool, Lane::Interactive);
        tokio::task::yield_now().await;
        assert_eq!(pool.queued(), 4);
        drop(w1);
        // Interactive and background both hold nothing: they come first,
        // the longest-waiting tie first.
        let got_b = settled(b).await.unwrap().unwrap();
        assert_eq!(got_b.lane(), Lane::Background);
        assert_eq!(pool.stats(Lane::Write).queued, 2);
        drop(got_b);
        let got_q = settled(q).await.unwrap().unwrap();
        assert_eq!(got_q.lane(), Lane::Interactive);
        drop(got_q);
        let got_w3 = settled(w3).await.unwrap().unwrap();
        drop(got_w3);
        let got_w4 = settled(w4).await.unwrap().unwrap();
        drop(got_w4);
        assert_eq!(pool.queued(), 0);
    }

    #[tokio::test]
    async fn lanes_share_permit_time_by_weight() {
        // One permit; interactive weighs 3, background 1.
        let pool = pool_with(1, |c| {
            c.interactive.weight = 3;
            c.background.weight = 1;
            c.interactive.min_permits = 0;
            c.background.min_permits = 0;
        });
        let held = admitted(&pool, Lane::Write).await;
        let mut interactive = Vec::new();
        let mut background = Vec::new();
        for _ in 0..12 {
            interactive.push(waiter(&pool, Lane::Interactive));
            background.push(waiter(&pool, Lane::Background));
            tokio::task::yield_now().await;
        }
        drop(held);
        // Each computation holds its permit for the same time, so the share
        // of grants follows the weights: three interactive per background.
        let mut order = Vec::new();
        for _ in 0..24 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let mut found = None;
            for (lane, waiters) in [
                (Lane::Interactive, &mut interactive),
                (Lane::Background, &mut background),
            ] {
                for w in waiters.iter_mut() {
                    if let Ok(Ok(permit)) = w.try_recv() {
                        found = Some((lane, permit));
                        break;
                    }
                }
                if found.is_some() {
                    break;
                }
            }
            let (lane, permit) = found.expect("one grant at a time");
            order.push(lane);
            // Hold a little, so the share is measured in time.
            tokio::time::sleep(Duration::from_millis(2)).await;
            drop(permit);
        }
        let first_eight = &order[..8];
        let interactive_first = first_eight
            .iter()
            .filter(|l| **l == Lane::Interactive)
            .count();
        assert!(
            (5..=7).contains(&interactive_first),
            "about three interactive per background in {order:?}"
        );
        assert_eq!(order.iter().filter(|l| **l == Lane::Background).count(), 12);
    }

    #[tokio::test]
    async fn a_lane_never_holds_more_than_its_cap() {
        let pool = pool_with(3, |c| c.write.max_permits = 1);
        let w1 = admitted(&pool, Lane::Write).await;
        let w2 = waiter(&pool, Lane::Write);
        assert!(settled(w2).await.is_none(), "the write lane is at its cap");
        // Other lanes use the room.
        let _q = admitted(&pool, Lane::Interactive).await;
        let _b = admitted(&pool, Lane::Background).await;
        assert_eq!(pool.in_use(), 3);
        drop(w1);
        let w3 = admitted(&pool, Lane::Write).await;
        assert_eq!(w3.lane(), Lane::Write);
    }

    #[tokio::test]
    async fn a_full_queue_refuses_at_once() {
        let pool = pool_with(1, |c| c.interactive.max_queued = 2);
        let _held = admitted(&pool, Lane::Write).await;
        let _q1 = waiter(&pool, Lane::Interactive);
        let _q2 = waiter(&pool, Lane::Interactive);
        tokio::task::yield_now().await;
        let refused = pool.acquire(Lane::Interactive, &control()).await;
        assert_eq!(
            refused.err(),
            Some(Refusal::QueueFull {
                lane: Lane::Interactive,
                limit: 2
            })
        );
        assert_eq!(pool.stats(Lane::Interactive).rejected_full, 1);
        // Other lanes are unaffected.
        let b = waiter(&pool, Lane::Background);
        tokio::task::yield_now().await;
        assert_eq!(pool.stats(Lane::Background).queued, 1);
        drop(b);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wait_past_the_longest_admission_wait_is_refused_as_overloaded() {
        let pool = pool_with(1, |c| c.max_wait_ms = 1_000);
        let _held = admitted(&pool, Lane::Write).await;
        let control = control();
        let refused = pool.acquire(Lane::Interactive, &control).await;
        assert!(matches!(
            refused,
            Err(Refusal::Timeout {
                lane: Lane::Interactive,
                ..
            })
        ));
        assert_eq!(pool.stats(Lane::Interactive).timed_out, 1);
        assert_eq!(pool.queued(), 0, "the refused request left the queue");
        assert!(
            !control.is_stopped(),
            "the request's own deadline did not pass"
        );
    }

    // Real time: the request's deadline is on the wall clock, which a paused
    // runtime does not advance.
    #[tokio::test]
    async fn a_deadline_sooner_than_the_longest_wait_stops_the_request() {
        let pool = pool_with(1, |c| c.max_wait_ms = 10_000);
        let _held = admitted(&pool, Lane::Write).await;
        let control = RequestControl::with_timeout(Some(Duration::from_millis(50)));
        let refused = pool.acquire(Lane::Interactive, &control).await;
        assert_eq!(refused.err(), Some(Refusal::Stopped(Stop::Deadline)));
        assert!(control.is_stopped());
        assert_eq!(pool.stats(Lane::Interactive).stopped_waiting, 1);
        assert_eq!(pool.queued(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_waiter_leaves_the_queue_and_never_gets_a_permit() {
        let pool = pool(1);
        let held = admitted(&pool, Lane::Write).await;
        let control = control();
        let waiting = {
            let pool = pool.clone();
            let control = Arc::clone(&control);
            tokio::spawn(async move { pool.acquire(Lane::Interactive, &control).await })
        };
        tokio::task::yield_now().await;
        assert_eq!(pool.queued(), 1);
        control.cancel();
        let refused = waiting.await.unwrap();
        assert_eq!(refused.err(), Some(Refusal::Stopped(Stop::Cancelled)));
        assert_eq!(pool.queued(), 0);
        drop(held);
        assert_eq!(
            pool.in_use(),
            0,
            "nothing was granted to the cancelled request"
        );
        assert_eq!(pool.stats(Lane::Interactive).admitted, 0);
    }

    #[tokio::test]
    async fn a_waiter_dropped_before_its_turn_frees_the_permit_it_was_granted() {
        let pool = pool(1);
        let held = admitted(&pool, Lane::Write).await;
        let waiting = waiter(&pool, Lane::Interactive);
        tokio::task::yield_now().await;
        // The task is gone before any permit comes.
        drop(waiting);
        drop(held);
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Whether the grant raced the drop or not, the pool is idle again.
        assert_eq!(pool.in_use(), 0);
        assert_eq!(pool.queued(), 0);
    }

    #[tokio::test]
    async fn an_idle_lane_starts_level_with_the_active_ones() {
        let pool = pool_with(1, |c| c.interactive.min_permits = 0);
        // Writes use the pool for a while.
        for _ in 0..20 {
            let permit = admitted(&pool, Lane::Write).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
            drop(permit);
        }
        let write_time = pool.vtime(Lane::Write);
        assert!(write_time > 0);
        assert_eq!(pool.vtime(Lane::Interactive), 0);
        // A query arrives: it starts level, with no credit for its idle time.
        let held = admitted(&pool, Lane::Write).await;
        let q = waiter(&pool, Lane::Interactive);
        tokio::task::yield_now().await;
        assert!(pool.vtime(Lane::Interactive) >= write_time);
        drop(held);
        drop(settled(q).await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn probes_hold_a_permit_beside_the_pool() {
        let pool = pool(1);
        let _held = admitted(&pool, Lane::Write).await;
        let probe = admitted(&pool, Lane::Probe).await;
        assert_eq!(probe.lane(), Lane::Probe);
        assert_eq!(pool.in_use(), 1, "the probe took no pool permit");
        assert_eq!(pool.stats(Lane::Probe).running, 1);
        let second = waiter(&pool, Lane::Probe);
        assert!(settled(second).await.is_none(), "one probe at a time");
        drop(probe);
        // The second probe's task takes the permit and, with no one to hand
        // it to, lets it go.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(pool.stats(Lane::Probe).running, 0);
    }

    #[tokio::test]
    async fn every_permit_can_be_held_for_a_test() {
        let pool = pool(3);
        let held = pool.try_hold_all().unwrap();
        assert_eq!(held.len(), 3);
        assert_eq!(pool.in_use(), 3);
        assert!(pool.try_hold_all().is_none());
        let q = waiter(&pool, Lane::Interactive);
        assert!(settled(q).await.is_none());
        drop(held);
        // The waiter's task is granted a permit and, with no one to hand it
        // to, lets it go.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn the_default_pool_leaves_cores_for_io() {
        let config = AdmissionConfig::default();
        assert_eq!(Admission::new(&config, 32).capacity(), 24);
        assert_eq!(Admission::new(&config, 8).capacity(), 6);
        assert_eq!(Admission::new(&config, 4).capacity(), 2);
        assert_eq!(Admission::new(&config, 2).capacity(), 1);
        assert_eq!(Admission::new(&config, 1).capacity(), 1);
        let mut fixed = config;
        fixed.compute_permits = 3;
        assert_eq!(Admission::new(&fixed, 32).capacity(), 3);
    }
}
