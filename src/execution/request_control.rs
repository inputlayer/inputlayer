//! Deadline, cancellation and commit precondition of one request.
//!
//! A [`RequestControl`] is created when a request arrives and shared by
//! whoever may stop it (its deadline, a client `cancel`) and the computation
//! running it. Its state moves once, out of `Running`:
//!
//! ```text
//! Running ──deadline──▶ Stopped(Deadline)
//!    │ ────cancel────▶ Stopped(Cancelled)
//!    │ ────memory────▶ Stopped(MemoryExhausted | ServerMemoryExhausted)
//!    │ ──begin_commit─▶ Committing   (durable work started: not interruptible)
//!    └────finish──────▶ Finished     (result computed: too late to stop)
//! ```
//!
//! The first transition wins, so a stop and the start of a commit can never
//! both succeed: a stopped request writes nothing, and a committing one
//! always runs to completion and reports what it committed.
//!
//! Computation polls [`RequestControl::is_stopped`] at its existing
//! cooperative checkpoints: one relaxed atomic load, no lock. At the same
//! checkpoints each thread computing for the request charges what it
//! allocated since its last checkpoint through
//! [`RequestControl::charge_memory`]: all threads of a request add to one
//! count, held against the request's memory limit, and every request adds to
//! the server's [`QueryMemoryPool`]. A request over its limit, or growing
//! while the pool is over its budget, is stopped. The memory limits hold for
//! every computation of the request, even one that runs after the request
//! began committing (a query after a write in the same program): the commit
//! is not interrupted, but that computation is.
//!
//! A request may also carry a [`Precondition`] its commit must meet
//! (`expect_revision`); the commit checks it under the knowledge graph's
//! write lock, so it reaches the commit the way the deadline does.

use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use crate::storage_engine::Precondition;

const RUNNING: u8 = 0;
const STOPPED_DEADLINE: u8 = 1;
const STOPPED_CANCELLED: u8 = 2;
const COMMITTING: u8 = 3;
const FINISHED: u8 = 4;
const STOPPED_MEMORY: u8 = 5;
const STOPPED_SERVER_MEMORY: u8 = 6;

/// The stop a state records, if it is a stopped state.
fn stop_of(state: u8) -> Option<Stop> {
    match state {
        STOPPED_DEADLINE => Some(Stop::Deadline),
        STOPPED_CANCELLED => Some(Stop::Cancelled),
        STOPPED_MEMORY => Some(Stop::MemoryExhausted),
        STOPPED_SERVER_MEMORY => Some(Stop::ServerMemoryExhausted),
        _ => None,
    }
}

/// Bytes held by the computations of every request in flight, against the
/// server's budget for them. Requests charge it by the same deltas they
/// charge themselves, and give back what they still hold when they end.
#[derive(Debug, Default)]
pub struct QueryMemoryPool {
    /// Most bytes all running computations may hold together; 0 = no limit.
    budget: u64,
    held: AtomicI64,
}

impl QueryMemoryPool {
    /// A pool of `budget` bytes (0: no limit).
    pub fn new(budget: u64) -> Arc<Self> {
        Arc::new(Self {
            budget,
            held: AtomicI64::new(0),
        })
    }

    /// Most bytes all running computations may hold together; 0 = no limit.
    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// Bytes the computations in flight hold.
    pub fn held(&self) -> i64 {
        self.held.load(Ordering::Acquire)
    }

    /// Add `delta` bytes; whether that grew the pool past its budget.
    fn charge(&self, delta: i64) -> bool {
        let held = self.held.fetch_add(delta, Ordering::AcqRel) + delta;
        delta > 0 && self.budget > 0 && held > 0 && held as u64 > self.budget
    }
}

/// Why a request was stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// Its deadline passed.
    Deadline,
    /// The client cancelled it.
    Cancelled,
    /// Its computation went over the per-query memory limit.
    MemoryExhausted,
    /// Its computation grew while the computations in flight held more than
    /// the server's budget for them.
    ServerMemoryExhausted,
}

impl Stop {
    /// The error message of a request stopped for this reason.
    pub fn message(self) -> &'static str {
        match self {
            Self::Deadline => {
                "Request deadline exceeded before it began committing; nothing was applied"
            }
            Self::Cancelled => "Request cancelled before it began committing; nothing was applied",
            Self::MemoryExhausted => {
                "Request exceeded the per-query memory limit \
                 (storage.performance.max_query_memory_bytes) before it began committing; \
                 nothing was applied. Narrow the query or bind more of its arguments"
            }
            Self::ServerMemoryExhausted => {
                "Request stopped: the queries running on the server hold their whole memory \
                 budget (storage.performance.max_total_query_memory_bytes); it was stopped \
                 before it began committing and nothing was applied. Retry later"
            }
        }
    }

    /// The error of a computation stopped for this reason after its request
    /// began committing: what the request committed stays.
    pub fn after_commit_message(self) -> &'static str {
        match self {
            Self::MemoryExhausted => {
                "Query exceeded the per-query memory limit \
                 (storage.performance.max_query_memory_bytes). \
                 Narrow the query or bind more of its arguments"
            }
            Self::ServerMemoryExhausted => {
                "Query stopped: the queries running on the server hold their whole memory \
                 budget (storage.performance.max_total_query_memory_bytes). Retry later"
            }
            Self::Deadline | Self::Cancelled => self.message(),
        }
    }
}

/// What stopping a request did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Halt {
    /// The request was running and is now stopped.
    Stopped,
    /// It already began committing or finished; it was not interrupted.
    TooLate,
    /// It had already been stopped.
    AlreadyStopped(Stop),
}

/// Deadline, stop state and commit precondition of one request; see the
/// module docs.
#[derive(Debug)]
pub struct RequestControl {
    deadline: Option<Instant>,
    /// Most bytes the computation may hold; 0 = no limit.
    memory_limit: u64,
    /// Bytes the computation holds, summed over its threads.
    held: AtomicI64,
    /// The server's pool the request also charges, if any.
    pool: Option<Arc<QueryMemoryPool>>,
    /// The memory stop a computation of the request hit, if any (a state).
    memory_exceeded: AtomicU8,
    precondition: Option<Precondition>,
    state: AtomicU8,
    /// Wakes [`Self::interrupted`] on an explicit cancel.
    cancelled: Notify,
}

impl RequestControl {
    /// A running request that must finish by `deadline`, if any.
    pub fn new(deadline: Option<Instant>) -> Arc<Self> {
        Self::limited_expecting(deadline, 0, None, None)
    }

    /// A running request that must finish by `deadline`, if any, holding at
    /// most `memory_limit` bytes while it computes (0: no limit), charged to
    /// `pool` as well.
    pub fn limited(
        deadline: Option<Instant>,
        memory_limit: u64,
        pool: Option<Arc<QueryMemoryPool>>,
    ) -> Arc<Self> {
        Self::limited_expecting(deadline, memory_limit, pool, None)
    }

    /// A running request that must finish by `deadline`, if any, and whose
    /// commit must meet `precondition`, if any.
    pub fn expecting(deadline: Option<Instant>, precondition: Option<Precondition>) -> Arc<Self> {
        Self::limited_expecting(deadline, 0, None, precondition)
    }

    /// [`Self::limited`] for a request whose commit must meet
    /// `precondition`, if any.
    pub fn limited_expecting(
        deadline: Option<Instant>,
        memory_limit: u64,
        pool: Option<Arc<QueryMemoryPool>>,
        precondition: Option<Precondition>,
    ) -> Arc<Self> {
        Arc::new(Self {
            deadline,
            memory_limit,
            held: AtomicI64::new(0),
            pool,
            memory_exceeded: AtomicU8::new(RUNNING),
            precondition,
            state: AtomicU8::new(RUNNING),
            cancelled: Notify::new(),
        })
    }

    /// A request arriving now with `timeout` to finish (`None`: no deadline).
    pub fn with_timeout(timeout: Option<Duration>) -> Arc<Self> {
        Self::new(timeout.map(|t| Instant::now() + t))
    }

    /// Most bytes the computation may hold; 0 = no limit.
    pub fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// When the request must have finished, if it has a deadline.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// The precondition the request's commit must meet, if any.
    pub fn precondition(&self) -> Option<&Precondition> {
        self.precondition.as_ref()
    }

    /// Why the request was stopped, if it was.
    pub fn stopped(&self) -> Option<Stop> {
        stop_of(self.state.load(Ordering::Acquire))
    }

    /// Whether computation should stop now. The hot-path check.
    pub fn is_stopped(&self) -> bool {
        matches!(
            self.state.load(Ordering::Relaxed),
            STOPPED_DEADLINE | STOPPED_CANCELLED | STOPPED_MEMORY | STOPPED_SERVER_MEMORY
        )
    }

    /// Report that a thread of the computation allocated `delta` more bytes
    /// than it freed since its last report, stopping the request if it now
    /// holds more than its memory limit, or grew the server's pool past its
    /// budget. Returns whether the computation must stop: the request was
    /// stopped, for whatever reason, or its computation went over a memory
    /// limit. A request that began committing is not stopped, as its commit
    /// always runs to completion, but a computation over a limit still stops.
    pub fn charge_memory(&self, delta: i64) -> bool {
        let held = self.held.fetch_add(delta, Ordering::AcqRel) + delta;
        let over_pool = self.pool.as_ref().is_some_and(|pool| pool.charge(delta));
        let over = if self.memory_limit > 0 && held > 0 && held as u64 > self.memory_limit {
            Some(STOPPED_MEMORY)
        } else if over_pool {
            Some(STOPPED_SERVER_MEMORY)
        } else {
            None
        };
        if let Some(stop) = over {
            let halt = self.stop(stop);
            if !matches!(halt, Halt::AlreadyStopped(_))
                && self
                    .memory_exceeded
                    .compare_exchange(RUNNING, stop, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                tracing::warn!(
                    held_bytes = held,
                    limit_bytes = self.memory_limit,
                    pool_held_bytes = self.pool.as_ref().map_or(0, |pool| pool.held()),
                    pool_budget_bytes = self.pool.as_ref().map_or(0, |pool| pool.budget()),
                    reason = ?stop_of(stop),
                    "query_memory_limit_exceeded"
                );
            }
        }
        self.is_stopped() || self.memory_exceeded().is_some()
    }

    /// The memory limit a computation of the request went over, if any.
    pub fn memory_exceeded(&self) -> Option<Stop> {
        stop_of(self.memory_exceeded.load(Ordering::Acquire))
    }

    /// Bytes the computation holds, summed over its threads.
    pub fn memory_held(&self) -> i64 {
        self.held.load(Ordering::Acquire)
    }

    /// Whether the request began durable work, which is not interrupted.
    pub fn is_committing(&self) -> bool {
        self.state.load(Ordering::Acquire) == COMMITTING
    }

    /// Cancel the request on the client's behalf.
    pub fn cancel(&self) -> Halt {
        let halt = self.stop(STOPPED_CANCELLED);
        if halt == Halt::Stopped {
            self.cancelled.notify_waiters();
        }
        halt
    }

    /// Stop the request if its deadline has passed. `false` while it has
    /// time left, or once it began committing or finished.
    pub fn expire_if_due(&self) -> bool {
        match self.deadline {
            Some(deadline) if Instant::now() >= deadline => {
                matches!(
                    self.stop(STOPPED_DEADLINE),
                    Halt::Stopped | Halt::AlreadyStopped(_)
                )
            }
            _ => self.is_stopped(),
        }
    }

    fn stop(&self, stopped: u8) -> Halt {
        match self
            .state
            .compare_exchange(RUNNING, stopped, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => Halt::Stopped,
            Err(state) => stop_of(state).map_or(Halt::TooLate, Halt::AlreadyStopped),
        }
    }

    /// Enter the commit: from here on the request is not interrupted. Fails
    /// if it was stopped, or its deadline passed, first. Idempotent once
    /// committing.
    pub fn begin_commit(&self) -> Result<(), Stop> {
        if self.expire_if_due() {
            return Err(self.stopped().unwrap_or(Stop::Deadline));
        }
        match self
            .state
            .compare_exchange(RUNNING, COMMITTING, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(COMMITTING) => Ok(()),
            // Finished requests do not commit again; treat as a stop.
            Err(state) => Err(stop_of(state).unwrap_or(Stop::Cancelled)),
        }
    }

    /// Mark the request's result as computed. `Err` if a stop won the race,
    /// in which case the result must be discarded. A committing request
    /// stays committing: its result is what it committed.
    pub fn finish(&self) -> Result<(), Stop> {
        match self
            .state
            .compare_exchange(RUNNING, FINISHED, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) | Err(COMMITTING | FINISHED) => Ok(()),
            Err(state) => Err(stop_of(state).unwrap_or(Stop::Cancelled)),
        }
    }

    /// Resolves once the request is stopped by its deadline or a cancel,
    /// with the reason; or with `None` once a stop can no longer interrupt
    /// it (it began committing or finished). Pending while it runs.
    pub async fn interrupted(&self) -> Option<Stop> {
        loop {
            let cancelled = self.cancelled.notified();
            tokio::pin!(cancelled);
            cancelled.as_mut().enable();
            match self.state.load(Ordering::Acquire) {
                RUNNING => {}
                state => return stop_of(state),
            }
            match self.deadline {
                Some(deadline) => {
                    tokio::select! {
                        () = tokio::time::sleep_until(deadline.into()) => {
                            if let Halt::TooLate = self.stop(STOPPED_DEADLINE) {
                                return None;
                            }
                        }
                        () = &mut cancelled => {}
                    }
                }
                None => cancelled.await,
            }
        }
    }
}

impl Drop for RequestControl {
    /// The request ended: what its computation still holds leaves the pool.
    fn drop(&mut self) {
        if let Some(pool) = &self.pool {
            pool.held.fetch_sub(*self.held.get_mut(), Ordering::AcqRel);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn a_stop_and_a_commit_cannot_both_win() {
        let control = RequestControl::new(None);
        assert_eq!(control.cancel(), Halt::Stopped);
        assert_eq!(control.begin_commit(), Err(Stop::Cancelled));
        assert_eq!(control.cancel(), Halt::AlreadyStopped(Stop::Cancelled));
        assert!(control.is_stopped());

        let control = RequestControl::new(None);
        control.begin_commit().unwrap();
        control.begin_commit().unwrap();
        assert_eq!(control.cancel(), Halt::TooLate);
        assert!(!control.is_stopped());
        assert!(control.is_committing());
        assert_eq!(control.finish(), Ok(()));
    }

    #[test]
    fn finishing_first_makes_a_later_stop_too_late() {
        let control = RequestControl::new(None);
        assert_eq!(control.finish(), Ok(()));
        assert_eq!(control.cancel(), Halt::TooLate);

        let control = RequestControl::new(None);
        control.cancel();
        assert_eq!(control.finish(), Err(Stop::Cancelled));
    }

    #[test]
    fn a_passed_deadline_refuses_the_commit() {
        let control = RequestControl::new(Some(Instant::now()));
        assert_eq!(control.begin_commit(), Err(Stop::Deadline));
        assert_eq!(control.stopped(), Some(Stop::Deadline));
        assert!(RequestControl::with_timeout(Some(Duration::from_secs(60)))
            .begin_commit()
            .is_ok());
    }

    #[tokio::test]
    async fn interrupted_resolves_at_the_deadline() {
        let control = RequestControl::with_timeout(Some(Duration::from_millis(20)));
        let started = Instant::now();
        assert_eq!(control.interrupted().await, Some(Stop::Deadline));
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert!(control.is_stopped());
    }

    #[tokio::test]
    async fn interrupted_resolves_on_cancel() {
        let control = RequestControl::new(None);
        let waiter = {
            let control = Arc::clone(&control);
            tokio::spawn(async move { control.interrupted().await })
        };
        tokio::task::yield_now().await;
        control.cancel();
        let stop = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stop, Some(Stop::Cancelled));
    }

    #[test]
    fn going_over_the_memory_limit_stops_the_request() {
        let control = RequestControl::limited(None, 1000, None);
        assert_eq!(control.memory_limit(), 1000);
        assert!(!control.charge_memory(1000));
        assert!(!control.charge_memory(-5000));
        assert!(!control.charge_memory(5000));
        assert_eq!(control.memory_held(), 1000);
        assert!(control.charge_memory(1));
        assert_eq!(control.memory_exceeded(), Some(Stop::MemoryExhausted));
        assert_eq!(control.stopped(), Some(Stop::MemoryExhausted));
        assert_eq!(control.finish(), Err(Stop::MemoryExhausted));
        assert_eq!(control.begin_commit(), Err(Stop::MemoryExhausted));
        assert_eq!(
            control.cancel(),
            Halt::AlreadyStopped(Stop::MemoryExhausted)
        );
    }

    #[test]
    fn the_threads_of_a_request_share_its_memory_limit() {
        let control = RequestControl::limited(None, 1000, None);
        let charged: Vec<bool> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..4)
                .map(|_| scope.spawn(|| control.charge_memory(300)))
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        });
        assert!(charged.iter().any(|&stopped| stopped));
        assert_eq!(control.memory_held(), 1200);
        assert_eq!(control.stopped(), Some(Stop::MemoryExhausted));
    }

    #[test]
    fn concurrent_requests_are_capped_by_the_pool() {
        let pool = QueryMemoryPool::new(1000);
        let requests: Vec<_> = (0..4)
            .map(|_| RequestControl::limited(None, 800, Some(Arc::clone(&pool))))
            .collect();
        // Each fits its own limit; together they would hold 1600 bytes.
        let stopped: Vec<bool> = std::thread::scope(|scope| {
            let running: Vec<_> = requests
                .iter()
                .map(|request| scope.spawn(|| request.charge_memory(400)))
                .collect();
            running.into_iter().map(|r| r.join().unwrap()).collect()
        });
        let over = stopped.iter().filter(|&&s| s).count();
        assert_eq!(over, 2, "{stopped:?}");
        for (request, stopped) in requests.iter().zip(&stopped) {
            let reason = stopped.then_some(Stop::ServerMemoryExhausted);
            assert_eq!(request.stopped(), reason);
        }
        assert_eq!(pool.held(), 1600);

        // Ended requests give back what they held.
        drop(requests);
        assert_eq!(pool.held(), 0);
        let next = RequestControl::limited(None, 800, Some(Arc::clone(&pool)));
        assert!(!next.charge_memory(700));
        assert_eq!(pool.held(), 700);
    }

    #[test]
    fn a_computation_after_the_commit_began_still_stops_at_its_memory_limit() {
        let unlimited = RequestControl::new(None);
        assert!(!unlimited.charge_memory(i64::MAX / 2));
        assert_eq!(unlimited.memory_exceeded(), None);

        let committing = RequestControl::limited(None, 10, None);
        committing.begin_commit().unwrap();
        assert!(!committing.charge_memory(10));
        assert!(committing.charge_memory(1 << 30));
        assert_eq!(committing.memory_exceeded(), Some(Stop::MemoryExhausted));
        // The commit itself is not interrupted.
        assert!(committing.is_committing());
        assert_eq!(committing.stopped(), None);
        assert_eq!(committing.finish(), Ok(()));

        // A stop that already happened is reported, whatever the charge,
        // and stays the reason.
        let cancelled = RequestControl::limited(None, 10, None);
        cancelled.cancel();
        assert!(cancelled.charge_memory(0));
        assert!(cancelled.charge_memory(1 << 30));
        assert_eq!(cancelled.stopped(), Some(Stop::Cancelled));
        assert_eq!(cancelled.memory_exceeded(), None);
    }

    #[tokio::test]
    async fn a_deadline_during_the_commit_does_not_interrupt_it() {
        let control = RequestControl::with_timeout(Some(Duration::from_millis(10)));
        control.begin_commit().unwrap();
        assert_eq!(control.interrupted().await, None);
        assert!(control.is_committing());
    }
}
