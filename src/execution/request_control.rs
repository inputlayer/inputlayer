//! Deadline and cancellation of one request.
//!
//! A [`RequestControl`] is created when a request arrives and shared by
//! whoever may stop it (its deadline, a client `cancel`) and the computation
//! running it. Its state moves once, out of `Running`:
//!
//! ```text
//! Running ──deadline──▶ Stopped(Deadline)
//!    │ ────cancel────▶ Stopped(Cancelled)
//!    │ ────memory────▶ Stopped(MemoryExhausted)
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
//! checkpoints the evaluator reports what its thread holds through
//! [`RequestControl::charge_memory`], which stops a request over its memory
//! limit. The memory limit holds for every computation of the request, even
//! one that runs after the request began committing (a query after a write
//! in the same program): the commit is not interrupted, but that
//! computation is.

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

const RUNNING: u8 = 0;
const STOPPED_DEADLINE: u8 = 1;
const STOPPED_CANCELLED: u8 = 2;
const COMMITTING: u8 = 3;
const FINISHED: u8 = 4;
const STOPPED_MEMORY: u8 = 5;

/// The stop a state records, if it is a stopped state.
fn stop_of(state: u8) -> Option<Stop> {
    match state {
        STOPPED_DEADLINE => Some(Stop::Deadline),
        STOPPED_CANCELLED => Some(Stop::Cancelled),
        STOPPED_MEMORY => Some(Stop::MemoryExhausted),
        _ => None,
    }
}

/// The error of a computation over the per-query memory limit that ran
/// after its request began committing: what the request committed stays.
pub const QUERY_MEMORY_EXCEEDED: &str = "Query exceeded the per-query memory limit \
     (storage.performance.max_query_memory_bytes). Narrow the query or bind more of its arguments";

/// Why a request was stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// Its deadline passed.
    Deadline,
    /// The client cancelled it.
    Cancelled,
    /// Its computation went over the per-query memory limit.
    MemoryExhausted,
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

/// Deadline and stop state of one request; see the module docs.
#[derive(Debug)]
pub struct RequestControl {
    deadline: Option<Instant>,
    /// Most bytes the computation may hold on one thread; 0 = no limit.
    memory_limit: u64,
    /// A computation of the request went over its memory limit.
    memory_exceeded: AtomicBool,
    state: AtomicU8,
    /// Wakes [`Self::interrupted`] on an explicit cancel.
    cancelled: Notify,
}

impl RequestControl {
    /// A running request that must finish by `deadline`, if any.
    pub fn new(deadline: Option<Instant>) -> Arc<Self> {
        Self::limited(deadline, 0)
    }

    /// A running request that must finish by `deadline`, if any, holding at
    /// most `memory_limit` bytes while it computes (0: no limit).
    pub fn limited(deadline: Option<Instant>, memory_limit: u64) -> Arc<Self> {
        Arc::new(Self {
            deadline,
            memory_limit,
            memory_exceeded: AtomicBool::new(false),
            state: AtomicU8::new(RUNNING),
            cancelled: Notify::new(),
        })
    }

    /// A request arriving now with `timeout` to finish (`None`: no deadline).
    pub fn with_timeout(timeout: Option<Duration>) -> Arc<Self> {
        Self::new(timeout.map(|t| Instant::now() + t))
    }

    /// Most bytes the computation may hold on one thread; 0 = no limit.
    pub fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// When the request must have finished, if it has a deadline.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Why the request was stopped, if it was.
    pub fn stopped(&self) -> Option<Stop> {
        stop_of(self.state.load(Ordering::Acquire))
    }

    /// Whether computation should stop now. The hot-path check.
    pub fn is_stopped(&self) -> bool {
        matches!(
            self.state.load(Ordering::Relaxed),
            STOPPED_DEADLINE | STOPPED_CANCELLED | STOPPED_MEMORY
        )
    }

    /// Report that the computation now holds `held` bytes on the calling
    /// thread, stopping the request if that is over its memory limit.
    /// Returns whether the computation must stop: the request was stopped,
    /// for whatever reason, or the computation is over its memory limit. A
    /// request that began committing is not stopped, as its commit always
    /// runs to completion, but a computation over the limit still stops.
    pub fn charge_memory(&self, held: i64) -> bool {
        if self.memory_limit > 0 && held > 0 && held as u64 > self.memory_limit {
            let halt = self.stop(STOPPED_MEMORY);
            if !matches!(halt, Halt::AlreadyStopped(_))
                && !self.memory_exceeded.swap(true, Ordering::AcqRel)
            {
                tracing::warn!(
                    held_bytes = held,
                    limit_bytes = self.memory_limit,
                    "query_memory_limit_exceeded"
                );
            }
        }
        self.is_stopped() || self.memory_exceeded()
    }

    /// Whether a computation of the request went over its memory limit.
    pub fn memory_exceeded(&self) -> bool {
        self.memory_exceeded.load(Ordering::Acquire)
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
        let control = RequestControl::limited(None, 1000);
        assert_eq!(control.memory_limit(), 1000);
        assert!(!control.charge_memory(1000));
        assert!(!control.charge_memory(-5000));
        assert!(control.charge_memory(1001));
        assert!(control.memory_exceeded());
        assert_eq!(control.stopped(), Some(Stop::MemoryExhausted));
        assert_eq!(control.finish(), Err(Stop::MemoryExhausted));
        assert_eq!(control.begin_commit(), Err(Stop::MemoryExhausted));
        assert_eq!(
            control.cancel(),
            Halt::AlreadyStopped(Stop::MemoryExhausted)
        );
    }

    #[test]
    fn a_computation_after_the_commit_began_still_stops_at_its_memory_limit() {
        let unlimited = RequestControl::new(None);
        assert!(!unlimited.charge_memory(i64::MAX));
        assert!(!unlimited.memory_exceeded());

        let committing = RequestControl::limited(None, 10);
        committing.begin_commit().unwrap();
        assert!(!committing.charge_memory(10));
        assert!(committing.charge_memory(1 << 30));
        assert!(committing.memory_exceeded());
        // The commit itself is not interrupted.
        assert!(committing.is_committing());
        assert_eq!(committing.stopped(), None);
        assert_eq!(committing.finish(), Ok(()));

        // A stop that already happened is reported, whatever the charge,
        // and stays the reason.
        let cancelled = RequestControl::limited(None, 10);
        cancelled.cancel();
        assert!(cancelled.charge_memory(0));
        assert!(cancelled.charge_memory(1 << 30));
        assert_eq!(cancelled.stopped(), Some(Stop::Cancelled));
        assert!(!cancelled.memory_exceeded());
    }

    #[tokio::test]
    async fn a_deadline_during_the_commit_does_not_interrupt_it() {
        let control = RequestControl::with_timeout(Some(Duration::from_millis(10)));
        control.begin_commit().unwrap();
        assert_eq!(control.interrupted().await, None);
        assert!(control.is_committing());
    }
}
