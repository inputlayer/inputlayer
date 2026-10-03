//! Per-connection request pipeline: bounded, overlapping, in-order.
//!
//! The connection loop admits each request with its [`Access`]. Requests run
//! as futures owned by the pipeline and polled by the loop itself (no task
//! per request, so no cross-thread hand-off on the hot path); the loop keeps
//! reading, pushing and heart-beating while they wait on computation. Three
//! rules make that safe:
//!
//! - **Bounded.** At most `capacity` requests are admitted and not yet
//!   released; the loop stops reading the socket while the pipeline is full.
//! - **Ordering barriers.** [`Access::Shared`] requests (pure reads of the
//!   connection's KG and session) overlap each other. An [`Access::Exclusive`]
//!   request (writes, KG switches, session or subscription changes) starts
//!   only once every earlier request is released, and nothing later starts
//!   until it is released. A query after `.kg use` therefore always runs on
//!   the new KG, and a write never lands on a KG an earlier request left.
//! - **In-order replies.** Replies are released in admission order, so a
//!   client without request IDs still reads its replies in the order it sent
//!   the requests. A request occupies its slot until released, which is when
//!   the loop applies its connection-state effects; only then can the next
//!   barrier-crossing request start.
//!
//! The pipeline owns no socket and no lock; the loop is its only driver.
//! Request futures must not block: computation belongs on the blocking pool.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use tracing::warn;

/// How a request interacts with the connection's KG and session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Access {
    /// Reads only: may overlap other shared requests.
    Shared,
    /// Changes data, the session or the connection: runs alone, in order.
    Exclusive,
}

/// Admission order of a request; replies are released in ticket order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Ticket(u64);

/// A request the barrier now allows to run; the loop starts it with
/// [`RequestPipeline::run`] or [`RequestPipeline::complete`].
pub(super) struct Startable<J> {
    pub ticket: Ticket,
    pub job: J,
}

/// A request's reply, released in admission order.
pub(super) struct Released<R> {
    pub ticket: Ticket,
    pub access: Access,
    /// `None` when the request panicked.
    pub reply: Option<R>,
}

struct Queued<J> {
    ticket: Ticket,
    access: Access,
    job: J,
}

pub(super) struct RequestPipeline<J, R> {
    capacity: usize,
    next_ticket: u64,
    /// Next ticket to release.
    next_release: u64,
    /// Admitted, not started, in admission order.
    queued: VecDeque<Queued<J>>,
    /// Started and not yet released.
    started: BTreeMap<Ticket, Access>,
    /// Started and not finished; a panic yields `None`.
    running: FuturesUnordered<BoxFuture<'static, (Ticket, Option<R>)>>,
    /// Finished, waiting for every earlier reply.
    finished: BTreeMap<Ticket, Option<R>>,
}

impl<J, R: Send + 'static> RequestPipeline<J, R> {
    /// Pipeline admitting at most `capacity` (at least 1) unreleased requests.
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            next_ticket: 0,
            next_release: 0,
            queued: VecDeque::new(),
            started: BTreeMap::new(),
            running: FuturesUnordered::new(),
            finished: BTreeMap::new(),
        }
    }

    /// Whether another request can be admitted.
    pub(super) fn has_capacity(&self) -> bool {
        self.unreleased() < self.capacity
    }

    /// Whether nothing is admitted and unreleased.
    pub(super) fn is_idle(&self) -> bool {
        self.unreleased() == 0
    }

    fn unreleased(&self) -> usize {
        self.queued.len() + self.started.len()
    }

    fn exclusive_started(&self) -> bool {
        self.started.values().any(|a| *a == Access::Exclusive)
    }

    /// Queue `job`; returns its ticket. The caller checks
    /// [`Self::has_capacity`] first; admission past capacity is still accepted
    /// so a request is never lost.
    pub(super) fn admit(&mut self, access: Access, job: J) -> Ticket {
        let ticket = Ticket(self.next_ticket);
        self.next_ticket += 1;
        self.queued.push_back(Queued {
            ticket,
            access,
            job,
        });
        ticket
    }

    /// The next queued request the ordering barriers allow to start, if any.
    pub(super) fn next_startable(&mut self) -> Option<Startable<J>> {
        let head = self.queued.front()?;
        let allowed = match head.access {
            Access::Shared => !self.exclusive_started(),
            Access::Exclusive => self.started.is_empty(),
        };
        if !allowed {
            return None;
        }
        let Queued {
            ticket,
            access,
            job,
        } = self.queued.pop_front()?;
        self.started.insert(ticket, access);
        Some(Startable { ticket, job })
    }

    /// Run a started request's work; it progresses while the loop awaits
    /// [`Self::next_reply`].
    pub(super) fn run<F>(&mut self, ticket: Ticket, work: F)
    where
        F: Future<Output = R> + Send + 'static,
    {
        self.running.push(Box::pin(async move {
            let reply = AssertUnwindSafe(work).catch_unwind().await;
            if reply.is_err() {
                warn!("ws_request_panicked");
            }
            (ticket, reply.ok())
        }));
    }

    /// Finish a started request without running anything.
    pub(super) fn complete(&mut self, ticket: Ticket, reply: R) {
        self.finished.insert(ticket, Some(reply));
    }

    /// The next reply in admission order; pending while there is none.
    pub(super) async fn next_reply(&mut self) -> Released<R> {
        loop {
            let head = Ticket(self.next_release);
            if let Some(reply) = self.finished.remove(&head) {
                self.next_release += 1;
                let access = self.started.remove(&head).unwrap_or(Access::Exclusive);
                return Released {
                    ticket: head,
                    access,
                    reply,
                };
            }
            match self.running.next().await {
                Some((ticket, reply)) => {
                    self.finished.insert(ticket, reply);
                }
                None => std::future::pending::<()>().await,
            }
        }
    }

    /// Stop on disconnect: abort shared requests, which only read, and let a
    /// started exclusive request finish, so a write and its bookkeeping are
    /// never cut in half. Replies are discarded.
    pub(super) async fn shutdown(mut self) {
        if self.exclusive_started() {
            while self.running.next().await.is_some() {}
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::oneshot;

    use super::*;

    type Pipeline = RequestPipeline<&'static str, &'static str>;

    fn start_all(pipeline: &mut Pipeline) -> Vec<(Ticket, &'static str)> {
        std::iter::from_fn(|| pipeline.next_startable())
            .map(|s| (s.ticket, s.job))
            .collect()
    }

    fn jobs(started: &[(Ticket, &'static str)]) -> Vec<&'static str> {
        started.iter().map(|(_, job)| *job).collect()
    }

    #[tokio::test]
    async fn shared_requests_overlap_and_reply_in_admission_order() {
        let mut pipeline = Pipeline::new(8);
        pipeline.admit(Access::Shared, "slow");
        pipeline.admit(Access::Shared, "fast");
        let started = start_all(&mut pipeline);
        assert_eq!(jobs(&started), ["slow", "fast"]);

        let (release_slow, slow_done) = oneshot::channel::<()>();
        pipeline.run(started[0].0, async move {
            slow_done.await.unwrap();
            "slow"
        });
        pipeline.run(started[1].0, async { "fast" });

        // "fast" finished first but waits for "slow".
        let first = tokio::time::timeout(Duration::from_millis(50), pipeline.next_reply()).await;
        assert!(first.is_err(), "reply released out of order");
        release_slow.send(()).unwrap();
        assert_eq!(pipeline.next_reply().await.reply.unwrap(), "slow");
        assert_eq!(pipeline.next_reply().await.reply.unwrap(), "fast");
        assert!(pipeline.is_idle());
    }

    #[tokio::test]
    async fn exclusive_request_waits_for_earlier_and_blocks_later() {
        let mut pipeline = Pipeline::new(8);
        pipeline.admit(Access::Shared, "read");
        pipeline.admit(Access::Exclusive, "kg use");
        pipeline.admit(Access::Shared, "read after");
        let started = start_all(&mut pipeline);
        assert_eq!(jobs(&started), ["read"], "barrier must hold the switch");

        pipeline.complete(started[0].0, "read");
        // Finished but not released: the barrier still holds.
        assert!(pipeline.next_startable().is_none());
        assert_eq!(pipeline.next_reply().await.reply.unwrap(), "read");

        let started = start_all(&mut pipeline);
        assert_eq!(jobs(&started), ["kg use"], "later read overtook the switch");
        pipeline.complete(started[0].0, "kg use");
        assert!(
            pipeline.next_startable().is_none(),
            "read started before the switch was released"
        );
        assert_eq!(pipeline.next_reply().await.reply.unwrap(), "kg use");
        assert_eq!(jobs(&start_all(&mut pipeline)), ["read after"]);
    }

    #[tokio::test]
    async fn consecutive_exclusive_requests_run_one_at_a_time() {
        let mut pipeline = Pipeline::new(8);
        pipeline.admit(Access::Exclusive, "insert");
        pipeline.admit(Access::Exclusive, "insert 2");
        let started = start_all(&mut pipeline);
        assert_eq!(jobs(&started), ["insert"]);
        pipeline.complete(started[0].0, "insert");
        assert_eq!(pipeline.next_reply().await.reply.unwrap(), "insert");
        assert_eq!(jobs(&start_all(&mut pipeline)), ["insert 2"]);
    }

    #[tokio::test]
    async fn panicked_request_is_released_as_failed_in_order() {
        let mut pipeline = Pipeline::new(8);
        pipeline.admit(Access::Exclusive, "panics");
        pipeline.admit(Access::Shared, "after");
        let started = start_all(&mut pipeline);
        pipeline.run(started[0].0, async { panic!("request bug") });
        let released = pipeline.next_reply().await;
        assert_eq!(released.access, Access::Exclusive);
        assert!(released.reply.is_none());

        let started = start_all(&mut pipeline);
        assert_eq!(jobs(&started), ["after"]);
        pipeline.complete(started[0].0, "after");
        let released = pipeline.next_reply().await;
        assert_eq!(
            (released.access, released.reply),
            (Access::Shared, Some("after"))
        );
        assert!(pipeline.is_idle());
    }

    #[test]
    fn capacity_counts_queued_and_started_requests() {
        let mut pipeline = Pipeline::new(2);
        pipeline.admit(Access::Shared, "a");
        assert!(pipeline.has_capacity());
        pipeline.admit(Access::Exclusive, "b");
        assert!(!pipeline.has_capacity());
        start_all(&mut pipeline);
        assert!(!pipeline.has_capacity());
        assert_eq!(Pipeline::new(0).capacity, 1);
    }

    #[tokio::test]
    async fn shutdown_aborts_shared_requests() {
        let mut pipeline = Pipeline::new(8);
        pipeline.admit(Access::Shared, "long query");
        let started = start_all(&mut pipeline);
        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        pipeline.run(started[0].0, async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            flag.store(true, Ordering::SeqCst);
            "long query"
        });
        tokio::time::timeout(Duration::from_secs(5), pipeline.shutdown())
            .await
            .expect("shutdown waited for a read");
        assert!(!finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_lets_a_started_exclusive_request_finish() {
        let mut pipeline = Pipeline::new(8);
        pipeline.admit(Access::Exclusive, "write");
        let started = start_all(&mut pipeline);
        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        pipeline.run(started[0].0, async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flag.store(true, Ordering::SeqCst);
            "write"
        });
        pipeline.shutdown().await;
        assert!(finished.load(Ordering::SeqCst), "write was cut off");
    }
}
