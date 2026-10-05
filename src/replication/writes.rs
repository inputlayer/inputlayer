//! Which replication events a client request produced.
//!
//! In synchronous mode a primary acknowledges a request that changed state
//! only once a follower has applied the newest event the request appended
//! to the [`ReplicationLog`](super::ReplicationLog). [`track`] runs a
//! request's future and returns that LSN. Events are appended on the thread
//! that commits them, under the lock that orders them, so the log notes each
//! LSN on that thread: in the request's task, or on a blocking-pool thread
//! the request handed its [`Writes`] to with [`Writes::enter`]. A durable
//! write that changes nothing appends no event, yet its reply reports state
//! that may rest on events no follower has applied: [`staged`] marks the
//! request so it waits for the log's head instead.

use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

tokio::task_local! {
    static REQUEST: Writes;
}

thread_local! {
    static THREAD: RefCell<Option<Writes>> = const { RefCell::new(None) };
}

/// The newest LSN a request appended (0 for none yet), and whether it
/// staged a durable write.
#[derive(Debug, Clone, Default)]
pub struct Writes(Arc<Noted>);

#[derive(Debug, Default)]
struct Noted {
    lsn: AtomicU64,
    staged: AtomicBool,
}

impl Writes {
    /// The newest LSN noted, 0 when nothing was.
    pub fn lsn(&self) -> u64 {
        self.0.lsn.load(Ordering::Acquire)
    }

    /// Whether the request staged a durable write, whether or not it
    /// appended an event.
    pub fn staged(&self) -> bool {
        self.0.staged.load(Ordering::Acquire)
    }

    fn note(&self, lsn: u64) {
        self.0.lsn.fetch_max(lsn, Ordering::AcqRel);
    }

    /// Note events appended on this thread for the request until the guard
    /// drops (for work the request runs on another thread).
    pub fn enter(self) -> Entered {
        let previous = THREAD.with(|thread| thread.replace(Some(self)));
        Entered { previous }
    }
}

/// Restores the thread's previous [`Writes`] on drop, panics included, so a
/// pooled thread never notes events for a finished request.
#[must_use = "events are noted only while the guard lives"]
pub struct Entered {
    previous: Option<Writes>,
}

impl Drop for Entered {
    fn drop(&mut self) {
        let previous = self.previous.take();
        THREAD.with(|thread| *thread.borrow_mut() = previous);
    }
}

/// The [`Writes`] of the request running here, if any is tracked.
pub fn current() -> Option<Writes> {
    THREAD
        .with(|thread| thread.borrow().clone())
        .or_else(|| REQUEST.try_with(Clone::clone).ok())
}

/// Record that the request running here appended event `lsn`.
pub(crate) fn note(lsn: u64) {
    if let Some(writes) = current() {
        writes.note(lsn);
    }
}

/// Record that the request running here staged a durable write.
pub(crate) fn staged() {
    if let Some(writes) = current() {
        writes.0.staged.store(true, Ordering::Release);
    }
}

/// Run `request`, noting the events it appends and the durable writes it
/// stages; returns its output and what it noted. Inside a request already
/// tracked, the outer request notes them instead and this returns `None`:
/// only the outermost request waits for followers.
pub async fn track<F: Future>(request: F) -> (F::Output, Option<Writes>) {
    if current().is_some() {
        return (request.await, None);
    }
    let writes = Writes::default();
    let output = REQUEST.scope(writes.clone(), request).await;
    (output, Some(writes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_request_sees_the_newest_lsn_it_appended() {
        let (output, lsn) = track(async {
            note(3);
            tokio::task::yield_now().await;
            note(7);
            note(5);
            "done"
        })
        .await;
        assert_eq!((output, lsn.map(|w| w.lsn())), ("done", Some(7)));

        let ((), writes) = track(async {}).await;
        let writes = writes.unwrap();
        assert_eq!(writes.lsn(), 0, "a request that wrote nothing");
        assert!(!writes.staged());

        let ((), writes) = track(async { staged() }).await;
        let writes = writes.unwrap();
        assert!(writes.staged(), "a write that appended nothing");
        assert_eq!(writes.lsn(), 0);

        // Outside any request nothing is noted, and nothing breaks.
        note(9);
        assert!(current().is_none());
    }

    #[tokio::test]
    async fn work_handed_to_a_blocking_thread_is_noted_for_the_request() {
        let ((), lsn) = track(async {
            let writes = current().expect("tracked");
            tokio::task::spawn_blocking(move || {
                let _entered = writes.enter();
                note(11);
            })
            .await
            .unwrap();
        })
        .await;
        assert_eq!(lsn.map(|w| w.lsn()), Some(11));

        // The pooled thread forgets the request once the guard drops.
        let ((), lsn) = track(async {
            tokio::task::spawn_blocking(|| note(12)).await.unwrap();
        })
        .await;
        assert_eq!(lsn.map(|w| w.lsn()), Some(0));
    }

    #[tokio::test]
    async fn a_nested_request_leaves_the_wait_to_the_outer_one() {
        let (inner, outer) = track(async {
            let ((), inner) = track(async { note(4) }).await;
            inner
        })
        .await;
        assert!(inner.is_none());
        assert_eq!(outer.map(|w| w.lsn()), Some(4));
    }
}
