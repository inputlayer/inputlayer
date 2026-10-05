//! The follower's receive buffer: what arrived from the primary and is not
//! applied yet.
//!
//! The follower reads the stream on one task and applies it on another, so
//! applying a large graph does not stop it reading. The buffer between them
//! holds at most [`BUFFER_BYTES`] of received messages. When it is full the
//! reading side waits, the socket fills, and the primary's send blocks: the
//! primary slows to the follower's pace. Nothing is dropped and nothing is
//! reordered.

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Bytes of received messages a follower holds while it applies earlier
/// ones. A single message larger than this is held alone.
pub(super) const BUFFER_BYTES: usize = 64 * 1024 * 1024;

/// A buffer holding up to `bytes` bytes: its filling and its draining end.
pub(super) fn buffer<T>(bytes: usize) -> (Filler<T>, Drain<T>) {
    let bound = u32::try_from(bytes).unwrap_or(u32::MAX).max(1);
    let (items, queued) = mpsc::unbounded_channel();
    let filler = Filler {
        space: Arc::new(Semaphore::new(bound as usize)),
        bound,
        items,
    };
    (filler, Drain { queued })
}

/// The reading side's end.
pub(super) struct Filler<T> {
    space: Arc<Semaphore>,
    bound: u32,
    items: mpsc::UnboundedSender<(T, OwnedSemaphorePermit)>,
}

impl<T> Filler<T> {
    /// Add `item` of `bytes` bytes, waiting while the buffer has no room for
    /// it. An item larger than the whole buffer waits until it is empty.
    /// Returns `false` once the draining end is gone.
    pub(super) async fn push(&self, item: T, bytes: usize) -> bool {
        let wanted = u32::try_from(bytes)
            .unwrap_or(u32::MAX)
            .clamp(1, self.bound);
        let Ok(held) = Arc::clone(&self.space).acquire_many_owned(wanted).await else {
            return false;
        };
        self.items.send((item, held)).is_ok()
    }
}

/// The applying side's end. Taking an item frees its room.
pub(super) struct Drain<T> {
    queued: mpsc::UnboundedReceiver<(T, OwnedSemaphorePermit)>,
}

impl<T> Drain<T> {
    /// The next item, in arrival order; `None` once the filling end is gone
    /// and the buffer is empty.
    pub(super) async fn next(&mut self) -> Option<T> {
        self.queued.recv().await.map(|(item, _)| item)
    }

    /// The next item when one already arrived.
    pub(super) fn ready(&mut self) -> Option<T> {
        self.queued.try_recv().ok().map(|(item, _)| item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[tokio::test]
    async fn a_full_buffer_makes_the_reader_wait_and_drops_nothing() {
        let (filler, mut drain) = buffer::<u32>(10);
        assert!(filler.push(1, 4).await);
        assert!(filler.push(2, 6).await);

        // Full: the third item waits for room instead of growing the buffer.
        let mut third = Box::pin(filler.push(3, 5));
        assert!(third.as_mut().now_or_never().is_none());
        assert_eq!(filler.space.available_permits(), 0);

        // Taking 4 bytes leaves room for 4, still short of 5.
        assert_eq!(drain.next().await, Some(1));
        assert!(third.as_mut().now_or_never().is_none());

        assert_eq!(drain.next().await, Some(2));
        assert!(third.await);
        assert_eq!(drain.ready(), Some(3));
        assert_eq!(drain.ready(), None);
        assert_eq!(filler.space.available_permits(), 10);
    }

    #[tokio::test]
    async fn an_item_larger_than_the_buffer_is_held_alone() {
        let (filler, mut drain) = buffer::<&str>(10);
        assert!(filler.push("small", 1).await);
        let mut large = Box::pin(filler.push("large", 1000));
        assert!(large.as_mut().now_or_never().is_none());
        assert_eq!(drain.next().await, Some("small"));
        assert!(large.await);

        let mut after = Box::pin(filler.push("after", 1));
        assert!(after.as_mut().now_or_never().is_none());
        assert_eq!(drain.next().await, Some("large"));
        assert!(after.await);
        assert_eq!(drain.next().await, Some("after"));
    }

    #[tokio::test]
    async fn items_arrive_in_order_and_the_ends_notice_each_other_leaving() {
        let (filler, mut drain) = buffer::<u32>(4);
        let feeding = tokio::spawn(async move {
            for i in 0..1000 {
                assert!(filler.push(i, 3).await);
            }
        });
        for i in 0..1000 {
            assert_eq!(drain.next().await, Some(i));
        }
        feeding.await.unwrap();
        assert_eq!(drain.next().await, None);

        let (filler, drain) = buffer::<u32>(4);
        drop(drain);
        assert!(!filler.push(1, 1).await);
    }
}
