//! Per-key async locks: one conversation's ledger writes run one at a time.
//!
//! Index allocation (read max, write max + 1) and retraction lookup are
//! read-then-write against the KG; two concurrent turns of one
//! conversation would otherwise allocate the same indices. Entries are
//! dropped once nobody holds or waits on them, so the map tracks only
//! conversations with a request in flight.
//!
//! The lock is process-local: replicas serving the same conversation
//! concurrently must route it to one replica (sticky by conversation id).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::OwnedMutexGuard;

#[derive(Default)]
pub struct KeyedLocks {
    map: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// Held for the critical section; releases (and prunes) on drop.
pub struct KeyGuard<'a> {
    locks: &'a KeyedLocks,
    key: String,
    guard: Option<OwnedMutexGuard<()>>,
}

impl KeyedLocks {
    pub async fn lock(&self, key: &str) -> KeyGuard<'_> {
        let mutex = {
            let mut map = self
                .map
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Arc::clone(map.entry(key.to_string()).or_default())
        };
        let guard = mutex.lock_owned().await;
        KeyGuard {
            locks: self,
            key: key.to_string(),
            guard: Some(guard),
        }
    }

    pub fn tracked(&self) -> usize {
        self.map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }
}

impl Drop for KeyGuard<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut map = self
            .locks
            .map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Only the map's own reference left: nobody holds or waits.
        if map
            .get(&self.key)
            .is_some_and(|m| Arc::strong_count(m) == 1)
        {
            map.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_key_serializes_and_entries_are_pruned() {
        let locks = Arc::new(KeyedLocks::default());
        let first = locks.lock("c1").await;
        let other = locks.lock("c2").await; // different key: no wait
        let waiter = {
            let locks = Arc::clone(&locks);
            tokio::spawn(async move {
                let _guard = locks.lock("c1").await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished(), "second holder of c1 must wait");
        drop(first);
        waiter.await.expect("waiter");
        drop(other);
        assert_eq!(locks.tracked(), 0);
    }
}
