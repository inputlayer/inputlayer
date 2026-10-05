//! Knowledge graph revisions, never reissued across engine runs.
//!
//! Revisions come from one counter shared by every knowledge graph. An engine
//! keeps in its data directory a durable bound that no revision it has issued
//! exceeds ([`RevisionReservation`]), raised a block at a time, and a starting
//! engine continues above the bound its directory holds. A write is refused
//! when the bound it needs cannot be made durable. A revision observed
//! in an earlier run is therefore lower than every revision of this one: it
//! predates each knowledge graph's first snapshot here and fails an
//! `expect_revision` precondition (see [`super::precondition`]), with or
//! without `expect_epoch`.

use crate::storage::metadata::save_json_atomic;
use crate::storage::StorageResult;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use tracing::error;

/// The bound's file, under the data directory.
const RESERVATION_FILE: &str = "metadata/revisions.json";

/// Revisions reserved per durable write of the bound.
const BLOCK: u64 = 1 << 20;

/// How far below the bound writes start raising it, so that a bound that
/// cannot be raised refuses them before the counter reaches it.
const HEADROOM: u64 = BLOCK / 2;

/// Where a data directory with state but no recorded bound continues: its
/// earlier runs each counted from 1, far below this.
const UNRECORDED_FLOOR: u64 = 1 << 40;

/// The revision counter and the open engines' bounds on it.
struct Counter {
    /// Last revision handed to a snapshot, across all knowledge graphs.
    last: AtomicU64,
    /// The lowest bound of the open engines: revisions up to it need no write.
    reserved: AtomicU64,
    /// The open engines' reservations (several only in tests).
    reservations: Mutex<Vec<Weak<RevisionReservation>>>,
}

/// The process's counter.
static REVISIONS: Counter = Counter::new();

/// The last revision handed to a snapshot of any knowledge graph; no snapshot
/// has a later one yet.
pub(super) fn last_revision() -> u64 {
    REVISIONS.last.load(Ordering::SeqCst)
}

/// The next revision, within every open engine's durable bound.
///
/// # Panics
/// The bound has to be raised and cannot be written. Writes are refused long
/// before that, by [`reserve_ahead`].
pub(super) fn next_revision() -> u64 {
    REVISIONS.next()
}

/// Keep every open engine's durable bound [`HEADROOM`] above the counter, for
/// the snapshots the caller goes on to build.
///
/// # Errors
/// A bound could not be written: the caller must not change state.
pub(super) fn reserve_ahead() -> StorageResult<()> {
    REVISIONS.reserve_ahead()
}

impl Counter {
    const fn new() -> Self {
        Self {
            last: AtomicU64::new(0),
            reserved: AtomicU64::new(u64::MAX),
            reservations: Mutex::new(Vec::new()),
        }
    }

    fn next(&self) -> u64 {
        let revision = self.last.fetch_add(1, Ordering::SeqCst) + 1;
        if revision > self.reserved.load(Ordering::SeqCst) {
            if let Err(e) = self.reserve_through(revision) {
                panic!("revision {revision} is above a bound that cannot be raised: {e}");
            }
        }
        revision
    }

    fn reserve_ahead(&self) -> StorageResult<()> {
        let ahead = self.last.load(Ordering::SeqCst).saturating_add(HEADROOM);
        if ahead <= self.reserved.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.reserve_through(ahead)
    }

    /// Raise every open engine's bound below `revision` and drop closed
    /// engines.
    ///
    /// # Errors
    /// A bound could not be written: it stays where it was.
    fn reserve_through(&self, revision: u64) -> StorageResult<()> {
        let mut reservations = self.reservations.lock();
        let mut lowest = u64::MAX;
        let mut raised = Ok(());
        reservations.retain(|reservation| {
            let Some(reservation) = reservation.upgrade() else {
                return false;
            };
            if reservation.reserved.load(Ordering::SeqCst) < revision {
                let last = self.last.load(Ordering::SeqCst);
                if let Err(e) = reservation.reserve(last.max(revision) + BLOCK) {
                    error!(error = %e, revision, "revision_reservation_failed");
                    raised = Err(e);
                }
            }
            lowest = lowest.min(reservation.reserved.load(Ordering::SeqCst));
            true
        });
        self.reserved.store(lowest, Ordering::SeqCst);
        raised
    }

    /// [`RevisionReservation::open`] on this counter.
    fn open(&self, data_dir: &Path, has_state: bool) -> StorageResult<Arc<RevisionReservation>> {
        let path = data_dir.join(RESERVATION_FILE);
        let floor = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Record>(&bytes)?.reserved,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if has_state {
                    UNRECORDED_FLOOR
                } else {
                    0
                }
            }
            Err(e) => return Err(e.into()),
        };
        let reservation = Arc::new(RevisionReservation {
            path,
            reserved: AtomicU64::new(0),
        });
        let mut reservations = self.reservations.lock();
        let last = self.last.fetch_max(floor, Ordering::SeqCst).max(floor);
        reservation.reserve(last + BLOCK)?;
        reservations.push(Arc::downgrade(&reservation));
        self.reserved.fetch_min(last + BLOCK, Ordering::SeqCst);
        Ok(reservation)
    }
}

#[derive(Serialize, Deserialize)]
struct Record {
    reserved: u64,
}

/// One engine's durable bound on the revisions it has issued, in its data
/// directory; see the module docs.
pub(super) struct RevisionReservation {
    path: PathBuf,
    /// The bound on disk.
    reserved: AtomicU64,
}

impl RevisionReservation {
    /// Continue above the bound `data_dir` holds and reserve the first
    /// block, before the engine builds any snapshot. `has_state`: the
    /// directory held engine state before this run.
    pub(super) fn open(data_dir: &Path, has_state: bool) -> StorageResult<Arc<Self>> {
        REVISIONS.open(data_dir, has_state)
    }

    fn reserve(&self, reserved: u64) -> StorageResult<()> {
        save_json_atomic(&Record { reserved }, &self.path)?;
        self.reserved.store(reserved, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
#[path = "revisions_tests.rs"]
mod tests;
