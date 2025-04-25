//! Batch and Update types for DD-native persistence
//!
//! This module defines the core data structures for persisting
//! Differential Dataflow-style (data, time, diff) updates.

use crate::value::Tuple;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// A single update to a relation in TVC (Time-Varying Collection) format.
///
/// This matches Differential Dataflow's internal model:
/// - `data`: The actual tuple (arbitrary arity with Value types)
/// - `time`: Logical timestamp when this update occurred
/// - `diff`: Multiplicity change (+1 for insert, -1 for delete)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Update {
    /// The tuple data (supports arbitrary arity)
    pub data: Tuple,
    /// Logical timestamp (monotonically increasing)
    pub time: u64,
    /// Multiplicity change: +1 for insert, -1 for delete
    pub diff: i64,
}

impl Update {
    /// Create an insert update from a Tuple
    pub fn insert(data: Tuple, time: u64) -> Self {
        Update {
            data,
            time,
            diff: 1,
        }
    }

    /// Create a delete update from a Tuple
    pub fn delete(data: Tuple, time: u64) -> Self {
        Update {
            data,
            time,
            diff: -1,
        }
    }

    /// Create an insert update from a pair of i32 values (convenience method)
    pub fn insert_pair(a: i32, b: i32, time: u64) -> Self {
        Update {
            data: Tuple::from_pair(a, b),
            time,
            diff: 1,
        }
    }

    /// Create a delete update from a pair of i32 values (convenience method)
    pub fn delete_pair(a: i32, b: i32, time: u64) -> Self {
        Update {
            data: Tuple::from_pair(a, b),
            time,
            diff: -1,
        }
    }
}

/// A batch of updates (immutable once written to disk).
///
/// Batches are the unit of durable storage - they're written to Parquet
/// files and never modified after creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Batch {
    /// The updates in this batch
    pub updates: Vec<Update>,
    /// Lower bound of timestamps in this batch (inclusive)
    pub lower: u64,
    /// Upper bound of timestamps in this batch (exclusive)
    pub upper: u64,
}

impl Batch {
    /// Create a new batch from updates
    pub fn new(updates: Vec<Update>) -> Self {
        let lower = updates.iter().map(|u| u.time).min().unwrap_or(0);
        let upper = updates.iter().map(|u| u.time).max().map_or(0, |t| t + 1);
        Batch {
            updates,
            lower,
            upper,
        }
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// Get the number of updates in this batch
    pub fn len(&self) -> usize {
        self.updates.len()
    }
}

/// Reference to a stored batch file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchRef {
    /// Unique batch identifier
    pub id: String,
    /// Path to the Parquet file
    pub path: PathBuf,
    /// Lower timestamp bound
    pub lower: u64,
    /// Upper timestamp bound
    pub upper: u64,
    /// Number of updates in this batch
    pub len: usize,
}

/// Shard metadata - represents a persistent Time-Varying Collection.
///
/// A shard corresponds to a single relation in a database.
/// The naming convention is "{database}:{relation}".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMeta {
    /// Shard name (format: "{db}:{relation}")
    pub name: String,
    /// References to all batch files for this shard
    pub batches: Vec<BatchRef>,
    /// Compaction frontier: can't read data before this time
    /// Advancing this allows garbage collection of old updates
    pub since: u64,
    /// Write frontier: all times < upper are complete
    pub upper: u64,
    /// Total number of updates across all batches
    pub total_updates: usize,
}

impl ShardMeta {
    /// Create a new empty shard
    pub fn new(name: String) -> Self {
        ShardMeta {
            name,
            batches: Vec::new(),
            since: 0,
            upper: 0,
            total_updates: 0,
        }
    }

    /// Add a batch reference to this shard
    pub fn add_batch(&mut self, batch_ref: BatchRef) {
        self.total_updates += batch_ref.len;
        if batch_ref.upper > self.upper {
            self.upper = batch_ref.upper;
        }
        self.batches.push(batch_ref);
    }

    /// Advance the compaction frontier
    /// This marks old data as eligible for garbage collection
    pub fn advance_since(&mut self, new_since: u64) {
        if new_since > self.since {
            self.since = new_since;
        }
    }
}

/// Information about a shard (read-only view)
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub name: String,
    pub since: u64,
    pub upper: u64,
    pub batch_count: usize,
    pub total_updates: usize,
}

impl From<&ShardMeta> for ShardInfo {
    fn from(meta: &ShardMeta) -> Self {
        ShardInfo {
            name: meta.name.clone(),
            since: meta.since,
            upper: meta.upper,
            batch_count: meta.batches.len(),
            total_updates: meta.total_updates,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_insert() {
        let update = Update::insert(Tuple::from_pair(1, 2), 100);
        assert_eq!(update.data, Tuple::from_pair(1, 2));
        assert_eq!(update.time, 100);
        assert_eq!(update.diff, 1);
    }

    #[test]
    fn test_update_delete() {
        let update = Update::delete(Tuple::from_pair(1, 2), 100);
        assert_eq!(update.data, Tuple::from_pair(1, 2));
        assert_eq!(update.time, 100);
        assert_eq!(update.diff, -1);
    }

    #[test]
    fn test_update_insert_pair() {
        let update = Update::insert_pair(1, 2, 100);
        assert_eq!(update.data.to_pair(), Some((1, 2)));
        assert_eq!(update.time, 100);
        assert_eq!(update.diff, 1);
    }

    #[test]
    fn test_update_delete_pair() {
        let update = Update::delete_pair(1, 2, 100);
        assert_eq!(update.data.to_pair(), Some((1, 2)));
        assert_eq!(update.time, 100);
        assert_eq!(update.diff, -1);
    }

    #[test]
    fn test_batch_bounds() {
        let updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(3, 4), 20),
            Update::insert(Tuple::from_pair(5, 6), 15),
        ];
        let batch = Batch::new(updates);
        assert_eq!(batch.lower, 10);
        assert_eq!(batch.upper, 21); // max + 1
        assert_eq!(batch.len(), 3);
    }

    #[test]
    fn test_shard_meta() {
        let mut shard = ShardMeta::new("default:edge".to_string());
        assert_eq!(shard.since, 0);
        assert_eq!(shard.upper, 0);

        shard.add_batch(BatchRef {
            id: "batch1".to_string(),
            path: PathBuf::from("batches/batch1.parquet"),
            lower: 0,
            upper: 100,
            len: 50,
        });

        assert_eq!(shard.upper, 100);
        assert_eq!(shard.total_updates, 50);
        assert_eq!(shard.batches.len(), 1);

        shard.advance_since(50);
        assert_eq!(shard.since, 50);
    }
}
