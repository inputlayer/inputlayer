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
            data: Tuple::from_pair(a, b.clone()),
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
