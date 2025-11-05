//! Statistics collection and selectivity estimation for query optimization.
//!
//! Tracks per-relation cardinality, per-column distinct counts, min/max values,
//! MCV lists, histograms, and join selectivity estimates.
//!
//! # Example
//!
//! ```
//! use inputlayer::statistics::{StatisticsManager, StatsConfig};
//! use inputlayer::value::{Tuple, Value};
//!
//! fn make_tuple(values: Vec<i64>) -> Tuple {
//!     Tuple::new(values.into_iter().map(Value::Int64).collect())
//! }
//!
//! let mut manager = StatisticsManager::new(StatsConfig::default());
//!
//! // Analyze a relation
//! let tuples = vec![
//!     make_tuple(vec![1, 10]),
//!     make_tuple(vec![2, 20]),
//!     make_tuple(vec![1, 30]),
//! ];
//! manager.analyze("my_relation", &tuples, 2);
//!
//! // Get statistics
//! let stats = manager.get("my_relation").unwrap();
//! assert_eq!(stats.cardinality, 3);
//! ```

use crate::value::{Tuple, Value};
use std::collections::HashMap;

/// Statistics for a single relation.
#[derive(Clone, Debug)]
pub struct RelationStats {
    /// Relation name
    pub name: String,
    /// Total number of tuples
    pub cardinality: usize,
    /// Per-column statistics
    pub column_stats: Vec<ColumnStats>,
    /// Timestamp of last statistics update
    pub updated_at: u64,
}

/// Statistics for a single column.
#[derive(Clone, Debug)]
pub struct ColumnStats {
    /// Column index (0-based)
    pub index: usize,
    /// Number of distinct (non-null) values
    pub distinct_count: usize,
    /// Number of null values
    pub null_count: usize,
    /// Minimum value (for orderable types)
    pub min_value: Option<Value>,
    /// Maximum value (for orderable types)
    pub max_value: Option<Value>,
    /// Most common values with their frequencies
    pub most_common: Vec<(Value, usize)>,
    /// Histogram for range estimation (numeric types only)
    pub histogram: Option<Histogram>,
}

/// Equi-depth histogram for selectivity estimation.
///
/// Each bucket contains approximately the same number of values.
#[derive(Clone, Debug)]
pub struct Histogram {
    /// Bucket boundaries (n+1 values for n buckets)
    pub boundaries: Vec<Value>,
    /// Count of values in each bucket
    pub counts: Vec<usize>,
}

/// Configuration for statistics collection.
#[derive(Clone, Debug)]
pub struct StatsConfig {
    /// Number of most common values to track per column
    pub mcv_count: usize,
    /// Number of histogram buckets
    pub histogram_buckets: usize,
    /// Threshold for auto-updating statistics (number of changes)
    pub auto_update_threshold: usize,
}

