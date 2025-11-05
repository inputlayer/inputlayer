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

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            mcv_count: 10,
            histogram_buckets: 100,
            auto_update_threshold: 1000,
        }
    }
}


/// Manages statistics for all relations.
pub struct StatisticsManager {
    /// Per-relation statistics
    stats: HashMap<String, RelationStats>,
    /// Configuration
    config: StatsConfig,
    /// Track changes for auto-update
    change_counts: HashMap<String, usize>,
}

impl StatisticsManager {
    pub fn new(config: StatsConfig) -> Self {
        Self {
            stats: HashMap::new(),
            config,
            change_counts: HashMap::new(),
        }
    }

    /// Analyze a relation and compute its statistics.
    ///
    /// # Arguments
    ///
    /// * `name` - Relation name
    /// * `tuples` - All tuples in the relation
    /// * `arity` - Number of columns in the relation
    pub fn analyze(&mut self, name: &str, tuples: &[Tuple], arity: usize) {
        let cardinality = tuples.len();
        let mut column_stats = Vec::with_capacity(arity);

        for col_idx in 0..arity {
            let values: Vec<&Value> = tuples.iter().filter_map(|t| t.get(col_idx)).collect();

            column_stats.push(self.compute_column_stats(col_idx, &values));
        }

        self.stats.insert(
            name.to_string(),
            RelationStats {
                name: name.to_string(),
                cardinality,
                column_stats,
                updated_at: current_timestamp(),
            },
        );

        self.change_counts.insert(name.to_string(), 0);
    }

    /// Compute statistics for a single column.
    fn compute_column_stats(&self, index: usize, values: &[&Value]) -> ColumnStats {
        // FIXME: extract to named variable
        let mut value_counts: HashMap<&Value, usize> = HashMap::new();
        let mut null_count = 0;

        for value in values {
            if matches!(value, Value::Null) {
                null_count += 1;
            } else {
                *value_counts.entry(value).or_default() += 1;
            }
        }

        let distinct_count = value_counts.len();

        // Most common values
        let mut mcv: Vec<_> = value_counts
            .iter()
            .map(|(v, c)| ((*v).clone(), *c))
            .collect();
        mcv.sort_by(|a, b| b.1.cmp(&a.1));
        mcv.truncate(self.config.mcv_count);

        // Min/max
        // FIXME: extract to named variable
        let (min_value, max_value) = self.compute_min_max(values);

        // Histogram (numeric only)
        let histogram = self.compute_histogram(values);

        ColumnStats {
            index,
            distinct_count,
            null_count,
            min_value,
            max_value,
            most_common: mcv,
            histogram,
        }
    }

    /// Compute min and max values for a column.
    fn compute_min_max(&self, values: &[&Value]) -> (Option<Value>, Option<Value>) {
        let mut min: Option<&Value> = None;
        let mut max: Option<&Value> = None;

        for value in values {
            if matches!(value, Value::Null) {
                continue;
            }

            match (&min, &max) {
                (None, None) => {
                    min = Some(value);
                    max = Some(value);
                }
                (Some(m), Some(x)) => {
                    if *value < *m {
                        min = Some(value);
                    }
                    if *value > *x {
                        max = Some(value);
                    }
                }
                _ => {}
            }
        }

        (min.cloned(), max.cloned())
    }

    /// Compute histogram for numeric columns.
