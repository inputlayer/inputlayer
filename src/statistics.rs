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
    fn compute_histogram(&self, values: &[&Value]) -> Option<Histogram> {
        // Extract numeric values
        let numeric_values: Vec<f64> = values
            .iter()
            .filter_map(|v| match v {
                Value::Int64(i) => Some(*i as f64),
                Value::Float64(f) => Some(*f),
                _ => None,
            })
            .collect();

        if numeric_values.is_empty() {
            return None;
        }

        let mut sorted = numeric_values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let bucket_size = sorted.len().div_ceil(self.config.histogram_buckets);
        let bucket_size = bucket_size.max(1);

        let mut boundaries = Vec::new();
        let mut counts = Vec::new();

        for chunk in sorted.chunks(bucket_size) {
            if let Some(first) = chunk.first() {
                boundaries.push(Value::Float64(*first));
                counts.push(chunk.len());
            }
        }

        if let Some(last) = sorted.last() {
            boundaries.push(Value::Float64(*last));
        }

        Some(Histogram { boundaries, counts })
    }

    pub fn get(&self, name: &str) -> Option<&RelationStats> {
        self.stats.get(name)
    }

    /// Record a change to a relation.
    ///
    /// When changes exceed the threshold, returns true to indicate
    /// statistics should be refreshed.
    pub fn record_change(&mut self, name: &str) -> bool {
        let count = self.change_counts.entry(name.to_string()).or_default();
        *count += 1;
        *count >= self.config.auto_update_threshold
    }

    /// Estimate selectivity for a join between two relations.
    ///
    /// # Arguments
    ///
    /// * `left_rel` - Left relation name
    /// * `left_keys` - Join key column indices in left relation
    /// * `right_rel` - Right relation name
    /// * `right_keys` - Join key column indices in right relation
    ///
    /// # Returns
    ///
    /// Estimated fraction of the cross-product that will be in the result.
    /// For example, 0.01 means ~1% of (left × right) tuples will join.
    pub fn estimate_join_selectivity(
        &self,
        left_rel: &str,
        left_keys: &[usize],
        right_rel: &str,
        right_keys: &[usize],
    ) -> f64 {
        let left_stats = match self.get(left_rel) {
            Some(s) => s,
            None => return 0.1, // Default estimate when no stats
        };

        let right_stats = match self.get(right_rel) {
            Some(s) => s,
            None => return 0.1,
        };

        // Estimate based on distinct values in join keys
        // Formula: selectivity ~= 1 / max(NDV_left, NDV_right)
        // where NDV = number of distinct values
        let left_distinct: usize = left_keys
            .iter()
            .filter_map(|&k| left_stats.column_stats.get(k))
            .map(|s| s.distinct_count.max(1))
            .max()
            .unwrap_or(1);

        let right_distinct: usize = right_keys
            .iter()
            .filter_map(|&k| right_stats.column_stats.get(k))
            .map(|s| s.distinct_count.max(1))
            .max()
            .unwrap_or(1);

        1.0 / (left_distinct.max(right_distinct) as f64)
    }

    /// Estimate the result cardinality of a join.
    ///
    /// # Formula
    ///
    /// |A JOIN B| ~= |A| × |B| × selectivity
    pub fn estimate_join_cardinality(
        &self,
        left_rel: &str,
        left_keys: &[usize],
        right_rel: &str,
        right_keys: &[usize],
    ) -> usize {
        let left_card = self.get(left_rel).map_or(1000, |s| s.cardinality);
        let right_card = self.get(right_rel).map_or(1000, |s| s.cardinality);

        let selectivity =
            self.estimate_join_selectivity(left_rel, left_keys, right_rel, right_keys);

        ((left_card as f64) * (right_card as f64) * selectivity).ceil() as usize
    }

    /// Estimate selectivity for a filter predicate.
    ///
    /// # Arguments
    ///
    /// * `relation` - Relation name
    /// * `column` - Column index being filtered
    /// * `value` - Filter value
    /// * `op` - Comparison operator ("=", "<", ">", "<=", ">=", "!=")
    ///
    /// # Returns
    ///
    /// Estimated fraction of tuples that pass the filter.
    pub fn estimate_filter_selectivity(
        &self,
        relation: &str,
        column: usize,
        value: &Value,
        op: &str,
    ) -> f64 {
        let stats = match self.get(relation) {
            Some(s) => s,
            None => return 0.5, // Default 50%
        };

        let col_stats = match stats.column_stats.get(column) {
            Some(s) => s,
            None => return 0.5,
        };

        match op {
            "=" => {
                // Check MCV first
                if let Some((_, freq)) = col_stats.most_common.iter().find(|(v, _)| v == value) {
                    return *freq as f64 / stats.cardinality.max(1) as f64;
                }
                // Default: 1/NDV
                1.0 / col_stats.distinct_count.max(1) as f64
            }
            "!=" => 1.0 - self.estimate_filter_selectivity(relation, column, value, "="),
            "<" | "<=" => {
                // Use histogram if available
                if let Some(ref hist) = col_stats.histogram {
                    return self.estimate_range_selectivity(hist, value, op);
                }
                // Default: 33%
                0.33
            }
            ">" | ">=" => {
                // Use histogram if available
                if let Some(ref hist) = col_stats.histogram {
                    return self.estimate_range_selectivity(hist, value, op);
                }
                // Default: 33%
                0.33
            }
            _ => 0.5,
        }
    }

    /// Estimate selectivity for a range predicate using histogram.
    fn estimate_range_selectivity(&self, hist: &Histogram, value: &Value, op: &str) -> f64 {
        let total: usize = hist.counts.iter().sum();
        if total == 0 {
            return 0.5;
        }

        let target = match value {
            Value::Int64(i) => *i as f64,
            Value::Float64(f) => *f,
            _ => return 0.5,
        };

        let mut cumulative = 0;
        for (i, boundary) in hist.boundaries.iter().enumerate() {
            let bound_val = match boundary {
                Value::Float64(f) => *f,
                _ => continue,
            };

            if target <= bound_val {
                let frac = cumulative as f64 / total as f64;
                return match op {
                    "<" | "<=" => frac,
                    ">" | ">=" => 1.0 - frac,
                    _ => 0.5,
                };
            }

            if i < hist.counts.len() {
                cumulative += hist.counts[i];
            }
        }

        // Value is beyond histogram
        match op {
            "<" | "<=" => 1.0,
            ">" | ">=" => 0.0,
            _ => 0.5,
        }
    }

    pub fn has_stats(&self, name: &str) -> bool {
        self.stats.contains_key(name)
    }

    pub fn relation_count(&self) -> usize {
        self.stats.len()
    }

    pub fn remove(&mut self, name: &str) -> bool {
        self.change_counts.remove(name);
        self.stats.remove(name).is_some()
    }
}

impl Default for StatisticsManager {
    fn default() -> Self {
        Self::new(StatsConfig::default())
    }
}

/// Get current timestamp in milliseconds.
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tuple(values: Vec<i64>) -> Tuple {
        Tuple::new(values.into_iter().map(Value::Int64).collect())
    }

    // HAPPY PATH TESTS
    #[test]
    fn test_stats_cardinality_correct() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples = vec![
            make_tuple(vec![1, 10]),
            make_tuple(vec![2, 20]),
            make_tuple(vec![3, 30]),
        ];
        manager.analyze("test", &tuples, 2);

        let stats = manager.get("test").unwrap();
        assert_eq!(stats.cardinality, 3);
    }

    #[test]
    fn test_stats_distinct_count_correct() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples = vec![
            make_tuple(vec![1, 10]),
            make_tuple(vec![1, 20]), // Duplicate key
            make_tuple(vec![2, 30]),
        ];
        manager.analyze("test", &tuples, 2);

        let stats = manager.get("test").unwrap();
        assert_eq!(stats.column_stats[0].distinct_count, 2); // 1, 2
        assert_eq!(stats.column_stats[1].distinct_count, 3); // 10, 20, 30
    }

    #[test]
    fn test_stats_min_max_correct() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples = vec![
            make_tuple(vec![5, 100]),
            make_tuple(vec![1, 300]),
            make_tuple(vec![9, 200]),
        ];
        manager.analyze("test", &tuples, 2);

        let stats = manager.get("test").unwrap();
        assert_eq!(stats.column_stats[0].min_value, Some(Value::Int64(1)));
        assert_eq!(stats.column_stats[0].max_value, Some(Value::Int64(9)));
    }

    #[test]
    fn test_stats_most_common_values() {
        let mut manager = StatisticsManager::new(StatsConfig {
            mcv_count: 2,
            ..Default::default()
        });
        let tuples = vec![
            make_tuple(vec![1]),
            make_tuple(vec![1]),
            make_tuple(vec![1]), // 3x
            make_tuple(vec![2]),
            make_tuple(vec![2]), // 2x
            make_tuple(vec![3]), // 1x
        ];
        manager.analyze("test", &tuples, 1);

        let stats = manager.get("test").unwrap();
        let mcv = &stats.column_stats[0].most_common;
        assert_eq!(mcv.len(), 2);
        assert_eq!(mcv[0], (Value::Int64(1), 3));
        assert_eq!(mcv[1], (Value::Int64(2), 2));
    }

    #[test]
    fn test_stats_histogram_created() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples: Vec<_> = (0..100).map(|i| make_tuple(vec![i])).collect();
        manager.analyze("test", &tuples, 1);

        let stats = manager.get("test").unwrap();
        assert!(stats.column_stats[0].histogram.is_some());
    }

    // EDGE CASE TESTS
    #[test]
    fn test_stats_empty_relation() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        manager.analyze("empty", &[], 2);

        let stats = manager.get("empty").unwrap();
        assert_eq!(stats.cardinality, 0);
    }

    #[test]
    fn test_stats_single_tuple() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        manager.analyze("single", &[make_tuple(vec![42])], 1);

        let stats = manager.get("single").unwrap();
        assert_eq!(stats.cardinality, 1);
        assert_eq!(stats.column_stats[0].distinct_count, 1);
    }

    #[test]
    fn test_stats_all_null_column() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples = vec![Tuple::new(vec![Value::Null]), Tuple::new(vec![Value::Null])];
        manager.analyze("nulls", &tuples, 1);

        let stats = manager.get("nulls").unwrap();
        assert_eq!(stats.column_stats[0].null_count, 2);
        assert_eq!(stats.column_stats[0].distinct_count, 0);
    }

    #[test]
    fn test_stats_all_same_value() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples = vec![
            make_tuple(vec![42]),
            make_tuple(vec![42]),
            make_tuple(vec![42]),
        ];
        manager.analyze("same", &tuples, 1);

        let stats = manager.get("same").unwrap();
        assert_eq!(stats.column_stats[0].distinct_count, 1);
    }

    #[test]
    fn test_stats_high_cardinality() {
        let mut manager = StatisticsManager::new(StatsConfig::default());
        let tuples: Vec<_> = (0..10000).map(|i| make_tuple(vec![i])).collect();
        manager.analyze("big", &tuples, 1);

        let stats = manager.get("big").unwrap();
        assert_eq!(stats.cardinality, 10000);
        assert_eq!(stats.column_stats[0].distinct_count, 10000);
    }

    // JOIN SELECTIVITY TESTS
    #[test]
    fn test_stats_join_selectivity_estimation() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        // Left: 1000 tuples, 100 distinct keys
        let left: Vec<_> = (0..1000).map(|i| make_tuple(vec![i % 100, i])).collect();
        manager.analyze("left", &left, 2);

        // Right: 500 tuples, 50 distinct keys (subset of left's keys)
        let right: Vec<_> = (0..500).map(|i| make_tuple(vec![i % 50, i])).collect();
        manager.analyze("right", &right, 2);

        let selectivity = manager.estimate_join_selectivity("left", &[0], "right", &[0]);

        // Selectivity should be approximately 1/100 (larger distinct count)
        assert!(
            selectivity > 0.005 && selectivity < 0.05,
            "Selectivity {} not in expected range",
            selectivity
        );
    }

    #[test]
    fn test_stats_join_cardinality_estimation() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        let left: Vec<_> = (0..100).map(|i| make_tuple(vec![i])).collect();
        manager.analyze("left", &left, 1);

        let right: Vec<_> = (0..50).map(|i| make_tuple(vec![i])).collect();
        manager.analyze("right", &right, 1);

        let cardinality = manager.estimate_join_cardinality("left", &[0], "right", &[0]);

        // Should estimate reasonable cardinality (not 100*50=5000, but ~50-100)
        assert!(cardinality < 500, "Cardinality {} too high", cardinality);
    }

    #[test]
    fn test_stats_join_selectivity_missing_stats() {
        let manager = StatisticsManager::new(StatsConfig::default());

        // No stats for these relations
        let selectivity = manager.estimate_join_selectivity("left", &[0], "right", &[0]);

        // Should return default (0.1)
        assert!((selectivity - 0.1).abs() < 0.001);
    }

    // FILTER SELECTIVITY TESTS
    #[test]
    fn test_stats_filter_equality_selectivity() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        // 100 tuples with 10 distinct values (each appears 10 times)
        let tuples: Vec<_> = (0..100).map(|i| make_tuple(vec![i % 10])).collect();
        manager.analyze("test", &tuples, 1);

        // Equality on a value should be ~1/10 = 0.1
        let selectivity = manager.estimate_filter_selectivity("test", 0, &Value::Int64(5), "=");

        assert!(
            selectivity > 0.05 && selectivity < 0.2,
            "Selectivity {} not in expected range",
            selectivity
        );
    }

    #[test]
    fn test_stats_filter_inequality_selectivity() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        let tuples: Vec<_> = (0..100).map(|i| make_tuple(vec![i % 10])).collect();
        manager.analyze("test", &tuples, 1);

        // Inequality should be ~1 - 1/10 = 0.9
        let selectivity = manager.estimate_filter_selectivity("test", 0, &Value::Int64(5), "!=");

        assert!(
            selectivity > 0.8 && selectivity < 1.0,
            "Selectivity {} not in expected range",
            selectivity
        );
    }

    // MANAGER TESTS
    #[test]
    fn test_stats_has_stats() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        assert!(!manager.has_stats("test"));

        manager.analyze("test", &[make_tuple(vec![1])], 1);
        assert!(manager.has_stats("test"));
    }

    #[test]
    fn test_stats_relation_count() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        assert_eq!(manager.relation_count(), 0);

        manager.analyze("a", &[make_tuple(vec![1])], 1);
        assert_eq!(manager.relation_count(), 1);

        manager.analyze("b", &[make_tuple(vec![2])], 1);
        assert_eq!(manager.relation_count(), 2);
    }

    #[test]
    fn test_stats_remove() {
        let mut manager = StatisticsManager::new(StatsConfig::default());

        manager.analyze("test", &[make_tuple(vec![1])], 1);
        assert!(manager.has_stats("test"));

        let removed = manager.remove("test");
        assert!(removed);
        assert!(!manager.has_stats("test"));

        // Remove again should return false
        let removed = manager.remove("test");
        assert!(!removed);
    }

    #[test]
    fn test_stats_record_change() {
        let mut manager = StatisticsManager::new(StatsConfig {
            auto_update_threshold: 3,
            ..Default::default()
        });

        manager.analyze("test", &[make_tuple(vec![1])], 1);

        // First two changes should return false
        assert!(!manager.record_change("test"));
        assert!(!manager.record_change("test"));

        // Third change should return true (threshold reached)
        assert!(manager.record_change("test"));
    }

    #[test]
    fn test_stats_default_manager() {
        let manager = StatisticsManager::default();
        assert_eq!(manager.relation_count(), 0);
    }
}
