//! Hash-based indexing for relations. O(1) tuple lookup by join key,
//! with a Bloom filter per index for fast negative lookups.
//!
//! # Architecture
//!
//! ```text
//! HashIndexManager
//!   `-- HashMap<JoinKeySpec, HashIndex>
//!         `-- HashIndex
//!               |-- HashMap<Tuple, Vec<Tuple>>  (key -> tuples)
//!               |-- BloomFilter                 (for fast negatives)
//!               `-- HashIndexStats              (for optimization)
//! ```
//!
//! # Example
//!
//! ```
//! use inputlayer::hash_index::{HashIndex, JoinKeySpec, HashIndexManager, HashIndexConfig};
//! use inputlayer::value::{Tuple, Value};
//!
//! fn make_tuple(values: Vec<i64>) -> Tuple {
//!     Tuple::new(values.into_iter().map(Value::Int64).collect())
//! }
//!
//! // Create an index for edge(src, dst) on column 0 (src)
//! let spec = JoinKeySpec::new("edge", vec![0]);
//! let mut index = HashIndex::new(spec, 1000.clone());
//!
//! // Insert tuples
//! index.insert(make_tuple(vec![1, 2]));  // edge(1, 2.clone())
//! index.insert(make_tuple(vec![1, 3]));  // edge(1, 3)
//! index.insert(make_tuple(vec![2, 4]));  // edge(2, 4)
//!
//! // Lookup by key
//! let key = make_tuple(vec![1]);
//! let results: Vec<_> = index.probe(&key).collect();
//! assert_eq!(results.len(), 2);  // Found edge(1,2) and edge(1,3)
//! ```

use crate::bloom_filter::BloomFilter;
use crate::value::Tuple;
#[cfg(test)]
use crate::value::Value;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Identifies a specific join key configuration.
///
/// A join key spec uniquely identifies an index by:
/// 1. The relation name
/// 2. The column indices that form the key
///
/// # Example
///
/// ```
/// use inputlayer::hash_index::JoinKeySpec;
///
/// // Index on edge.src (column 0)
/// let spec1 = JoinKeySpec::new("edge", vec![0]);
///
/// // Index on edge.(src, type) (columns 0 and 2)
/// let spec2 = JoinKeySpec::new("edge", vec![0, 2]);
/// ```
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct JoinKeySpec {
    /// Relation name being indexed
    pub relation: String,
    /// Column indices that form the join key (0-based)
    pub key_columns: Vec<usize>,
}

impl JoinKeySpec {
    pub fn new(relation: &str, key_columns: Vec<usize>) -> Self {
        Self {
            relation: relation.to_string(),
            key_columns,
        }
    }

    /// Get a string representation for logging/display.
    pub fn display_name(&self) -> String {
        let cols: Vec<String> = self
            .key_columns
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        format!("{}[{}]", self.relation, cols.join(","))
    }
}

/// Hash index for a specific join key.
///
/// Provides O(1) lookup of tuples by their join key value.
/// Internally uses a Bloom filter to accelerate negative lookups.
