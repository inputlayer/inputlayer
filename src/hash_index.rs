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
//! let mut index = HashIndex::new(spec, 1000);
//!
//! // Insert tuples
//! index.insert(make_tuple(vec![1, 2]));  // edge(1, 2)
//! index.insert(make_tuple(vec![1, 3]));  // edge(1, 3)
//! index.insert(make_tuple(vec![2, 4]));  // edge(2, 4)
//!
//! // Lookup by key
//! let key = make_tuple(vec![1]);
//! let results: Vec<_> = index.probe(&key).collect();
//! assert_eq!(results.len(), 2);  // Found edge(1,2) and edge(1,3)
//! ```

use crate::bloom_filter::BloomFilter;
