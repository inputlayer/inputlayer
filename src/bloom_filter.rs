//! Bloom filter implementation for efficient set membership testing.
//!
//! Used by SIP (Sideways Information Passing) to quickly test if a
//! join key might exist in a relation without materializing the full
//! relation.
//!
//! # Properties
//!
//! - No false negatives: If `might_contain` returns `false`, the
//!   element is definitely not in the set.
//! - Possible false positives: If `might_contain` returns `true`,
//!   the element might or might not be in the set.
//! - Space efficient: Uses ~10 bits per element for 1% FP rate.
//!
//! # Example
//!
//! ```
//! use inputlayer::bloom_filter::{BloomFilter, BloomFilterBuilder};
//!
//! // Create a filter expecting 10000 elements with 1% false positive rate
//! let mut filter = BloomFilter::new(10000, 0.01);
//!
//! // Insert some values
//! filter.insert(&"hello");
//! filter.insert(&"world");
//!
//! // Check membership
//! assert!(filter.might_contain(&"hello"));  // true (definitely present)
//! assert!(filter.might_contain(&"world"));  // true (definitely present)
//! // filter.might_contain(&"foo") might return true or false
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A Bloom filter for efficient probabilistic set membership testing.
///
/// # Implementation Details
///
/// This implementation uses double hashing to generate k hash values from
/// two base hashes. The bit array is stored as a vector of u64 words.
///
/// ## Memory Layout
///
/// For a filter with m bits, we allocate ceil(m/64) u64 words.
/// Each word stores 64 bits of the filter.
///
/// ## Hash Function
///
/// We use double hashing: h_i(x) = h1(x) + i * h2(x) mod m
///
/// This is equivalent to using k independent hash functions but requires
/// only two hash computations.
#[derive(Clone, Debug)]
