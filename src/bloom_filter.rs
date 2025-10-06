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
pub struct BloomFilter {
    /// Bit array stored as u64 words
    bits: Vec<u64>,
    /// Total number of bits (m)
    num_bits: usize,
    /// Number of hash functions (k)
    num_hashes: usize,
    /// Number of elements inserted (for statistics)
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with optimal parameters.
    ///
    /// # Arguments
    ///
    /// * `expected_elements` - Expected number of elements to insert (n).
    ///   If you insert more than this, the false positive rate will increase.
    ///
    /// * `false_positive_rate` - Target false positive rate (p).
    ///   Common values: 0.01 (1%), 0.001 (0.1%).
    ///
    /// # Optimal Parameters
    ///
    /// The optimal number of bits is: m = -n * ln(p) / (ln(2)^2)
    /// The optimal number of hash functions is: k = (m/n) * ln(2)
    ///
    /// # Example
    ///
    /// ```
    /// use inputlayer::bloom_filter::BloomFilter;
    ///
    /// // Filter for 10000 elements with 1% false positive rate
    /// let filter = BloomFilter::new(10000, 0.01);
    /// // This will use approximately 95851 bits (12KB) and 7 hash functions
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `expected_elements` is 0 or `false_positive_rate` is not
    /// in the range (0, 1).
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        assert!(expected_elements > 0, "expected_elements must be > 0");
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "false_positive_rate must be in (0, 1)"
        );

        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let n = expected_elements as f64;
        let p = false_positive_rate;
        let num_bits = (-(n) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;

        // Minimum 64 bits to ensure at least one word
        let num_bits = num_bits.max(64);

        // Optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / n) * 2.0_f64.ln()).ceil() as usize;
        // Bound between 1 and 16 for practical performance
        let num_hashes = num_hashes.clamp(1, 16);

        // Round up to multiple of 64 for word alignment
        let num_words = num_bits.div_ceil(64);

        Self {
            bits: vec![0u64; num_words],
            num_bits: num_words * 64,
            num_hashes,
            count: 0,
        }
    }

    /// Create a Bloom filter with explicit parameters.
    ///
    /// This is primarily useful for testing to create filters with
    /// specific sizes.
    ///
    /// # Arguments
    ///
    /// * `num_bits` - Number of bits (will be rounded up to multiple of 64)
    /// * `num_hashes` - Number of hash functions to use
    ///
    /// # Example
    ///
    /// ```
    /// use inputlayer::bloom_filter::BloomFilter;
    ///
    /// // Create a small filter for testing
    /// let filter = BloomFilter::with_params(1024, 7);
    /// ```
    pub fn with_params(num_bits: usize, num_hashes: usize) -> Self {
        let num_words = num_bits.max(64).div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_bits: num_words * 64,
            num_hashes: num_hashes.clamp(1, 32),
            count: 0,
        }
    }

    /// Insert a value into the Bloom filter.
    ///
    /// After insertion, `might_contain` will always return `true` for
    /// this value (no false negatives).
    ///
    /// # Type Requirements
    ///
    /// The value must implement `Hash`. For compound keys (like tuples),
    /// ensure all components implement `Hash`.
    ///
    /// # Example
    ///
    /// ```
    /// use inputlayer::bloom_filter::BloomFilter;
    ///
    /// let mut filter = BloomFilter::new(100, 0.01);
    ///
    /// // Insert various types
    /// filter.insert(&42i64);
    /// filter.insert(&"hello");
    /// filter.insert(&vec![1, 2, 3]);  // Vectors implement Hash
    /// ```
    pub fn insert<T: Hash>(&mut self, value: &T) {
        let (h1, h2) = self.hash_pair(value);

        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;
            self.bits[word_idx] |= 1u64 << bit_offset;
        }

        self.count += 1;
    }

    /// Check if a value might be in the Bloom filter.
    ///
    /// # Returns
    ///
    /// - `false` - The value is definitely NOT in the set (no false negatives)
    /// - `true` - The value MIGHT be in the set (possible false positive)
    ///
    /// # Performance
    ///
    /// This is an O(k) operation where k is the number of hash functions,
    /// typically 7-10 for a 1% false positive rate.
    ///
    /// # Example
    ///
    /// ```
    /// use inputlayer::bloom_filter::BloomFilter;
    ///
    /// let mut filter = BloomFilter::new(100, 0.01);
    /// filter.insert(&"hello");
    ///
    /// assert!(filter.might_contain(&"hello"));   // true - definitely present
    /// // filter.might_contain(&"world") could be true or false
    /// ```
    pub fn might_contain<T: Hash>(&self, value: &T) -> bool {
        let (h1, h2) = self.hash_pair(value);

        for i in 0..self.num_hashes {
            let bit_idx = self.get_bit_index(h1, h2, i);
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;

            if (self.bits[word_idx] & (1u64 << bit_offset)) == 0 {
                return false; // Definitely not present
            }
        }

        true // Might be present
    }

    /// Estimate the current false positive rate.
    ///
    /// This is calculated based on the actual fill ratio of the bit array,
    /// not the target rate.
    ///
    /// # Formula
    ///
    /// FP rate ~= (set_bits / total_bits)^k
    ///
    /// # Returns
    ///
    /// Estimated false positive rate in the range [0, 1].
    pub fn estimated_false_positive_rate(&self) -> f64 {
        let set_bits = self
            .bits
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum::<usize>();

        let fill_ratio = set_bits as f64 / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Get the number of elements that have been inserted.
    ///
    /// Note: This counts insertions, not unique elements. Inserting
    /// the same element twice increments this counter twice.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Clear all elements from the filter.
    ///
    /// This resets the filter to its initial empty state.
    /// The capacity (number of bits) remains unchanged.
    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.count = 0;
    }

    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

