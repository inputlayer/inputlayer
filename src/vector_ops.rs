//! Vector Operations Module
//!
//! High-performance vector operations for similarity search in Datalog.
//! Includes distance functions, LSH hashing, and vector utilities.
//!
//! # Performance Considerations
//! - Uses f32 for memory efficiency (embeddings rarely need f64 precision)
//! - Iterator-based implementations enable SIMD autovectorization
//! - LSH hyperplanes generated on-the-fly for memory efficiency
//! - All functions are pure and thread-safe

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

// Orderable Float Wrapper for BinaryHeap
/// Wrapper for f64 that implements Ord for use in `BinaryHeap`.
/// NaN values are treated as less than all other values.
#[derive(Clone, Copy, PartialEq)]
struct OrdF64(f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or_else(|| match (self.0.is_nan(), other.0.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                (false, false) => unreachable!(),
            })
    }
}

/// Wrapper for (score, item) pairs that implements Ord based only on score.
/// This allows us to use `BinaryHeap` without requiring T: Ord.
struct HeapEntry<T> {
    score: OrdF64,
    item: T,
}

impl<T> PartialEq for HeapEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl<T> Eq for HeapEntry<T> {}

impl<T> PartialOrd for HeapEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for HeapEntry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.cmp(&other.score)
    }
}

// Distance Functions
/// Compute Euclidean (L2) distance between two vectors.
///
/// Formula: d(a, b) = sqrt(sum((a\[i\] - b\[i\])^2))
///
/// # Performance
/// - O(n) where n is vector dimension
/// - Uses iterator fusion for cache efficiency
/// - Compiler can autovectorize the inner loop
///
/// # Panics
/// Returns `f64::INFINITY` if vectors have different lengths.
#[inline]
pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    let sum_sq: f32 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum();

    f64::from(sum_sq).sqrt()
}

/// Compute squared Euclidean distance (avoids sqrt for comparisons).
///
/// Use this when you only need to compare distances, not absolute values.
/// This is faster because it avoids the sqrt operation.
#[inline]
pub fn euclidean_distance_squared(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    let sum_sq: f32 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum();

    f64::from(sum_sq)
}

/// Compute cosine distance between two vectors.
///
/// Formula: d(a, b) = 1 - (a · b) / (||a|| * ||b||)
///
/// Returns a value in [0, 2] where:
/// - 0 = identical direction
/// - 1 = orthogonal
/// - 2 = opposite direction
///
/// # Edge Cases
/// - Returns 0.0 if either vector is zero (treats as identical)
/// - Returns `f64::INFINITY` for mismatched dimensions
#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    let mut dot_product: f32 = 0.0;
    let mut norm_a_sq: f32 = 0.0;
    let mut norm_b_sq: f32 = 0.0;

    // Single pass through both vectors for cache efficiency
    for (x, y) in a.iter().zip(b.iter()) {
        dot_product += x * y;
        norm_a_sq += x * x;
        norm_b_sq += y * y;
    }

    let norm_a = f64::from(norm_a_sq).sqrt();
    let norm_b = f64::from(norm_b_sq).sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0; // Treat zero vectors as identical
    }

    let similarity = f64::from(dot_product) / (norm_a * norm_b);
    // Clamp to handle floating point errors
    1.0 - similarity.clamp(-1.0, 1.0)
}

/// Compute dot product of two vectors.
///
/// Formula: a · b = sum(a\[i\] * b\[i\])
///
/// # Returns
/// - The scalar dot product
/// - 0.0 for mismatched dimensions
#[inline]
pub fn dot_product(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return 0.0;
    }

    a.iter()
        .zip(b.iter())
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum()
}

/// Compute Manhattan (L1) distance between two vectors.
///
/// Formula: d(a, b) = sum(|a\[i\] - b\[i\]|)
///
/// # Performance
/// - O(n) where n is vector dimension
/// - Good for sparse vectors
#[inline]
pub fn manhattan_distance(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    a.iter()
        .zip(b.iter())
        .map(|(x, y)| f64::from(*x - *y).abs())
        .sum()
}

// Utility Functions (Hamming, Abs)
/// Compute Hamming distance between two integers.
///
/// Counts the number of bit positions where the two integers differ.
/// Useful for comparing perceptual hashes (pHash, dHash) for image similarity.
///
/// # Arguments
/// * `a` - First integer (e.g., perceptual hash)
/// * `b` - Second integer (e.g., perceptual hash)
///
/// # Returns
/// Number of differing bits (0 to 64 for i64)
///
/// # Example
/// ```rust
/// use inputlayer::vector_ops::hamming_distance;
///
/// let h1 = 0b1010_1010i64;
/// let h2 = 0b1010_1000i64;  // Differs in 1 bit
/// assert_eq!(hamming_distance(h1, h2), 1);
///
/// // For perceptual hashes, typically:
/// // - distance < 5: very similar images
/// // - distance < 10: similar images
/// // - distance >= 10: different images
/// ```
#[inline]
pub fn hamming_distance(a: i64, b: i64) -> i64 {
    i64::from((a ^ b).count_ones())
}

/// Compute absolute value of an integer.
///
/// # Note
/// Returns `i64::MAX` for `i64::MIN` (since -`i64::MIN` overflows).
#[inline]
pub fn abs_i64(x: i64) -> i64 {
    x.saturating_abs()
}

/// Compute absolute value of a float.
#[inline]
