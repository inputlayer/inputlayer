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
