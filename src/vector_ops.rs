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
pub fn abs_f64(x: f64) -> f64 {
    x.abs()
}

// Vector Error Type for Checked Operations
/// Error type for vector operations that can fail due to dimension mismatch.
///
/// Use checked function variants (e.g., `euclidean_distance_checked`) when you
/// want explicit error handling instead of silent INFINITY/0.0 returns.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VectorError {
    /// Vectors have different dimensions
    #[error("Vector dimension mismatch: expected {expected}-dimensional, got {got}-dimensional")]
    DimensionMismatch {
        /// Dimension of the first vector
        expected: usize,
        /// Dimension of the second vector
        got: usize,
    },
    /// Vector is empty
    #[error("Cannot compute distance on empty vector")]
    EmptyVector,
}

// Checked Distance Functions (Return Result<f64, VectorError>)
/// Compute Euclidean distance with explicit error handling.
///
/// Returns `Err(VectorError::DimensionMismatch)` if vectors have different lengths,
/// instead of silently returning INFINITY.
#[inline]
pub fn euclidean_distance_checked(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    if a.is_empty() && b.is_empty() {
        return Ok(0.0);
    }
    if a.len() != b.len() {
        return Err(VectorError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }

    let sum_sq: f32 = a
        .iter()
        .zip(b.iter())
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum();

    Ok(f64::from(sum_sq).sqrt())
}

/// Compute cosine distance with explicit error handling.
///
/// Returns `Err(VectorError::DimensionMismatch)` if vectors have different lengths.
#[inline]
pub fn cosine_distance_checked(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    if a.is_empty() && b.is_empty() {
        return Ok(0.0);
    }
    if a.len() != b.len() {
        return Err(VectorError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }

    let mut dot_product: f32 = 0.0;
    let mut norm_a_sq: f32 = 0.0;
    let mut norm_b_sq: f32 = 0.0;

    for (x, y) in a.iter().zip(b.iter()) {
        dot_product += x * y;
        norm_a_sq += x * x;
        norm_b_sq += y * y;
    }

    let norm_a = f64::from(norm_a_sq).sqrt();
    let norm_b = f64::from(norm_b_sq).sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return Ok(0.0); // Treat zero vectors as identical
    }

    let similarity = f64::from(dot_product) / (norm_a * norm_b);
    Ok(1.0 - similarity.clamp(-1.0, 1.0))
}

/// Compute dot product with explicit error handling.
///
/// Returns `Err(VectorError::DimensionMismatch)` if vectors have different lengths.
#[inline]
pub fn dot_product_checked(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    if a.is_empty() && b.is_empty() {
        return Ok(0.0);
    }
    if a.len() != b.len() {
        return Err(VectorError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }

    Ok(a.iter()
        .zip(b.iter())
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum())
}

/// Compute Manhattan distance with explicit error handling.
///
/// Returns `Err(VectorError::DimensionMismatch)` if vectors have different lengths.
#[inline]
pub fn manhattan_distance_checked(a: &[f32], b: &[f32]) -> Result<f64, VectorError> {
    if a.is_empty() && b.is_empty() {
        return Ok(0.0);
    }
    if a.len() != b.len() {
        return Err(VectorError::DimensionMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }

    Ok(a.iter()
        .zip(b.iter())
        .map(|(x, y)| f64::from(*x - *y).abs())
        .sum())
}

// Vector Utilities
/// Compute the L2 norm (magnitude) of a vector.
#[inline]
pub fn vector_norm(v: &[f32]) -> f64 {
    let sum_sq: f32 = v.iter().map(|x| x * x).sum();
    f64::from(sum_sq).sqrt()
}

/// Normalize a vector to unit length.
///
/// Returns a new vector with ||v|| = 1.
/// Returns zero vector if input is zero vector.
pub fn normalize(v: &[f32]) -> Vec<f32> {
    let norm = vector_norm(v);
    if norm == 0.0 {
        return vec![0.0; v.len()];
    }
    let norm_f32 = norm as f32;
    v.iter().map(|x| x / norm_f32).collect()
}

/// Add two vectors element-wise.
///
/// Returns None if dimensions don't match.
pub fn vector_add(a: &[f32], b: &[f32]) -> Option<Vec<f32>> {
    if a.len() != b.len() {
        return None;
    }
    Some(a.iter().zip(b.iter()).map(|(x, y)| x + y).collect())
}

/// Scale a vector by a scalar.
pub fn vector_scale(v: &[f32], scalar: f32) -> Vec<f32> {
    v.iter().map(|x| x * scalar).collect()
}

/// Get the dimension of a vector.
#[inline]
pub fn vector_dim(v: &[f32]) -> usize {
    v.len()
}

// Int8 Quantization
/// Method for quantizing f32 vectors to int8.
/// Different methods offer different trade-offs:
/// - Linear: Maps [min, max] to [-128, 127], best for non-centered data
/// - `MinMax`: Alias for Linear (same algorithm)
/// - Symmetric: Maps [-`max_abs`, `max_abs`] to [-127, 127], preserves zero
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantizationMethod {
    /// Linear scaling: maps value range [min, max] to [-128, 127]
    Linear,
    /// `MinMax` scaling: same as Linear
    MinMax,
    /// Symmetric scaling: maps [-`max_abs`, `max_abs`] to [-127, 127], preserves zero
    Symmetric,
}

/// Quantize f32 vector to int8 using linear scaling.
///
/// Maps the value range [min, max] to [-128, 127].
/// This method uses the full int8 range but doesn't preserve zero.
///
/// # Example
/// ```rust
/// use inputlayer::vector_ops::quantize_vector_linear;
///
/// let v = vec![0.0, 0.5, 1.0];
/// let q = quantize_vector_linear(&v);
/// assert_eq!(q[0], -128); // min -> -128
/// assert_eq!(q[2], 127);  // max -> 127
/// ```
pub fn quantize_vector_linear(v: &[f32]) -> Vec<i8> {
    if v.is_empty() {
        return Vec::new();
    }

    let min = v.iter().copied().fold(f32::INFINITY, f32::min);
    let max = v.iter().copied().fold(f32::NEG_INFINITY, f32::max);
    let range = max - min;

    if range == 0.0 {
        return vec![0i8; v.len()];
    }

    v.iter()
        .map(|&x| {
            let normalized = (x - min) / range; // [0, 1]
            let scaled = normalized * 255.0 - 128.0; // [-128, 127]
            scaled.round().clamp(-128.0, 127.0) as i8
        })
        .collect()
}

/// Quantize f32 vector to int8 using symmetric scaling.
///
/// Maps [-`max_abs`, `max_abs`] to [-127, 127], preserving zero.
/// This method is preferred when zero is meaningful (e.g., centered embeddings).
///
/// # Example
/// ```rust
/// use inputlayer::vector_ops::quantize_vector_symmetric;
///
/// let v = vec![-1.0, 0.0, 1.0];
/// let q = quantize_vector_symmetric(&v);
/// assert_eq!(q[0], -127); // -max_abs -> -127
/// assert_eq!(q[1], 0);    // 0 -> 0
/// assert_eq!(q[2], 127);  // max_abs -> 127
/// ```
pub fn quantize_vector_symmetric(v: &[f32]) -> Vec<i8> {
    if v.is_empty() {
        return Vec::new();
    }

    let max_abs = v.iter().map(|x| x.abs()).fold(0.0f32, f32::max);

    if max_abs == 0.0 {
        return vec![0i8; v.len()];
    }

    let scale = 127.0 / max_abs;
    v.iter()
        .map(|&x| (x * scale).round().clamp(-127.0, 127.0) as i8)
        .collect()
}

/// Quantize f32 vector to int8 using min-max normalization.
///
/// This is an alias for `quantize_vector_linear`.
#[inline]
pub fn quantize_vector_minmax(v: &[f32]) -> Vec<i8> {
    quantize_vector_linear(v)
}

/// Quantize f32 vector to int8 using the specified method.
///
/// # Arguments
/// * `v` - The f32 vector to quantize
/// * `method` - The quantization method to use
///
/// # Returns
/// An int8 vector with 75% memory savings compared to f32.
pub fn quantize_vector(v: &[f32], method: QuantizationMethod) -> Vec<i8> {
    match method {
        QuantizationMethod::Linear => quantize_vector_linear(v),
        QuantizationMethod::MinMax => quantize_vector_minmax(v),
        QuantizationMethod::Symmetric => quantize_vector_symmetric(v),
    }
}

/// Dequantize int8 vector to f32.
///
/// Without scale factor, simply converts i8 to f32.
/// The user can apply their own scaling if needed.
///
/// Note: This is lossy - the original values cannot be perfectly recovered.
#[inline]
pub fn dequantize_vector(v: &[i8]) -> Vec<f32> {
    v.iter().map(|&x| f32::from(x)).collect()
}

/// Dequantize int8 vector to f32 with explicit scale factor.
///
/// Use this when you track the scale factor externally.
///
/// # Arguments
/// * `v` - The int8 vector to dequantize
/// * `scale` - The scale factor to multiply by
#[inline]
pub fn dequantize_vector_with_scale(v: &[i8], scale: f32) -> Vec<f32> {
    v.iter().map(|&x| f32::from(x) * scale).collect()
}

// Int8 Distance Functions
/// Euclidean distance for int8 vectors.
///
/// Uses i32 accumulation to avoid overflow during squared difference computation.
/// Maximum squared difference per element: (127 - (-128))^2 = 65025
/// Maximum safe vector length without overflow: `i64::MAX` / 65025 ~= 141 trillion elements
///
/// # Returns
/// - The Euclidean distance as f64
/// - `f64::INFINITY` if dimensions don't match
#[inline]
pub fn euclidean_distance_int8(a: &[i8], b: &[i8]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    let sum_sq: i64 = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let diff = i32::from(x) - i32::from(y);
            i64::from(diff * diff)
        })
        .sum();

    (sum_sq as f64).sqrt()
}

/// Cosine distance for int8 vectors.
///
/// Uses i64 accumulation for dot products to avoid overflow.
/// Maximum dot product per element: 127 * 127 = 16129
/// Maximum safe vector length: `i64::MAX` / 16129 ~= 571 trillion elements
///
/// # Returns
/// - Cosine distance in [0, 2] where 0=same direction, 1=orthogonal, 2=opposite
/// - `f64::INFINITY` if dimensions don't match
/// - 1.0 for zero vectors (maximum distance)
#[inline]
pub fn cosine_distance_int8(a: &[i8], b: &[i8]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    let mut dot: i64 = 0;
    let mut norm_a: i64 = 0;
    let mut norm_b: i64 = 0;

    for (&x, &y) in a.iter().zip(b.iter()) {
        let x = i64::from(x);
        let y = i64::from(y);
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    if norm_a == 0 || norm_b == 0 {
        return 1.0; // Maximum distance for zero vectors
    }

    let similarity = (dot as f64) / ((norm_a as f64).sqrt() * (norm_b as f64).sqrt());
    1.0 - similarity.clamp(-1.0, 1.0)
}

/// Dot product for int8 vectors.
///
/// Uses i64 accumulation to avoid overflow.
///
/// # Returns
/// - The dot product as f64
/// - 0.0 if dimensions don't match
#[inline]
pub fn dot_product_int8(a: &[i8], b: &[i8]) -> f64 {
    if a.len() != b.len() {
        return 0.0;
    }

    let sum: i64 = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| i64::from(x) * i64::from(y))
        .sum();

    sum as f64
}

/// Manhattan (L1) distance for int8 vectors.
///
/// Uses i64 accumulation to avoid overflow.
///
/// # Returns
/// - The Manhattan distance as f64
/// - `f64::INFINITY` if dimensions don't match
#[inline]
pub fn manhattan_distance_int8(a: &[i8], b: &[i8]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }

    let sum: i64 = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| i64::from((i32::from(x) - i32::from(y)).abs()))
        .sum();

    sum as f64
}

/// Euclidean distance by dequantizing int8 to f32 first.
///
/// This provides higher accuracy than native int8 distance
/// at the cost of more computation.
#[inline]
pub fn euclidean_distance_dequantized(a: &[i8], b: &[i8]) -> f64 {
    let a_f32 = dequantize_vector(a);
    let b_f32 = dequantize_vector(b);
    euclidean_distance(&a_f32, &b_f32)
}

/// Cosine distance by dequantizing int8 to f32 first.
///
/// This provides higher accuracy than native int8 distance
/// at the cost of more computation.
#[inline]
pub fn cosine_distance_dequantized(a: &[i8], b: &[i8]) -> f64 {
    let a_f32 = dequantize_vector(a);
    let b_f32 = dequantize_vector(b);
    cosine_distance(&a_f32, &b_f32)
}

/// Compute LSH bucket for int8 vector.
///
/// Casts int8 values to f64 for hyperplane dot product computation.
/// Uses the same hyperplanes as f32 vectors for consistency.
///
/// # Arguments
/// * `v` - The int8 vector to hash
/// * `table_idx` - Index of the hash table
/// * `num_hyperplanes` - Number of hyperplanes (bits in the hash)
pub fn lsh_bucket_int8(v: &[i8], table_idx: i64, num_hyperplanes: usize) -> i64 {
    if v.is_empty() || num_hyperplanes == 0 {
        return 0;
    }

    let num_bits = num_hyperplanes.min(62);
    let hyperplanes = get_or_create_hyperplanes(table_idx, num_bits, v.len());

    let mut bucket: i64 = 0;

    for h in 0..hyperplanes.num_hyperplanes {
        let hp = hyperplanes.hyperplane(h);
        let dot: f64 = v
            .iter()
            .zip(hp.iter())
            .map(|(&vi, &hi)| f64::from(vi) * f64::from(hi))
            .sum();
        if dot > 0.0 {
            bucket |= 1i64 << h;
        }
    }

    bucket
}

// Locality Sensitive Hashing (LSH)
/// LSH parameters for a hash table.
#[derive(Debug, Clone)]
pub struct LshParams {
    /// Dimension of input vectors
    pub dimension: usize,
    /// Number of hyperplanes per table (bits in hash)
    pub num_hyperplanes: usize,
}

// LSH Hyperplane Cache
/// Cache key for LSH hyperplanes: (`table_idx`, `num_hyperplanes`, dimension)
type HyperplaneCacheKey = (i64, usize, usize);

/// Cached hyperplanes - Arc for zero-copy sharing across threads.
/// Data layout: hyperplanes[h * dimension + d] = component d of hyperplane h
///
/// Uses `Arc<[f32]>` instead of `Vec<f32>` so that `clone()` is O(1)
/// (atomic refcount increment) instead of O(n) (deep copy).
#[derive(Clone)]
struct CachedHyperplanes {
    data: Arc<[f32]>, // Zero-copy clone via Arc
    num_hyperplanes: usize,
    dimension: usize,
}

impl CachedHyperplanes {
    fn new(data: Vec<f32>, num_hyperplanes: usize, dimension: usize) -> Self {
        debug_assert_eq!(data.len(), num_hyperplanes * dimension);
        Self {
            data: data.into(), // Vec<f32> -> Arc<[f32]>
            num_hyperplanes,
            dimension,
        }
    }

    /// Get a slice for hyperplane h (for efficient dot product computation)
    #[inline]
    fn hyperplane(&self, h: usize) -> &[f32] {
        let start = h * self.dimension;
        &self.data[start..start + self.dimension]
    }
}

/// Global monotonic counter for LRU ordering (avoids syscalls)
static ACCESS_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Get next access timestamp (monotonically increasing, no syscall)
#[inline]
fn next_access_time() -> u64 {
    ACCESS_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Cache entry with atomic access tracking for true LRU eviction.
/// Uses `AtomicU64` so timestamps can be updated through shared references.
struct HyperplaneCacheEntry {
    hyperplanes: CachedHyperplanes,
    last_accessed: AtomicU64,
    access_count: AtomicUsize,
}

impl HyperplaneCacheEntry {
    fn new(hyperplanes: CachedHyperplanes) -> Self {
        Self {
            hyperplanes,
            last_accessed: AtomicU64::new(next_access_time()),
            access_count: AtomicUsize::new(1),
        }
    }

    /// Update access time (can be called through shared reference on read path)
    #[inline]
    fn touch(&self) {
        self.last_accessed
            .store(next_access_time(), Ordering::Relaxed);
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get last access time for LRU comparison
    #[inline]
    fn last_access(&self) -> u64 {
        self.last_accessed.load(Ordering::Relaxed)
    }
}

/// LSH hyperplane cache statistics
#[derive(Debug, Clone, Default)]
pub struct LshCacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub entries: usize,
}

impl LshCacheStats {
    /// Get the cache hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Atomic stats for lock-free updates on fast path
struct AtomicCacheStats {
    hits: AtomicUsize,
    misses: AtomicUsize,
    evictions: AtomicUsize,
}

impl AtomicCacheStats {
    fn new() -> Self {
        Self {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
            evictions: AtomicUsize::new(0),
        }
    }

    fn to_stats(&self, entries: usize) -> LshCacheStats {
        LshCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            entries,
        }
    }

    fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
}

/// Thread-safe LSH hyperplane cache
struct LshHyperplaneCache {
    cache: HashMap<HyperplaneCacheKey, HyperplaneCacheEntry>,
    max_entries: usize,
}

impl LshHyperplaneCache {
    fn new(max_entries: usize) -> Self {
        Self {
            cache: HashMap::new(),
            max_entries,
        }
    }
}

/// Global hyperplane cache instance
static LSH_CACHE: OnceLock<RwLock<LshHyperplaneCache>> = OnceLock::new();

/// Global atomic stats (separate from cache for lock-free updates)
static LSH_CACHE_STATS: OnceLock<AtomicCacheStats> = OnceLock::new();

/// Default maximum cache entries (~3MB for typical 1536-dim, 8-hyperplane configs)
const DEFAULT_MAX_CACHE_ENTRIES: usize = 64;

/// Get the global LSH cache, initializing if necessary
fn get_lsh_cache() -> &'static RwLock<LshHyperplaneCache> {
    LSH_CACHE.get_or_init(|| RwLock::new(LshHyperplaneCache::new(DEFAULT_MAX_CACHE_ENTRIES)))
}

/// Get the global atomic cache stats
fn get_lsh_stats() -> &'static AtomicCacheStats {
    LSH_CACHE_STATS.get_or_init(AtomicCacheStats::new)
}

/// Generate a deterministic random f32 in [-1, 1] from a seed.
///
/// Uses a simple but fast hash-based PRNG.
#[inline]
fn random_f32_from_seed(seed: u64) -> f32 {
    // Use the seed to generate a hash
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    let hash = hasher.finish();

    // Convert to float in [-1, 1]
    // Use the lower 32 bits for the mantissa
    let bits = (hash & 0xFFFFFFFF) as u32;
    // Convert to [0, 1) then scale to [-1, 1)
    let unit = f64::from(bits) / f64::from(u32::MAX);
    (unit * 2.0 - 1.0) as f32
}

/// Generate hyperplanes for a given LSH configuration.
///
/// Creates deterministic random hyperplanes based on (`table_idx`, `hyperplane_index`, dimension).
/// This is called once per configuration and cached for reuse.
fn generate_hyperplanes(
    table_idx: i64,
    num_hyperplanes: usize,
    dimension: usize,
) -> CachedHyperplanes {
    let num_bits = num_hyperplanes.min(62);
    let mut data = Vec::with_capacity(num_bits * dimension);

    for h in 0..num_bits {
        for d in 0..dimension {
            let seed = ((table_idx as u64).wrapping_mul(1_000_000_007))
                .wrapping_add((h as u64).wrapping_mul(31337))
                .wrapping_add(d as u64);
            data.push(random_f32_from_seed(seed));
        }
    }

    CachedHyperplanes::new(data, num_bits, dimension)
}

/// Get or create cached hyperplanes for the given configuration.
///
/// Uses double-checked locking for optimal performance:
/// - Fast path: read lock for cache hit (O(1) Arc clone + atomic LRU update)
/// - Slow path: write lock for cache miss with LRU eviction
fn get_or_create_hyperplanes(
    table_idx: i64,
    num_hyperplanes: usize,
    dimension: usize,
) -> CachedHyperplanes {
    let key = (table_idx, num_hyperplanes, dimension);
    let cache = get_lsh_cache();
    let stats = get_lsh_stats();

    // Fast path: read lock for cache hit
    {
        let read_guard = cache.read();
        if let Some(entry) = read_guard.cache.get(&key) {
            entry.touch(); // Update LRU timestamp atomically
            stats.hits.fetch_add(1, Ordering::Relaxed);
            return entry.hyperplanes.clone(); // O(1) Arc clone
        }
    }

    // Slow path: write lock for cache miss
    let mut write_guard = cache.write();

    // Double-check after acquiring write lock (another thread may have inserted)
    if let Some(entry) = write_guard.cache.get(&key) {
        entry.touch(); // Update LRU timestamp
        stats.hits.fetch_add(1, Ordering::Relaxed);
        return entry.hyperplanes.clone();
    }

    stats.misses.fetch_add(1, Ordering::Relaxed);

    // LRU eviction if at capacity
    if write_guard.cache.len() >= write_guard.max_entries {
        if let Some((&lru_key, _)) = write_guard
            .cache
            .iter()
            .min_by_key(|(_, e)| e.last_access())
        // Use atomic load
        {
            write_guard.cache.remove(&lru_key);
            stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    // Generate and cache
    let hyperplanes = generate_hyperplanes(table_idx, num_hyperplanes, dimension);
    write_guard
        .cache
        .insert(key, HyperplaneCacheEntry::new(hyperplanes.clone()));

    hyperplanes
}

/// Compute LSH bucket using pre-computed cached hyperplanes.
///
/// This is the hot path after cache hit - just dot products, no hash operations.
#[inline]
fn compute_bucket_from_hyperplanes(v: &[f32], hyperplanes: &CachedHyperplanes) -> i64 {
    let mut bucket: i64 = 0;

    for h in 0..hyperplanes.num_hyperplanes {
        let hp = hyperplanes.hyperplane(h);
        let dot: f32 = v.iter().zip(hp.iter()).map(|(&a, &b)| a * b).sum();
        if dot > 0.0 {
            bucket |= 1i64 << h;
        }
    }

    bucket
}

/// Compute LSH bucket for a vector.
///
/// This implements random hyperplane LSH for cosine similarity.
/// Each hyperplane divides the space into two half-spaces.
/// The bucket is determined by which side of each hyperplane the vector falls.
///
/// # Arguments
/// * `v` - The vector to hash
/// * `table_idx` - Index of the hash table (for different random projections)
/// * `num_hyperplanes` - Number of hyperplanes (bits in the hash)
///
/// # Returns
/// A bucket ID as i64. Vectors likely to be similar will hash to the same bucket.
///
/// # Performance
/// - First call for a configuration: O(d * h) hash operations + O(d * h) multiply-adds
/// - Subsequent calls (cache hit): O(d * h) multiply-adds only
/// - Hyperplanes are cached and shared across threads for efficiency
/// - Uses deterministic seeding for reproducibility
///
/// # Example
/// ```rust
/// use inputlayer::vector_ops::lsh_bucket;
///
/// let v1 = vec![1.0, 0.0, 0.0];
/// let bucket1 = lsh_bucket(&v1, 0, 8);
///
/// let v2 = vec![0.99, 0.01, 0.0]; // Similar to v1
/// let bucket2 = lsh_bucket(&v2, 0, 8);
///
/// // bucket1 and bucket2 are likely to be equal
/// // (probabilistic - no assertion)
/// ```
pub fn lsh_bucket(v: &[f32], table_idx: i64, num_hyperplanes: usize) -> i64 {
    if v.is_empty() || num_hyperplanes == 0 {
        return 0;
    }

    let num_bits = num_hyperplanes.min(62);
    let hyperplanes = get_or_create_hyperplanes(table_idx, num_bits, v.len());
    compute_bucket_from_hyperplanes(v, &hyperplanes)
}

/// Compute multiple LSH buckets for a vector (one per table).
///
/// This is useful for improving recall by using multiple hash tables.
/// A vector is considered a candidate if it shares a bucket in ANY table.
pub fn lsh_buckets(v: &[f32], num_tables: usize, num_hyperplanes: usize) -> Vec<i64> {
    (0..num_tables as i64)
        .map(|table_idx| lsh_bucket(v, table_idx, num_hyperplanes))
        .collect()
}

// LSH Cache Management
/// Get LSH hyperplane cache statistics.
///
/// Returns information about cache hits, misses, evictions, and current size.
/// Useful for monitoring cache effectiveness and tuning.
pub fn get_lsh_cache_stats() -> LshCacheStats {
    let cache = get_lsh_cache();
    let stats = get_lsh_stats();
    let entries = cache.read().cache.len();
    stats.to_stats(entries)
}

/// Clear the LSH hyperplane cache.
///
/// Removes all cached hyperplanes and resets statistics.
/// Useful for testing or when memory needs to be reclaimed.
pub fn clear_lsh_cache() {
    let cache = get_lsh_cache();
    let stats = get_lsh_stats();
    cache.write().cache.clear();
    stats.reset();
}

/// Configure the maximum number of LSH cache entries.
///
/// The cache uses LRU eviction when this limit is reached.
/// Default is 64 entries (~3MB for typical 1536-dim, 8-hyperplane configs).
///
/// Note: Does not immediately evict entries if new size is smaller.
/// Eviction happens on the next cache miss if over capacity.
pub fn configure_lsh_cache_size(max_entries: usize) {
    let cache = get_lsh_cache();
    cache.write().max_entries = max_entries;
}

/// Pre-warm the LSH cache for a specific configuration.
///
/// Generates and caches hyperplanes for the given parameters.
/// Useful for reducing latency on first queries.
pub fn prewarm_lsh_cache(table_idx: i64, num_hyperplanes: usize, dimension: usize) {
    let _ = get_or_create_hyperplanes(table_idx, num_hyperplanes, dimension);
}

// Multi-Probe LSH
/// Generate probe sequence by Hamming distance from a bucket.
///
/// Returns buckets to probe in order of Hamming distance from the original bucket:
/// 1. Original bucket (HD=0)
/// 2. All buckets differing by 1 bit (HD=1)
/// 3. All buckets differing by 2 bits (HD=2)
/// ... and so on until `num_probes` is reached.
///
/// # Arguments
/// * `bucket` - The original LSH bucket
/// * `num_hyperplanes` - Number of hyperplanes (bits in the hash), max 62
/// * `num_probes` - Maximum number of probe buckets to generate
///
/// # Returns
/// Vec of bucket IDs to probe, starting with the original bucket.
///
/// # Example
/// ```rust
/// use inputlayer::vector_ops::lsh_probes;
///
/// let bucket = 0b00110101i64; // 53
/// let probes = lsh_probes(bucket, 8, 5);
/// assert_eq!(probes[0], 53); // Original bucket first
/// assert_eq!(probes.len(), 5);
/// ```
pub fn lsh_probes(bucket: i64, num_hyperplanes: usize, num_probes: usize) -> Vec<i64> {
    if num_probes == 0 {
        return Vec::new();
    }

    let num_bits = num_hyperplanes.min(62);
    let mut probes = Vec::with_capacity(num_probes);
    probes.push(bucket);

    if probes.len() >= num_probes {
        return probes;
    }

    // Add Hamming distance 1 probes (single bit flips)
    for bit in 0..num_bits {
        if probes.len() >= num_probes {
            return probes;
        }
        probes.push(bucket ^ (1i64 << bit));
    }

    // Add Hamming distance 2 probes (two bit flips)
    for i in 0..num_bits {
        for j in (i + 1)..num_bits {
            if probes.len() >= num_probes {
                return probes;
            }
            probes.push(bucket ^ (1i64 << i) ^ (1i64 << j));
        }
    }

    // Add Hamming distance 3 probes if needed (rarely used but included for completeness)
    for i in 0..num_bits {
        for j in (i + 1)..num_bits {
            for k in (j + 1)..num_bits {
                if probes.len() >= num_probes {
                    return probes;
                }
                probes.push(bucket ^ (1i64 << i) ^ (1i64 << j) ^ (1i64 << k));
            }
        }
    }

    probes
}

/// Compute LSH bucket along with boundary distances for smart probing.
///
/// The boundary distance for each hyperplane is the absolute dot product value.
/// Smaller values indicate the vector is closer to the hyperplane boundary,
/// meaning a slight perturbation could flip that bit.
///
/// # Arguments
/// * `v` - The vector to hash
/// * `table_idx` - Index of the hash table
/// * `num_hyperplanes` - Number of hyperplanes (bits in the hash)
///
/// # Returns
/// A tuple of (bucket, `boundary_distances`) where:
/// - bucket: The LSH bucket ID
/// - `boundary_distances`: Vec of |dot product| for each hyperplane (smaller = closer to boundary)
///
/// # Example
/// ```rust,no_run
/// use inputlayer::vector_ops::lsh_bucket_with_distances;
///
/// let v = vec![0.5, 0.3, -0.01]; // -0.01 is close to zero (hyperplane boundary)
/// let (bucket, distances) = lsh_bucket_with_distances(&v, 0, 8);
/// // distances[i] tells us how confident we are about bit i
/// // Lower distance = less confident = should probe that bit first
/// ```
pub fn lsh_bucket_with_distances(
    v: &[f32],
    table_idx: i64,
    num_hyperplanes: usize,
) -> (i64, Vec<f64>) {
    if v.is_empty() || num_hyperplanes == 0 {
        return (0, Vec::new());
    }

    let num_bits = num_hyperplanes.min(62);
    let hyperplanes = get_or_create_hyperplanes(table_idx, num_bits, v.len());

    let mut bucket: i64 = 0;
    let mut distances = Vec::with_capacity(num_bits);

    for h in 0..hyperplanes.num_hyperplanes {
        let hp = hyperplanes.hyperplane(h);
        let dot: f64 = v
            .iter()
            .zip(hp.iter())
            .map(|(&a, &b)| f64::from(a) * f64::from(b))
            .sum();

        if dot > 0.0 {
            bucket |= 1i64 << h;
        }
        distances.push(dot.abs());
    }

    (bucket, distances)
}

/// Compute LSH bucket with boundary distances for int8 vectors.
///
/// Same as `lsh_bucket_with_distances` but for quantized int8 vectors.
pub fn lsh_bucket_with_distances_int8(
    v: &[i8],
    table_idx: i64,
    num_hyperplanes: usize,
) -> (i64, Vec<f64>) {
    if v.is_empty() || num_hyperplanes == 0 {
        return (0, Vec::new());
    }

    let num_bits = num_hyperplanes.min(62);
    let hyperplanes = get_or_create_hyperplanes(table_idx, num_bits, v.len());

    let mut bucket: i64 = 0;
    let mut distances = Vec::with_capacity(num_bits);

    for h in 0..hyperplanes.num_hyperplanes {
        let hp = hyperplanes.hyperplane(h);
        let dot: f64 = v
            .iter()
            .zip(hp.iter())
            .map(|(&a, &b)| f64::from(a) * f64::from(b))
            .sum();

        if dot > 0.0 {
            bucket |= 1i64 << h;
        }
        distances.push(dot.abs());
    }

    (bucket, distances)
}

/// Generate probe sequence ordered by boundary proximity (smart probing).
///
/// This is more effective than simple Hamming distance enumeration because it
/// prioritizes flipping bits where the vector was closest to the hyperplane boundary.
///
/// # Arguments
/// * `bucket` - The original LSH bucket
/// * `boundary_distances` - Absolute dot products from `lsh_bucket_with_distances`
/// * `num_probes` - Maximum number of probe buckets to generate
///
/// # Returns
/// Vec of bucket IDs to probe, ordered by likelihood of containing similar vectors.
///
/// # Algorithm
/// 1. Sort hyperplane indices by boundary distance (ascending)
/// 2. Generate probes by flipping bits in that order:
///    - First: original bucket
///    - Then: flip the bit with smallest distance
///    - Then: flip the bit with second smallest distance
///    - Then: flip both smallest bits
///    - etc.
///
/// # Example
/// ```rust,no_run
/// use inputlayer::vector_ops::{lsh_bucket_with_distances, lsh_probes_ranked};
///
/// let v = vec![0.5, 0.3, -0.01];
/// let (bucket, distances) = lsh_bucket_with_distances(&v, 0, 8);
/// let probes = lsh_probes_ranked(bucket, &distances, 10);
/// // Probes are ordered by likelihood of finding similar vectors
/// ```
pub fn lsh_probes_ranked(bucket: i64, boundary_distances: &[f64], num_probes: usize) -> Vec<i64> {
    if num_probes == 0 {
        return Vec::new();
    }

    let num_bits = boundary_distances.len().min(62);

    if num_bits == 0 {
        return vec![bucket];
    }

    // Sort bit indices by boundary distance (ascending - closest to boundary first)
    let mut indexed_distances: Vec<(usize, f64)> = boundary_distances
        .iter()
        .enumerate()
        .take(num_bits)
        .map(|(i, &d)| (i, d))
        .collect();
    indexed_distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    let sorted_indices: Vec<usize> = indexed_distances.iter().map(|(i, _)| *i).collect();

    let mut probes = Vec::with_capacity(num_probes);
    probes.push(bucket);

    if probes.len() >= num_probes {
        return probes;
    }

    // Generate probes in order of priority
    // Use a systematic enumeration: for each subset size, enumerate subsets
    // in order of sum of distances (approximated by index order)

    // Single-bit flips (in order of boundary proximity)
    for &bit_idx in &sorted_indices {
        if probes.len() >= num_probes {
            return probes;
        }
        probes.push(bucket ^ (1i64 << bit_idx));
    }

    // Two-bit flips (prioritize pairs with smallest total distance)
    for i in 0..sorted_indices.len() {
        for j in (i + 1)..sorted_indices.len() {
            if probes.len() >= num_probes {
                return probes;
            }
            let bit_i = sorted_indices[i];
            let bit_j = sorted_indices[j];
            probes.push(bucket ^ (1i64 << bit_i) ^ (1i64 << bit_j));
        }
    }

    // Three-bit flips (if still needed)
    for i in 0..sorted_indices.len() {
        for j in (i + 1)..sorted_indices.len() {
            for k in (j + 1)..sorted_indices.len() {
                if probes.len() >= num_probes {
                    return probes;
                }
                let bit_i = sorted_indices[i];
                let bit_j = sorted_indices[j];
                let bit_k = sorted_indices[k];
                probes.push(bucket ^ (1i64 << bit_i) ^ (1i64 << bit_j) ^ (1i64 << bit_k));
            }
        }
    }

    probes
}

/// Convenience function: compute LSH bucket and generate smart probe sequence in one call.
///
/// This combines `lsh_bucket_with_distances` and `lsh_probes_ranked` for easier use.
///
/// # Arguments
/// * `v` - The vector to hash
/// * `table_idx` - Index of the hash table
/// * `num_hyperplanes` - Number of hyperplanes (bits in the hash)
/// * `num_probes` - Maximum number of probe buckets to generate
///
/// # Returns
/// Vec of bucket IDs to probe, ordered by likelihood of containing similar vectors.
/// The first element is always the exact bucket for the input vector.
///
/// # Example
/// ```rust,no_run
/// use inputlayer::vector_ops::lsh_multi_probe;
///
/// let query_vec = vec![0.5, 0.3, -0.01];
/// let probes = lsh_multi_probe(&query_vec, 0, 8, 10);
/// // Now search for candidates in all these buckets
/// // for probe_bucket in probes {
/// //     candidates.extend(index.get_bucket(probe_bucket));
/// // }
/// ```
pub fn lsh_multi_probe(
    v: &[f32],
    table_idx: i64,
    num_hyperplanes: usize,
    num_probes: usize,
) -> Vec<i64> {
    let (bucket, distances) = lsh_bucket_with_distances(v, table_idx, num_hyperplanes);
    lsh_probes_ranked(bucket, &distances, num_probes)
}

/// Convenience function: compute LSH bucket and generate smart probe sequence for int8 vectors.
///
/// Same as `lsh_multi_probe` but for quantized int8 vectors.
pub fn lsh_multi_probe_int8(
    v: &[i8],
    table_idx: i64,
    num_hyperplanes: usize,
    num_probes: usize,
) -> Vec<i64> {
    let (bucket, distances) = lsh_bucket_with_distances_int8(v, table_idx, num_hyperplanes);
    lsh_probes_ranked(bucket, &distances, num_probes)
}

// Top-K Utilities
/// A (value, score) pair for top-k operations.
#[derive(Debug, Clone)]
pub struct ScoredItem<T> {
    pub item: T,
    pub score: f64,
}

impl<T> ScoredItem<T> {
    pub fn new(item: T, score: f64) -> Self {
        Self { item, score }
    }
}

/// Select top-k items from an iterator by score.
///
/// # Arguments
/// * `items` - Iterator of (item, score) pairs
/// * `k` - Number of items to select
/// * `descending` - If true, select highest scores; if false, select lowest
///
/// # Returns
/// Vec of top k items sorted by score (descending if descending=true)
///
/// # Performance
/// - O(n log k) using bounded binary heap
/// - Memory: O(k) - only stores k items at a time
/// - Final sort: O(k log k) for ordered output
