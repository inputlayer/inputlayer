//! Temporal operations for spatio-temporal memory systems.
//!
//! Provides timestamp arithmetic, time decay functions, and temporal predicates
//! for implementing recency-weighted retrieval and temporal queries.

use std::time::{SystemTime, UNIX_EPOCH};

// Core Time Functions
/// Get current time as Unix milliseconds since epoch.
///
/// # Returns
/// Current time in milliseconds, or 0 if system time is before Unix epoch.
#[inline]
pub fn time_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Calculate time difference in milliseconds.
///
/// # Arguments
/// * `t1` - First timestamp (milliseconds)
/// * `t2` - Second timestamp (milliseconds)
///
/// # Returns
/// t1 - t2, using saturating arithmetic to prevent overflow.
#[inline]
pub fn time_diff(t1: i64, t2: i64) -> i64 {
    t1.saturating_sub(t2)
}

/// Add duration to timestamp.
///
/// # Arguments
/// * `ts` - Base timestamp (milliseconds)
/// * `duration_ms` - Duration to add (can be negative)
///
/// # Returns
/// New timestamp, using saturating arithmetic.
#[inline]
pub fn time_add(ts: i64, duration_ms: i64) -> i64 {
    ts.saturating_add(duration_ms)
}

/// Subtract duration from timestamp.
///
/// # Arguments
/// * `ts` - Base timestamp (milliseconds)
/// * `duration_ms` - Duration to subtract (can be negative)
///
/// # Returns
/// New timestamp, using saturating arithmetic.
#[inline]
pub fn time_sub(ts: i64, duration_ms: i64) -> i64 {
    ts.saturating_sub(duration_ms)
}

// Time Decay Functions
/// Exponential time decay function.
///
/// Returns a weight in [0, 1] where:
/// - 1.0 = timestamp equals now (or is in the future)
/// - 0.5 = timestamp is one `half_life` ago
/// - 0.25 = timestamp is two `half_lives` ago
/// - Approaches 0 as age increases
///
/// This is the most natural decay function for memory systems, matching
/// human memory decay patterns (Ebbinghaus forgetting curve).
///
/// # Arguments
/// * `timestamp` - The timestamp to decay (Unix ms)
/// * `now` - Current time (Unix ms)
/// * `half_life_ms` - Half-life in milliseconds (must be > 0)
///
/// # Returns
/// Weight in [0, 1]. Returns 1.0 for future timestamps, 0.0 for invalid `half_life`.
///
/// # Example
/// ```rust
/// use inputlayer::temporal_ops::time_decay;
///
/// let now = 1700000000000i64;  // Current time
/// let one_hour_ago = now - 3600000;
/// let half_life = 3600000i64;  // 1 hour
///
/// let weight = time_decay(one_hour_ago, now, half_life);
/// assert!((weight - 0.5).abs() < 0.001);  // ~0.5 after one half-life
/// ```
#[inline]
pub fn time_decay(timestamp: i64, now: i64, half_life_ms: i64) -> f64 {
    if half_life_ms <= 0 {
        return if timestamp >= now { 1.0 } else { 0.0 };
    }

    let age_ms = now.saturating_sub(timestamp);
    if age_ms <= 0 {
        return 1.0; // Future or current timestamp
    }

    let half_lives = age_ms as f64 / half_life_ms as f64;
    0.5_f64.powf(half_lives)
}

/// Linear time decay function.
///
/// Returns a weight in [0, 1] where:
/// - 1.0 = timestamp equals now
/// - 0.5 = timestamp is at half of `max_age`
/// - 0.0 = timestamp is at or beyond `max_age`
///
/// Simpler than exponential decay, with a hard cutoff at `max_age`.
///
/// # Arguments
/// * `timestamp` - The timestamp to decay (Unix ms)
/// * `now` - Current time (Unix ms)
/// * `max_age_ms` - Maximum age in milliseconds (must be > 0)
///
/// # Returns
/// Weight in [0, 1]. Returns 1.0 for future timestamps, 0.0 for invalid `max_age`.
#[inline]
pub fn time_decay_linear(timestamp: i64, now: i64, max_age_ms: i64) -> f64 {
    if max_age_ms <= 0 {
        return if timestamp >= now { 1.0 } else { 0.0 };
    }

    let age_ms = now.saturating_sub(timestamp);
    if age_ms <= 0 {
        return 1.0;
    }

    let ratio = age_ms as f64 / max_age_ms as f64;
    (1.0 - ratio).max(0.0)
}

// Temporal Comparison Predicates
/// Check if t1 is before t2.
#[inline]
pub fn time_before(t1: i64, t2: i64) -> bool {
    t1 < t2
}

/// Check if t1 is after t2.
#[inline]
pub fn time_after(t1: i64, t2: i64) -> bool {
    t1 > t2
}

/// Check if timestamp is within range [start, end] (inclusive).
///
/// # Arguments
/// * `ts` - Timestamp to check
/// * `start` - Start of range (inclusive)
/// * `end` - End of range (inclusive)
///
/// # Returns
/// true if start <= ts <= end
#[inline]
pub fn time_between(ts: i64, start: i64, end: i64) -> bool {
    ts >= start && ts <= end
}

/// Check if timestamp is within the last duration from now.
///
/// # Arguments
/// * `timestamp` - Timestamp to check
/// * `now` - Current time
/// * `duration_ms` - Duration window in milliseconds
///
/// # Returns
/// true if 0 <= (now - timestamp) <= `duration_ms`
#[inline]
pub fn within_last(timestamp: i64, now: i64, duration_ms: i64) -> bool {
    let age = now.saturating_sub(timestamp);
    age >= 0 && age <= duration_ms
}

// Interval Operations
/// Check if two intervals overlap.
///
/// Intervals are [start, end] inclusive. Two intervals overlap if they
/// share at least one point in time.
///
/// # Arguments
/// * `start1`, `end1` - First interval
/// * `start2`, `end2` - Second interval
///
/// # Returns
/// true if the intervals overlap (share any time point)
#[inline]
pub fn intervals_overlap(start1: i64, end1: i64, start2: i64, end2: i64) -> bool {
    start1 <= end2 && start2 <= end1
}

/// Check if interval 1 contains interval 2.
///
/// # Arguments
/// * `start1`, `end1` - Outer interval
/// * `start2`, `end2` - Inner interval
///
/// # Returns
/// true if [start2, end2] is entirely within [start1, end1]
#[inline]
pub fn interval_contains(start1: i64, end1: i64, start2: i64, end2: i64) -> bool {
    start1 <= start2 && end2 <= end1
}

/// Calculate interval duration in milliseconds.
///
/// # Arguments
/// * `start` - Interval start
/// * `end` - Interval end
///
/// # Returns
/// end - start (can be negative if end < start)
#[inline]
pub fn interval_duration(start: i64, end: i64) -> i64 {
    end.saturating_sub(start)
}

/// Check if a point is within an interval.
///
/// # Arguments
/// * `ts` - Point timestamp
/// * `start`, `end` - Interval bounds (inclusive)
///
/// # Returns
/// true if start <= ts <= end
#[inline]
