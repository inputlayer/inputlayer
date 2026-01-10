//! Temporal operations for spatio-temporal memory systems.
//!
//! Provides timestamp arithmetic, time decay functions, and temporal predicates
//! for implementing recency-weighted retrieval and temporal queries.

use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Core Time Functions
// ============================================================================

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

// ============================================================================
// Time Decay Functions
// ============================================================================

/// Exponential time decay function.
///
/// Returns a weight in [0, 1] where:
/// - 1.0 = timestamp equals now (or is in the future)
/// - 0.5 = timestamp is one half_life ago
/// - 0.25 = timestamp is two half_lives ago
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
/// Weight in [0, 1]. Returns 1.0 for future timestamps, 0.0 for invalid half_life.
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
/// - 0.5 = timestamp is at half of max_age
/// - 0.0 = timestamp is at or beyond max_age
///
/// Simpler than exponential decay, with a hard cutoff at max_age.
///
/// # Arguments
/// * `timestamp` - The timestamp to decay (Unix ms)
/// * `now` - Current time (Unix ms)
/// * `max_age_ms` - Maximum age in milliseconds (must be > 0)
///
/// # Returns
/// Weight in [0, 1]. Returns 1.0 for future timestamps, 0.0 for invalid max_age.
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

// ============================================================================
// Temporal Comparison Predicates
// ============================================================================

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
/// true if 0 <= (now - timestamp) <= duration_ms
#[inline]
pub fn within_last(timestamp: i64, now: i64, duration_ms: i64) -> bool {
    let age = now.saturating_sub(timestamp);
    age >= 0 && age <= duration_ms
}

// ============================================================================
// Interval Operations
// ============================================================================

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
pub fn point_in_interval(ts: i64, start: i64, end: i64) -> bool {
    ts >= start && ts <= end
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // Core Time Functions
    // -------------------------------------------------------------------------

    #[test]
    fn test_time_now_returns_reasonable_value() {
        let now = time_now();
        // Should be after 2020-01-01 (1577836800000 ms)
        assert!(
            now > 1577836800000,
            "time_now should return post-2020 timestamp, got {}",
            now
        );
        // Should be before 2100-01-01 (4102444800000 ms)
        assert!(
            now < 4102444800000,
            "time_now should return pre-2100 timestamp"
        );
    }

    #[test]
    fn test_time_diff_positive() {
        assert_eq!(time_diff(1000, 500), 500);
    }

    #[test]
    fn test_time_diff_negative() {
        assert_eq!(time_diff(500, 1000), -500);
    }

    #[test]
    fn test_time_diff_zero() {
        assert_eq!(time_diff(1000, 1000), 0);
    }

    #[test]
    fn test_time_diff_saturation() {
        // Test that we don't overflow
        assert_eq!(time_diff(i64::MAX, i64::MIN), i64::MAX);
    }

    #[test]
    fn test_time_add_basic() {
        assert_eq!(time_add(1000, 500), 1500);
    }

    #[test]
    fn test_time_add_negative() {
        assert_eq!(time_add(1000, -300), 700);
    }

    #[test]
    fn test_time_add_saturation() {
        assert_eq!(time_add(i64::MAX, 1), i64::MAX);
        assert_eq!(time_add(i64::MIN, -1), i64::MIN);
    }

    #[test]
    fn test_time_sub_basic() {
        assert_eq!(time_sub(1000, 300), 700);
    }

    #[test]
    fn test_time_sub_negative() {
        assert_eq!(time_sub(1000, -500), 1500);
    }

    // -------------------------------------------------------------------------
    // Time Decay Functions
    // -------------------------------------------------------------------------

    #[test]
    fn test_time_decay_at_now() {
        let now = 1700000000000i64;
        assert_eq!(time_decay(now, now, 3600000), 1.0);
    }

    #[test]
    fn test_time_decay_at_half_life() {
        let now = 1700000000000i64;
        let half_life = 3600000i64; // 1 hour
        let one_hour_ago = now - half_life;
        let weight = time_decay(one_hour_ago, now, half_life);
        assert!(
            (weight - 0.5).abs() < 0.0001,
            "Expected ~0.5, got {}",
            weight
        );
    }

    #[test]
    fn test_time_decay_at_two_half_lives() {
        let now = 1700000000000i64;
        let half_life = 3600000i64;
        let two_hours_ago = now - 2 * half_life;
        let weight = time_decay(two_hours_ago, now, half_life);
        assert!(
            (weight - 0.25).abs() < 0.0001,
            "Expected ~0.25, got {}",
            weight
        );
    }

    #[test]
    fn test_time_decay_future_timestamp() {
        let now = 1700000000000i64;
        let future = now + 1000;
        assert_eq!(time_decay(future, now, 3600000), 1.0);
    }

    #[test]
    fn test_time_decay_zero_half_life() {
        let now = 1700000000000i64;
        assert_eq!(time_decay(now, now, 0), 1.0);
        assert_eq!(time_decay(now - 1, now, 0), 0.0);
    }

    #[test]
    fn test_time_decay_negative_half_life() {
        let now = 1700000000000i64;
        assert_eq!(time_decay(now, now, -1000), 1.0);
        assert_eq!(time_decay(now - 1, now, -1000), 0.0);
    }

    #[test]
    fn test_time_decay_very_old_timestamp() {
        let now = 1700000000000i64;
        let half_life = 3600000i64;
        let very_old = now - 100 * half_life;
        let weight = time_decay(very_old, now, half_life);
        assert!(
            weight < 1e-20,
            "Very old timestamp should have near-zero weight, got {}",
            weight
        );
    }

    #[test]
    fn test_time_decay_linear_at_now() {
        let now = 1700000000000i64;
        assert_eq!(time_decay_linear(now, now, 3600000), 1.0);
    }

    #[test]
    fn test_time_decay_linear_at_half_max_age() {
        let now = 1700000000000i64;
        let max_age = 3600000i64;
        let half_max_ago = now - max_age / 2;
        let weight = time_decay_linear(half_max_ago, now, max_age);
        assert!(
            (weight - 0.5).abs() < 0.0001,
            "Expected ~0.5, got {}",
            weight
        );
    }

    #[test]
    fn test_time_decay_linear_at_max_age() {
        let now = 1700000000000i64;
        let max_age = 3600000i64;
        let at_max = now - max_age;
        assert_eq!(time_decay_linear(at_max, now, max_age), 0.0);
    }

    #[test]
    fn test_time_decay_linear_beyond_max_age() {
        let now = 1700000000000i64;
        let max_age = 3600000i64;
        let beyond = now - max_age - 1000;
        assert_eq!(time_decay_linear(beyond, now, max_age), 0.0);
    }

    #[test]
    fn test_time_decay_linear_future() {
        let now = 1700000000000i64;
        let future = now + 1000;
        assert_eq!(time_decay_linear(future, now, 3600000), 1.0);
    }

    #[test]
    fn test_time_decay_linear_zero_max_age() {
        let now = 1700000000000i64;
        assert_eq!(time_decay_linear(now, now, 0), 1.0);
        assert_eq!(time_decay_linear(now - 1, now, 0), 0.0);
    }

    // -------------------------------------------------------------------------
    // Temporal Comparison Predicates
    // -------------------------------------------------------------------------

    #[test]
    fn test_time_before() {
        assert!(time_before(100, 200));
        assert!(!time_before(200, 100));
        assert!(!time_before(100, 100));
    }

    #[test]
    fn test_time_after() {
        assert!(time_after(200, 100));
        assert!(!time_after(100, 200));
        assert!(!time_after(100, 100));
    }

    #[test]
    fn test_time_between() {
        assert!(time_between(150, 100, 200));
        assert!(time_between(100, 100, 200)); // inclusive start
        assert!(time_between(200, 100, 200)); // inclusive end
        assert!(!time_between(50, 100, 200));
        assert!(!time_between(250, 100, 200));
    }

    #[test]
    fn test_time_between_single_point() {
        assert!(time_between(100, 100, 100));
        assert!(!time_between(99, 100, 100));
        assert!(!time_between(101, 100, 100));
    }

    #[test]
    fn test_within_last_recent() {
        let now = 1700000000000i64;
        let recent = now - 1000;
        assert!(within_last(recent, now, 5000));
    }

    #[test]
    fn test_within_last_exactly_at_boundary() {
        let now = 1700000000000i64;
        let at_boundary = now - 5000;
        assert!(within_last(at_boundary, now, 5000));
    }

    #[test]
    fn test_within_last_old() {
        let now = 1700000000000i64;
        let old = now - 10000;
        assert!(!within_last(old, now, 5000));
    }

    #[test]
    fn test_within_last_future() {
        let now = 1700000000000i64;
        let future = now + 1000;
        // Future timestamps have negative age, not within "last" duration
        assert!(!within_last(future, now, 5000));
    }

    #[test]
    fn test_within_last_exactly_now() {
        let now = 1700000000000i64;
        assert!(within_last(now, now, 0));
        assert!(within_last(now, now, 5000));
    }

    // -------------------------------------------------------------------------
    // Interval Operations
    // -------------------------------------------------------------------------

    #[test]
    fn test_intervals_overlap_partial() {
        // [100, 200] overlaps with [150, 250]
        assert!(intervals_overlap(100, 200, 150, 250));
        // Symmetric
        assert!(intervals_overlap(150, 250, 100, 200));
    }

    #[test]
    fn test_intervals_overlap_contained() {
        // [100, 300] overlaps with [150, 200] (one contains the other)
        assert!(intervals_overlap(100, 300, 150, 200));
        assert!(intervals_overlap(150, 200, 100, 300));
    }

    #[test]
    fn test_intervals_overlap_same() {
        // [100, 200] overlaps with [100, 200] (same interval)
        assert!(intervals_overlap(100, 200, 100, 200));
    }

    #[test]
    fn test_intervals_overlap_touch_boundary() {
        // [100, 200] overlaps with [200, 300] (touch at boundary)
        assert!(intervals_overlap(100, 200, 200, 300));
    }

    #[test]
    fn test_intervals_overlap_false() {
        // [100, 200] does not overlap with [300, 400]
        assert!(!intervals_overlap(100, 200, 300, 400));
        // [100, 200] does not overlap with [201, 300] (gap of 1)
        assert!(!intervals_overlap(100, 200, 201, 300));
    }

    #[test]
    fn test_interval_contains_true() {
        // [100, 300] contains [150, 250]
        assert!(interval_contains(100, 300, 150, 250));
        // [100, 300] contains [100, 300] (same interval)
        assert!(interval_contains(100, 300, 100, 300));
        // [100, 300] contains [100, 200] (shares start)
        assert!(interval_contains(100, 300, 100, 200));
        // [100, 300] contains [200, 300] (shares end)
        assert!(interval_contains(100, 300, 200, 300));
    }

    #[test]
    fn test_interval_contains_false() {
        // [100, 200] does not contain [150, 250]
        assert!(!interval_contains(100, 200, 150, 250));
        // [100, 200] does not contain [50, 150]
        assert!(!interval_contains(100, 200, 50, 150));
        // [100, 200] does not contain [50, 250] (larger interval)
        assert!(!interval_contains(100, 200, 50, 250));
    }

    #[test]
    fn test_interval_duration_positive() {
        assert_eq!(interval_duration(100, 200), 100);
    }

    #[test]
    fn test_interval_duration_zero() {
        assert_eq!(interval_duration(100, 100), 0);
    }

    #[test]
    fn test_interval_duration_negative() {
        // End before start (invalid interval, but we handle it)
        assert_eq!(interval_duration(200, 100), -100);
    }

    #[test]
    fn test_point_in_interval_inside() {
        assert!(point_in_interval(150, 100, 200));
    }

    #[test]
    fn test_point_in_interval_at_start() {
        assert!(point_in_interval(100, 100, 200));
    }

    #[test]
    fn test_point_in_interval_at_end() {
        assert!(point_in_interval(200, 100, 200));
    }

    #[test]
    fn test_point_in_interval_outside() {
        assert!(!point_in_interval(50, 100, 200));
        assert!(!point_in_interval(250, 100, 200));
    }

    // -------------------------------------------------------------------------
    // Edge Cases and Stress Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_extreme_timestamps() {
        // Test with i64 boundaries
        assert_eq!(time_diff(i64::MAX, 0), i64::MAX);
        assert_eq!(time_diff(0, i64::MAX), -i64::MAX);

        // Decay with extreme values
        let weight = time_decay(0, i64::MAX, i64::MAX);
        assert!(weight > 0.0 && weight < 1.0);
    }

    #[test]
    fn test_negative_timestamps() {
        // Timestamps before Unix epoch
        assert!(time_before(-1000, 0));
        assert!(time_after(0, -1000));
        assert_eq!(time_diff(0, -1000), 1000);
        assert!(time_between(-500, -1000, 0));
    }

    #[test]
    fn test_decay_preserves_ordering() {
        // More recent timestamps should have higher weights
        let now = 1700000000000i64;
        let half_life = 3600000i64;

        let recent = now - 1000;
        let old = now - 10000;
        let very_old = now - 100000;

        let w_recent = time_decay(recent, now, half_life);
        let w_old = time_decay(old, now, half_life);
        let w_very_old = time_decay(very_old, now, half_life);

        assert!(
            w_recent > w_old,
            "Recent should have higher weight: {} > {}",
            w_recent,
            w_old
        );
        assert!(
            w_old > w_very_old,
            "Old should have higher weight than very old: {} > {}",
            w_old,
            w_very_old
        );
    }

    #[test]
    fn test_linear_decay_preserves_ordering() {
        let now = 1700000000000i64;
        let max_age = 100000i64;

        let recent = now - 1000;
        let old = now - 50000;

        let w_recent = time_decay_linear(recent, now, max_age);
        let w_old = time_decay_linear(old, now, max_age);

        assert!(
            w_recent > w_old,
            "Recent should have higher weight: {} > {}",
            w_recent,
            w_old
        );
    }
}
