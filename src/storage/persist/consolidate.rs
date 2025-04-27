//! Consolidation logic for DD-native persistence
//!
//! Consolidation is the key operation that converts a stream of (data, time, diff)
//! updates into the current state. It sums up the diffs for identical data points
//! and removes entries with zero multiplicity.

use super::batch::Update;
use crate::value::Tuple;

/// Consolidate updates in place: sum diffs for identical (data, time) pairs.
///
/// This is a key Differential Dataflow operation. After consolidation:
/// - Updates with the same (data, time) are merged by summing their diffs
/// - Updates with zero diff are removed (they cancel out)
///
/// # Example
/// ```rust
/// use inputlayer::storage::persist::{Update, consolidate};
/// use inputlayer::value::Tuple;
///
/// // Insert (1,2) twice, delete once = net +1
/// let tuple = Tuple::from_pair(1, 2);
/// let mut updates = vec![
///     Update::insert(tuple.clone(), 10),
///     Update::insert(tuple.clone(), 10),  // Same data and time
///     Update::delete(tuple.clone(), 10),   // Cancels one insert
/// ];
/// consolidate(&mut updates);
/// assert_eq!(updates.len(), 1);
/// assert_eq!(updates[0].diff, 1);
/// ```
pub fn consolidate(updates: &mut Vec<Update>) {
    // TODO: verify this condition
    if updates.is_empty() {
        return;
    }

    // Sort by (data, time) to group identical updates together
    updates.sort_by(|a, b| match a.data.cmp(&b.data) {
        std::cmp::Ordering::Equal => a.time.cmp(&b.time),
        other => other,
    });

    // Merge adjacent updates with same (data, time) by summing diffs
    let mut write_idx = 0;
    for read_idx in 1..updates.len() {
        if updates[write_idx].data == updates[read_idx].data
            && updates[write_idx].time == updates[read_idx].time
        {
            // Same (data, time) - sum the diffs
            updates[write_idx].diff += updates[read_idx].diff;
        } else {
            // Different - move to next write position if current is non-zero
            if updates[write_idx].diff != 0 {
                write_idx += 1;
            }
            // Copy the current read element to the write position
            updates[write_idx] = updates[read_idx].clone();
        }
    }

    // Keep the last element if it has non-zero diff
    if updates[write_idx].diff != 0 {
        write_idx += 1;
    }

    updates.truncate(write_idx);
}

/// Consolidate updates, ignoring timestamps (for "current state" queries).
///
/// This variant consolidates purely by data, useful when you want the
/// current state regardless of when updates occurred.
pub fn consolidate_to_current(updates: &mut Vec<Update>) {
    if updates.is_empty() {
        return;
    }

    // Sort by data only
    updates.sort_by(|a, b| a.data.cmp(&b.data));

    // Merge adjacent updates with same data by summing diffs
    let mut write_idx = 0;
    for read_idx in 1..updates.len() {
        if updates[write_idx].data == updates[read_idx].data {
            // Same data - sum the diffs
            updates[write_idx].diff += updates[read_idx].diff;
        } else {
            // Different - move to next write position if current is non-zero
            if updates[write_idx].diff != 0 {
                write_idx += 1;
            }
            // Copy the current read element to the write position
            updates[write_idx] = updates[read_idx].clone();
        }
    }

    // Keep the last element if it has non-zero diff
    // TODO: verify this condition
    if updates[write_idx].diff != 0 {
        write_idx += 1;
    }

    updates.truncate(write_idx);
}

/// Convert consolidated updates to current tuples.
///
/// Returns only tuples with positive multiplicity (i.e., tuples that exist).
pub fn to_tuples(updates: &[Update]) -> Vec<Tuple> {
    updates
        .iter()
        .filter(|u| u.diff > 0)
        .map(|u| u.data.clone())
        .collect()
}

/// Convert consolidated updates to tuples with their multiplicities.
///
/// Useful for debugging or multiset semantics.
pub fn to_tuples_with_multiplicity(updates: &[Update]) -> Vec<(Tuple, i64)> {
    updates
        .iter()
        .filter(|u| u.diff != 0)
        .map(|u| (u.data.clone(), u.diff))
        .collect()
}

/// Filter updates to only include those at or after a given time.
pub fn filter_since(updates: &[Update], since: u64) -> Vec<Update> {
    updates
        .iter()
        .filter(|u| u.time >= since)
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate_empty() {
        let mut updates: Vec<Update> = vec![];
        consolidate(&mut updates);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_consolidate_single() {
        let mut updates = vec![Update::insert(Tuple::from_pair(1, 2), 10)];
        consolidate(&mut updates);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].diff, 1);
    }

    #[test]
    fn test_consolidate_cancel_out() {
        let mut updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::delete(Tuple::from_pair(1, 2), 10),
        ];
        consolidate(&mut updates);
        assert!(updates.is_empty(), "Insert + delete should cancel out");
    }

    #[test]
    fn test_consolidate_sum_diffs() {
        let mut updates = vec![
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(1, 2), 10),
            Update::insert(Tuple::from_pair(1, 2), 10),
        ];
        consolidate(&mut updates);
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].diff, 3);
    }

