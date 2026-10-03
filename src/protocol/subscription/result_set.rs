//! A standing query's result set: its rows under set semantics.
//!
//! Two rows are the same row exactly when their canonical JSON is the same
//! text, but neither side builds that text: rows are hashed and compared
//! structurally, numbers by representation and bits, so `1` and `1.0`, or
//! `0.0` and `-0.0`, stay as distinct as their JSON. The hash only picks a
//! bucket; equality decides membership, so colliding rows are never confused.
//!
//! Rows leave a set sorted by their canonical JSON, the order clients see in
//! snapshots and deltas. Only the rows that leave are rendered for sorting,
//! never the whole set.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::fmt;
use std::hash::{BuildHasher, Hash, Hasher};

use serde_json::{Number, Value};

use super::Row;

/// The complete result of a standing query at one revision.
pub struct ResultSet<S = RandomState> {
    rows: HashSet<RowKey, S>,
}

impl<S: BuildHasher + Default> Default for ResultSet<S> {
    fn default() -> Self {
        Self {
            rows: HashSet::default(),
        }
    }
}

impl<S: BuildHasher + Default> FromIterator<Row> for ResultSet<S> {
    fn from_iter<I: IntoIterator<Item = Row>>(rows: I) -> Self {
        Self {
            rows: rows.into_iter().map(RowKey).collect(),
        }
    }
}

impl<S: BuildHasher> ResultSet<S> {
    /// Number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when the result has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Rows of `self` missing from `other`, sorted.
    pub fn difference(&self, other: &Self) -> Vec<Row> {
        sorted(
            self.rows
                .iter()
                .filter(|row| !other.rows.contains(*row))
                .map(|row| row.0.clone())
                .collect(),
        )
    }

    /// Every row, sorted.
    pub fn sorted_rows(&self) -> Vec<Row> {
        sorted(self.rows.iter().map(|row| row.0.clone()).collect())
    }
}

impl<S> fmt::Debug for ResultSet<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResultSet")
            .field("rows", &self.rows.len())
            .finish()
    }
}

/// `rows` in canonical JSON order.
fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by_cached_key(|row| CanonicalJson(row).to_string());
    rows
}

/// A row's canonical JSON text: compact, as `serde_json` writes the array.
struct CanonicalJson<'a>(&'a Row);

impl fmt::Display for CanonicalJson<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (index, value) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{value}")?;
        }
        f.write_str("]")
    }
}

/// A row as a set member: structural hash, exact equality.
struct RowKey(Row);

impl PartialEq for RowKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len() && self.0.iter().zip(&other.0).all(|(a, b)| identical(a, b))
    }
}

impl Eq for RowKey {}

impl Hash for RowKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.0.len());
        for value in &self.0 {
            hash_value(value, state);
        }
    }
}

/// Whether `a` and `b` have the same canonical JSON.
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => identical_numbers(x, y),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| identical(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((ka, va), (kb, vb))| ka == kb && identical(va, vb))
        }
        _ => a == b,
    }
}

/// Integers by value, floats by bits; an integer never equals a float.
fn identical_numbers(x: &Number, y: &Number) -> bool {
    match (float_bits(x), float_bits(y)) {
        (Some(x), Some(y)) => x == y,
        (None, None) => x == y,
        _ => false,
    }
}

fn float_bits(n: &Number) -> Option<u64> {
    n.is_f64().then(|| n.as_f64().map_or(0, f64::to_bits))
}

/// Feed `value` to `state` consistently with [`identical`].
fn hash_value<H: Hasher>(value: &Value, state: &mut H) {
    match value {
        Value::Null => state.write_u8(0),
        Value::Bool(b) => {
            state.write_u8(1);
            b.hash(state);
        }
        Value::Number(n) => {
            state.write_u8(2);
            if let Some(bits) = float_bits(n) {
                state.write_u8(0);
                state.write_u64(bits);
            } else if let Some(u) = n.as_u64() {
                state.write_u8(1);
                state.write_u64(u);
            } else if let Some(i) = n.as_i64() {
                state.write_u8(2);
                state.write_i64(i);
            }
        }
        Value::String(s) => {
            state.write_u8(3);
            s.hash(state);
        }
        Value::Array(items) => {
            state.write_u8(4);
            state.write_usize(items.len());
            for item in items {
                hash_value(item, state);
            }
        }
        Value::Object(map) => {
            state.write_u8(5);
            state.write_usize(map.len());
            for (key, item) in map {
                key.hash(state);
                hash_value(item, state);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::hash::BuildHasherDefault;

    use serde_json::json;

    use super::*;

    /// Every row hashes to the same bucket.
    #[derive(Default)]
    struct Colliding;

    impl Hasher for Colliding {
        fn finish(&self) -> u64 {
            42
        }
        fn write(&mut self, _: &[u8]) {}
    }

    type CollidingSet = ResultSet<BuildHasherDefault<Colliding>>;

    fn rows(values: &[Value]) -> Vec<Row> {
        values
            .iter()
            .map(|v| v.as_array().unwrap().clone())
            .collect()
    }

    fn set<S: BuildHasher + Default>(values: &[Value]) -> ResultSet<S> {
        rows(values).into_iter().collect()
    }

    #[test]
    fn difference_is_sorted_by_canonical_json() {
        let a: ResultSet = set(&[json!([10]), json!([9]), json!(["b"]), json!([1, 2])]);
        let b: ResultSet = set(&[json!([9])]);
        assert_eq!(
            a.difference(&b),
            rows(&[json!(["b"]), json!([1, 2]), json!([10])])
        );
        assert!(b.difference(&a).is_empty());
    }

    #[test]
    fn duplicates_collapse_and_sorted_rows_lists_each_once() {
        let a: ResultSet = set(&[json!([2]), json!([1]), json!([2])]);
        assert_eq!(a.len(), 2);
        assert_eq!(a.sorted_rows(), rows(&[json!([1]), json!([2])]));
    }

    #[test]
    fn rows_with_different_json_are_different_rows() {
        let distinct = [
            json!([1]),
            json!([1.0]),
            json!([-1]),
            json!([0.0]),
            json!([-0.0]),
            json!(["1"]),
            json!([true]),
            json!([null]),
            json!([[1, 2]]),
            json!([[1], 2]),
            json!([{"a": 1}]),
            json!([{"a": 1.0}]),
            json!([1, 2]),
        ];
        let a: ResultSet = set(&distinct);
        assert_eq!(a.len(), distinct.len());
        let b: ResultSet = set(&distinct);
        assert!(a.difference(&b).is_empty());
    }

    #[test]
    fn colliding_hashes_never_confuse_rows() {
        let before: CollidingSet = set(&[json!([1, "x"]), json!([2, "y"]), json!([0.0])]);
        let after: CollidingSet = set(&[json!([2, "y"]), json!([3, "z"]), json!([-0.0])]);
        assert_eq!(after.len(), 3);
        assert_eq!(
            after.difference(&before),
            rows(&[json!([-0.0]), json!([3, "z"])])
        );
        assert_eq!(
            before.difference(&after),
            rows(&[json!([0.0]), json!([1, "x"])])
        );
    }

    #[test]
    fn canonical_json_matches_serde_json() {
        let row =
            rows(&[json!([1, -2, 0.5, "q\"uote", null, true, [1.5, 2], {"k": [1]}])]).remove(0);
        assert_eq!(
            CanonicalJson(&row).to_string(),
            Value::Array(row.clone()).to_string()
        );
    }
}
