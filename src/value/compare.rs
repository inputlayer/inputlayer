//! Query-level comparison semantics for `Value`.
//!
//! `Ord for Value` is a total order used for sorting and DD arrangements; it
//! orders across types (e.g. every Int < every String). Query comparisons
//! (`X < Y`, `X = Y`) must not leak that cross-type order into results, so
//! they go through these helpers instead.

use super::Value;
use std::cmp::Ordering;

impl Value {
    /// Compare two values for a query comparison (`<`, `<=`, `>`, `>=`).
    ///
    /// - Integers (`Int32`, `Int64`, `Timestamp`) compare as `i64`.
    /// - Any integer/float mix compares as `f64`.
    /// - `String` and `Bool` compare with their same-type order from `Ord`.
    /// - Everything else (mixed types, vectors, null, NaN) is incomparable
    ///   and returns `None`, so every ordering comparison is false.
    pub fn query_cmp(&self, other: &Value) -> Option<Ordering> {
        if let (Some(a), Some(b)) = (self.as_i64(), other.as_i64()) {
            return Some(a.cmp(&b));
        }
        if let (Some(a), Some(b)) = (self.as_f64(), other.as_f64()) {
            return a.partial_cmp(&b);
        }
        match (self, other) {
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    /// Equality for a query comparison (`=`, `!=`).
    ///
    /// Numerically equal values are equal across integer/float types;
    /// otherwise falls back to structural `Value` equality.
    pub fn query_eq(&self, other: &Value) -> bool {
        self.query_cmp(other) == Some(Ordering::Equal) || self == other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn s(v: &str) -> Value {
        Value::String(Arc::from(v))
    }

    #[test]
    fn test_query_cmp_strings_lexicographic() {
        assert_eq!(s("apple").query_cmp(&s("banana")), Some(Ordering::Less));
        assert_eq!(s("b").query_cmp(&s("a")), Some(Ordering::Greater));
        assert_eq!(s("x").query_cmp(&s("x")), Some(Ordering::Equal));
    }

    #[test]
    fn test_query_cmp_mixed_numeric_widths() {
        assert_eq!(
            Value::Int32(1).query_cmp(&Value::Int64(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Int64(3).query_cmp(&Value::Float64(2.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Timestamp(100).query_cmp(&Value::Timestamp(50)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Timestamp(100).query_cmp(&Value::Int64(100)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn test_query_cmp_large_ints_avoid_float_rounding() {
        let a = Value::Int64(i64::MAX);
        let b = Value::Int64(i64::MAX - 1);
        assert_eq!(a.query_cmp(&b), Some(Ordering::Greater));
    }

    #[test]
    fn test_query_cmp_incomparable_returns_none() {
        assert_eq!(Value::Int32(1).query_cmp(&s("1")), None);
        assert_eq!(s("a").query_cmp(&Value::Bool(true)), None);
        assert_eq!(Value::Null.query_cmp(&Value::Null), None);
        assert_eq!(
            Value::Float64(f64::NAN).query_cmp(&Value::Float64(1.0)),
            None
        );
        assert_eq!(Value::Timestamp(1).query_cmp(&Value::Float64(1.0)), None);
        let v = Value::Vector(Arc::new(vec![1.0]));
        assert_eq!(v.query_cmp(&v), None);
    }

    #[test]
    fn test_query_eq_numeric_and_structural() {
        assert!(Value::Int32(1).query_eq(&Value::Int64(1)));
        assert!(Value::Int64(2).query_eq(&Value::Float64(2.0)));
        assert!(!Value::Int32(1).query_eq(&s("1")));
        assert!(Value::Null.query_eq(&Value::Null));
        let v = Value::Vector(Arc::new(vec![1.0, 2.0]));
        assert!(v.query_eq(&v.clone()));
    }
}
