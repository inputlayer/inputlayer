//! Value folds for the scalar aggregates `sum`, `avg`, `min` and `max`.
//!
//! `sum` and `avg` skip `Null` and non-numeric values. `min` and `max` skip
//! `Null` and order numerically across integer and float types.

use super::Value;
use std::cmp::Ordering;

/// Aggregate ordering: [`Value::query_cmp`], falling back to `Ord` for
/// incomparable pairs so the result stays deterministic.
pub fn agg_cmp(a: &Value, b: &Value) -> Ordering {
    a.query_cmp(b).unwrap_or_else(|| a.cmp(b))
}

/// Sum as `Int64` when every input is an integer and the total fits;
/// otherwise as `Float64`.
pub fn sum<'a>(values: impl IntoIterator<Item = &'a Value>) -> Value {
    let (mut int, mut float, mut any_float) = (0i128, 0.0f64, false);
    for v in values {
        if let Some(i) = v.as_i64() {
            int += i128::from(i);
        } else if let Value::Float64(f) = v {
            float += f;
            any_float = true;
        }
    }
    match i64::try_from(int) {
        Ok(i) if !any_float => Value::Int64(i),
        _ => Value::Float64(int as f64 + float),
    }
}

/// Mean of the numeric values, or `Null` when there are none.
pub fn avg<'a>(values: impl IntoIterator<Item = &'a Value>) -> Value {
    let (mut total, mut count) = (0.0f64, 0u64);
    for v in values {
        if let Some(f) = v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)) {
            total += f;
            count += 1;
        }
    }
    if count == 0 {
        Value::Null
    } else {
        Value::Float64(total / count as f64)
    }
}

/// Smallest non-null value, or `Null` when there is none.
pub fn min<'a>(values: impl IntoIterator<Item = &'a Value>) -> Value {
    non_null(values)
        .min_by(|a, b| agg_cmp(a, b))
        .cloned()
        .unwrap_or(Value::Null)
}

/// Largest non-null value, or `Null` when there is none.
pub fn max<'a>(values: impl IntoIterator<Item = &'a Value>) -> Value {
    non_null(values)
        .max_by(|a, b| agg_cmp(a, b))
        .cloned()
        .unwrap_or(Value::Null)
}

fn non_null<'a>(values: impl IntoIterator<Item = &'a Value>) -> impl Iterator<Item = &'a Value> {
    values.into_iter().filter(|v| !matches!(v, Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn s(v: &str) -> Value {
        Value::String(Arc::from(v))
    }

    #[test]
    fn test_sum_ints_stays_int() {
        let vals = [Value::Int32(1), Value::Int64(2), Value::Int64(3)];
        assert_eq!(sum(&vals), Value::Int64(6));
    }

    #[test]
    fn test_sum_floats_not_truncated() {
        let vals = [Value::Float64(1.5), Value::Float64(2.5)];
        assert_eq!(sum(&vals), Value::Float64(4.0));
    }

    #[test]
    fn test_sum_mixed_promotes_to_float() {
        let vals = [Value::Int64(1), Value::Float64(0.5), Value::Int64(2)];
        assert_eq!(sum(&vals), Value::Float64(3.5));
    }

    #[test]
    fn test_sum_overflow_promotes_to_float() {
        let vals = [Value::Int64(i64::MAX), Value::Int64(1)];
        assert_eq!(sum(&vals), Value::Float64(i64::MAX as f64 + 1.0));
    }

    #[test]
    fn test_sum_intermediate_overflow_stays_exact() {
        let vals = [Value::Int64(i64::MIN), Value::Int64(-1), Value::Int64(5)];
        assert_eq!(sum(&vals), Value::Int64(i64::MIN + 4));
    }

    #[test]
    fn test_sum_skips_null_and_non_numeric() {
        let vals = [Value::Int64(2), Value::Null, s("x"), Value::Bool(true)];
        assert_eq!(sum(&vals), Value::Int64(2));
        assert_eq!(sum(&[s("a"), s("b")]), Value::Int64(0));
    }

    #[test]
    fn test_avg_skips_null_and_non_numeric() {
        let vals = [Value::Int64(2), Value::Null, s("x"), Value::Float64(4.0)];
        assert_eq!(avg(&vals), Value::Float64(3.0));
        assert_eq!(avg(&[s("a")]), Value::Null);
    }

    #[test]
    fn test_min_max_numeric_across_types() {
        let vals = [Value::Int64(5), Value::Float64(2.5), Value::Int32(3)];
        assert_eq!(min(&vals), Value::Float64(2.5));
        assert_eq!(max(&vals), Value::Int64(5));
    }

    #[test]
    fn test_min_max_skip_null() {
        let vals = [Value::Null, Value::Int64(4), Value::Int64(7)];
        assert_eq!(min(&vals), Value::Int64(4));
        assert_eq!(max(&vals), Value::Int64(7));
        assert_eq!(min(&[Value::Null]), Value::Null);
    }

    #[test]
    fn test_min_max_strings() {
        let vals = [s("b"), s("a"), s("c")];
        assert_eq!(min(&vals), s("a"));
        assert_eq!(max(&vals), s("c"));
    }
}
