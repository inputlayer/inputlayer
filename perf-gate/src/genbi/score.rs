//! Exact result equivalence, as genbi-trust's `score_suite.py` defines it:
//! order-independent, duplicate multiplicity preserved, integers must stay
//! integers, floats within 1e-9, everything else equal in type and value.

use serde::Serialize;
use serde_json::Value;

/// How an actual row multiset differs from the expected one. Counts only:
/// expected rows themselves are evaluator-private and never reported.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Diff {
    /// Expected rows with no matching actual row.
    pub missing: usize,
    /// Actual rows with no matching expected row.
    pub extra: usize,
}

impl Diff {
    pub fn is_equal(self) -> bool {
        self.missing == 0 && self.extra == 0
    }
}

/// Greedy multiset matching, as in the reference scorer.
pub fn compare(actual: &[Vec<Value>], expected: &[Vec<Value>]) -> Diff {
    let mut unmatched: Vec<&Vec<Value>> = expected.iter().collect();
    let mut extra = 0;
    for row in actual {
        match unmatched.iter().position(|e| row_equivalent(row, e)) {
            Some(i) => {
                unmatched.swap_remove(i);
            }
            None => extra += 1,
        }
    }
    Diff {
        missing: unmatched.len(),
        extra,
    }
}

fn row_equivalent(actual: &[Value], expected: &[Value]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(a, e)| value_equivalent(a, e))
}

fn value_equivalent(actual: &Value, expected: &Value) -> bool {
    match (actual, expected) {
        (Value::Number(a), Value::Number(e)) if !e.is_f64() => !a.is_f64() && a == e,
        (Value::Number(a), Value::Number(e)) => match (a.as_f64(), e.as_f64()) {
            (Some(a), Some(e)) => a.is_finite() && (a - e).abs() <= 1e-9,
            _ => false,
        },
        _ => actual == expected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows(v: Value) -> Vec<Vec<Value>> {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn order_does_not_matter_but_multiplicity_does() {
        let expected = rows(json!([["a", 1], ["a", 1], ["b", 2]]));
        assert!(compare(&rows(json!([["b", 2], ["a", 1], ["a", 1]])), &expected).is_equal());
        assert_eq!(
            compare(&rows(json!([["a", 1], ["b", 2]])), &expected),
            Diff {
                missing: 1,
                extra: 0
            }
        );
    }

    #[test]
    fn integers_must_stay_integers() {
        let expected = rows(json!([[100]]));
        assert!(!compare(&rows(json!([[100.0]])), &expected).is_equal());
        assert!(!compare(&rows(json!([["100"]])), &expected).is_equal());
        assert!(compare(&rows(json!([[0.1]])), &rows(json!([[0.100_000_000_000_1]]))).is_equal());
        assert!(compare(&rows(json!([[1]])), &rows(json!([[1.0]]))).is_equal());
    }

    #[test]
    fn wrong_values_count_as_missing_and_extra() {
        assert_eq!(
            compare(&rows(json!([["a", 2]])), &rows(json!([["a", 1]]))),
            Diff {
                missing: 1,
                extra: 1
            }
        );
    }
}
