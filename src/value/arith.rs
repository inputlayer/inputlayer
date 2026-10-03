//! Typed arithmetic and comparison for query expressions.
//!
//! Used by head and assignment arithmetic, runtime comparison filters and
//! compile-time constant folding, so all three agree:
//!
//! - Integer `+ - * %` is checked `i64`; overflow yields `Null`.
//! - `/` always yields `Float64`.
//! - A `Float64` operand promotes the result to `Float64`.
//! - `Timestamp ± Int` stays `Timestamp`; `Timestamp - Timestamp` is `Int64`;
//!   otherwise a timestamp acts as its `i64`.
//! - Division or modulo by zero, or any non-numeric operand, yields `Null`.

use super::Value;
use crate::ast::{ArithExpr, ComparisonOp};
use crate::ir::ArithOp;

/// Tolerance for float equality in filters, so rounding such as `0.1 + 0.2`
/// still compares equal to `0.3`.
pub const FLOAT_EQ_TOLERANCE: f64 = 1e-10;

impl From<crate::ast::ArithOp> for ArithOp {
    fn from(op: crate::ast::ArithOp) -> Self {
        use crate::ast::ArithOp as A;
        match op {
            A::Add => ArithOp::Add,
            A::Sub => ArithOp::Sub,
            A::Mul => ArithOp::Mul,
            A::Div => ArithOp::Div,
            A::Mod => ArithOp::Mod,
        }
    }
}

/// Apply `op` to two values.
pub fn eval(op: ArithOp, left: &Value, right: &Value) -> Value {
    if matches!(left, Value::Float64(_)) || matches!(right, Value::Float64(_)) || op == ArithOp::Div
    {
        return match (left.as_f64_numeric(), right.as_f64_numeric()) {
            (Some(l), Some(r)) => eval_f64(op, l, r),
            _ => Value::Null,
        };
    }
    let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) else {
        return Value::Null;
    };
    let is_ts = |v: &Value| matches!(v, Value::Timestamp(_));
    let timestamp = match op {
        ArithOp::Add => is_ts(left) != is_ts(right),
        ArithOp::Sub => is_ts(left) && !is_ts(right),
        _ => false,
    };
    let wrap = if timestamp {
        Value::Timestamp
    } else {
        Value::Int64
    };
    checked_i64(op, l, r).map_or(Value::Null, wrap)
}

fn checked_i64(op: ArithOp, l: i64, r: i64) -> Option<i64> {
    match op {
        ArithOp::Add => l.checked_add(r),
        ArithOp::Sub => l.checked_sub(r),
        ArithOp::Mul => l.checked_mul(r),
        ArithOp::Div => l.checked_div(r),
        ArithOp::Mod => l.checked_rem(r),
    }
}

fn eval_f64(op: ArithOp, l: f64, r: f64) -> Value {
    let v = match op {
        ArithOp::Add => l + r,
        ArithOp::Sub => l - r,
        ArithOp::Mul => l * r,
        ArithOp::Div | ArithOp::Mod if r == 0.0 => return Value::Null,
        ArithOp::Div => l / r,
        ArithOp::Mod => l % r,
    };
    Value::Float64(v)
}

/// Evaluate an AST arithmetic expression. `lookup` resolves variables;
/// returns `None` when it cannot.
pub fn eval_expr(expr: &ArithExpr, lookup: &dyn Fn(&str) -> Option<Value>) -> Option<Value> {
    Some(match expr {
        ArithExpr::Constant(v) => Value::Int64(*v),
        ArithExpr::FloatConstant(bits) => Value::Float64(f64::from_bits(*bits)),
        ArithExpr::Variable(name) => lookup(name)?,
        ArithExpr::Binary { op, left, right } => {
            let l = eval_expr(left, lookup)?;
            let r = eval_expr(right, lookup)?;
            eval((*op).into(), &l, &r)
        }
    })
}

/// Comparison filter over arithmetic results. Orders like
/// [`Value::query_cmp`], except a `Float64` against any numeric (including a
/// timestamp) compares as `f64`; float equality uses [`FLOAT_EQ_TOLERANCE`].
/// Incomparable values (including `Null`) fail every operator.
pub fn compare(left: &Value, op: &ComparisonOp, right: &Value) -> bool {
    let float = matches!(left, Value::Float64(_)) || matches!(right, Value::Float64(_));
    let ord = if float {
        match (left.as_f64_numeric(), right.as_f64_numeric()) {
            (Some(l), Some(r)) => l.partial_cmp(&r),
            _ => None,
        }
    } else {
        left.query_cmp(right)
    };
    let Some(ord) = ord else {
        return false;
    };
    let close = || match (left.as_f64_numeric(), right.as_f64_numeric()) {
        (Some(l), Some(r)) => (l - r).abs() < FLOAT_EQ_TOLERANCE,
        _ => false,
    };
    match op {
        ComparisonOp::Equal if float => close(),
        ComparisonOp::NotEqual if float => !close(),
        ComparisonOp::Equal => ord.is_eq(),
        ComparisonOp::NotEqual => ord.is_ne(),
        ComparisonOp::LessThan => ord.is_lt(),
        ComparisonOp::LessOrEqual => ord.is_le(),
        ComparisonOp::GreaterThan => ord.is_gt(),
        ComparisonOp::GreaterOrEqual => ord.is_ge(),
    }
}

impl Value {
    /// Numeric value as `f64`, including timestamps.
    pub(crate) fn as_f64_numeric(&self) -> Option<f64> {
        match self {
            Value::Timestamp(t) => Some(*t as f64),
            v => v.as_f64(),
        }
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
    fn test_int_ops_checked() {
        assert_eq!(
            eval(ArithOp::Add, &Value::Int32(3), &Value::Int64(4)),
            Value::Int64(7)
        );
        assert_eq!(
            eval(ArithOp::Mod, &Value::Int64(10), &Value::Int64(3)),
            Value::Int64(1)
        );
        assert_eq!(
            eval(ArithOp::Add, &Value::Int64(i64::MAX), &Value::Int64(1)),
            Value::Null
        );
        assert_eq!(
            eval(ArithOp::Mul, &Value::Int64(i64::MAX), &Value::Int64(2)),
            Value::Null
        );
        assert_eq!(
            eval(ArithOp::Mod, &Value::Int64(i64::MIN), &Value::Int64(-1)),
            Value::Null
        );
        assert_eq!(
            eval(ArithOp::Mod, &Value::Int64(1), &Value::Int64(0)),
            Value::Null
        );
    }

    #[test]
    fn test_large_int_exact() {
        let x = Value::Int64((1 << 60) + 1);
        assert_eq!(
            eval(ArithOp::Add, &x, &Value::Int64(1)),
            Value::Int64((1 << 60) + 2)
        );
    }

    #[test]
    fn test_div_is_float() {
        assert_eq!(
            eval(ArithOp::Div, &Value::Int64(7), &Value::Int64(2)),
            Value::Float64(3.5)
        );
        assert_eq!(
            eval(ArithOp::Div, &Value::Int64(i64::MIN), &Value::Int64(-1)),
            Value::Float64(-(i64::MIN as f64))
        );
        assert_eq!(
            eval(ArithOp::Div, &Value::Int64(1), &Value::Int64(0)),
            Value::Null
        );
    }

    #[test]
    fn test_float_promotion() {
        assert_eq!(
            eval(ArithOp::Mul, &Value::Float64(1.5), &Value::Int64(2)),
            Value::Float64(3.0)
        );
        assert_eq!(
            eval(ArithOp::Mod, &Value::Float64(1.0), &Value::Float64(0.0)),
            Value::Null
        );
    }

    #[test]
    fn test_timestamp_arith() {
        let t = Value::Timestamp(1000);
        assert_eq!(
            eval(ArithOp::Add, &t, &Value::Int64(1)),
            Value::Timestamp(1001)
        );
        assert_eq!(
            eval(ArithOp::Add, &Value::Int32(5), &t),
            Value::Timestamp(1005)
        );
        assert_eq!(
            eval(ArithOp::Sub, &t, &Value::Int64(1)),
            Value::Timestamp(999)
        );
        assert_eq!(
            eval(ArithOp::Sub, &t, &Value::Timestamp(400)),
            Value::Int64(600)
        );
    }

    #[test]
    fn test_non_numeric_is_null() {
        for v in [Value::Null, s("abc"), Value::Bool(true)] {
            assert_eq!(eval(ArithOp::Add, &v, &Value::Int64(1)), Value::Null);
            assert_eq!(eval(ArithOp::Div, &Value::Int64(1), &v), Value::Null);
        }
    }

    #[test]
    fn test_eval_expr() {
        use crate::ast::ArithOp as A;
        let expr = ArithExpr::Binary {
            op: A::Mul,
            left: Box::new(ArithExpr::from_float(0.5)),
            right: Box::new(ArithExpr::Variable("X".into())),
        };
        let x = |n: &str| (n == "X").then_some(Value::Int64(3));
        assert_eq!(eval_expr(&expr, &x), Some(Value::Float64(1.5)));
        assert_eq!(eval_expr(&expr, &|_| None), None);
    }

    #[test]
    fn test_compare() {
        use ComparisonOp::*;
        assert!(compare(
            &Value::Float64(3.0),
            &GreaterThan,
            &Value::Float64(2.0)
        ));
        assert!(compare(
            &Value::Float64(0.3),
            &Equal,
            &Value::Float64(0.1 + 0.2)
        ));
        assert!(compare(
            &Value::Int64(i64::MAX),
            &GreaterThan,
            &Value::Int64(i64::MAX - 1)
        ));
        let t = Value::Timestamp(1000);
        assert!(compare(&t, &LessThan, &Value::Float64(1000.5)));
        assert!(compare(&t, &Equal, &Value::Float64(1000.0)));
        assert!(compare(&Value::Float64(999.5), &NotEqual, &t));
        for op in [Equal, NotEqual, LessThan, GreaterOrEqual] {
            assert!(!compare(&Value::Int64(1), &op, &Value::Null));
            assert!(!compare(&Value::Float64(1.0), &op, &Value::Null));
            assert!(!compare(&s("1"), &op, &Value::Int64(1)));
        }
    }
}
