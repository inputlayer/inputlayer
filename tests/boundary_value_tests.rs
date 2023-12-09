//! Boundary Value Tests
//!
//! Tests at exact boundary conditions - many bugs occur at exact limits.
//!
//! Tests for value handling at system boundaries:
//! - Integer boundaries (INT64_MIN, INT64_MAX)
//! - Float boundaries
//! - String boundaries (empty, long, Unicode)
//! - Vector boundaries
//! - Arity boundaries
//! - Collection size boundaries

use inputlayer::{Tuple, Value};
use std::i32;
use std::i64;
use std::sync::Arc;

// Integer Boundary Tests
#[test]
fn test_tuple_with_int64_max() {
    let max_val = i64::MAX;
    let tuple = Tuple::new(vec![Value::Int64(max_val)]);

    assert_eq!(tuple.arity(), 1);
    assert_eq!(tuple.get(0), Some(&Value::Int64(max_val)));
}

#[test]
fn test_tuple_with_int64_min() {
    let min_val = i64::MIN;
    let tuple = Tuple::new(vec![Value::Int64(min_val)]);

    assert_eq!(tuple.arity(), 1);
    assert_eq!(tuple.get(0), Some(&Value::Int64(min_val)));
}

#[test]
fn test_tuple_with_int64_boundary_values() {
    let values = vec![
        Value::Int64(i64::MAX),
        Value::Int64(i64::MAX - 1),
        Value::Int64(i64::MIN),
        Value::Int64(i64::MIN + 1),
        Value::Int64(0),
        Value::Int64(-1),
        Value::Int64(1),
    ];

    let tuple = Tuple::new(values.clone());
    assert_eq!(tuple.arity(), 7);

    for (i, expected) in values.iter().enumerate() {
        assert_eq!(tuple.get(i), Some(expected));
    }
}

#[test]
fn test_int32_boundary_values() {
    let values = vec![
        Value::Int32(i32::MAX),
        Value::Int32(i32::MAX - 1),
        Value::Int32(i32::MIN),
        Value::Int32(i32::MIN + 1),
        Value::Int32(0),
        Value::Int32(-1),
        Value::Int32(1),
    ];

    let tuple = Tuple::new(values.clone());
    assert_eq!(tuple.arity(), 7);

    for (i, expected) in values.iter().enumerate() {
        assert_eq!(tuple.get(i), Some(expected));
    }
}

#[test]
