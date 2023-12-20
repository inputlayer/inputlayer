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
fn test_zero_variants() {
    let tuple = Tuple::new(vec![Value::Int32(0), Value::Int64(0), Value::Float64(0.0)]);

    assert_eq!(tuple.arity(), 3);
}

// Float Boundary Tests
#[test]
fn test_float_small_values() {
    let tuple = Tuple::new(vec![
        Value::Float64(f64::MIN_POSITIVE),
        Value::Float64(-f64::MIN_POSITIVE),
        Value::Float64(1e-300),
        Value::Float64(-1e-300),
    ]);

    assert_eq!(tuple.arity(), 4);
}

#[test]
fn test_float_large_values() {
    let tuple = Tuple::new(vec![
        Value::Float64(1e100),
        Value::Float64(-1e100),
        Value::Float64(1e308),
        Value::Float64(-1e308),
    ]);

    assert_eq!(tuple.arity(), 4);
}

#[test]
fn test_float_precision() {
    // Very close numbers
    let v1 = Value::Float64(1.0000000000000001);
    let v2 = Value::Float64(1.0000000000000002);

    let tuple = Tuple::new(vec![v1.clone(), v2.clone()]);
    assert_eq!(tuple.arity(), 2);
}

#[test]
fn test_float_special_zero() {
    let pos_zero = Value::Float64(0.0);
    let neg_zero = Value::Float64(-0.0);

    // Both should be valid
    let tuple = Tuple::new(vec![pos_zero, neg_zero]);
    assert_eq!(tuple.arity(), 2);
}

// String Boundary Tests
#[test]
fn test_empty_string() {
    let tuple = Tuple::new(vec![Value::string("")]);
    assert_eq!(tuple.get(0).and_then(|v| v.as_str()), Some(""));
}

#[test]
fn test_single_char_string() {
    let tuple = Tuple::new(vec![Value::string("a")]);
    assert_eq!(tuple.get(0).and_then(|v| v.as_str()), Some("a"));
}

#[test]
fn test_long_string_1kb() {
    let long_str = "a".repeat(1024);
    let tuple = Tuple::new(vec![Value::string(&long_str)]);

    let retrieved = tuple.get(0).and_then(|v| v.as_str()).unwrap();
    assert_eq!(retrieved.len(), 1024);
}

#[test]
fn test_long_string_100kb() {
    let long_str = "b".repeat(102400);
    let tuple = Tuple::new(vec![Value::string(&long_str)]);

    let retrieved = tuple.get(0).and_then(|v| v.as_str()).unwrap();
    assert_eq!(retrieved.len(), 102400);
}

#[test]
fn test_unicode_strings() {
    let strings = vec!["hello", "héllo", "日本語", "Привет", "مرحبا", "你好世界"];

    for s in strings {
        let tuple = Tuple::new(vec![Value::string(s)]);
        assert_eq!(tuple.get(0).and_then(|v| v.as_str()), Some(s));
    }
}

#[test]
fn test_string_with_special_chars() {
    let special = "hello\nworld\ttab\r\nend";
    let tuple = Tuple::new(vec![Value::string(special)]);
    assert_eq!(tuple.get(0).and_then(|v| v.as_str()), Some(special));
}

#[test]
fn test_string_with_quotes() {
    let quoted = r#"say "hello" to 'world'"#;
    let tuple = Tuple::new(vec![Value::string(quoted)]);
    assert_eq!(tuple.get(0).and_then(|v| v.as_str()), Some(quoted));
}

// Arity Boundary Tests
#[test]
fn test_unary_tuple() {
    let tuple = Tuple::new(vec![Value::Int32(1)]);
    assert_eq!(tuple.arity(), 1);
}

#[test]
fn test_binary_tuple() {
    let tuple = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);
    assert_eq!(tuple.arity(), 2);
}

#[test]
fn test_high_arity_tuple_10() {
    let values: Vec<Value> = (0..10).map(|i| Value::Int32(i)).collect();
    let tuple = Tuple::new(values);
    assert_eq!(tuple.arity(), 10);
}

#[test]
fn test_high_arity_tuple_20() {
    let values: Vec<Value> = (0..20).map(|i| Value::Int32(i)).collect();
    let tuple = Tuple::new(values);
    assert_eq!(tuple.arity(), 20);
}

#[test]
fn test_high_arity_tuple_100() {
    let values: Vec<Value> = (0..100).map(|i| Value::Int32(i)).collect();
    let tuple = Tuple::new(values);
    assert_eq!(tuple.arity(), 100);
}

// Vector Dimension Tests
#[test]
fn test_vector_single_dimension() {
    let vec: Vec<f32> = vec![1.0];
    let tuple = Tuple::new(vec![Value::Vector(Arc::new(vec.clone()))]);

    if let Some(Value::Vector(v)) = tuple.get(0) {
        assert_eq!(v.len(), 1);
    }
}

#[test]
fn test_vector_two_dimensions() {
    let vec: Vec<f32> = vec![1.0, 2.0];
    let tuple = Tuple::new(vec![Value::Vector(Arc::new(vec.clone()))]);

    if let Some(Value::Vector(v)) = tuple.get(0) {
        assert_eq!(v.len(), 2);
    }
}

#[test]
