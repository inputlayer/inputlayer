//! JSON serialization round-trips for Value types, DTOs, and edge cases.

use inputlayer::value::{DataType, Tuple, Value};
use serde_json;
use std::sync::Arc;

// Value Serialization Tests
#[test]
fn test_int32_json_roundtrip() {
    let values = vec![
        Value::Int32(0),
        Value::Int32(1),
        Value::Int32(-1),
        Value::Int32(i32::MAX),
        Value::Int32(i32::MIN),
        Value::Int32(42),
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(
            original, deserialized,
            "Int32 roundtrip failed for {:?}",
            original
        );
    }
}

#[test]
fn test_int64_json_roundtrip() {
    let values = vec![
        Value::Int64(0),
        Value::Int64(1),
        Value::Int64(-1),
        Value::Int64(i64::MAX),
        Value::Int64(i64::MIN),
        Value::Int64(i32::MAX as i64 + 1), // Beyond i32 range
        Value::Int64(i32::MIN as i64 - 1), // Beyond i32 range
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(
            original, deserialized,
            "Int64 roundtrip failed for {:?}",
            original
        );
    }
}

#[test]
fn test_float64_json_roundtrip() {
    let values = vec![
        Value::Float64(0.0),
        Value::Float64(1.0),
        Value::Float64(-1.0),
        Value::Float64(3.14159265358979),
        Value::Float64(f64::MIN),
        Value::Float64(f64::MAX),
        Value::Float64(f64::MIN_POSITIVE),
        Value::Float64(1e-300),
        Value::Float64(1e300),
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");

        // For floats, compare with tolerance due to JSON precision
        match (&original, &deserialized) {
            (Value::Float64(a), Value::Float64(b)) => {
                if a.is_finite() && b.is_finite() {
                    assert!(
                        (a - b).abs() < 1e-10 || (a - b).abs() / a.abs().max(b.abs()) < 1e-10,
                        "Float64 roundtrip failed: {} vs {}",
                        a,
                        b
                    );
                } else {
                    // For infinity/special values, use exact comparison
                    assert_eq!(a, b);
                }
            }
            _ => panic!("Expected Float64 values"),
        }
    }
}

#[test]
fn test_string_json_roundtrip() {
    let values = vec![
        Value::String(Arc::from("")),
        Value::String(Arc::from("hello")),
        Value::String(Arc::from("Hello, World!")),
        Value::String(Arc::from("with spaces")),
        Value::String(Arc::from("with\nnewline")),
        Value::String(Arc::from("with\ttab")),
        Value::String(Arc::from("with\"quotes\"")),
        Value::String(Arc::from("with\\backslash")),
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(
            original, deserialized,
            "String roundtrip failed for {:?}",
            original
        );
    }
}

#[test]
