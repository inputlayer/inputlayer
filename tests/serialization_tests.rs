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
