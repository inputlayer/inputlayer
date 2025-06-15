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
fn test_bool_json_roundtrip() {
    let values = vec![Value::Bool(true), Value::Bool(false)];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(
            original, deserialized,
            "Bool roundtrip failed for {:?}",
            original
        );
    }
}

#[test]
fn test_null_json_roundtrip() {
    let original = Value::Null;
    let json = serde_json::to_string(&original).expect("Serialization failed");
    let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(original, deserialized, "Null roundtrip failed");
}

#[test]
fn test_vector_json_roundtrip() {
    let values = vec![
        Value::vector(vec![]),
        Value::vector(vec![1.0]),
        Value::vector(vec![1.0, 2.0, 3.0]),
        Value::vector(vec![0.0, -1.0, 1.0, 0.5, -0.5]),
        Value::vector(vec![f32::MIN, f32::MAX]),
        Value::vector((0..128).map(|i| i as f32 * 0.1).collect()), // 128-dim embedding
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");

        match (&original, &deserialized) {
            (Value::Vector(a), Value::Vector(b)) => {
                assert_eq!(a.len(), b.len(), "Vector length mismatch");
                for (va, vb) in a.iter().zip(b.iter()) {
                    // Use 1e-5 tolerance for f32 (has ~7 significant digits)
                    assert!(
                        (va - vb).abs() < 1e-5,
                        "Vector element mismatch: {} vs {}",
                        va,
                        vb
                    );
                }
            }
            _ => panic!("Expected Vector values"),
        }
    }
}

#[test]
fn test_vector_int8_json_roundtrip() {
    let values = vec![
        Value::vector_int8(vec![]),
        Value::vector_int8(vec![0]),
        Value::vector_int8(vec![1, -1, 0]),
        Value::vector_int8(vec![i8::MIN, i8::MAX]),
        Value::vector_int8((-64..64).collect()), // 128-dim quantized embedding
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(original, deserialized, "VectorInt8 roundtrip failed");
    }
}

#[test]
fn test_timestamp_json_roundtrip() {
    let values = vec![
        Value::Timestamp(0),
        Value::Timestamp(1),
        Value::Timestamp(-1),
        Value::Timestamp(1704067200000), // 2024-01-01 00:00:00 UTC
        Value::Timestamp(i64::MAX),
        Value::Timestamp(i64::MIN),
    ];

    for original in values {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Value = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(
            original, deserialized,
            "Timestamp roundtrip failed for {:?}",
            original
        );
    }
}

// Tuple Serialization Tests
#[test]
fn test_tuple_json_roundtrip() {
    let tuples = vec![
        Tuple::new(vec![]),
        Tuple::new(vec![Value::Int32(1)]),
        Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
        Tuple::new(vec![
            Value::Int32(1),
            Value::String(Arc::from("hello")),
            Value::Float64(3.14),
        ]),
        Tuple::new(vec![
            Value::Int64(100),
            Value::Bool(true),
            Value::Null,
            Value::Timestamp(1704067200000),
        ]),
    ];

    for original in tuples {
        let json = serde_json::to_string(&original).expect("Serialization failed");
        let deserialized: Tuple = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(original, deserialized, "Tuple roundtrip failed");
    }
}

#[test]
fn test_tuple_with_vectors_roundtrip() {
    let original = Tuple::new(vec![
        Value::Int32(1),
        Value::vector(vec![0.1, 0.2, 0.3]),
        Value::vector_int8(vec![1, 2, 3]),
    ]);

    let json = serde_json::to_string(&original).expect("Serialization failed");
    let deserialized: Tuple = serde_json::from_str(&json).expect("Deserialization failed");

    // Compare element by element for vectors
    assert_eq!(original.arity(), deserialized.arity());
    assert_eq!(original.get(0), deserialized.get(0));

    // Vector comparison with tolerance (f32 has ~7 significant digits, use 1e-5)
    match (original.get(1), deserialized.get(1)) {
        (Some(Value::Vector(a)), Some(Value::Vector(b))) => {
            assert_eq!(a.len(), b.len());
            for (va, vb) in a.iter().zip(b.iter()) {
                assert!((va - vb).abs() < 1e-5);
            }
        }
        _ => panic!("Expected Vector"),
    }

    assert_eq!(original.get(2), deserialized.get(2));
}

// Edge Case Tests
#[test]
fn test_special_float_values_serialization() {
    // Note: JSON doesn't natively support NaN/Infinity, so we test that
    // our serialization handles these gracefully

    let infinity = Value::Float64(f64::INFINITY);
    let neg_infinity = Value::Float64(f64::NEG_INFINITY);
    let nan = Value::Float64(f64::NAN);

    // Our Value type serializes Infinity/NaN as null (which is valid JSON)
    // This is a graceful fallback since standard JSON doesn't support these values
    let inf_result = serde_json::to_string(&infinity);
    let neg_inf_result = serde_json::to_string(&neg_infinity);
    let nan_result = serde_json::to_string(&nan);

    // Serialization should succeed (converting to null or similar representation)
    assert!(
        inf_result.is_ok(),
        "Infinity should serialize (typically to null): {:?}",
        inf_result
    );
    assert!(
        neg_inf_result.is_ok(),
        "Negative infinity should serialize (typically to null): {:?}",
        neg_inf_result
    );
    assert!(
        nan_result.is_ok(),
        "NaN should serialize (typically to null): {:?}",
        nan_result
    );

    // Verify the serialized values contain null for the value field
    let inf_json = inf_result.unwrap();
    let neg_inf_json = neg_inf_result.unwrap();
    let nan_json = nan_result.unwrap();

    assert!(
        inf_json.contains("null"),
        "Infinity should serialize value as null: {}",
        inf_json
    );
    assert!(
        neg_inf_json.contains("null"),
        "Neg infinity should serialize value as null: {}",
        neg_inf_json
    );
    assert!(
        nan_json.contains("null"),
        "NaN should serialize value as null: {}",
        nan_json
    );
}

#[test]
