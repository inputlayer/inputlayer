//! REST API Handlers
//!
//! Contains all HTTP endpoint handlers organized by domain.

pub mod admin;
pub mod data;
pub mod knowledge_graph;
pub mod query;
pub mod relations;
pub mod rules;
pub mod sessions;
pub mod views;
pub mod ws;

use crate::protocol::wire::WireValue;
use crate::value::Value;

/// Convert a JSON value to a storage Value
///
/// Used by data, session, and WebSocket handlers for consistent JSON → Value conversion.
pub fn json_to_value(json: &serde_json::Value) -> Result<Value, String> {
    match json {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int64(i))
            } else if let Some(f) = n.as_f64() {
                if !f.is_finite() {
                    return Err(format!("Non-finite float not supported: {n}"));
                }
                Ok(Value::Float64(f))
            } else {
                Err(format!("Unsupported number: {n}"))
            }
        }
        serde_json::Value::String(s) => Ok(Value::string(s)),
        serde_json::Value::Array(arr) => {
            let floats: Result<Vec<f32>, String> = arr
                .iter()
                .map(|v| {
                    v.as_f64()
                        .ok_or_else(|| format!("Vector element must be a number: {v}"))
                        .and_then(|f| {
                            let f32_val = f as f32;
                            if !f32_val.is_finite() {
                                return Err(format!("Vector element overflows f32: {v}"));
                            }
                            Ok(f32_val)
                        })
                })
                .collect();
            Ok(Value::vector(floats?))
        }
        serde_json::Value::Object(_) => Err("Object values not supported".to_string()),
    }
}

/// Convert `WireValue` to `serde_json::Value`
///
/// Used by multiple handlers to convert query results to JSON responses.
pub fn wire_value_to_json(value: WireValue) -> serde_json::Value {
    match value {
        WireValue::Null => serde_json::Value::Null,
        WireValue::Int32(i) => serde_json::Value::Number(i.into()),
        WireValue::Int64(i) => serde_json::json!(i),
        WireValue::Float64(f) => serde_json::json!(f),
        WireValue::String(s) => serde_json::Value::String(s),
        WireValue::Bool(b) => serde_json::Value::Bool(b),
        WireValue::Timestamp(t) => serde_json::json!(t),
        WireValue::Vector(v) => serde_json::json!(v),
        WireValue::VectorInt8(v) => serde_json::json!(v),
        WireValue::Bytes(b) => serde_json::json!(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_value_to_json_null() {
        assert_eq!(wire_value_to_json(WireValue::Null), serde_json::Value::Null);
    }

    #[test]
    fn test_wire_value_to_json_int32() {
        let json = wire_value_to_json(WireValue::Int32(42));
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_wire_value_to_json_int64() {
        let json = wire_value_to_json(WireValue::Int64(i64::MAX));
        assert_eq!(json, serde_json::json!(i64::MAX));
    }

    #[test]
    fn test_wire_value_to_json_float64() {
        let json = wire_value_to_json(WireValue::Float64(3.14));
        assert_eq!(json, serde_json::json!(3.14));
    }

    #[test]
    fn test_wire_value_to_json_string() {
        let json = wire_value_to_json(WireValue::String("hello".to_string()));
        assert_eq!(json, serde_json::json!("hello"));
    }

    #[test]
    fn test_wire_value_to_json_bool_true() {
        let json = wire_value_to_json(WireValue::Bool(true));
        assert_eq!(json, serde_json::json!(true));
    }

    #[test]
    fn test_wire_value_to_json_bool_false() {
        let json = wire_value_to_json(WireValue::Bool(false));
        assert_eq!(json, serde_json::json!(false));
    }

    #[test]
    fn test_wire_value_to_json_vector() {
        let json = wire_value_to_json(WireValue::Vector(vec![1.0, 2.0, 3.0]));
        assert_eq!(json, serde_json::json!([1.0, 2.0, 3.0]));
    }

    #[test]
    fn test_wire_value_to_json_vector_int8() {
        let json = wire_value_to_json(WireValue::VectorInt8(vec![1, 2, 3]));
        assert_eq!(json, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_wire_value_to_json_bytes() {
        let json = wire_value_to_json(WireValue::Bytes(vec![0xDE, 0xAD]));
        assert_eq!(json, serde_json::json!([222, 173]));
    }

    #[test]
    fn test_wire_value_to_json_timestamp() {
        let json = wire_value_to_json(WireValue::Timestamp(1234567890));
        assert_eq!(json, serde_json::json!(1234567890));
    }

    #[test]
    fn test_wire_value_to_json_negative_int() {
        let json = wire_value_to_json(WireValue::Int64(-100));
        assert_eq!(json, serde_json::json!(-100));
    }

    #[test]
    fn test_wire_value_to_json_empty_string() {
        let json = wire_value_to_json(WireValue::String(String::new()));
        assert_eq!(json, serde_json::json!(""));
    }

    #[test]
    fn test_wire_value_to_json_empty_vector() {
        let json = wire_value_to_json(WireValue::Vector(vec![]));
        assert_eq!(json, serde_json::json!([]));
    }

    // json_to_value tests

    #[test]
    fn test_json_to_value_integer() {
        assert_eq!(
            json_to_value(&serde_json::json!(42)).unwrap(),
            Value::Int64(42)
        );
    }

    #[test]
    fn test_json_to_value_negative_int() {
        assert_eq!(
            json_to_value(&serde_json::json!(-100)).unwrap(),
            Value::Int64(-100)
        );
    }

    #[test]
    fn test_json_to_value_float() {
        assert_eq!(
            json_to_value(&serde_json::json!(3.14)).unwrap(),
            Value::Float64(3.14)
        );
    }

    #[test]
    fn test_json_to_value_string() {
        assert_eq!(
            json_to_value(&serde_json::json!("hello")).unwrap(),
            Value::string("hello")
        );
    }

    #[test]
    fn test_json_to_value_bool_true() {
        assert_eq!(
            json_to_value(&serde_json::json!(true)).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_json_to_value_bool_false() {
        assert_eq!(
            json_to_value(&serde_json::json!(false)).unwrap(),
            Value::Bool(false)
        );
    }

    #[test]
    fn test_json_to_value_null() {
        assert_eq!(
            json_to_value(&serde_json::json!(null)).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_json_to_value_object_error() {
        assert!(json_to_value(&serde_json::json!({"key": "val"})).is_err());
    }

    #[test]
    fn test_json_to_value_vector() {
        assert_eq!(
            json_to_value(&serde_json::json!([1.0, 2.0, 3.0])).unwrap(),
            Value::vector(vec![1.0, 2.0, 3.0])
        );
    }

    #[test]
    fn test_json_to_value_vector_with_non_number() {
        assert!(json_to_value(&serde_json::json!([1.0, "bad"])).is_err());
    }

    #[test]
    fn test_json_to_value_empty_array() {
        assert_eq!(
            json_to_value(&serde_json::json!([])).unwrap(),
            Value::vector(vec![])
        );
    }

    #[test]
    fn test_json_to_value_large_int() {
        assert_eq!(
            json_to_value(&serde_json::json!(i64::MAX)).unwrap(),
            Value::Int64(i64::MAX)
        );
    }

    #[test]
    fn test_json_to_value_vector_f64_overflow_to_f32() {
        // f64 value that overflows f32 should be rejected
        let huge = f64::MAX;
        let arr = serde_json::json!([huge]);
        assert!(json_to_value(&arr).is_err());
    }

    #[test]
    fn test_json_to_value_vector_normal_f32_range() {
        // Normal f32-range values should work fine
        let arr = serde_json::json!([1.5, -2.5, 0.0]);
        let result = json_to_value(&arr).unwrap();
        assert_eq!(result, Value::vector(vec![1.5, -2.5, 0.0]));
    }
}
