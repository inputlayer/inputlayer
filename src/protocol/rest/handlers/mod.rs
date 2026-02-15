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
}
