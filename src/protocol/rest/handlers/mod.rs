//! REST API Handlers
//!
//! Contains all HTTP endpoint handlers organized by domain.

pub mod admin;
pub mod data;
pub mod knowledge_graph;
pub mod query;
pub mod relations;
pub mod rules;
pub mod views;

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
