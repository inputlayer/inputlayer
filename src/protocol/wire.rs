//! Wire Format Types
//!
//! Serializable types for client-server communication: WireValue, WireTuple, QueryResult, ColumnDef.

use serde::{Deserialize, Serialize};

// Wire Data Type
/// Wire-serializable data type enum.
///
/// Represents the schema type of a column, used for describing relation schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireDataType {
    Int32,
    Int64,
    Float64,
    String,
    Bool,
    Timestamp,
    Vector { dim: Option<usize> },
    VectorInt8 { dim: Option<usize> },
    Bytes,
}

impl std::fmt::Display for WireDataType {
    fn fmt(self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireDataType::Int32 => write!(f, "Int32"),
            WireDataType::Int64 => write!(f, "Int64"),
            WireDataType::Float64 => write!(f, "Float64"),
            WireDataType::String => write!(f, "String"),
            WireDataType::Bool => write!(f, "Bool"),
            WireDataType::Timestamp => write!(f, "Timestamp"),
            WireDataType::Vector { dim: Some(d) } => write!(f, "Vector[{d}]"),
            WireDataType::Vector { dim: None } => write!(f, "Vector"),
            WireDataType::VectorInt8 { dim: Some(d) } => write!(f, "VectorInt8[{d}]"),
            WireDataType::VectorInt8 { dim: None } => write!(f, "VectorInt8"),
            WireDataType::Bytes => write!(f, "Bytes"),
        }
    }
}

// Wire Value
/// Wire-serializable value enum.
///
/// Represents a single cell value in a tuple. Supports all `InputLayer` value types
/// including vectors and timestamps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    /// Timestamp as Unix milliseconds
    Timestamp(i64),
    /// Full-precision f32 vector
    Vector(Vec<f32>),
    /// Quantized int8 vector
    VectorInt8(Vec<i8>),
    /// Binary data
    Bytes(Vec<u8>),
}

