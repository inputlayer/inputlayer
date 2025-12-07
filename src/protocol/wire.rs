//! Wire Format Types
//!
//! Serializable types for network communication between InputLayer client and server.
//! These types bridge the gap between the engine's internal types (Value, Tuple)
//! and the network protocol.

use serde::{Deserialize, Serialize};

// ============================================================================
// Wire Data Type
// ============================================================================

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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireDataType::Int32 => write!(f, "Int32"),
            WireDataType::Int64 => write!(f, "Int64"),
            WireDataType::Float64 => write!(f, "Float64"),
            WireDataType::String => write!(f, "String"),
            WireDataType::Bool => write!(f, "Bool"),
            WireDataType::Timestamp => write!(f, "Timestamp"),
            WireDataType::Vector { dim: Some(d) } => write!(f, "Vector[{}]", d),
            WireDataType::Vector { dim: None } => write!(f, "Vector"),
            WireDataType::VectorInt8 { dim: Some(d) } => write!(f, "VectorInt8[{}]", d),
            WireDataType::VectorInt8 { dim: None } => write!(f, "VectorInt8"),
            WireDataType::Bytes => write!(f, "Bytes"),
        }
    }
}

// ============================================================================
// Wire Value
// ============================================================================

/// Wire-serializable value enum.
///
/// Represents a single cell value in a tuple. Supports all InputLayer value types
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

impl WireValue {
    /// Get the data type of this value
    pub fn data_type(&self) -> WireDataType {
        match self {
            WireValue::Null => WireDataType::Int64, // Default null type
            WireValue::Int32(_) => WireDataType::Int32,
            WireValue::Int64(_) => WireDataType::Int64,
            WireValue::Float64(_) => WireDataType::Float64,
            WireValue::String(_) => WireDataType::String,
            WireValue::Bool(_) => WireDataType::Bool,
            WireValue::Timestamp(_) => WireDataType::Timestamp,
            WireValue::Vector(v) => WireDataType::Vector { dim: Some(v.len()) },
            WireValue::VectorInt8(v) => WireDataType::VectorInt8 { dim: Some(v.len()) },
            WireValue::Bytes(_) => WireDataType::Bytes,
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, WireValue::Null)
    }

    /// Try to get as i32
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            WireValue::Int32(v) => Some(*v),
            WireValue::Int64(v) => i32::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            WireValue::Int32(v) => Some(*v as i64),
            WireValue::Int64(v) => Some(*v),
            WireValue::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            WireValue::Int32(v) => Some(*v as f64),
            WireValue::Int64(v) => Some(*v as f64),
            WireValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            WireValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            WireValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl std::fmt::Display for WireValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireValue::Null => write!(f, "NULL"),
            WireValue::Int32(v) => write!(f, "{}", v),
            WireValue::Int64(v) => write!(f, "{}", v),
            WireValue::Float64(v) => write!(f, "{}", v),
            WireValue::String(s) => write!(f, "\"{}\"", s),
            WireValue::Bool(b) => write!(f, "{}", b),
            WireValue::Timestamp(t) => write!(f, "ts:{}", t),
            WireValue::Vector(v) => write!(f, "vec[{}]", v.len()),
            WireValue::VectorInt8(v) => write!(f, "vec8[{}]", v.len()),
            WireValue::Bytes(b) => write!(f, "bytes[{}]", b.len()),
        }
    }
}

// ============================================================================
// Wire Tuple
// ============================================================================

/// Wire-serializable tuple (row of values).
///
/// Represents a single row in a relation or query result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireTuple {
    pub values: Vec<WireValue>,
}

impl WireTuple {
    /// Create a new tuple from values
    pub fn new(values: Vec<WireValue>) -> Self {
        Self { values }
    }

    /// Create an empty tuple
    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Get the number of columns
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if tuple is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&WireValue> {
        self.values.get(index)
    }

    /// Get a mutable value by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut WireValue> {
        self.values.get_mut(index)
    }
}

// ============================================================================
// Conversions: (i32, i32) <-> WireTuple
// ============================================================================

impl From<(i32, i32)> for WireTuple {
    fn from((a, b): (i32, i32)) -> Self {
        WireTuple {
            values: vec![WireValue::Int32(a), WireValue::Int32(b)],
        }
    }
}

impl TryFrom<WireTuple> for (i32, i32) {
    type Error = String;

    fn try_from(tuple: WireTuple) -> Result<Self, Self::Error> {
        if tuple.values.len() < 2 {
            return Err(format!(
                "Expected 2 values, got {}",
                tuple.values.len()
            ));
        }

        let a = tuple.values[0]
            .as_i32()
            .ok_or_else(|| format!("First value is not an integer: {:?}", tuple.values[0]))?;

        let b = tuple.values[1]
            .as_i32()
            .ok_or_else(|| format!("Second value is not an integer: {:?}", tuple.values[1]))?;

        Ok((a, b))
    }
}

impl From<&(i32, i32)> for WireTuple {
    fn from((a, b): &(i32, i32)) -> Self {
        WireTuple {
            values: vec![WireValue::Int32(*a), WireValue::Int32(*b)],
        }
    }
}

// ============================================================================
// Column Definition
// ============================================================================

/// Column definition for schema description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: WireDataType,
}

impl ColumnDef {
    /// Create a new column definition
    pub fn new(name: impl Into<String>, data_type: WireDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    /// Create an Int32 column
    pub fn int32(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::Int32)
    }

    /// Create an Int64 column
    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::Int64)
    }

    /// Create a Float64 column
    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::Float64)
    }

    /// Create a String column
    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::String)
    }

    /// Create a Vector column
    pub fn vector(name: impl Into<String>, dim: Option<usize>) -> Self {
        Self::new(name, WireDataType::Vector { dim })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_value_types() {
        assert_eq!(WireValue::Int32(42).data_type(), WireDataType::Int32);
        assert_eq!(WireValue::Int64(42).data_type(), WireDataType::Int64);
        assert_eq!(WireValue::Float64(3.14).data_type(), WireDataType::Float64);
        assert_eq!(
            WireValue::String("hello".to_string()).data_type(),
            WireDataType::String
        );
        assert_eq!(WireValue::Bool(true).data_type(), WireDataType::Bool);
        assert_eq!(WireValue::Timestamp(1234567890).data_type(), WireDataType::Timestamp);
    }

    #[test]
    fn test_wire_value_accessors() {
        assert_eq!(WireValue::Int32(42).as_i32(), Some(42));
        assert_eq!(WireValue::Int64(42).as_i64(), Some(42));
        assert_eq!(WireValue::Float64(3.14).as_f64(), Some(3.14));
        assert_eq!(WireValue::String("hello".to_string()).as_str(), Some("hello"));
        assert_eq!(WireValue::Bool(true).as_bool(), Some(true));
        assert!(WireValue::Null.is_null());
    }

    #[test]
    fn test_wire_tuple_from_tuple2() {
        let tuple: WireTuple = (1, 2).into();
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), Some(&WireValue::Int32(1)));
        assert_eq!(tuple.get(1), Some(&WireValue::Int32(2)));
    }

    #[test]
    fn test_wire_tuple_to_tuple2() {
        let wire = WireTuple::new(vec![WireValue::Int32(1), WireValue::Int32(2)]);
        let tuple: (i32, i32) = wire.try_into().unwrap();
        assert_eq!(tuple, (1, 2));
    }

    #[test]
    fn test_wire_tuple_to_tuple2_error() {
        let wire = WireTuple::new(vec![WireValue::Int32(1)]);
        let result: Result<(i32, i32), _> = wire.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_column_def() {
        let col = ColumnDef::int32("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, WireDataType::Int32);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = WireTuple::new(vec![
            WireValue::Int32(42),
            WireValue::String("hello".to_string()),
            WireValue::Vector(vec![1.0, 2.0, 3.0]),
        ]);

        // Serialize to bincode
        let bytes = bincode::serialize(&original).unwrap();

        // Deserialize back
        let restored: WireTuple = bincode::deserialize(&bytes).unwrap();

        assert_eq!(original, restored);
    }
}
