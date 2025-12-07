//! # Value Type System
//!
//! This module provides the core value types for the Datalog engine, supporting
//! arbitrary arity tuples with multiple data types.
//!
//! ## Design Decisions
//!
//! - **Arrow-compatible**: Types align with Apache Arrow's type system for efficient
//!   columnar operations and Parquet persistence
//! - **Dynamic arity**: Tuples can have any number of columns
//! - **Multiple types**: Supports integers, floats, strings, and null values
//! - **Differential Dataflow compatible**: Implements required traits for DD collections
//!
//! ## Usage
//!
//! ```rust,ignore
//! use datalog_engine::value::{Value, Tuple, TupleSchema, DataType};
//!
//! // Create a 3-tuple with mixed types
//! let tuple = Tuple::new(vec![
//!     Value::Int32(1),
//!     Value::String("hello".into()),
//!     Value::Float64(3.14),
//! ]);
//!
//! // Define a schema
//! let schema = TupleSchema::new(vec![
//!     ("id".to_string(), DataType::Int32),
//!     ("name".to_string(), DataType::String),
//!     ("score".to_string(), DataType::Float64),
//! ]);
//! ```

pub mod arrow_convert;

pub use arrow_convert::{
    tuples_to_record_batch, record_batch_to_tuples, ArrowConvertError,
    tuple2_to_tuple, tuple2_vec_to_tuples, tuples_to_tuple2_vec, infer_schema_from_tuples,
};

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::io::Write;
use abomonation::Abomonation;
use serde::{Deserialize, Serialize, Serializer, Deserializer};

// Re-export Arrow's DataType for schema definitions
pub use arrow::datatypes::DataType as ArrowDataType;

/// Supported data types for Datalog values
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Int32,
    Int64,
    Float64,
    String,
    Bool,
    Null,
    /// Vector of f32 values (for embeddings, similarity search)
    /// Optional dimension for schema validation:
    /// - `None` = any dimension (backward compatible)
    /// - `Some(n)` = strict validation, only n-dimensional vectors
    Vector { dim: Option<usize> },
    /// Vector of i8 values (quantized embeddings for 75% memory savings)
    /// Same dimension semantics as Vector
    VectorInt8 { dim: Option<usize> },
    /// Unix timestamp in milliseconds (for temporal operations)
    Timestamp,
}

impl DataType {
    /// Create a Vector type with known dimension (strict validation)
    pub fn vector_with_dim(dim: usize) -> Self {
        DataType::Vector { dim: Some(dim) }
    }

    /// Create a Vector type with unknown/any dimension (no validation)
    pub fn vector_any() -> Self {
        DataType::Vector { dim: None }
    }

    /// Create an Int8 Vector type with known dimension (strict validation)
    pub fn vector_int8_with_dim(dim: usize) -> Self {
        DataType::VectorInt8 { dim: Some(dim) }
    }

    /// Create an Int8 Vector type with unknown/any dimension (no validation)
    pub fn vector_int8_any() -> Self {
        DataType::VectorInt8 { dim: None }
    }

    /// Check if a value matches this type, including dimension validation for vectors
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (DataType::Vector { dim: Some(expected) }, Value::Vector(v)) => v.len() == *expected,
            (DataType::Vector { dim: None }, Value::Vector(_)) => true,
            (DataType::VectorInt8 { dim: Some(expected) }, Value::VectorInt8(v)) => v.len() == *expected,
            (DataType::VectorInt8 { dim: None }, Value::VectorInt8(_)) => true,
            (DataType::Int32, Value::Int32(_)) => true,
            (DataType::Int64, Value::Int64(_)) => true,
            (DataType::Float64, Value::Float64(_)) => true,
            (DataType::String, Value::String(_)) => true,
            (DataType::Bool, Value::Bool(_)) => true,
            (DataType::Null, Value::Null) => true,
            (DataType::Timestamp, Value::Timestamp(_)) => true,
            _ => false,
        }
    }
}

impl DataType {
    /// Convert to Arrow DataType
    pub fn to_arrow(&self) -> ArrowDataType {
        match self {
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Null => ArrowDataType::Null,
            // Vectors with known dimension use FixedSizeList (preserves dimension info)
            DataType::Vector { dim: Some(n) } => ArrowDataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new("item", ArrowDataType::Float32, false)),
                *n as i32,
            ),
            // Vectors with unknown dimension use LargeList (variable length)
            DataType::Vector { dim: None } => ArrowDataType::LargeList(Arc::new(
                arrow::datatypes::Field::new("item", ArrowDataType::Float32, false)
            )),
            // Int8 vectors with known dimension use FixedSizeList
            DataType::VectorInt8 { dim: Some(n) } => ArrowDataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new("item", ArrowDataType::Int8, false)),
                *n as i32,
            ),
            // Int8 vectors with unknown dimension use LargeList
            DataType::VectorInt8 { dim: None } => ArrowDataType::LargeList(Arc::new(
                arrow::datatypes::Field::new("item", ArrowDataType::Int8, false)
            )),
            // Timestamps stored as Int64 (milliseconds since Unix epoch)
            DataType::Timestamp => ArrowDataType::Int64,
        }
    }

    /// Create from Arrow DataType
    pub fn from_arrow(arrow_type: &ArrowDataType) -> Option<Self> {
        match arrow_type {
            ArrowDataType::Int32 => Some(DataType::Int32),
            ArrowDataType::Int64 => Some(DataType::Int64),
            ArrowDataType::Float64 => Some(DataType::Float64),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Some(DataType::String),
            ArrowDataType::Boolean => Some(DataType::Bool),
            ArrowDataType::Null => Some(DataType::Null),
            // FixedSizeList preserves dimension information
            ArrowDataType::FixedSizeList(field, size)
                if matches!(field.data_type(), ArrowDataType::Float32) => {
                Some(DataType::Vector { dim: Some(*size as usize) })
            }
            // Variable-length lists have unknown dimension
            ArrowDataType::LargeList(field) | ArrowDataType::List(field)
                if matches!(field.data_type(), ArrowDataType::Float32) => {
                Some(DataType::Vector { dim: None })
            }
            // Int8 FixedSizeList preserves dimension information
            ArrowDataType::FixedSizeList(field, size)
                if matches!(field.data_type(), ArrowDataType::Int8) => {
                Some(DataType::VectorInt8 { dim: Some(*size as usize) })
            }
            // Int8 variable-length lists have unknown dimension
            ArrowDataType::LargeList(field) | ArrowDataType::List(field)
                if matches!(field.data_type(), ArrowDataType::Int8) => {
                Some(DataType::VectorInt8 { dim: None })
            }
            _ => None,
        }
    }
}

/// A dynamically-typed value that can be stored in a tuple
#[derive(Debug, Clone)]
pub enum Value {
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 64-bit floating point
    Float64(f64),
    /// UTF-8 string (reference counted for efficient cloning)
    String(Arc<str>),
    /// Boolean value
    Bool(bool),
    /// Null/missing value
    Null,
    /// Vector of f32 values (for embeddings, similarity search)
    /// Uses f32 for memory efficiency (embeddings rarely need f64 precision)
    Vector(Arc<Vec<f32>>),
    /// Vector of i8 values (quantized embeddings for 75% memory savings)
    /// Uses int8 quantization for large-scale embedding storage
    VectorInt8(Arc<Vec<i8>>),
    /// Unix timestamp in milliseconds since epoch (1970-01-01 00:00:00 UTC)
    /// For temporal operations in spatio-temporal memory systems
    Timestamp(i64),
}

impl Value {
    /// Get the data type of this value
    /// For vectors, includes the actual dimension
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Int32(_) => DataType::Int32,
            Value::Int64(_) => DataType::Int64,
            Value::Float64(_) => DataType::Float64,
            Value::String(_) => DataType::String,
            Value::Bool(_) => DataType::Bool,
            Value::Null => DataType::Null,
            Value::Vector(v) => DataType::Vector { dim: Some(v.len()) },
            Value::VectorInt8(v) => DataType::VectorInt8 { dim: Some(v.len()) },
            Value::Timestamp(_) => DataType::Timestamp,
        }
    }

    /// Try to get as i32
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int32(v) => Some(*v),
            Value::Int64(v) => (*v).try_into().ok(),
            _ => None,
        }
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            Value::Int32(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Try to get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to get as vector slice
    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            Value::Vector(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Try to get as int8 vector slice
    pub fn as_vector_int8(&self) -> Option<&[i8]> {
        match self {
            Value::VectorInt8(v) => Some(v.as_slice()),
            _ => None,
        }
    }

    /// Try to get as timestamp (milliseconds since Unix epoch)
    /// Also accepts Int64 for flexibility in temporal operations
    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Value::Timestamp(t) => Some(*t),
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Create a string value from a &str
    pub fn string(s: &str) -> Self {
        Value::String(Arc::from(s))
    }

    /// Create a vector value from Vec<f32>
    pub fn vector(data: Vec<f32>) -> Self {
        Value::Vector(Arc::new(data))
    }

    /// Create a vector value from an iterator of f32
    pub fn vector_from_iter<I: IntoIterator<Item = f32>>(iter: I) -> Self {
        Value::Vector(Arc::new(iter.into_iter().collect()))
    }

    /// Create an int8 vector value from Vec<i8>
    pub fn vector_int8(data: Vec<i8>) -> Self {
        Value::VectorInt8(Arc::new(data))
    }

    /// Create an int8 vector value from an iterator of i8
    pub fn vector_int8_from_iter<I: IntoIterator<Item = i8>>(iter: I) -> Self {
        Value::VectorInt8(Arc::new(iter.into_iter().collect()))
    }

    /// Create a timestamp value from milliseconds since Unix epoch
    pub fn timestamp(ms: i64) -> Self {
        Value::Timestamp(ms)
    }

    /// Convert to i64 (for aggregation operations)
    /// Returns 0 for non-numeric types
    pub fn to_i64(&self) -> i64 {
        match self {
            Value::Int32(v) => *v as i64,
            Value::Int64(v) => *v,
            Value::Float64(v) => *v as i64,
            Value::Bool(b) => if *b { 1 } else { 0 },
            Value::Timestamp(t) => *t,
            _ => 0,
        }
    }

    /// Convert to f64 (for aggregation operations)
    /// Returns 0.0 for non-numeric types
    pub fn to_f64(&self) -> f64 {
        match self {
            Value::Int32(v) => *v as f64,
            Value::Int64(v) => *v as f64,
            Value::Float64(v) => *v,
            Value::Bool(b) => if *b { 1.0 } else { 0.0 },
            Value::Timestamp(t) => *t as f64,
            _ => 0.0,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(v) => write!(f, "{}", v),
            Value::Int64(v) => write!(f, "{}", v),
            Value::Float64(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
            Value::Vector(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    // Show first 5 elements, then "... N more" for large vectors
                    if i < 5 || v.len() <= 6 {
                        write!(f, "{:.4}", val)?;
                    } else if i == 5 {
                        write!(f, "... {} more", v.len() - 5)?;
                        break;
                    }
                }
                write!(f, "]")
            }
            Value::VectorInt8(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    // Show first 5 elements, then "... N more" for large vectors
                    if i < 5 || v.len() <= 6 {
                        write!(f, "{}", val)?;
                    } else if i == 5 {
                        write!(f, "... {} more", v.len() - 5)?;
                        break;
                    }
                }
                write!(f, "]i8")
            }
            Value::Timestamp(ts) => write!(f, "{}ms", ts),
        }
    }
}

// Implement PartialEq manually to handle f64 comparison
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (Value::Vector(a), Value::Vector(b)) => a == b,
            (Value::VectorInt8(a), Value::VectorInt8(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

// Implement Hash manually to handle f64 and vectors
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Int32(v) => v.hash(state),
            Value::Int64(v) => v.hash(state),
            Value::Float64(v) => v.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Bool(b) => b.hash(state),
            Value::Null => {}
            Value::Vector(v) => {
                v.len().hash(state);
                for &f in v.iter() {
                    f.to_bits().hash(state);
                }
            }
            Value::VectorInt8(v) => {
                v.len().hash(state);
                for &i in v.iter() {
                    i.hash(state);
                }
            }
            Value::Timestamp(t) => t.hash(state),
        }
    }
}

// Implement Ord for Value to support sorting and comparison
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a.cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
            (Value::Float64(a), Value::Float64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Vector(a), Value::Vector(b)) => {
                // Compare lengths first, then element by element using f32 bits
                match a.len().cmp(&b.len()) {
                    Ordering::Equal => {
                        for (x, y) in a.iter().zip(b.iter()) {
                            match x.to_bits().cmp(&y.to_bits()) {
                                Ordering::Equal => continue,
                                other => return other,
                            }
                        }
                        Ordering::Equal
                    }
                    other => other,
                }
            }
            (Value::VectorInt8(a), Value::VectorInt8(b)) => {
                // Compare lengths first, then element by element
                match a.len().cmp(&b.len()) {
                    Ordering::Equal => {
                        for (x, y) in a.iter().zip(b.iter()) {
                            match x.cmp(y) {
                                Ordering::Equal => continue,
                                other => return other,
                            }
                        }
                        Ordering::Equal
                    }
                    other => other,
                }
            }
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            // Cross-type ordering: Null < Bool < Int32 < Int64 < Float64 < Timestamp < String < Vector < VectorInt8
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Bool(_), _) => Ordering::Less,
            (_, Value::Bool(_)) => Ordering::Greater,
            (Value::Int32(_), _) => Ordering::Less,
            (_, Value::Int32(_)) => Ordering::Greater,
            (Value::Int64(_), _) => Ordering::Less,
            (_, Value::Int64(_)) => Ordering::Greater,
            (Value::Float64(_), _) => Ordering::Less,
            (_, Value::Float64(_)) => Ordering::Greater,
            (Value::Timestamp(_), _) => Ordering::Less,
            (_, Value::Timestamp(_)) => Ordering::Greater,
            (Value::String(_), _) => Ordering::Less,
            (_, Value::String(_)) => Ordering::Greater,
            (Value::Vector(_), _) => Ordering::Less,
            (_, Value::Vector(_)) => Ordering::Greater,
        }
    }
}

// Convenience conversions
impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(Arc::from(s))
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(Arc::from(s.as_str()))
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(Arc::new(v))
    }
}

impl From<Vec<i8>> for Value {
    fn from(v: Vec<i8>) -> Self {
        Value::VectorInt8(Arc::new(v))
    }
}

// Implement Serialize for Value (needed for WAL)
impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        match self {
            Value::Int32(v) => {
                map.serialize_entry("type", "Int32")?;
                map.serialize_entry("value", v)?;
            }
            Value::Int64(v) => {
                map.serialize_entry("type", "Int64")?;
                map.serialize_entry("value", v)?;
            }
            Value::Float64(v) => {
                map.serialize_entry("type", "Float64")?;
                map.serialize_entry("value", v)?;
            }
            Value::String(s) => {
                map.serialize_entry("type", "String")?;
                map.serialize_entry("value", s.as_ref())?;
            }
            Value::Bool(b) => {
                map.serialize_entry("type", "Bool")?;
                map.serialize_entry("value", b)?;
            }
            Value::Null => {
                map.serialize_entry("type", "Null")?;
                map.serialize_entry("value", &())?;
            }
            Value::Vector(v) => {
                map.serialize_entry("type", "Vector")?;
                map.serialize_entry("value", v.as_ref())?;
            }
            Value::VectorInt8(v) => {
                map.serialize_entry("type", "VectorInt8")?;
                map.serialize_entry("value", v.as_ref())?;
            }
            Value::Timestamp(t) => {
                map.serialize_entry("type", "Timestamp")?;
                map.serialize_entry("value", t)?;
            }
        }
        map.end()
    }
}

// Implement Deserialize for Value
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor};

        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a Value object with type and value fields")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut type_str: Option<String> = None;
                let mut raw_value: Option<serde_json::Value> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "type" => {
                            type_str = Some(map.next_value()?);
                        }
                        "value" => {
                            raw_value = Some(map.next_value()?);
                        }
                        _ => {
                            let _: serde_json::Value = map.next_value()?;
                        }
                    }
                }

                let type_str = type_str.ok_or_else(|| serde::de::Error::missing_field("type"))?;
                let raw_value = raw_value.ok_or_else(|| serde::de::Error::missing_field("value"))?;

                match type_str.as_str() {
                    "Int32" => {
                        let v: i32 = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::Int32(v))
                    }
                    "Int64" => {
                        let v: i64 = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::Int64(v))
                    }
                    "Float64" => {
                        let v: f64 = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::Float64(v))
                    }
                    "String" => {
                        let v: String = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::String(Arc::from(v.as_str())))
                    }
                    "Bool" => {
                        let v: bool = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::Bool(v))
                    }
                    "Null" => Ok(Value::Null),
                    "Vector" => {
                        let v: Vec<f32> = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::Vector(Arc::new(v)))
                    }
                    "VectorInt8" => {
                        let v: Vec<i8> = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::VectorInt8(Arc::new(v)))
                    }
                    "Timestamp" => {
                        let v: i64 = serde_json::from_value(raw_value)
                            .map_err(serde::de::Error::custom)?;
                        Ok(Value::Timestamp(v))
                    }
                    _ => Err(serde::de::Error::unknown_variant(
                        &type_str,
                        &["Int32", "Int64", "Float64", "String", "Bool", "Null", "Vector", "VectorInt8", "Timestamp"],
                    )),
                }
            }
        }

        deserializer.deserialize_map(ValueVisitor)
    }
}

// Implement Abomonation for Value (required for Differential Dataflow)
// We serialize as a tagged union: discriminant byte + payload
impl Abomonation for Value {
    #[inline]
    unsafe fn entomb<W: Write>(&self, write: &mut W) -> std::io::Result<()> {
        match self {
            Value::Int32(v) => {
                write.write_all(&[0u8])?;
                write.write_all(&v.to_le_bytes())
            }
            Value::Int64(v) => {
                write.write_all(&[1u8])?;
                write.write_all(&v.to_le_bytes())
            }
            Value::Float64(v) => {
                write.write_all(&[2u8])?;
                write.write_all(&v.to_bits().to_le_bytes())
            }
            Value::String(s) => {
                write.write_all(&[3u8])?;
                let bytes = s.as_bytes();
                let len = bytes.len() as u64;
                write.write_all(&len.to_le_bytes())?;
                write.write_all(bytes)
            }
            Value::Bool(b) => {
                write.write_all(&[4u8])?;
                write.write_all(&[if *b { 1u8 } else { 0u8 }])
            }
            Value::Null => {
                write.write_all(&[5u8])
            }
            Value::Vector(v) => {
                write.write_all(&[6u8])?;  // Tag 6 for Vector
                let len = v.len() as u64;
                write.write_all(&len.to_le_bytes())?;
                for &f in v.iter() {
                    write.write_all(&f.to_le_bytes())?;
                }
                Ok(())
            }
            Value::VectorInt8(v) => {
                write.write_all(&[8u8])?;  // Tag 8 for VectorInt8
                let len = v.len() as u64;
                write.write_all(&len.to_le_bytes())?;
                // i8 values can be written directly as bytes
                let bytes: &[u8] = unsafe {
                    std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len())
                };
                write.write_all(bytes)
            }
            Value::Timestamp(t) => {
                write.write_all(&[7u8])?;  // Tag 7 for Timestamp
                write.write_all(&t.to_le_bytes())
            }
        }
    }

    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        // For Value, we need to reconstruct from bytes
        // This is tricky because we need to know the discriminant
        // We'll handle this in the Tuple implementation
        Some(bytes)
    }

    #[inline]
    fn extent(&self) -> usize {
        match self {
            Value::Int32(_) => 1 + 4,
            Value::Int64(_) => 1 + 8,
            Value::Float64(_) => 1 + 8,
            Value::String(s) => 1 + 8 + s.len(),
            Value::Bool(_) => 1 + 1,
            Value::Null => 1,
            Value::Vector(v) => 1 + 8 + v.len() * 4,  // tag + len + floats
            Value::VectorInt8(v) => 1 + 8 + v.len(),  // tag + len + bytes
            Value::Timestamp(_) => 1 + 8,  // tag + i64
        }
    }
}

/// A tuple with arbitrary arity containing Values
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Tuple {
    values: Vec<Value>,
}

// Implement Ord for Tuple (lexicographic ordering)
impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Tuple {
    fn cmp(&self, other: &Self) -> Ordering {
        self.values.iter().cmp(other.values.iter())
    }
}

// Implement Abomonation for Tuple
impl Abomonation for Tuple {
    #[inline]
    unsafe fn entomb<W: Write>(&self, write: &mut W) -> std::io::Result<()> {
        // Write length first
        let len = self.values.len() as u64;
        write.write_all(&len.to_le_bytes())?;
        // Write each value
        for v in &self.values {
            v.entomb(write)?;
        }
        Ok(())
    }

    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        // For exhume, we need to reconstruct the values from bytes
        // This is complex for our dynamic Value type
        // For now, return bytes unchanged (data stays in-place)
        Some(bytes)
    }

    #[inline]
    fn extent(&self) -> usize {
        8 + self.values.iter().map(|v| v.extent()).sum::<usize>()
    }
}

impl Tuple {
    /// Create a new tuple from a vector of values
    pub fn new(values: Vec<Value>) -> Self {
        Tuple { values }
    }

    /// Create an empty tuple
    pub fn empty() -> Self {
        Tuple { values: Vec::new() }
    }

    /// Get the number of columns in this tuple
    pub fn arity(&self) -> usize {
        self.values.len()
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a mutable reference to a value by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Get all values as a slice
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Convert to owned values
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Create a new tuple by selecting specific columns
    pub fn project(&self, indices: &[usize]) -> Self {
        let values = indices
            .iter()
            .filter_map(|&i| self.values.get(i).cloned())
            .collect();
        Tuple { values }
    }

    /// Create a tuple from specific indices
    pub fn from_indices(&self, indices: &[usize]) -> Self {
        self.project(indices)
    }

    /// Create a tuple excluding specific indices
    pub fn excluding_indices(&self, exclude: &[usize]) -> Self {
        let values = self
            .values
            .iter()
            .enumerate()
            .filter(|(i, _)| !exclude.contains(i))
            .map(|(_, v)| v.clone())
            .collect();
        Tuple { values }
    }

    /// Concatenate two tuples
    pub fn concat(&self, other: &Tuple) -> Self {
        let mut values = self.values.clone();
        values.extend(other.values.iter().cloned());
        Tuple { values }
    }

    /// Create from a 2-tuple of i32 (for backward compatibility)
    /// Uses Int64 internally for consistency with production API
    pub fn from_pair(a: i32, b: i32) -> Self {
        Tuple {
            values: vec![Value::Int64(a as i64), Value::Int64(b as i64)],
        }
    }

    /// Try to convert to a 2-tuple of i32 (for backward compatibility)
    /// Handles both Int32 and Int64 values
    pub fn to_pair(&self) -> Option<(i32, i32)> {
        if self.values.len() == 2 {
            match (&self.values[0], &self.values[1]) {
                (Value::Int32(a), Value::Int32(b)) => Some((*a, *b)),
                (Value::Int64(a), Value::Int64(b)) => Some((*a as i32, *b as i32)),
                (Value::Int32(a), Value::Int64(b)) => Some((*a, *b as i32)),
                (Value::Int64(a), Value::Int32(b)) => Some((*a as i32, *b)),
                _ => None,
            }
        } else {
            None
        }
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, v) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", v)?;
        }
        write!(f, ")")
    }
}

// Allow iterating over tuple values
impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl IntoIterator for Tuple {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

/// Error type for schema validation
#[derive(Debug, Clone)]
pub enum SchemaValidationError {
    /// Tuple has wrong number of columns
    ArityMismatch { expected: usize, got: usize },
    /// Column has wrong type
    TypeMismatch { column: String, expected: DataType, got: DataType },
    /// Vector column has wrong dimension
    VectorDimensionMismatch { column: String, expected: usize, got: usize },
    /// VectorInt8 column has wrong dimension
    VectorInt8DimensionMismatch { column: String, expected: usize, got: usize },
}

impl std::fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArityMismatch { expected, got } => {
                write!(f, "Arity mismatch: expected {} columns, got {}", expected, got)
            }
            Self::TypeMismatch { column, expected, got } => {
                write!(f, "Type mismatch in column '{}': expected {:?}, got {:?}", column, expected, got)
            }
            Self::VectorDimensionMismatch { column, expected, got } => {
                write!(f, "Vector dimension mismatch in column '{}': expected {}-dim, got {}-dim", column, expected, got)
            }
            Self::VectorInt8DimensionMismatch { column, expected, got } => {
                write!(f, "VectorInt8 dimension mismatch in column '{}': expected {}-dim, got {}-dim", column, expected, got)
            }
        }
    }
}

impl std::error::Error for SchemaValidationError {}

/// Schema definition for a relation's tuples
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleSchema {
    fields: Vec<(String, DataType)>,
}

impl TupleSchema {
    /// Create a new schema from field names and types
    pub fn new(fields: Vec<(String, DataType)>) -> Self {
        TupleSchema { fields }
    }

    /// Create an empty schema (zero columns)
    pub fn empty() -> Self {
        TupleSchema { fields: Vec::new() }
    }

    /// Create a schema with just column names (types inferred as Int32 for compatibility)
    pub fn from_names(names: Vec<String>) -> Self {
        let fields = names
            .into_iter()
            .map(|name| (name, DataType::Int32))
            .collect();
        TupleSchema { fields }
    }

    /// Get the number of fields
    pub fn arity(&self) -> usize {
        self.fields.len()
    }

    /// Get field name by index
    pub fn field_name(&self, index: usize) -> Option<&str> {
        self.fields.get(index).map(|(name, _)| name.as_str())
    }

    /// Get field type by index
    pub fn field_type(&self, index: usize) -> Option<&DataType> {
        self.fields.get(index).map(|(_, ty)| ty)
    }

    /// Get field index by name
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|(n, _)| n == name)
    }

    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|(n, _)| n.as_str()).collect()
    }

    /// Get all fields
    pub fn fields(&self) -> &[(String, DataType)] {
        &self.fields
    }

    /// Convert to Arrow schema
    pub fn to_arrow(&self) -> arrow::datatypes::Schema {
        let fields: Vec<arrow::datatypes::Field> = self
            .fields
            .iter()
            .map(|(name, ty)| arrow::datatypes::Field::new(name, ty.to_arrow(), true))
            .collect();
        arrow::datatypes::Schema::new(fields)
    }

    /// Create from Arrow schema
    pub fn from_arrow(schema: &arrow::datatypes::Schema) -> Option<Self> {
        let fields: Option<Vec<_>> = schema
            .fields()
            .iter()
            .map(|f| {
                DataType::from_arrow(f.data_type())
                    .map(|ty| (f.name().clone(), ty))
            })
            .collect();
        fields.map(TupleSchema::new)
    }

    /// Create a schema for a projection
    pub fn project(&self, indices: &[usize]) -> Self {
        let fields = indices
            .iter()
            .filter_map(|&i| self.fields.get(i).cloned())
            .collect();
        TupleSchema { fields }
    }

    /// Concatenate two schemas (for join output)
    pub fn concat(&self, other: &TupleSchema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend(other.fields.iter().cloned());
        TupleSchema { fields }
    }

    /// Validate a tuple against this schema, including vector dimensions
    pub fn validate(&self, tuple: &Tuple) -> Result<(), SchemaValidationError> {
        if tuple.arity() != self.arity() {
            return Err(SchemaValidationError::ArityMismatch {
                expected: self.arity(),
                got: tuple.arity(),
            });
        }

        for (i, (name, dtype)) in self.fields.iter().enumerate() {
            if let Some(value) = tuple.get(i) {
                // Specific vector dimension check with clear error message
                if let (DataType::Vector { dim: Some(expected) }, Value::Vector(v)) = (dtype, value) {
                    if v.len() != *expected {
                        return Err(SchemaValidationError::VectorDimensionMismatch {
                            column: name.clone(),
                            expected: *expected,
                            got: v.len(),
                        });
                    }
                }
                // VectorInt8 dimension check
                if let (DataType::VectorInt8 { dim: Some(expected) }, Value::VectorInt8(v)) = (dtype, value) {
                    if v.len() != *expected {
                        return Err(SchemaValidationError::VectorInt8DimensionMismatch {
                            column: name.clone(),
                            expected: *expected,
                            got: v.len(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Infer vector dimensions from the first tuple and update schema
    /// This converts Vector { dim: None } to Vector { dim: Some(n) }
    /// and VectorInt8 { dim: None } to VectorInt8 { dim: Some(n) }
    pub fn infer_vector_dimensions(&mut self, tuples: &[Tuple]) {
        if tuples.is_empty() {
            return;
        }

        for (i, (_, dtype)) in self.fields.iter_mut().enumerate() {
            if let DataType::Vector { dim: None } = dtype {
                if let Some(Value::Vector(v)) = tuples[0].get(i) {
                    *dtype = DataType::Vector { dim: Some(v.len()) };
                }
            }
            if let DataType::VectorInt8 { dim: None } = dtype {
                if let Some(Value::VectorInt8(v)) = tuples[0].get(i) {
                    *dtype = DataType::VectorInt8 { dim: Some(v.len()) };
                }
            }
        }
    }
}

impl Default for TupleSchema {
    fn default() -> Self {
        TupleSchema { fields: Vec::new() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_types() {
        let int_val = Value::Int32(42);
        let str_val = Value::string("hello");
        let float_val = Value::Float64(3.14);

        assert_eq!(int_val.as_i32(), Some(42));
        assert_eq!(str_val.as_str(), Some("hello"));
        assert!((float_val.as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Int32(42), Value::Int32(42));
        assert_ne!(Value::Int32(42), Value::Int64(42));
        assert_eq!(Value::string("hello"), Value::string("hello"));
    }

    #[test]
    fn test_tuple_creation() {
        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::string("test"),
            Value::Float64(2.5),
        ]);

        assert_eq!(tuple.arity(), 3);
        assert_eq!(tuple.get(0), Some(&Value::Int32(1)));
        assert_eq!(tuple.get(1).and_then(|v| v.as_str()), Some("test"));
    }

    #[test]
    fn test_tuple_project() {
        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(3),
        ]);

        let projected = tuple.project(&[2, 0]);
        assert_eq!(projected.arity(), 2);
        assert_eq!(projected.get(0), Some(&Value::Int32(3)));
        assert_eq!(projected.get(1), Some(&Value::Int32(1)));
    }

    #[test]
    fn test_tuple_concat() {
        let t1 = Tuple::new(vec![Value::Int32(1), Value::Int32(2)]);
        let t2 = Tuple::new(vec![Value::Int32(3)]);

        let combined = t1.concat(&t2);
        assert_eq!(combined.arity(), 3);
        assert_eq!(combined.get(2), Some(&Value::Int32(3)));
    }

    #[test]
    fn test_tuple_backward_compat() {
        let tuple = Tuple::from_pair(1, 2);
        assert_eq!(tuple.to_pair(), Some((1, 2)));

        let tuple3 = Tuple::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]);
        assert_eq!(tuple3.to_pair(), None);
    }

    #[test]
    fn test_schema_creation() {
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::String),
        ]);

        assert_eq!(schema.arity(), 2);
        assert_eq!(schema.field_name(0), Some("id"));
        assert_eq!(schema.field_type(1), Some(&DataType::String));
        assert_eq!(schema.field_index("name"), Some(1));
    }

    #[test]
    fn test_schema_project() {
        let schema = TupleSchema::new(vec![
            ("a".to_string(), DataType::Int32),
            ("b".to_string(), DataType::String),
            ("c".to_string(), DataType::Float64),
        ]);

        let projected = schema.project(&[2, 0]);
        assert_eq!(projected.arity(), 2);
        assert_eq!(projected.field_name(0), Some("c"));
        assert_eq!(projected.field_name(1), Some("a"));
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Int32(1) < Value::Int32(2));
        assert!(Value::Null < Value::Int32(0));
        assert!(Value::string("a") < Value::string("b"));
    }

    #[test]
    fn test_value_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Value::Int32(42));
        set.insert(Value::string("hello"));

        assert!(set.contains(&Value::Int32(42)));
        assert!(set.contains(&Value::string("hello")));
        assert!(!set.contains(&Value::Int32(43)));
    }

    #[test]
    fn test_vector_creation() {
        let v = Value::vector(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.data_type(), DataType::Vector { dim: Some(3) });
        assert_eq!(v.as_vector(), Some([1.0f32, 2.0, 3.0].as_slice()));
    }

    #[test]
    fn test_vector_from_iter() {
        let v = Value::vector_from_iter([1.0f32, 2.0, 3.0]);
        assert_eq!(v.as_vector().unwrap().len(), 3);
    }

    #[test]
    fn test_vector_equality() {
        let v1 = Value::vector(vec![1.0, 2.0, 3.0]);
        let v2 = Value::vector(vec![1.0, 2.0, 3.0]);
        let v3 = Value::vector(vec![1.0, 2.0, 4.0]);

        assert_eq!(v1, v2);
        assert_ne!(v1, v3);
    }

    #[test]
    fn test_vector_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(Value::vector(vec![1.0, 2.0, 3.0]));

        assert!(set.contains(&Value::vector(vec![1.0, 2.0, 3.0])));
        assert!(!set.contains(&Value::vector(vec![1.0, 2.0, 4.0])));
    }

    #[test]
    fn test_vector_ordering() {
        let v1 = Value::vector(vec![1.0, 2.0]);
        let v2 = Value::vector(vec![1.0, 3.0]);
        let v3 = Value::vector(vec![1.0, 2.0, 3.0]);

        assert!(v1 < v2);  // Same length, second element differs
        assert!(v1 < v3);  // Shorter length comes first
        assert!(Value::string("a") < v1);  // String < Vector in type ordering
    }

    #[test]
    fn test_vector_display() {
        let v_small = Value::vector(vec![1.0, 2.0, 3.0]);
        let display = format!("{}", v_small);
        assert!(display.contains("1.0000"));
        assert!(display.contains("2.0000"));
        assert!(display.contains("3.0000"));

        let v_large = Value::vector(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]);
        let display_large = format!("{}", v_large);
        assert!(display_large.contains("... 3 more"));
    }

    #[test]
    fn test_vector_in_tuple() {
        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::vector(vec![1.0, 2.0, 3.0]),
            Value::string("test"),
        ]);

        assert_eq!(tuple.arity(), 3);
        assert_eq!(tuple.get(1).and_then(|v| v.as_vector()), Some([1.0f32, 2.0, 3.0].as_slice()));
    }

    #[test]
    fn test_vector_from_trait() {
        let v: Value = vec![1.0f32, 2.0, 3.0].into();
        assert_eq!(v.data_type(), DataType::Vector { dim: Some(3) });
    }

    // =========================================================================
    // Timestamp Tests
    // =========================================================================

    #[test]
    fn test_timestamp_creation() {
        let ts = Value::timestamp(1700000000000i64);
        assert!(matches!(ts, Value::Timestamp(_)));
        assert_eq!(ts.as_timestamp(), Some(1700000000000i64));
    }

    #[test]
    fn test_timestamp_data_type() {
        let ts = Value::Timestamp(1700000000000i64);
        assert_eq!(ts.data_type(), DataType::Timestamp);
    }

    #[test]
    fn test_timestamp_equality() {
        let ts1 = Value::Timestamp(1700000000000i64);
        let ts2 = Value::Timestamp(1700000000000i64);
        let ts3 = Value::Timestamp(1700000000001i64);
        assert_eq!(ts1, ts2);
        assert_ne!(ts1, ts3);
    }

    #[test]
    fn test_timestamp_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(Value::Timestamp(1700000000000i64));
        assert!(set.contains(&Value::Timestamp(1700000000000i64)));
        assert!(!set.contains(&Value::Timestamp(1700000000001i64)));
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = Value::Timestamp(1000);
        let ts2 = Value::Timestamp(2000);
        assert!(ts1 < ts2);
        assert!(ts2 > ts1);
    }

    #[test]
    fn test_timestamp_type_ordering() {
        // Type ordering: Null < Bool < Int32 < Int64 < Float64 < Timestamp < String < Vector
        let float = Value::Float64(1.0);
        let ts = Value::Timestamp(1000);
        let string = Value::string("test");

        assert!(float < ts, "Float64 should be less than Timestamp");
        assert!(ts < string, "Timestamp should be less than String");
    }

    #[test]
    fn test_timestamp_display() {
        let ts = Value::Timestamp(1700000000000i64);
        let display = format!("{}", ts);
        assert!(display.contains("1700000000000"));
        assert!(display.contains("ms"));
    }

    #[test]
    fn test_timestamp_as_i64() {
        let ts = Value::Timestamp(1700000000000i64);
        assert_eq!(ts.as_i64(), Some(1700000000000i64));
    }

    #[test]
    fn test_timestamp_to_i64() {
        let ts = Value::Timestamp(1700000000000i64);
        assert_eq!(ts.to_i64(), 1700000000000i64);
    }

    #[test]
    fn test_timestamp_to_f64() {
        let ts = Value::Timestamp(1700000000000i64);
        assert_eq!(ts.to_f64(), 1700000000000.0f64);
    }

    #[test]
    fn test_timestamp_negative() {
        // Timestamps before Unix epoch
        let ts = Value::Timestamp(-1000i64);
        assert_eq!(ts.as_timestamp(), Some(-1000i64));
    }

    #[test]
    fn test_timestamp_int64_interop() {
        // as_timestamp should accept Int64 for flexibility
        let int = Value::Int64(1700000000000i64);
        assert_eq!(int.as_timestamp(), Some(1700000000000i64));
    }

    #[test]
    fn test_timestamp_in_tuple() {
        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::Timestamp(1700000000000i64),
            Value::string("event"),
        ]);

        assert_eq!(tuple.arity(), 3);
        assert_eq!(
            tuple.get(1).and_then(|v| v.as_timestamp()),
            Some(1700000000000i64)
        );
    }

    // =========================================================================
    // Vector Dimension Validation Tests
    // =========================================================================

    #[test]
    fn test_datatype_vector_with_dim() {
        let dt = DataType::vector_with_dim(1536);
        assert_eq!(dt, DataType::Vector { dim: Some(1536) });
    }

    #[test]
    fn test_datatype_vector_any() {
        let dt = DataType::vector_any();
        assert_eq!(dt, DataType::Vector { dim: None });
    }

    #[test]
    fn test_datatype_matches_vector_with_dimension() {
        let dt = DataType::vector_with_dim(3);
        let v3 = Value::vector(vec![1.0, 2.0, 3.0]);
        let v4 = Value::vector(vec![1.0, 2.0, 3.0, 4.0]);

        assert!(dt.matches(&v3), "3-dim type should match 3-dim vector");
        assert!(!dt.matches(&v4), "3-dim type should not match 4-dim vector");
    }

    #[test]
    fn test_datatype_matches_vector_any() {
        let dt = DataType::vector_any();
        let v3 = Value::vector(vec![1.0, 2.0, 3.0]);
        let v1000 = Value::vector(vec![0.0; 1000]);

        assert!(dt.matches(&v3), "any-dim type should match any vector");
        assert!(dt.matches(&v1000), "any-dim type should match any vector");
    }

    #[test]
    fn test_datatype_matches_non_vector() {
        let dt = DataType::vector_with_dim(3);
        let int = Value::Int32(42);
        let string = Value::string("test");

        assert!(!dt.matches(&int), "vector type should not match int");
        assert!(!dt.matches(&string), "vector type should not match string");
    }

    #[test]
    fn test_schema_validation_success() {
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("embedding".to_string(), DataType::vector_with_dim(3)),
        ]);

        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::vector(vec![1.0, 2.0, 3.0]),
        ]);

        assert!(schema.validate(&tuple).is_ok());
    }

    #[test]
    fn test_schema_validation_arity_mismatch() {
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("embedding".to_string(), DataType::vector_with_dim(3)),
        ]);

        let tuple = Tuple::new(vec![Value::Int32(1)]);

        let result = schema.validate(&tuple);
        assert!(matches!(
            result,
            Err(SchemaValidationError::ArityMismatch { expected: 2, got: 1 })
        ));
    }

    #[test]
    fn test_schema_validation_vector_dimension_mismatch() {
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("embedding".to_string(), DataType::vector_with_dim(3)),
        ]);

        let tuple = Tuple::new(vec![
            Value::Int32(1),
            Value::vector(vec![1.0, 2.0, 3.0, 4.0]), // 4-dim instead of 3
        ]);

        let result = schema.validate(&tuple);
        assert!(matches!(
            result,
            Err(SchemaValidationError::VectorDimensionMismatch {
                column: _,
                expected: 3,
                got: 4
            })
        ));
    }

    #[test]
    fn test_schema_validation_any_dimension_passes() {
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("embedding".to_string(), DataType::vector_any()), // Any dimension
        ]);

        let tuple_3d = Tuple::new(vec![
            Value::Int32(1),
            Value::vector(vec![1.0, 2.0, 3.0]),
        ]);

        let tuple_1536d = Tuple::new(vec![
            Value::Int32(2),
            Value::vector(vec![0.0; 1536]),
        ]);

        assert!(schema.validate(&tuple_3d).is_ok());
        assert!(schema.validate(&tuple_1536d).is_ok());
    }

    #[test]
    fn test_infer_vector_dimensions() {
        let mut schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("embedding".to_string(), DataType::vector_any()),
        ]);

        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::vector(vec![1.0, 2.0, 3.0])]),
            Tuple::new(vec![Value::Int32(2), Value::vector(vec![4.0, 5.0, 6.0])]),
        ];

        schema.infer_vector_dimensions(&tuples);

        // The vector dimension should now be inferred as 3
        assert_eq!(
            schema.field_type(1),
            Some(&DataType::Vector { dim: Some(3) })
        );
    }

    #[test]
    fn test_schema_validation_error_display() {
        let err = SchemaValidationError::ArityMismatch { expected: 3, got: 2 };
        assert!(err.to_string().contains("3"));
        assert!(err.to_string().contains("2"));

        let err = SchemaValidationError::TypeMismatch {
            column: "foo".to_string(),
            expected: DataType::Int32,
            got: DataType::String,
        };
        assert!(err.to_string().contains("foo"));

        let err = SchemaValidationError::VectorDimensionMismatch {
            column: "embedding".to_string(),
            expected: 1536,
            got: 768,
        };
        assert!(err.to_string().contains("embedding"));
        assert!(err.to_string().contains("1536"));
        assert!(err.to_string().contains("768"));
    }

    // =========================================================================
    // Additional Edge Case Tests for Vector Dimension Validation
    // =========================================================================

    #[test]
    fn test_datatype_matches_type_mismatch() {
        // Vector type should not match non-vector values
        let dt = DataType::vector_with_dim(3);
        assert!(!dt.matches(&Value::Int32(42)));
        assert!(!dt.matches(&Value::Int64(42)));
        assert!(!dt.matches(&Value::Float64(3.14)));
        assert!(!dt.matches(&Value::string("test")));
        assert!(!dt.matches(&Value::Bool(true)));
        assert!(!dt.matches(&Value::Null));
        assert!(!dt.matches(&Value::Timestamp(1000)));

        // Non-vector types should not match vectors
        assert!(!DataType::Int32.matches(&Value::vector(vec![1.0, 2.0])));
        assert!(!DataType::String.matches(&Value::vector(vec![1.0, 2.0])));
    }

    #[test]
    fn test_datatype_matches_all_types() {
        // Test all type matches work correctly
        assert!(DataType::Int32.matches(&Value::Int32(42)));
        assert!(DataType::Int64.matches(&Value::Int64(42)));
        assert!(DataType::Float64.matches(&Value::Float64(3.14)));
        assert!(DataType::String.matches(&Value::string("test")));
        assert!(DataType::Bool.matches(&Value::Bool(true)));
        assert!(DataType::Null.matches(&Value::Null));
        assert!(DataType::Timestamp.matches(&Value::Timestamp(1000)));
    }

    #[test]
    fn test_empty_vector_validation() {
        // Empty vectors have dimension 0
        let schema = TupleSchema::new(vec![
            ("embedding".to_string(), DataType::Vector { dim: Some(0) }),
        ]);

        let tuple = Tuple::new(vec![Value::vector(vec![])]);
        assert!(schema.validate(&tuple).is_ok());

        // Non-empty vector should fail against 0-dim schema
        let tuple_nonempty = Tuple::new(vec![Value::vector(vec![1.0])]);
        assert!(matches!(
            schema.validate(&tuple_nonempty),
            Err(SchemaValidationError::VectorDimensionMismatch { expected: 0, got: 1, .. })
        ));
    }

    #[test]
    fn test_large_dimension_vectors() {
        // Test OpenAI embedding dimension (1536)
        let schema = TupleSchema::new(vec![
            ("embedding".to_string(), DataType::vector_with_dim(1536)),
        ]);

        let large_vec = Value::vector(vec![0.0f32; 1536]);
        let tuple = Tuple::new(vec![large_vec]);
        assert!(schema.validate(&tuple).is_ok());

        // Wrong dimension
        let wrong_vec = Value::vector(vec![0.0f32; 768]); // Common smaller model
        let wrong_tuple = Tuple::new(vec![wrong_vec]);
        assert!(matches!(
            schema.validate(&wrong_tuple),
            Err(SchemaValidationError::VectorDimensionMismatch { expected: 1536, got: 768, .. })
        ));
    }

    #[test]
    fn test_infer_vector_dimensions_preserves_non_vectors() {
        let mut schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::String),
            ("embedding".to_string(), DataType::vector_any()),
        ]);

        let tuples = vec![
            Tuple::new(vec![
                Value::Int32(1),
                Value::string("test"),
                Value::vector(vec![1.0, 2.0, 3.0]),
            ]),
        ];

        schema.infer_vector_dimensions(&tuples);

        // Non-vector types should be unchanged
        assert_eq!(schema.field_type(0), Some(&DataType::Int32));
        assert_eq!(schema.field_type(1), Some(&DataType::String));
        // Vector dimension should be inferred
        assert_eq!(schema.field_type(2), Some(&DataType::Vector { dim: Some(3) }));
    }

    #[test]
    fn test_infer_vector_dimensions_empty_tuples() {
        let mut schema = TupleSchema::new(vec![
            ("embedding".to_string(), DataType::vector_any()),
        ]);

        // Empty tuples should not change schema
        schema.infer_vector_dimensions(&[]);
        assert_eq!(schema.field_type(0), Some(&DataType::Vector { dim: None }));
    }

    #[test]
    fn test_infer_dimensions_does_not_override_known() {
        let mut schema = TupleSchema::new(vec![
            ("embedding".to_string(), DataType::vector_with_dim(1536)),
        ]);

        let tuples = vec![
            Tuple::new(vec![Value::vector(vec![1.0, 2.0, 3.0])]), // Different dim
        ];

        // Should NOT change already-known dimension
        schema.infer_vector_dimensions(&tuples);
        assert_eq!(schema.field_type(0), Some(&DataType::Vector { dim: Some(1536) }));
    }

    #[test]
    fn test_empty_schema_validation() {
        let schema = TupleSchema::empty();

        // Empty tuple against empty schema should succeed
        let empty_tuple = Tuple::new(vec![]);
        assert!(schema.validate(&empty_tuple).is_ok());

        // Non-empty tuple should fail
        let non_empty = Tuple::new(vec![Value::Int32(1)]);
        assert!(matches!(
            schema.validate(&non_empty),
            Err(SchemaValidationError::ArityMismatch { expected: 0, got: 1 })
        ));
    }

    #[test]
    fn test_validate_multiple_vector_columns() {
        let schema = TupleSchema::new(vec![
            ("query".to_string(), DataType::vector_with_dim(3)),
            ("doc".to_string(), DataType::vector_with_dim(3)),
        ]);

        // Both correct
        let good = Tuple::new(vec![
            Value::vector(vec![1.0, 2.0, 3.0]),
            Value::vector(vec![4.0, 5.0, 6.0]),
        ]);
        assert!(schema.validate(&good).is_ok());

        // First wrong dimension
        let bad_first = Tuple::new(vec![
            Value::vector(vec![1.0, 2.0]),  // 2-dim instead of 3
            Value::vector(vec![4.0, 5.0, 6.0]),
        ]);
        let err = schema.validate(&bad_first).unwrap_err();
        assert!(matches!(err, SchemaValidationError::VectorDimensionMismatch { ref column, .. } if column == "query"));

        // Second wrong dimension
        let bad_second = Tuple::new(vec![
            Value::vector(vec![1.0, 2.0, 3.0]),
            Value::vector(vec![4.0, 5.0, 6.0, 7.0]),  // 4-dim instead of 3
        ]);
        let err = schema.validate(&bad_second).unwrap_err();
        assert!(matches!(err, SchemaValidationError::VectorDimensionMismatch { ref column, .. } if column == "doc"));
    }

    #[test]
    fn test_value_data_type_includes_dimension() {
        // Value::data_type() should include the actual dimension
        let v3 = Value::vector(vec![1.0, 2.0, 3.0]);
        assert_eq!(v3.data_type(), DataType::Vector { dim: Some(3) });

        let v1536 = Value::vector(vec![0.0; 1536]);
        assert_eq!(v1536.data_type(), DataType::Vector { dim: Some(1536) });

        let v0 = Value::vector(vec![]);
        assert_eq!(v0.data_type(), DataType::Vector { dim: Some(0) });
    }

    #[test]
    fn test_datatype_equality_with_dimension() {
        // Same dimension should be equal
        assert_eq!(
            DataType::Vector { dim: Some(3) },
            DataType::Vector { dim: Some(3) }
        );

        // Different dimensions should not be equal
        assert_ne!(
            DataType::Vector { dim: Some(3) },
            DataType::Vector { dim: Some(4) }
        );

        // None vs Some should not be equal
        assert_ne!(
            DataType::Vector { dim: None },
            DataType::Vector { dim: Some(3) }
        );

        // Two None should be equal
        assert_eq!(
            DataType::Vector { dim: None },
            DataType::Vector { dim: None }
        );
    }
}
