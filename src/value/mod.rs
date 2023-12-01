//! # Value Type System
//!
//! Core value types: Int32, Int64, Float64, String, Bool, Null, Vector, VectorInt8, Timestamp.
//! Arbitrary arity tuples with Arrow-compatible types and DD trait implementations.
//!
//! ## Usage
//!
//! ```rust
//! use inputlayer::value::{Value, Tuple, TupleSchema, DataType};
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
    infer_schema_from_tuples, record_batch_to_tuples, tuples_to_record_batch, ArrowConvertError,
};

use abomonation::Abomonation;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::sync::Arc;

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
    Vector {
        dim: Option<usize>,
    },
    /// Vector of i8 values (quantized embeddings for 75% memory savings)
    /// Same dimension semantics as Vector
    VectorInt8 {
        dim: Option<usize>,
    },
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
            (
                DataType::Vector {
                    dim: Some(expected),
                },
                Value::Vector(v),
            ) => v.len() == *expected,
            (DataType::Vector { dim: None }, Value::Vector(_)) => true,
            (
                DataType::VectorInt8 {
                    dim: Some(expected),
                },
                Value::VectorInt8(v),
            ) => v.len() == *expected,
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
    /// Convert to Arrow `DataType`
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
                Arc::new(arrow::datatypes::Field::new(
                    "item",
                    ArrowDataType::Float32,
                    false,
                )),
                *n as i32,
            ),
            // Vectors with unknown dimension use LargeList (variable length)
            DataType::Vector { dim: None } => ArrowDataType::LargeList(Arc::new(
                arrow::datatypes::Field::new("item", ArrowDataType::Float32, false),
            )),
            // Int8 vectors with known dimension use FixedSizeList
            DataType::VectorInt8 { dim: Some(n) } => ArrowDataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(
                    "item",
                    ArrowDataType::Int8,
                    false,
                )),
                *n as i32,
            ),
            // Int8 vectors with unknown dimension use LargeList
            DataType::VectorInt8 { dim: None } => ArrowDataType::LargeList(Arc::new(
                arrow::datatypes::Field::new("item", ArrowDataType::Int8, false),
            )),
            // Timestamps stored as Int64 (milliseconds since Unix epoch)
            DataType::Timestamp => ArrowDataType::Int64,
        }
    }

    /// Create from Arrow `DataType`
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
                if matches!(field.data_type(), ArrowDataType::Float32) =>
            {
                Some(DataType::Vector {
                    dim: Some(*size as usize),
                })
            }
            // Variable-length lists have unknown dimension
            ArrowDataType::LargeList(field) | ArrowDataType::List(field)
                if matches!(field.data_type(), ArrowDataType::Float32) =>
            {
                Some(DataType::Vector { dim: None })
            }
            // Int8 FixedSizeList preserves dimension information
            ArrowDataType::FixedSizeList(field, size)
                if matches!(field.data_type(), ArrowDataType::Int8) =>
            {
                Some(DataType::VectorInt8 {
                    dim: Some(*size as usize),
                })
            }
            // Int8 variable-length lists have unknown dimension
            ArrowDataType::LargeList(field) | ArrowDataType::List(field)
                if matches!(field.data_type(), ArrowDataType::Int8) =>
            {
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
    /// For vectors, includes the actual dimension.
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
            Value::Int32(v) => Some(i64::from(*v)),
            Value::Int64(v) => Some(*v),
            Value::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            Value::Int32(v) => Some(f64::from(*v)),
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

    pub fn string(s: &str) -> Self {
        Value::String(Arc::from(s))
    }

    pub fn vector(data: Vec<f32>) -> Self {
        Value::Vector(Arc::new(data))
    }

    pub fn vector_from_iter<I: IntoIterator<Item = f32>>(iter: I) -> Self {
        Value::Vector(Arc::new(iter.into_iter().collect()))
    }

    pub fn vector_int8(data: Vec<i8>) -> Self {
        Value::VectorInt8(Arc::new(data))
    }

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
            Value::Int32(v) => i64::from(*v),
            Value::Int64(v) => *v,
            Value::Float64(v) => *v as i64,
            Value::Bool(b) => i64::from(*b),
            Value::Timestamp(t) => *t,
            _ => 0,
        }
    }

    /// Convert to f64 (for aggregation operations)
    /// Returns 0.0 for non-numeric types
    pub fn to_f64(&self) -> f64 {
        match self {
            Value::Int32(v) => f64::from(*v),
            Value::Int64(v) => *v as f64,
            Value::Float64(v) => *v,
            Value::Bool(b) => {
                if *b {
                    1.0
                } else {
                    0.0
                }
            }
            Value::Timestamp(t) => *t as f64,
            _ => 0.0,
        }
    }
}

/// Format an f64 consistently across platforms.
/// Rust's default Display for f64 produces `1e+20` on Linux but `1e20` on macOS.
/// This normalizes to always omit the redundant `+` in exponents.
fn format_f64(v: f64) -> String {
    let s = format!("{v}");
    s.replace("e+", "e")
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int32(v) => write!(f, "{v}"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{}", format_f64(*v)),
            Value::String(s) => write!(f, "\"{s}\""),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Null => write!(f, "NULL"),
            Value::Vector(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    // Show first 5 elements, then "... N more" for large vectors
                    if i < 5 || v.len() <= 6 {
                        write!(f, "{val:.4}")?;
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
                        write!(f, "{val}")?;
                    } else if i == 5 {
                        write!(f, "... {} more", v.len() - 5)?;
                        break;
                    }
                }
                write!(f, "]i8")
            }
            Value::Timestamp(ts) => write!(f, "{ts}ms"),
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
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
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
