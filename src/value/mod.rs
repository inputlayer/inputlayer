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

