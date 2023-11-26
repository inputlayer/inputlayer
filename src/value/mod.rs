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

