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
