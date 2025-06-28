//! Schema validation for Datalog relations: typed columns,
//! arity checking, and all-or-nothing insert semantics.
//!
//! ## Example Schema Declaration
//!
//! ```datalog
//! +User(id: symbol, name: string, age: int).
//! ```

pub mod catalog;
pub mod validator;

use crate::value::{DataType, Value};
use serde::{Deserialize, Serialize};
use std::fmt;

// Re-export public types
pub use catalog::SchemaCatalog;
pub use validator::{ValidationEngine, ValidationError, Violation};

/// Schema type in Datalog syntax
/// Maps to internal `DataType` enum
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SchemaType {
    /// Integer type (maps to Int32 or Int64)
    Int,
    /// Floating-point type (maps to Float64)
    Float,
    /// Symbol type - interned atoms (lowercase identifiers like `alice`, `bob`)
    Symbol,
    /// String type - variable-length text
    String,
    /// Boolean type
    Bool,
    /// Unix timestamp in milliseconds
    Timestamp,
    /// Vector of f32 values (embeddings)
    Vector,
    /// Any type (no type constraint)
    Any,
    /// Named type alias (e.g., Email, Age)
    Named(String),
}

impl SchemaType {
    /// Convert to internal `DataType`
    pub fn to_data_type(&self) -> DataType {
        match self {
            SchemaType::Int => DataType::Int64,
            SchemaType::Float => DataType::Float64,
            SchemaType::Symbol => DataType::String,
            SchemaType::String => DataType::String,
            SchemaType::Bool => DataType::Bool,
            SchemaType::Timestamp => DataType::Timestamp,
            SchemaType::Vector => DataType::vector_any(),
            SchemaType::Any => DataType::Null, // Null used as "any" marker
            SchemaType::Named(_) => DataType::String, // Named types resolve at validation time
        }
    }

    /// Check if a value matches this schema type
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (SchemaType::Int, Value::Int32(_)) => true,
            (SchemaType::Int, Value::Int64(_)) => true,
            (SchemaType::Float, Value::Float64(_)) => true,
            (SchemaType::Float, Value::Int32(_)) => true, // Allow int->float coercion
            (SchemaType::Float, Value::Int64(_)) => true,
            (SchemaType::Symbol, Value::String(_)) => true,
            (SchemaType::String, Value::String(_)) => true,
            (SchemaType::Bool, Value::Bool(_)) => true,
            (SchemaType::Timestamp, Value::Timestamp(_)) => true,
            (SchemaType::Timestamp, Value::Int64(_)) => true, // Allow int as timestamp
            (SchemaType::Vector, Value::Vector(_)) => true,
            (SchemaType::Vector, Value::VectorInt8(_)) => true,
            (SchemaType::Any, _) => true,
            (SchemaType::Named(_), _) => true, // Named types need catalog lookup for full validation
            _ => false,
        }
    }

    /// Parse from string (case-insensitive for base types)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "int" | "integer" | "i32" | "i64" => Some(SchemaType::Int),
            "float" | "double" | "f64" | "number" => Some(SchemaType::Float),
            "symbol" => Some(SchemaType::Symbol),
            "string" | "str" | "text" => Some(SchemaType::String),
            "bool" | "boolean" => Some(SchemaType::Bool),
            "timestamp" | "time" | "datetime" => Some(SchemaType::Timestamp),
            "vector" | "embedding" | "vec" => Some(SchemaType::Vector),
            "any" => Some(SchemaType::Any),
            _ => {
                // Check if it's a valid identifier (potential type alias)
                if s.chars().next().is_some_and(char::is_uppercase)
                    && s.chars().all(|c| c.is_alphanumeric() || c == '_')
                {
                    Some(SchemaType::Named(s.to_string()))
                } else {
                    None
                }
            }
        }
    }

    /// Check if this is a base type (not a named alias)
    pub fn is_base_type(&self) -> bool {
        !matches!(self, SchemaType::Named(_))
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaType::Int => write!(f, "int"),
            SchemaType::Float => write!(f, "float"),
            SchemaType::Symbol => write!(f, "symbol"),
            SchemaType::String => write!(f, "string"),
            SchemaType::Bool => write!(f, "bool"),
            SchemaType::Timestamp => write!(f, "timestamp"),
            SchemaType::Vector => write!(f, "vector"),
            SchemaType::Any => write!(f, "any"),
            SchemaType::Named(name) => write!(f, "{name}"),
        }
    }
}

/// Column definition with name and type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
