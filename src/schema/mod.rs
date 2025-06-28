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

