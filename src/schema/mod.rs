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
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: SchemaType,
}

impl ColumnSchema {
    /// Create a new column schema
    pub fn new(name: impl Into<String>, data_type: SchemaType) -> Self {
        ColumnSchema {
            name: name.into(),
            data_type,
        }
    }
}

impl fmt::Display for ColumnSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}

/// Complete schema definition for a relation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationSchema {
    /// Relation name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnSchema>,
}

impl RelationSchema {
    /// Create a new relation schema
    pub fn new(name: impl Into<String>) -> Self {
        RelationSchema {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    /// Add a column to the schema
    pub fn with_column(mut self, column: ColumnSchema) -> Self {
        self.columns.push(column);
        self
    }

    /// Get the arity (number of columns)
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Get column by index
    pub fn column(&self, index: usize) -> Option<&ColumnSchema> {
        self.columns.get(index)
    }

    /// Get column by name
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnSchema> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get column index by name
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Convert to `TupleSchema` (for compatibility with existing code)
    pub fn to_tuple_schema(&self) -> crate::value::TupleSchema {
        let fields: Vec<(String, DataType)> = self
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.to_data_type()))
            .collect();
        crate::value::TupleSchema::new(fields)
    }
}

impl fmt::Display for RelationSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{col}")?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_type_matching() {
        assert!(SchemaType::Int.matches(&Value::Int32(42)));
        assert!(SchemaType::Int.matches(&Value::Int64(42)));
        assert!(!SchemaType::Int.matches(&Value::string("42")));

        assert!(SchemaType::Float.matches(&Value::Float64(3.14)));
        assert!(SchemaType::Float.matches(&Value::Int32(42))); // Coercion

        assert!(SchemaType::Symbol.matches(&Value::string("hello")));
        assert!(!SchemaType::Symbol.matches(&Value::Int32(42)));

        assert!(SchemaType::Any.matches(&Value::Int32(42)));
        assert!(SchemaType::Any.matches(&Value::string("anything")));
    }

    #[test]
    fn test_schema_type_from_str() {
        assert_eq!(SchemaType::from_str("int"), Some(SchemaType::Int));
        assert_eq!(SchemaType::from_str("INT"), Some(SchemaType::Int));
        assert_eq!(SchemaType::from_str("symbol"), Some(SchemaType::Symbol));
        assert_eq!(SchemaType::from_str("SYMBOL"), Some(SchemaType::Symbol));
        assert_eq!(SchemaType::from_str("string"), Some(SchemaType::String));
        assert_eq!(SchemaType::from_str("STRING"), Some(SchemaType::String));
        assert_eq!(SchemaType::from_str("unknown"), None);
        // Named types (uppercase identifiers)
        assert_eq!(
            SchemaType::from_str("Email"),
            Some(SchemaType::Named("Email".to_string()))
        );
        assert_eq!(
            SchemaType::from_str("Age"),
            Some(SchemaType::Named("Age".to_string()))
        );
    }

    #[test]
    fn test_column_schema() {
        let col = ColumnSchema::new("age", SchemaType::Int);
        assert_eq!(col.name, "age");
        assert_eq!(col.data_type, SchemaType::Int);
    }

    #[test]
    fn test_relation_schema() {
        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("age", SchemaType::Int));

        assert_eq!(schema.name, "User");
        assert_eq!(schema.arity(), 2);
        assert_eq!(schema.column_names(), vec!["id", "age"]);
    }

    #[test]
    fn test_relation_schema_display() {
        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("age", SchemaType::Int));

        let display = format!("{}", schema);
        assert_eq!(display, "User(id: symbol, age: int)");
    }

    #[test]
    fn test_to_tuple_schema() {
        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::Symbol));

        let tuple_schema = schema.to_tuple_schema();
        assert_eq!(tuple_schema.arity(), 2);
        assert_eq!(tuple_schema.field_name(0), Some("id"));
        assert_eq!(tuple_schema.field_name(1), Some("name"));
    }
}
