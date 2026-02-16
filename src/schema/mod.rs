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

    // === Additional Coverage ===

    #[test]
    fn test_schema_type_to_data_type() {
        use crate::value::DataType;
        assert!(matches!(SchemaType::Int.to_data_type(), DataType::Int64));
        assert!(matches!(
            SchemaType::Float.to_data_type(),
            DataType::Float64
        ));
        assert!(matches!(
            SchemaType::Symbol.to_data_type(),
            DataType::String
        ));
        assert!(matches!(
            SchemaType::String.to_data_type(),
            DataType::String
        ));
        assert!(matches!(SchemaType::Bool.to_data_type(), DataType::Bool));
        assert!(matches!(
            SchemaType::Timestamp.to_data_type(),
            DataType::Timestamp
        ));
        assert!(matches!(SchemaType::Any.to_data_type(), DataType::Null));
        assert!(matches!(
            SchemaType::Named("Foo".to_string()).to_data_type(),
            DataType::String
        ));
    }

    #[test]
    fn test_schema_type_matches_bool() {
        assert!(SchemaType::Bool.matches(&Value::Bool(true)));
        assert!(!SchemaType::Bool.matches(&Value::Int32(1)));
    }

    #[test]
    fn test_schema_type_matches_timestamp() {
        assert!(SchemaType::Timestamp.matches(&Value::Timestamp(12345)));
        assert!(SchemaType::Timestamp.matches(&Value::Int64(12345))); // int as timestamp
        assert!(!SchemaType::Timestamp.matches(&Value::string("now")));
    }

    #[test]
    fn test_schema_type_matches_named_accepts_all() {
        let named = SchemaType::Named("Email".to_string());
        assert!(named.matches(&Value::string("test@example.com")));
        assert!(named.matches(&Value::Int64(42)));
    }

    #[test]
    fn test_schema_type_from_str_aliases() {
        assert_eq!(SchemaType::from_str("integer"), Some(SchemaType::Int));
        assert_eq!(SchemaType::from_str("i32"), Some(SchemaType::Int));
        assert_eq!(SchemaType::from_str("i64"), Some(SchemaType::Int));
        assert_eq!(SchemaType::from_str("double"), Some(SchemaType::Float));
        assert_eq!(SchemaType::from_str("f64"), Some(SchemaType::Float));
        assert_eq!(SchemaType::from_str("number"), Some(SchemaType::Float));
        assert_eq!(SchemaType::from_str("str"), Some(SchemaType::String));
        assert_eq!(SchemaType::from_str("text"), Some(SchemaType::String));
        assert_eq!(SchemaType::from_str("boolean"), Some(SchemaType::Bool));
        assert_eq!(
            SchemaType::from_str("timestamp"),
            Some(SchemaType::Timestamp)
        );
        assert_eq!(
            SchemaType::from_str("datetime"),
            Some(SchemaType::Timestamp)
        );
        assert_eq!(SchemaType::from_str("vector"), Some(SchemaType::Vector));
        assert_eq!(SchemaType::from_str("embedding"), Some(SchemaType::Vector));
        assert_eq!(SchemaType::from_str("vec"), Some(SchemaType::Vector));
        assert_eq!(SchemaType::from_str("any"), Some(SchemaType::Any));
    }

    #[test]
    fn test_schema_type_from_str_invalid_lowercase() {
        assert_eq!(SchemaType::from_str("foo"), None);
        assert_eq!(SchemaType::from_str("123"), None);
    }

    #[test]
    fn test_schema_type_is_base_type() {
        assert!(SchemaType::Int.is_base_type());
        assert!(SchemaType::Float.is_base_type());
        assert!(SchemaType::Any.is_base_type());
        assert!(!SchemaType::Named("Email".to_string()).is_base_type());
    }

    #[test]
    fn test_schema_type_display() {
        assert_eq!(format!("{}", SchemaType::Int), "int");
        assert_eq!(format!("{}", SchemaType::Float), "float");
        assert_eq!(format!("{}", SchemaType::Symbol), "symbol");
        assert_eq!(format!("{}", SchemaType::String), "string");
        assert_eq!(format!("{}", SchemaType::Bool), "bool");
        assert_eq!(format!("{}", SchemaType::Timestamp), "timestamp");
        assert_eq!(format!("{}", SchemaType::Vector), "vector");
        assert_eq!(format!("{}", SchemaType::Any), "any");
        assert_eq!(
            format!("{}", SchemaType::Named("Email".to_string())),
            "Email"
        );
    }

    #[test]
    fn test_column_schema_display() {
        let col = ColumnSchema::new("age", SchemaType::Int);
        assert_eq!(format!("{}", col), "age: int");
    }

    #[test]
    fn test_relation_schema_column_by_index() {
        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));
        assert_eq!(schema.column(0).unwrap().name, "id");
        assert_eq!(schema.column(1).unwrap().name, "name");
        assert!(schema.column(2).is_none());
    }

    #[test]
    fn test_relation_schema_column_by_name() {
        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));
        assert_eq!(
            schema.column_by_name("id").unwrap().data_type,
            SchemaType::Int
        );
        assert!(schema.column_by_name("nonexistent").is_none());
    }

    #[test]
    fn test_relation_schema_column_index() {
        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));
        assert_eq!(schema.column_index("id"), Some(0));
        assert_eq!(schema.column_index("name"), Some(1));
        assert_eq!(schema.column_index("nonexistent"), None);
    }

    #[test]
    fn test_relation_schema_empty() {
        let schema = RelationSchema::new("Empty");
        assert_eq!(schema.arity(), 0);
        assert!(schema.column_names().is_empty());
    }

    #[test]
    fn test_schema_type_serde_roundtrip() {
        let types = vec![
            SchemaType::Int,
            SchemaType::Float,
            SchemaType::String,
            SchemaType::Bool,
            SchemaType::Symbol,
            SchemaType::Timestamp,
            SchemaType::Vector,
            SchemaType::Any,
            SchemaType::Named("Email".to_string()),
        ];
        for t in types {
            let json = serde_json::to_string(&t).unwrap();
            let back: SchemaType = serde_json::from_str(&json).unwrap();
            assert_eq!(back, t);
        }
    }

    #[test]
    fn test_relation_schema_serde_roundtrip() {
        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(ColumnSchema::new("name", SchemaType::String));
        let json = serde_json::to_string(&schema).unwrap();
        let back: RelationSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "User");
        assert_eq!(back.arity(), 2);
    }
}
