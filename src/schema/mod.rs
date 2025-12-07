//! # Schema Validation Module
//!
//! This module provides schema validation for Datalog relations, enabling:
//! - Declarative schema definitions with typed columns and constraint annotations
//! - Datalog-based validation rules via @check constraints
//! - All-or-nothing insert semantics (reject entire batch if any tuple fails)
//! - Quarantine tables for non-compliant legacy data
//!
//! ## Example Schema Declaration
//!
//! ```datalog
//! User := schema(
//!   id:    symbol @primary @not_empty,
//!   name:  symbol @not_empty,
//!   age:   int    @range(0, 120),
//!   email: symbol @pattern("^[^@]+@[^@]+$")
//! ) @validate(
//!   @check(no_minors_in_admin)
//! )
//! ```

pub mod catalog;
pub mod validator;

use std::fmt;
use serde::{Deserialize, Serialize};
use crate::value::{DataType, Value};

// Re-export public types
pub use catalog::SchemaCatalog;
pub use validator::{ValidationEngine, ValidationError, Violation};

/// Schema type in Datalog syntax
/// Maps to internal DataType enum
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SchemaType {
    /// Integer type (maps to Int32 or Int64)
    Int,
    /// Floating-point type (maps to Float64)
    Float,
    /// Symbol type - interned atoms (lowercase identifiers like `alice`, `bob`)
    /// Cannot have @pattern constraint
    Symbol,
    /// String type - variable-length text that can have @pattern constraints
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
    /// Convert to internal DataType
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
            (SchemaType::Float, Value::Int32(_)) => true,  // Allow int->float coercion
            (SchemaType::Float, Value::Int64(_)) => true,
            (SchemaType::Symbol, Value::String(_)) => true,
            (SchemaType::String, Value::String(_)) => true,
            (SchemaType::Bool, Value::Bool(_)) => true,
            (SchemaType::Timestamp, Value::Timestamp(_)) => true,
            (SchemaType::Timestamp, Value::Int64(_)) => true,  // Allow int as timestamp
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
                if s.chars().next().map_or(false, |c| c.is_uppercase())
                   && s.chars().all(|c| c.is_alphanumeric() || c == '_') {
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
            SchemaType::Named(name) => write!(f, "{}", name),
        }
    }
}

/// Column-level annotation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnAnnotation {
    /// Column is part of primary key (uniqueness enforced)
    Primary,
    /// Column cannot be NULL or empty string
    NotEmpty,
    /// Column values must be unique across all tuples
    Unique,
    /// Numeric value must be in [min, max]
    Range { min: i64, max: i64 },
    /// String must match regex pattern
    Pattern { regex: String },
    /// Value must exist in referenced relation.column
    ForeignKey { relation: String, column: String },
    /// Default value if not provided
    Default { value: Value },
}

impl ColumnAnnotation {
    /// Get the annotation name for display
    pub fn name(&self) -> &'static str {
        match self {
            ColumnAnnotation::Primary => "primary",
            ColumnAnnotation::NotEmpty => "not_empty",
            ColumnAnnotation::Unique => "unique",
            ColumnAnnotation::Range { .. } => "range",
            ColumnAnnotation::Pattern { .. } => "pattern",
            ColumnAnnotation::ForeignKey { .. } => "foreign_key",
            ColumnAnnotation::Default { .. } => "default",
        }
    }
}

impl fmt::Display for ColumnAnnotation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnAnnotation::Primary => write!(f, "@primary"),
            ColumnAnnotation::NotEmpty => write!(f, "@not_empty"),
            ColumnAnnotation::Unique => write!(f, "@unique"),
            ColumnAnnotation::Range { min, max } => write!(f, "@range({}, {})", min, max),
            ColumnAnnotation::Pattern { regex } => write!(f, "@pattern(\"{}\")", regex),
            ColumnAnnotation::ForeignKey { relation, column } => {
                write!(f, "@foreign_key({}.{})", relation, column)
            }
            ColumnAnnotation::Default { value } => write!(f, "@default({})", value),
        }
    }
}

/// Column definition with name, type, and annotations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSchema {
    /// Column name
    pub name: String,
    /// Column type
    pub data_type: SchemaType,
    /// Column-level annotations (constraints)
    pub annotations: Vec<ColumnAnnotation>,
}

impl ColumnSchema {
    /// Create a new column schema
    pub fn new(name: impl Into<String>, data_type: SchemaType) -> Self {
        ColumnSchema {
            name: name.into(),
            data_type,
            annotations: Vec::new(),
        }
    }

    /// Add an annotation to this column
    pub fn with_annotation(mut self, annotation: ColumnAnnotation) -> Self {
        self.annotations.push(annotation);
        self
    }

    /// Check if this column has the @primary annotation
    pub fn is_primary(&self) -> bool {
        self.annotations.iter().any(|a| matches!(a, ColumnAnnotation::Primary))
    }

    /// Check if this column has the @not_empty annotation
    pub fn is_not_empty(&self) -> bool {
        self.annotations.iter().any(|a| matches!(a, ColumnAnnotation::NotEmpty))
    }

    /// Check if this column has the @unique annotation
    pub fn is_unique(&self) -> bool {
        self.annotations.iter().any(|a| matches!(a, ColumnAnnotation::Unique))
    }

    /// Get the @range constraint if present
    pub fn range(&self) -> Option<(i64, i64)> {
        for ann in &self.annotations {
            if let ColumnAnnotation::Range { min, max } = ann {
                return Some((*min, *max));
            }
        }
        None
    }

    /// Get the @pattern constraint if present
    pub fn pattern(&self) -> Option<&str> {
        for ann in &self.annotations {
            if let ColumnAnnotation::Pattern { regex } = ann {
                return Some(regex);
            }
        }
        None
    }

    /// Get the @foreign_key constraint if present
    pub fn foreign_key(&self) -> Option<(&str, &str)> {
        for ann in &self.annotations {
            if let ColumnAnnotation::ForeignKey { relation, column } = ann {
                return Some((relation, column));
            }
        }
        None
    }

    /// Get the @default value if present
    pub fn default_value(&self) -> Option<&Value> {
        for ann in &self.annotations {
            if let ColumnAnnotation::Default { value } = ann {
                return Some(value);
            }
        }
        None
    }
}

impl fmt::Display for ColumnSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)?;
        for ann in &self.annotations {
            write!(f, " {}", ann)?;
        }
        Ok(())
    }
}

/// Check constraint (named rule or inline expression)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CheckConstraint {
    /// Reference to a named rule (e.g., @check(no_minors_in_admin))
    NamedRule(String),
    /// Inline expression (e.g., @check(age >= 18 OR !admin_group(id)))
    InlineExpr(String),
}

impl fmt::Display for CheckConstraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckConstraint::NamedRule(name) => write!(f, "@check({})", name),
            CheckConstraint::InlineExpr(expr) => write!(f, "@check({})", expr),
        }
    }
}

/// When validation runs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ValidationTiming {
    /// Validate synchronously during insert (default)
    #[default]
    Sync,
    /// Defer validation (useful for bulk loads)
    Deferred,
}

impl fmt::Display for ValidationTiming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationTiming::Sync => write!(f, "sync"),
            ValidationTiming::Deferred => write!(f, "deferred"),
        }
    }
}

/// Action on validation failure
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FailureAction {
    /// Reject the insert (default)
    #[default]
    Reject,
    /// Move invalid tuples to quarantine table
    Quarantine,
}

impl fmt::Display for FailureAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FailureAction::Reject => write!(f, "reject"),
            FailureAction::Quarantine => write!(f, "quarantine"),
        }
    }
}

/// Relation-level validation configuration
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// @check constraints (named rules or inline expressions)
    pub checks: Vec<CheckConstraint>,
    /// When validation runs
    pub timing: ValidationTiming,
    /// What to do on failure
    pub on_fail: FailureAction,
}

impl ValidationConfig {
    /// Create a new empty validation config
    pub fn new() -> Self {
        ValidationConfig::default()
    }

    /// Add a check constraint
    pub fn with_check(mut self, check: CheckConstraint) -> Self {
        self.checks.push(check);
        self
    }

    /// Set the timing mode
    pub fn with_timing(mut self, timing: ValidationTiming) -> Self {
        self.timing = timing;
        self
    }

    /// Set the failure action
    pub fn with_on_fail(mut self, action: FailureAction) -> Self {
        self.on_fail = action;
        self
    }
}

/// Complete schema definition for a relation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationSchema {
    /// Relation name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnSchema>,
    /// Validation configuration
    pub validation: ValidationConfig,
}

impl RelationSchema {
    /// Create a new relation schema
    pub fn new(name: impl Into<String>) -> Self {
        RelationSchema {
            name: name.into(),
            columns: Vec::new(),
            validation: ValidationConfig::default(),
        }
    }

    /// Add a column to the schema
    pub fn with_column(mut self, column: ColumnSchema) -> Self {
        self.columns.push(column);
        self
    }

    /// Set the validation config
    pub fn with_validation(mut self, validation: ValidationConfig) -> Self {
        self.validation = validation;
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

    /// Get primary key columns
    pub fn primary_key_columns(&self) -> Vec<&ColumnSchema> {
        self.columns.iter().filter(|c| c.is_primary()).collect()
    }

    /// Get primary key column indices
    pub fn primary_key_indices(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.is_primary())
            .map(|(i, _)| i)
            .collect()
    }

    /// Get the quarantine table name for this relation
    pub fn quarantine_table_name(&self) -> String {
        format!("_invalid_{}", self.name)
    }

    /// Check if this schema has any @check constraints
    pub fn has_checks(&self) -> bool {
        !self.validation.checks.is_empty()
    }

    /// Convert to TupleSchema (for compatibility with existing code)
    pub fn to_tuple_schema(&self) -> crate::value::TupleSchema {
        let fields: Vec<(String, DataType)> = self.columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.to_data_type()))
            .collect();
        crate::value::TupleSchema::new(fields)
    }
}

/// Type alias definition: `type Email = string pattern("^[^@]+@[^@]+$")`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeAlias {
    /// Name of the type alias (e.g., "Email", "Age")
    pub name: String,
    /// Base type this alias refers to
    pub base_type: SchemaType,
    /// Constraints applied to this type
    pub annotations: Vec<ColumnAnnotation>,
}

impl TypeAlias {
    /// Create a new type alias
    pub fn new(name: impl Into<String>, base_type: SchemaType) -> Self {
        TypeAlias {
            name: name.into(),
            base_type,
            annotations: Vec::new(),
        }
    }

    /// Add an annotation to this type
    pub fn with_annotation(mut self, annotation: ColumnAnnotation) -> Self {
        self.annotations.push(annotation);
        self
    }
}

impl fmt::Display for TypeAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "type {} = {}", self.name, self.base_type)?;
        for ann in &self.annotations {
            write!(f, " {}", ann)?;
        }
        Ok(())
    }
}

impl fmt::Display for RelationSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{} = schema(", self.name)?;
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                writeln!(f, ",")?;
            }
            write!(f, "  {}", col)?;
        }
        write!(f, "\n)")?;

        if !self.validation.checks.is_empty()
            || self.validation.timing != ValidationTiming::Sync
            || self.validation.on_fail != FailureAction::Reject
        {
            write!(f, " @validate(")?;
            let mut first = true;

            for check in &self.validation.checks {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "{}", check)?;
                first = false;
            }

            if self.validation.timing != ValidationTiming::Sync {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "@timing({})", self.validation.timing)?;
                first = false;
            }

            if self.validation.on_fail != FailureAction::Reject {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "@on_fail({})", self.validation.on_fail)?;
            }

            write!(f, ")")?;
        }

        Ok(())
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
        assert_eq!(SchemaType::from_str("Email"), Some(SchemaType::Named("Email".to_string())));
        assert_eq!(SchemaType::from_str("Age"), Some(SchemaType::Named("Age".to_string())));
    }

    #[test]
    fn test_column_schema() {
        let col = ColumnSchema::new("age", SchemaType::Int)
            .with_annotation(ColumnAnnotation::NotEmpty)
            .with_annotation(ColumnAnnotation::Range { min: 0, max: 120 });

        assert_eq!(col.name, "age");
        assert!(col.is_not_empty());
        assert!(!col.is_primary());
        assert_eq!(col.range(), Some((0, 120)));
    }

    #[test]
    fn test_relation_schema() {
        let schema = RelationSchema::new("User")
            .with_column(
                ColumnSchema::new("id", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Primary)
                    .with_annotation(ColumnAnnotation::NotEmpty),
            )
            .with_column(
                ColumnSchema::new("age", SchemaType::Int)
                    .with_annotation(ColumnAnnotation::Range { min: 0, max: 120 }),
            )
            .with_validation(
                ValidationConfig::new()
                    .with_check(CheckConstraint::NamedRule("no_minors_in_admin".to_string())),
            );

        assert_eq!(schema.name, "User");
        assert_eq!(schema.arity(), 2);
        assert_eq!(schema.primary_key_indices(), vec![0]);
        assert!(schema.has_checks());
        assert_eq!(schema.quarantine_table_name(), "_invalid_User");
    }

    #[test]
    fn test_relation_schema_display() {
        let schema = RelationSchema::new("User")
            .with_column(
                ColumnSchema::new("id", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Primary),
            )
            .with_column(
                ColumnSchema::new("age", SchemaType::Int)
                    .with_annotation(ColumnAnnotation::Range { min: 0, max: 120 }),
            );

        let display = format!("{}", schema);
        assert!(display.contains("User"));
        assert!(display.contains("id: symbol @primary"));
        assert!(display.contains("age: int @range(0, 120)"));
    }

    #[test]
    fn test_validation_config() {
        let config = ValidationConfig::new()
            .with_check(CheckConstraint::NamedRule("rule1".to_string()))
            .with_check(CheckConstraint::InlineExpr("age >= 18".to_string()))
            .with_timing(ValidationTiming::Deferred)
            .with_on_fail(FailureAction::Quarantine);

        assert_eq!(config.checks.len(), 2);
        assert_eq!(config.timing, ValidationTiming::Deferred);
        assert_eq!(config.on_fail, FailureAction::Quarantine);
    }

    #[test]
    fn test_column_annotation_display() {
        assert_eq!(format!("{}", ColumnAnnotation::Primary), "@primary");
        assert_eq!(format!("{}", ColumnAnnotation::NotEmpty), "@not_empty");
        assert_eq!(
            format!("{}", ColumnAnnotation::Range { min: 0, max: 100 }),
            "@range(0, 100)"
        );
        assert_eq!(
            format!("{}", ColumnAnnotation::Pattern { regex: "^a.*".to_string() }),
            "@pattern(\"^a.*\")"
        );
        assert_eq!(
            format!(
                "{}",
                ColumnAnnotation::ForeignKey {
                    relation: "User".to_string(),
                    column: "id".to_string()
                }
            ),
            "@foreign_key(User.id)"
        );
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
