//! # Schema Catalog
//!
//! Storage and lookup for relation schemas with validation constraints.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use super::{RelationSchema, ColumnSchema, SchemaType, ColumnAnnotation, ValidationConfig};

/// Error types for schema operations
#[derive(Debug, Clone)]
pub enum SchemaError {
    /// Schema already exists for this relation
    AlreadyExists(String),
    /// Schema not found for relation
    NotFound(String),
    /// Invalid schema definition
    InvalidSchema(String),
    /// Duplicate column name
    DuplicateColumn(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::AlreadyExists(name) => {
                write!(f, "Schema already exists for relation '{}'", name)
            }
            SchemaError::NotFound(name) => {
                write!(f, "No schema found for relation '{}'", name)
            }
            SchemaError::InvalidSchema(msg) => {
                write!(f, "Invalid schema: {}", msg)
            }
            SchemaError::DuplicateColumn(name) => {
                write!(f, "Duplicate column name: '{}'", name)
            }
        }
    }
}

impl std::error::Error for SchemaError {}

/// Catalog for storing and looking up relation schemas
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaCatalog {
    /// Map from relation name to schema definition
    schemas: HashMap<String, RelationSchema>,
}

impl SchemaCatalog {
    /// Create a new empty schema catalog
    pub fn new() -> Self {
        SchemaCatalog {
            schemas: HashMap::new(),
        }
    }

    /// Register a new schema
    pub fn register(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        // Validate the schema
        self.validate_schema(&schema)?;

        // Check for existing schema
        if self.schemas.contains_key(&schema.name) {
            return Err(SchemaError::AlreadyExists(schema.name.clone()));
        }

        self.schemas.insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Register or update a schema
    pub fn register_or_update(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        self.validate_schema(&schema)?;
        self.schemas.insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Get schema for a relation (None if no schema defined)
    pub fn get(&self, relation: &str) -> Option<&RelationSchema> {
        self.schemas.get(relation)
    }

    /// Get mutable schema for a relation
    pub fn get_mut(&mut self, relation: &str) -> Option<&mut RelationSchema> {
        self.schemas.get_mut(relation)
    }

    /// Check if a schema exists for a relation
    pub fn has_schema(&self, relation: &str) -> bool {
        self.schemas.contains_key(relation)
    }

    /// Remove a schema
    pub fn remove(&mut self, relation: &str) -> Option<RelationSchema> {
        self.schemas.remove(relation)
    }

    /// Get all registered relation names
    pub fn relations(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| s.as_str()).collect()
    }

    /// Get all schemas
    pub fn all_schemas(&self) -> impl Iterator<Item = &RelationSchema> {
        self.schemas.values()
    }

    /// Get the number of registered schemas
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Check if the catalog is empty
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// Clear all schemas
    pub fn clear(&mut self) {
        self.schemas.clear();
    }

    /// Validate a schema definition
    fn validate_schema(&self, schema: &RelationSchema) -> Result<(), SchemaError> {
        // Check for empty name
        if schema.name.is_empty() {
            return Err(SchemaError::InvalidSchema("Relation name cannot be empty".to_string()));
        }

        // Check for reserved prefixes
        if schema.name.starts_with("_invalid_") {
            return Err(SchemaError::InvalidSchema(
                "Relation name cannot start with '_invalid_' (reserved for quarantine tables)".to_string(),
            ));
        }

        // Check for duplicate column names
        let mut seen_columns = std::collections::HashSet::new();
        for col in &schema.columns {
            if col.name.is_empty() {
                return Err(SchemaError::InvalidSchema("Column name cannot be empty".to_string()));
            }
            if !seen_columns.insert(&col.name) {
                return Err(SchemaError::DuplicateColumn(col.name.clone()));
            }
        }

        // Validate column annotations
        for col in &schema.columns {
            self.validate_column_annotations(col)?;
        }

        // Validate @check constraints reference valid rules (basic check)
        for check in &schema.validation.checks {
            match check {
                super::CheckConstraint::NamedRule(name) if name.is_empty() => {
                    return Err(SchemaError::InvalidSchema(
                        "@check rule name cannot be empty".to_string(),
                    ));
                }
                super::CheckConstraint::InlineExpr(expr) if expr.is_empty() => {
                    return Err(SchemaError::InvalidSchema(
                        "@check inline expression cannot be empty".to_string(),
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Validate column annotations
    fn validate_column_annotations(&self, col: &ColumnSchema) -> Result<(), SchemaError> {
        for ann in &col.annotations {
            match ann {
                ColumnAnnotation::Range { min, max } => {
                    if min > max {
                        return Err(SchemaError::InvalidSchema(format!(
                            "@range({}, {}) for column '{}': min cannot be greater than max",
                            min, max, col.name
                        )));
                    }
                    // Range only makes sense for numeric types
                    if !matches!(col.data_type, SchemaType::Int | SchemaType::Float) {
                        return Err(SchemaError::InvalidSchema(format!(
                            "@range for column '{}': only valid for int or float types",
                            col.name
                        )));
                    }
                }
                ColumnAnnotation::Pattern { regex } => {
                    // Validate regex is compilable
                    if let Err(e) = regex::Regex::new(regex) {
                        return Err(SchemaError::InvalidSchema(format!(
                            "@pattern for column '{}': invalid regex '{}': {}",
                            col.name, regex, e
                        )));
                    }
                    // Pattern only makes sense for string types
                    if !matches!(col.data_type, SchemaType::Symbol) {
                        return Err(SchemaError::InvalidSchema(format!(
                            "@pattern for column '{}': only valid for symbol type",
                            col.name
                        )));
                    }
                }
                ColumnAnnotation::ForeignKey { relation, column } => {
                    if relation.is_empty() || column.is_empty() {
                        return Err(SchemaError::InvalidSchema(format!(
                            "@foreign_key for column '{}': relation and column names cannot be empty",
                            col.name
                        )));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Create a schema builder for fluent API
    pub fn define(relation: impl Into<String>) -> SchemaBuilder {
        SchemaBuilder::new(relation)
    }
}

/// Builder for creating schemas with a fluent API
pub struct SchemaBuilder {
    schema: RelationSchema,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new(relation: impl Into<String>) -> Self {
        SchemaBuilder {
            schema: RelationSchema::new(relation),
        }
    }

    /// Add a column with just name and type
    pub fn column(mut self, name: impl Into<String>, dtype: SchemaType) -> Self {
        self.schema.columns.push(ColumnSchema::new(name, dtype));
        self
    }

    /// Add a column with annotations
    pub fn column_with(
        mut self,
        name: impl Into<String>,
        dtype: SchemaType,
        annotations: Vec<ColumnAnnotation>,
    ) -> Self {
        let mut col = ColumnSchema::new(name, dtype);
        col.annotations = annotations;
        self.schema.columns.push(col);
        self
    }

    /// Add a primary key column
    pub fn primary(self, name: impl Into<String>, dtype: SchemaType) -> Self {
        self.column_with(
            name,
            dtype,
            vec![ColumnAnnotation::Primary, ColumnAnnotation::NotEmpty],
        )
    }

    /// Set validation config
    pub fn validation(mut self, config: ValidationConfig) -> Self {
        self.schema.validation = config;
        self
    }

    /// Build the schema
    pub fn build(self) -> RelationSchema {
        self.schema
    }

    /// Build and register the schema in a catalog
    pub fn register_in(self, catalog: &mut SchemaCatalog) -> Result<(), SchemaError> {
        catalog.register(self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{CheckConstraint, FailureAction, ValidationTiming};

    #[test]
    fn test_catalog_register() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("name", SchemaType::Symbol));

        assert!(catalog.register(schema.clone()).is_ok());
        assert!(catalog.has_schema("User"));
        assert!(!catalog.has_schema("Unknown"));

        // Duplicate registration should fail
        assert!(catalog.register(schema).is_err());
    }

    #[test]
    fn test_catalog_get() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol));

        catalog.register(schema).unwrap();

        let retrieved = catalog.get("User").unwrap();
        assert_eq!(retrieved.name, "User");
        assert_eq!(retrieved.arity(), 1);
    }

    #[test]
    fn test_catalog_remove() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol));

        catalog.register(schema).unwrap();
        assert!(catalog.has_schema("User"));

        let removed = catalog.remove("User");
        assert!(removed.is_some());
        assert!(!catalog.has_schema("User"));
    }

    #[test]
    fn test_schema_builder() {
        let mut catalog = SchemaCatalog::new();

        SchemaCatalog::define("User")
            .primary("id", SchemaType::Symbol)
            .column("name", SchemaType::Symbol)
            .column_with("age", SchemaType::Int, vec![
                ColumnAnnotation::Range { min: 0, max: 120 },
            ])
            .validation(
                ValidationConfig::new()
                    .with_check(CheckConstraint::NamedRule("rule1".to_string())),
            )
            .register_in(&mut catalog)
            .unwrap();

        let schema = catalog.get("User").unwrap();
        assert_eq!(schema.arity(), 3);
        assert_eq!(schema.primary_key_indices(), vec![0]);
        assert!(schema.has_checks());
    }

    #[test]
    fn test_validate_empty_name() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol));

        assert!(catalog.register(schema).is_err());
    }

    #[test]
    fn test_validate_reserved_prefix() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("_invalid_test")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol));

        let result = catalog.register(schema);
        assert!(result.is_err());
        assert!(matches!(result, Err(SchemaError::InvalidSchema(_))));
    }

    #[test]
    fn test_validate_duplicate_columns() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("id", SchemaType::Int)); // Duplicate!

        let result = catalog.register(schema);
        assert!(matches!(result, Err(SchemaError::DuplicateColumn(_))));
    }

    #[test]
    fn test_validate_range_invalid() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Test")
            .with_column(
                ColumnSchema::new("age", SchemaType::Int)
                    .with_annotation(ColumnAnnotation::Range { min: 100, max: 0 }), // Invalid!
            );

        let result = catalog.register(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_range_wrong_type() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Test")
            .with_column(
                ColumnSchema::new("name", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Range { min: 0, max: 100 }), // Wrong type!
            );

        let result = catalog.register(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_pattern_invalid_regex() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Test")
            .with_column(
                ColumnSchema::new("email", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Pattern { regex: "[invalid".to_string() }),
            );

        let result = catalog.register(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_pattern_wrong_type() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Test")
            .with_column(
                ColumnSchema::new("age", SchemaType::Int)
                    .with_annotation(ColumnAnnotation::Pattern { regex: ".*".to_string() }),
            );

        let result = catalog.register(schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_len() {
        let mut catalog = SchemaCatalog::new();
        assert!(catalog.is_empty());
        assert_eq!(catalog.len(), 0);

        catalog
            .register(RelationSchema::new("R1").with_column(ColumnSchema::new("x", SchemaType::Int)))
            .unwrap();

        assert!(!catalog.is_empty());
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn test_catalog_relations() {
        let mut catalog = SchemaCatalog::new();

        catalog
            .register(RelationSchema::new("A").with_column(ColumnSchema::new("x", SchemaType::Int)))
            .unwrap();
        catalog
            .register(RelationSchema::new("B").with_column(ColumnSchema::new("y", SchemaType::Int)))
            .unwrap();

        let mut relations: Vec<_> = catalog.relations().into_iter().collect();
        relations.sort();
        assert_eq!(relations, vec!["A", "B"]);
    }

    #[test]
    fn test_catalog_clear() {
        let mut catalog = SchemaCatalog::new();

        catalog
            .register(RelationSchema::new("A").with_column(ColumnSchema::new("x", SchemaType::Int)))
            .unwrap();
        catalog
            .register(RelationSchema::new("B").with_column(ColumnSchema::new("y", SchemaType::Int)))
            .unwrap();

        assert_eq!(catalog.len(), 2);
        catalog.clear();
        assert_eq!(catalog.len(), 0);
    }
}
