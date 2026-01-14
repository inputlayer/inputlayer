//! # Schema Catalog
//!
//! Storage and lookup for relation schemas with type definitions.
//! Supports both session (temporary) and persistent schemas.

use super::{ColumnSchema, RelationSchema, SchemaType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Error types for schema operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaError {
    /// Schema already exists for this relation
    #[error("Schema already exists for relation '{0}'")]
    AlreadyExists(String),
    /// Schema not found for relation
    #[error("No schema found for relation '{0}'")]
    NotFound(String),
    /// Invalid schema definition
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    /// Duplicate column name
    #[error("Duplicate column name: '{0}'")]
    DuplicateColumn(String),
    /// Existing data violates schema
    #[error("Existing data in '{relation}' violates schema: {message}")]
    DataViolation { relation: String, message: String },
    /// IO error
    #[error("IO error: {0}")]
    IoError(String),
}

/// Catalog for storing and looking up relation schemas.
/// Supports both persistent schemas (saved to disk) and session schemas (memory only).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaCatalog {
    /// Persistent schemas (saved to disk)
    persistent: HashMap<String, RelationSchema>,
    /// Session schemas (memory only, cleared on disconnect)
    #[serde(skip)]
    session: HashMap<String, RelationSchema>,
}

impl SchemaCatalog {
    /// Create a new empty schema catalog
    pub fn new() -> Self {
        SchemaCatalog {
            persistent: HashMap::new(),
            session: HashMap::new(),
        }
    }

    /// Register a persistent schema
    pub fn register(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        self.register_persistent(schema)
    }

    /// Register a persistent schema (saved to disk)
    pub fn register_persistent(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        self.validate_schema(&schema)?;

        let name = schema.name.clone();

        // Check for existing persistent schema
        if self.persistent.contains_key(&name) {
            return Err(SchemaError::AlreadyExists(name));
        }

        self.persistent.insert(name, schema);
        Ok(())
    }

    /// Register a session schema (memory only)
    pub fn register_session(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        self.validate_schema(&schema)?;

        let name = schema.name.clone();

        // Check for existing session schema
        if self.session.contains_key(&name) {
            return Err(SchemaError::AlreadyExists(name));
        }

        self.session.insert(name, schema);
        Ok(())
    }

    /// Register or update a persistent schema
    pub fn register_or_update(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        self.validate_schema(&schema)?;
        self.persistent.insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Register or update a session schema
    pub fn register_or_update_session(&mut self, schema: RelationSchema) -> Result<(), SchemaError> {
        self.validate_schema(&schema)?;
        self.session.insert(schema.name.clone(), schema);
        Ok(())
    }

    /// Get schema for a relation.
    /// Session schemas shadow persistent schemas.
    pub fn get(&self, relation: &str) -> Option<&RelationSchema> {
        self.session
            .get(relation)
            .or_else(|| self.persistent.get(relation))
    }

    /// Get mutable schema for a relation (session first, then persistent)
    pub fn get_mut(&mut self, relation: &str) -> Option<&mut RelationSchema> {
        if self.session.contains_key(relation) {
            self.session.get_mut(relation)
        } else {
            self.persistent.get_mut(relation)
        }
    }

    /// Check if a schema exists for a relation (session or persistent)
    pub fn has_schema(&self, relation: &str) -> bool {
        self.session.contains_key(relation) || self.persistent.contains_key(relation)
    }

    /// Check if a persistent schema exists for a relation
    pub fn has_persistent_schema(&self, relation: &str) -> bool {
        self.persistent.contains_key(relation)
    }

    /// Check if a session schema exists for a relation
    pub fn has_session_schema(&self, relation: &str) -> bool {
        self.session.contains_key(relation)
    }

    /// Remove a schema (from both session and persistent)
    pub fn remove(&mut self, relation: &str) -> Option<RelationSchema> {
        self.session
            .remove(relation)
            .or_else(|| self.persistent.remove(relation))
    }

    /// Remove a persistent schema
    pub fn remove_persistent(&mut self, relation: &str) -> Option<RelationSchema> {
        self.persistent.remove(relation)
    }

    /// Remove a session schema
    pub fn remove_session(&mut self, relation: &str) -> Option<RelationSchema> {
        self.session.remove(relation)
    }

    /// Get all registered relation names (session + persistent, deduplicated)
    pub fn relations(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .session
            .keys()
            .chain(self.persistent.keys())
            .map(|s| s.as_str())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    /// Get all persistent schema names
    pub fn persistent_relations(&self) -> Vec<&str> {
        self.persistent.keys().map(|s| s.as_str()).collect()
    }

    /// Get all session schema names
    pub fn session_relations(&self) -> Vec<&str> {
        self.session.keys().map(|s| s.as_str()).collect()
    }

    /// Get all schemas (session shadows persistent)
    pub fn all_schemas(&self) -> impl Iterator<Item = &RelationSchema> {
        let session_names: std::collections::HashSet<_> = self.session.keys().collect();
        let persistent_iter = self
            .persistent
            .iter()
            .filter(move |(name, _)| !session_names.contains(name))
            .map(|(_, schema)| schema);

        self.session.values().chain(persistent_iter)
    }

    /// Get all persistent schemas
    pub fn persistent_schemas(&self) -> impl Iterator<Item = &RelationSchema> {
        self.persistent.values()
    }

    /// Get all session schemas
    pub fn session_schemas(&self) -> impl Iterator<Item = &RelationSchema> {
        self.session.values()
    }

    /// Get the total number of registered schemas (session + persistent, deduplicated)
    pub fn len(&self) -> usize {
        self.relations().len()
    }

    /// Get the number of persistent schemas
    pub fn persistent_len(&self) -> usize {
        self.persistent.len()
    }

    /// Get the number of session schemas
    pub fn session_len(&self) -> usize {
        self.session.len()
    }

    /// Check if the catalog is empty
    pub fn is_empty(&self) -> bool {
        self.persistent.is_empty() && self.session.is_empty()
    }

    /// Clear all schemas (session and persistent)
    pub fn clear(&mut self) {
        self.persistent.clear();
        self.session.clear();
    }

    /// Clear only session schemas (called on disconnect)
    pub fn clear_session(&mut self) {
        self.session.clear();
    }

    /// Clear only persistent schemas
    pub fn clear_persistent(&mut self) {
        self.persistent.clear();
    }

    /// Validate a schema definition
    fn validate_schema(&self, schema: &RelationSchema) -> Result<(), SchemaError> {
        // Check for empty name
        if schema.name.is_empty() {
            return Err(SchemaError::InvalidSchema(
                "Relation name cannot be empty".to_string(),
            ));
        }

        // Check for reserved prefixes
        if schema.name.starts_with("_invalid_") {
            return Err(SchemaError::InvalidSchema(
                "Relation name cannot start with '_invalid_' (reserved)".to_string(),
            ));
        }

        // Check for duplicate column names
        let mut seen_columns = std::collections::HashSet::new();
        for col in &schema.columns {
            if col.name.is_empty() {
                return Err(SchemaError::InvalidSchema(
                    "Column name cannot be empty".to_string(),
                ));
            }
            if !seen_columns.insert(&col.name) {
                return Err(SchemaError::DuplicateColumn(col.name.clone()));
            }
        }

        Ok(())
    }

    /// Create a schema builder for fluent API
    pub fn define(relation: impl Into<String>) -> SchemaBuilder {
        SchemaBuilder::new(relation)
    }

    // ========================================================================
    // Persistence
    // ========================================================================

    /// Load a schema catalog from a JSON file
    /// Only loads persistent schemas; session schemas are not saved.
    pub fn load(path: &Path) -> Result<Self, SchemaError> {
        if !path.exists() {
            return Ok(SchemaCatalog::new());
        }

        let content = fs::read_to_string(path)
            .map_err(|e| SchemaError::IoError(format!("Failed to read schema catalog: {}", e)))?;

        let catalog: SchemaCatalog = serde_json::from_str(&content)
            .map_err(|e| SchemaError::IoError(format!("Failed to parse schema catalog: {}", e)))?;

        Ok(catalog)
    }

    /// Save the persistent schemas to a JSON file
    /// Session schemas are not saved.
    pub fn save(&self, path: &Path) -> Result<(), SchemaError> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                SchemaError::IoError(format!("Failed to create schema directory: {}", e))
            })?;
        }

        let content = serde_json::to_string_pretty(self)
            .map_err(|e| SchemaError::IoError(format!("Failed to serialize schemas: {}", e)))?;

        fs::write(path, content)
            .map_err(|e| SchemaError::IoError(format!("Failed to write schema catalog: {}", e)))?;

        Ok(())
    }

    /// Merge another catalog's persistent schemas into this one
    pub fn merge(&mut self, other: SchemaCatalog) {
        for (name, schema) in other.persistent {
            self.persistent.insert(name, schema);
        }
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

    /// Add a column with name and type
    pub fn column(mut self, name: impl Into<String>, dtype: SchemaType) -> Self {
        self.schema.columns.push(ColumnSchema::new(name, dtype));
        self
    }

    /// Build the schema
    pub fn build(self) -> RelationSchema {
        self.schema
    }

    /// Build and register as persistent schema in a catalog
    pub fn register_in(self, catalog: &mut SchemaCatalog) -> Result<(), SchemaError> {
        catalog.register_persistent(self.schema)
    }

    /// Build and register as session schema in a catalog
    pub fn register_session_in(self, catalog: &mut SchemaCatalog) -> Result<(), SchemaError> {
        catalog.register_session(self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_register_persistent() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("name", SchemaType::Symbol));

        assert!(catalog.register_persistent(schema.clone()).is_ok());
        assert!(catalog.has_schema("User"));
        assert!(catalog.has_persistent_schema("User"));
        assert!(!catalog.has_session_schema("User"));

        // Duplicate registration should fail
        assert!(catalog.register_persistent(schema).is_err());
    }

    #[test]
    fn test_catalog_register_session() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Temp")
            .with_column(ColumnSchema::new("id", SchemaType::Int));

        assert!(catalog.register_session(schema.clone()).is_ok());
        assert!(catalog.has_schema("Temp"));
        assert!(catalog.has_session_schema("Temp"));
        assert!(!catalog.has_persistent_schema("Temp"));

        // Duplicate registration should fail
        assert!(catalog.register_session(schema).is_err());
    }

    #[test]
    fn test_session_shadows_persistent() {
        let mut catalog = SchemaCatalog::new();

        // Register persistent schema with 2 columns
        let persistent_schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("name", SchemaType::Symbol));
        catalog.register_persistent(persistent_schema).unwrap();

        // Register session schema with 3 columns (shadows persistent)
        let session_schema = RelationSchema::new("User")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("name", SchemaType::Symbol))
            .with_column(ColumnSchema::new("age", SchemaType::Int));
        catalog.register_session(session_schema).unwrap();

        // get() should return session schema (3 columns)
        let schema = catalog.get("User").unwrap();
        assert_eq!(schema.arity(), 3);

        // Clear session - now get persistent
        catalog.clear_session();
        let schema = catalog.get("User").unwrap();
        assert_eq!(schema.arity(), 2);
    }

    #[test]
    fn test_catalog_get() {
        let mut catalog = SchemaCatalog::new();

        let schema =
            RelationSchema::new("User").with_column(ColumnSchema::new("id", SchemaType::Symbol));

        catalog.register_persistent(schema).unwrap();

        let retrieved = catalog.get("User").unwrap();
        assert_eq!(retrieved.name, "User");
        assert_eq!(retrieved.arity(), 1);
    }

    #[test]
    fn test_catalog_remove() {
        let mut catalog = SchemaCatalog::new();

        let schema =
            RelationSchema::new("User").with_column(ColumnSchema::new("id", SchemaType::Symbol));

        catalog.register_persistent(schema).unwrap();
        assert!(catalog.has_schema("User"));

        let removed = catalog.remove("User");
        assert!(removed.is_some());
        assert!(!catalog.has_schema("User"));
    }

    #[test]
    fn test_clear_session() {
        let mut catalog = SchemaCatalog::new();

        // Add persistent
        catalog
            .register_persistent(
                RelationSchema::new("A").with_column(ColumnSchema::new("x", SchemaType::Int)),
            )
            .unwrap();

        // Add session
        catalog
            .register_session(
                RelationSchema::new("B").with_column(ColumnSchema::new("y", SchemaType::Int)),
            )
            .unwrap();

        assert_eq!(catalog.len(), 2);
        assert_eq!(catalog.persistent_len(), 1);
        assert_eq!(catalog.session_len(), 1);

        // Clear session only
        catalog.clear_session();

        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog.persistent_len(), 1);
        assert_eq!(catalog.session_len(), 0);
        assert!(catalog.has_schema("A"));
        assert!(!catalog.has_schema("B"));
    }

    #[test]
    fn test_schema_builder() {
        let mut catalog = SchemaCatalog::new();

        SchemaCatalog::define("User")
            .column("id", SchemaType::Symbol)
            .column("name", SchemaType::Symbol)
            .column("age", SchemaType::Int)
            .register_in(&mut catalog)
            .unwrap();

        let schema = catalog.get("User").unwrap();
        assert_eq!(schema.arity(), 3);
        assert!(catalog.has_persistent_schema("User"));
    }

    #[test]
    fn test_schema_builder_session() {
        let mut catalog = SchemaCatalog::new();

        SchemaCatalog::define("Temp")
            .column("x", SchemaType::Int)
            .register_session_in(&mut catalog)
            .unwrap();

        let schema = catalog.get("Temp").unwrap();
        assert_eq!(schema.arity(), 1);
        assert!(catalog.has_session_schema("Temp"));
        assert!(!catalog.has_persistent_schema("Temp"));
    }

    #[test]
    fn test_validate_empty_name() {
        let mut catalog = SchemaCatalog::new();

        let schema =
            RelationSchema::new("").with_column(ColumnSchema::new("id", SchemaType::Symbol));

        assert!(catalog.register_persistent(schema).is_err());
    }

    #[test]
    fn test_validate_reserved_prefix() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("_invalid_test")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol));

        let result = catalog.register_persistent(schema);
        assert!(result.is_err());
        assert!(matches!(result, Err(SchemaError::InvalidSchema(_))));
    }

    #[test]
    fn test_validate_duplicate_columns() {
        let mut catalog = SchemaCatalog::new();

        let schema = RelationSchema::new("Test")
            .with_column(ColumnSchema::new("id", SchemaType::Symbol))
            .with_column(ColumnSchema::new("id", SchemaType::Int)); // Duplicate!

        let result = catalog.register_persistent(schema);
        assert!(matches!(result, Err(SchemaError::DuplicateColumn(_))));
    }

    #[test]
    fn test_catalog_len() {
        let mut catalog = SchemaCatalog::new();
        assert!(catalog.is_empty());
        assert_eq!(catalog.len(), 0);

        catalog
            .register_persistent(
                RelationSchema::new("R1").with_column(ColumnSchema::new("x", SchemaType::Int)),
            )
            .unwrap();

        assert!(!catalog.is_empty());
        assert_eq!(catalog.len(), 1);
    }

    #[test]
    fn test_catalog_relations() {
        let mut catalog = SchemaCatalog::new();

        catalog
            .register_persistent(
                RelationSchema::new("A").with_column(ColumnSchema::new("x", SchemaType::Int)),
            )
            .unwrap();
        catalog
            .register_session(
                RelationSchema::new("B").with_column(ColumnSchema::new("y", SchemaType::Int)),
            )
            .unwrap();

        let mut relations = catalog.relations();
        relations.sort();
        assert_eq!(relations, vec!["A", "B"]);
    }

    #[test]
    fn test_catalog_clear() {
        let mut catalog = SchemaCatalog::new();

        catalog
            .register_persistent(
                RelationSchema::new("A").with_column(ColumnSchema::new("x", SchemaType::Int)),
            )
            .unwrap();
        catalog
            .register_session(
                RelationSchema::new("B").with_column(ColumnSchema::new("y", SchemaType::Int)),
            )
            .unwrap();

        assert_eq!(catalog.len(), 2);
        catalog.clear();
        assert_eq!(catalog.len(), 0);
    }
}
