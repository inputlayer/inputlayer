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
    pub fn register_or_update_session(
        &mut self,
        schema: RelationSchema,
    ) -> Result<(), SchemaError> {
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
