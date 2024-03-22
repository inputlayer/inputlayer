//! Catalog: Schema management for relations
//!
//! Tracks schemas (column names and types) for all relations in the database.
//! Uses `TupleSchema` as the single source of truth for schema information.
//! Column names can be derived from `TupleSchema.field_names()`.

use crate::value::{DataType, SchemaValidationError, Tuple, TupleSchema};
use std::collections::HashMap;

/// Catalog tracks schemas for all relations
#[derive(Debug, Clone)]
pub struct Catalog {
    /// Map from relation name to typed schema (single source of truth)
    schemas: HashMap<String, TupleSchema>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Catalog {
            schemas: HashMap::new(),
        }
    }

    /// Register a relation with its schema (column names only)
    /// For backward compatibility - types will default to Int32
    pub fn register_relation(&mut self, relation: String, schema: Vec<String>) {
        let typed_schema = TupleSchema::from_names(schema);
        self.schemas.insert(relation, typed_schema);
    }

    /// Register a relation with a fully typed schema
    pub fn register_typed_relation(&mut self, relation: String, typed_schema: TupleSchema) {
        self.schemas.insert(relation, typed_schema);
    }

    /// Get schema for a relation (column names only)
    /// Returns a Vec since the names are derived from `TupleSchema`
    pub fn get_schema(&self, relation: &str) -> Option<Vec<String>> {
        self.schemas
            .get(relation)
            .map(|s| s.field_names().iter().map(|n| (*n).to_string()).collect())
    }

    /// Get typed schema for a relation
    pub fn get_typed_schema(&self, relation: &str) -> Option<&TupleSchema> {
        self.schemas.get(relation)
    }

    /// Check if a relation exists
    pub fn has_relation(&self, relation: &str) -> bool {
        self.schemas.contains_key(relation)
    }

    /// Get all registered relations
    pub fn all_relations(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Find position of a variable in a schema
    pub fn find_variable_position(&self, relation: &str, var: &str) -> Option<usize> {
        let schema = self.schemas.get(relation)?;
        schema.field_names().iter().position(|v| *v == var)
    }

    /// Get the type of a column in a relation
    pub fn get_column_type(&self, relation: &str, column: &str) -> Option<&DataType> {
        let schema = self.schemas.get(relation)?;
        let idx = schema.field_index(column)?;
        schema.field_type(idx)
    }

    /// Validate a tuple against a relation's schema
    pub fn validate_tuple(
        &self,
        relation: &str,
        tuple: &Tuple,
    ) -> Result<(), SchemaValidationError> {
        if let Some(schema) = self.schemas.get(relation) {
            schema.validate(tuple)
        } else {
            // No schema registered - allow any tuple
            Ok(())
        }
    }

    /// Validate multiple tuples against a relation's schema
    pub fn validate_tuples(
        &self,
        relation: &str,
        tuples: &[Tuple],
    ) -> Result<(), SchemaValidationError> {
        if let Some(schema) = self.schemas.get(relation) {
            for tuple in tuples {
                schema.validate(tuple)?;
            }
        }
        Ok(())
    }

    /// Infer and update schema types from actual data
    /// Useful when schema was created with default types
    pub fn infer_types_from_tuples(&mut self, relation: &str, tuples: &[Tuple]) {
        if tuples.is_empty() {
            return;
        }

        // Infer types from first tuple
        let first = &tuples[0];
        let fields: Vec<(String, DataType)> = if let Some(existing) = self.schemas.get(relation) {
            // Use existing field names, update types
            existing
                .field_names()
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    let dtype = first
                        .get(i)
                        .map_or(DataType::Null, super::value::Value::data_type);
                    ((*name).to_string(), dtype)
                })
                .collect()
        } else {
            // No schema - create anonymous column names
            (0..first.arity())
                .map(|i| {
                    let dtype = first
                        .get(i)
                        .map_or(DataType::Null, super::value::Value::data_type);
                    (format!("col{i}"), dtype)
                })
                .collect()
        };

        let typed_schema = TupleSchema::new(fields);
        self.schemas.insert(relation.to_string(), typed_schema);
    }

    /// Infer join keys between two relations based on shared variables
    pub fn infer_join_keys(
        &self,
        left_schema: &[String],
        right_schema: &[String],
        shared_vars: &[String],
    ) -> (Vec<usize>, Vec<usize>) {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();

        for var in shared_vars {
            if let Some(left_pos) = left_schema.iter().position(|v| v == var) {
                if let Some(right_pos) = right_schema.iter().position(|v| v == var) {
                    left_keys.push(left_pos);
                    right_keys.push(right_pos);
                }
            }
        }

        (left_keys, right_keys)
    }

    /// Clear all registered schemas
    pub fn clear(&mut self) {
        self.schemas.clear();
    }

    /// Remove a specific relation from the catalog
    pub fn unregister_relation(&mut self, relation: &str) {
        self.schemas.remove(relation);
    }
}

