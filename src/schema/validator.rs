//! # Schema Validation Engine
//!
//! Validates tuples against schema definitions with support for:
//! - Type checking
//! - Arity checking (correct number of columns)
//! - All-or-nothing batch semantics
//! - Violation reporting

use super::RelationSchema;
use crate::value::Tuple;

/// Represents a validation violation
#[derive(Debug, Clone)]
pub struct Violation {
    /// Index of the tuple in the batch that violated
    pub tuple_index: usize,
    /// The violating tuple
    pub tuple: Tuple,
    /// Column that failed validation (if applicable)
    pub column: Option<String>,
    /// Type of violation
    pub violation_type: ViolationType,
    /// Human-readable message
    pub message: String,
}

impl Violation {
    /// Create a new violation
    pub fn new(
        tuple_index: usize,
        tuple: Tuple,
        column: Option<String>,
        violation_type: ViolationType,
        message: impl Into<String>,
    ) -> Self {
        Violation {
            tuple_index,
            tuple,
            column,
            violation_type,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.column {
            Some(col) => write!(
                f,
                "Tuple #{}: column '{}' - {} ({})",
                self.tuple_index, col, self.violation_type, self.message
            ),
            None => write!(
                f,
                "Tuple #{}: {} ({})",
                self.tuple_index, self.violation_type, self.message
            ),
        }
    }
}

/// Types of validation violations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViolationType {
    /// Wrong number of columns
    ArityMismatch,
    /// Column value has wrong type
    TypeMismatch,
}

impl std::fmt::Display for ViolationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViolationType::ArityMismatch => write!(f, "ARITY_MISMATCH"),
            ViolationType::TypeMismatch => write!(f, "TYPE_MISMATCH"),
        }
    }
}

/// Validation error for batch operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum ValidationError {
    /// No schema found for the relation
    #[error("No schema defined for relation '{0}'")]
    NoSchema(String),
    /// All-or-nothing: batch rejected due to violations
    #[error(
        "Insert rejected for '{relation}': batch of {total_tuples} tuples had type/arity errors"
    )]
    BatchRejected {
        relation: String,
        total_tuples: usize,
        violations: Vec<Violation>,
    },
    /// Internal error
    #[error("Internal validation error: {0}")]
    Internal(String),
}

/// Result of successful validation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Number of tuples validated
    pub validated_count: usize,
    /// Warnings (non-fatal issues)
    pub warnings: Vec<String>,
}

impl ValidationResult {
    /// Create a successful result
    pub fn success(count: usize) -> Self {
        ValidationResult {
            validated_count: count,
            warnings: Vec::new(),
        }
    }

    /// Add a warning
    #[allow(dead_code)]
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

/// Validation engine for checking tuples against schemas
#[derive(Default)]
pub struct ValidationEngine;

impl ValidationEngine {
    /// Create a new validation engine
    pub fn new() -> Self {
        ValidationEngine
    }

    /// Validate a batch of tuples against a schema
    /// Returns Ok if all tuples pass, Err with all violations if any fail
    pub fn validate_batch(
        &mut self,
        schema: &RelationSchema,
        tuples: &[Tuple],
    ) -> Result<ValidationResult, ValidationError> {
        let mut violations = Vec::new();

        for (idx, tuple) in tuples.iter().enumerate() {
            // Collect all violations for this tuple
            if let Err(mut tuple_violations) = self.validate_tuple(schema, tuple, idx) {
                violations.append(&mut tuple_violations);
            }
        }

        if violations.is_empty() {
            Ok(ValidationResult::success(tuples.len()))
        } else {
            Err(ValidationError::BatchRejected {
                relation: schema.name.clone(),
                total_tuples: tuples.len(),
                violations,
            })
        }
    }

    /// Validate a single tuple against a schema
    /// Returns Ok(()) if valid, Err with violations if invalid
    pub fn validate_tuple(
        &mut self,
        schema: &RelationSchema,
        tuple: &Tuple,
        tuple_index: usize,
    ) -> Result<(), Vec<Violation>> {
        let mut violations = Vec::new();

        // Check arity
        if tuple.arity() != schema.arity() {
            violations.push(Violation::new(
                tuple_index,
                tuple.clone(),
                None,
                ViolationType::ArityMismatch,
                format!("Expected {} columns, got {}", schema.arity(), tuple.arity()),
            ));
            // If arity is wrong, skip column-level validation
            return Err(violations);
        }

        // Validate each column's type
        for (col_idx, col_schema) in schema.columns.iter().enumerate() {
            if let Some(value) = tuple.get(col_idx) {
                // Type check
                if !col_schema.data_type.matches(value) {
                    violations.push(Violation::new(
                        tuple_index,
                        tuple.clone(),
                        Some(col_schema.name.clone()),
                        ViolationType::TypeMismatch,
                        format!(
                            "Expected type '{}', got '{:?}'",
                            col_schema.data_type,
                            value.data_type()
                        ),
                    ));
                }
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Validate with existing data (for data-first schema registration)
    /// This validates existing tuples when a schema is registered after data exists
    #[allow(unused_variables)]
    pub fn validate_existing_data(
        &mut self,
        schema: &RelationSchema,
        existing_data: &[Tuple],
    ) -> Result<ValidationResult, ValidationError> {
        self.validate_batch(schema, existing_data)
    }
}

