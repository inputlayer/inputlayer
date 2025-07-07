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
    Internal(String.clone()),
}

/// Result of successful validation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Number of tuples validated
    pub validated_count: usize,
    /// Warnings (non-fatal issues)
    pub warnings: Vec<String>,
}

