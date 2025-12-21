//! # Schema Validation Engine
//!
//! Validates tuples against schema definitions with support for:
//! - Type checking
//! - Column-level constraints (@not_empty, @range, @pattern)
//! - All-or-nothing batch semantics
//! - Violation reporting

use std::collections::HashSet;
use regex::Regex;
use crate::value::{Tuple, Value};
use super::{
    RelationSchema, ColumnAnnotation, SchemaType, FailureAction,
    catalog::SchemaCatalog,
};

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
    /// Column is NULL or empty when @not_empty
    EmptyValue,
    /// Numeric value outside @range bounds
    RangeViolation,
    /// String doesn't match @pattern regex
    PatternViolation,
    /// Duplicate value in @unique column
    UniqueViolation,
    /// Duplicate primary key
    PrimaryKeyViolation,
    /// Referenced value doesn't exist (@foreign_key)
    ForeignKeyViolation,
    /// @check constraint failed
    CheckViolation,
}

impl std::fmt::Display for ViolationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViolationType::ArityMismatch => write!(f, "ARITY_MISMATCH"),
            ViolationType::TypeMismatch => write!(f, "TYPE_MISMATCH"),
            ViolationType::EmptyValue => write!(f, "NOT_EMPTY_VIOLATION"),
            ViolationType::RangeViolation => write!(f, "RANGE_VIOLATION"),
            ViolationType::PatternViolation => write!(f, "PATTERN_VIOLATION"),
            ViolationType::UniqueViolation => write!(f, "UNIQUE_VIOLATION"),
            ViolationType::PrimaryKeyViolation => write!(f, "PRIMARY_KEY_VIOLATION"),
            ViolationType::ForeignKeyViolation => write!(f, "FOREIGN_KEY_VIOLATION"),
            ViolationType::CheckViolation => write!(f, "CHECK_VIOLATION"),
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
    #[error("Insert rejected for '{relation}': batch of {total_tuples} tuples violated constraints")]
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
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

/// Validation engine for checking tuples against schemas
pub struct ValidationEngine {
    /// Compiled regex patterns (cached for performance)
    compiled_patterns: std::collections::HashMap<String, Regex>,
}

impl ValidationEngine {
    /// Create a new validation engine
    pub fn new() -> Self {
        ValidationEngine {
            compiled_patterns: std::collections::HashMap::new(),
        }
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
                format!(
                    "Expected {} columns, got {}",
                    schema.arity(),
                    tuple.arity()
                ),
            ));
            // If arity is wrong, skip column-level validation
            return Err(violations);
        }

        // Validate each column
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
                    continue; // Skip other checks for this column
                }

                // Check column annotations
                for violation in self.check_annotations(
                    col_schema,
                    value,
                    tuple_index,
                    tuple,
                ) {
                    violations.push(violation);
                }
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Check column annotations against a value
    fn check_annotations(
        &mut self,
        col_schema: &super::ColumnSchema,
        value: &Value,
        tuple_index: usize,
        tuple: &Tuple,
    ) -> Vec<Violation> {
        let mut violations = Vec::new();

        for annotation in &col_schema.annotations {
            match annotation {
                ColumnAnnotation::NotEmpty => {
                    if self.is_empty_value(value) {
                        violations.push(Violation::new(
                            tuple_index,
                            tuple.clone(),
                            Some(col_schema.name.clone()),
                            ViolationType::EmptyValue,
                            "Value cannot be empty or null",
                        ));
                    }
                }
                ColumnAnnotation::Range { min, max } => {
                    if let Some(v) = self.value_to_i64(value) {
                        if v < *min || v > *max {
                            violations.push(Violation::new(
                                tuple_index,
                                tuple.clone(),
                                Some(col_schema.name.clone()),
                                ViolationType::RangeViolation,
                                format!("Value {} not in range [{}, {}]", v, min, max),
                            ));
                        }
                    }
                }
                ColumnAnnotation::Pattern { regex } => {
                    if let Some(s) = value.as_str() {
                        let re = self.get_or_compile_regex(regex);
                        if let Some(re) = re {
                            if !re.is_match(s) {
                                violations.push(Violation::new(
                                    tuple_index,
                                    tuple.clone(),
                                    Some(col_schema.name.clone()),
                                    ViolationType::PatternViolation,
                                    format!("Value '{}' doesn't match pattern '{}'", s, regex),
                                ));
                            }
                        }
                    }
                }
                // Primary, Unique, ForeignKey require cross-tuple checks
                // These are handled separately in validate_batch_with_existing_data
                _ => {}
            }
        }

        violations
    }

    /// Check if a value is "empty" (NULL or empty string)
    fn is_empty_value(&self, value: &Value) -> bool {
        match value {
            Value::Null => true,
            Value::String(s) => s.is_empty(),
            _ => false,
        }
    }

    /// Convert a value to i64 for range checks
    fn value_to_i64(&self, value: &Value) -> Option<i64> {
        match value {
            Value::Int32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            Value::Float64(v) => Some(*v as i64),
            _ => None,
        }
    }

    /// Get or compile a regex pattern
    fn get_or_compile_regex(&mut self, pattern: &str) -> Option<&Regex> {
        if !self.compiled_patterns.contains_key(pattern) {
            if let Ok(re) = Regex::new(pattern) {
                self.compiled_patterns.insert(pattern.to_string(), re);
            }
        }
        self.compiled_patterns.get(pattern)
    }

    /// Validate with uniqueness checks (requires existing data)
    pub fn validate_batch_with_uniqueness(
        &mut self,
        schema: &RelationSchema,
        tuples: &[Tuple],
        existing_data: &[Tuple],
    ) -> Result<ValidationResult, ValidationError> {
        // First do basic validation
        let mut violations = match self.validate_batch(schema, tuples) {
            Ok(_) => Vec::new(),
            Err(ValidationError::BatchRejected { violations, .. }) => violations,
            Err(e) => return Err(e),
        };

        // Check primary key uniqueness
        let pk_indices = schema.primary_key_indices();
        if !pk_indices.is_empty() {
            let pk_violations = self.check_primary_key_uniqueness(
                schema,
                tuples,
                existing_data,
                &pk_indices,
            );
            violations.extend(pk_violations);
        }

        // Check @unique columns
        for (col_idx, col_schema) in schema.columns.iter().enumerate() {
            if col_schema.is_unique() {
                let unique_violations = self.check_unique_column(
                    schema,
                    tuples,
                    existing_data,
                    col_idx,
                );
                violations.extend(unique_violations);
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

    /// Check primary key uniqueness
    fn check_primary_key_uniqueness(
        &self,
        schema: &RelationSchema,
        tuples: &[Tuple],
        existing_data: &[Tuple],
        pk_indices: &[usize],
    ) -> Vec<Violation> {
        let mut violations = Vec::new();
        let mut seen_keys: HashSet<Vec<Value>> = HashSet::new();

        // Add existing primary keys
        for existing in existing_data {
            let key: Vec<Value> = pk_indices.iter().filter_map(|&i| existing.get(i).cloned()).collect();
            seen_keys.insert(key);
        }

        // Check new tuples
        for (idx, tuple) in tuples.iter().enumerate() {
            let key: Vec<Value> = pk_indices.iter().filter_map(|&i| tuple.get(i).cloned()).collect();
            if !seen_keys.insert(key.clone()) {
                let pk_names: Vec<_> = pk_indices
                    .iter()
                    .filter_map(|&i| schema.columns.get(i))
                    .map(|c| c.name.as_str())
                    .collect();
                violations.push(Violation::new(
                    idx,
                    tuple.clone(),
                    Some(pk_names.join(", ")),
                    ViolationType::PrimaryKeyViolation,
                    format!("Duplicate primary key: {:?}", key),
                ));
            }
        }

        violations
    }

    /// Check @unique column
    fn check_unique_column(
        &self,
        schema: &RelationSchema,
        tuples: &[Tuple],
        existing_data: &[Tuple],
        col_idx: usize,
    ) -> Vec<Violation> {
        let mut violations = Vec::new();
        let mut seen_values: HashSet<Value> = HashSet::new();
        let col_name = schema.columns.get(col_idx).map(|c| c.name.clone());

        // Add existing values
        for existing in existing_data {
            if let Some(value) = existing.get(col_idx) {
                seen_values.insert(value.clone());
            }
        }

        // Check new tuples
        for (idx, tuple) in tuples.iter().enumerate() {
            if let Some(value) = tuple.get(col_idx) {
                if !seen_values.insert(value.clone()) {
                    violations.push(Violation::new(
                        idx,
                        tuple.clone(),
                        col_name.clone(),
                        ViolationType::UniqueViolation,
                        format!("Duplicate value: {}", value),
                    ));
                }
            }
        }

        violations
    }

    /// Validate with foreign key checks (requires access to referenced data)
    pub fn validate_foreign_key(
        &self,
        schema: &RelationSchema,
        tuples: &[Tuple],
        col_idx: usize,
        referenced_values: &HashSet<Value>,
    ) -> Vec<Violation> {
        let mut violations = Vec::new();
        let col_name = schema.columns.get(col_idx).map(|c| c.name.clone());

        for (idx, tuple) in tuples.iter().enumerate() {
            if let Some(value) = tuple.get(col_idx) {
                if !value.is_null() && !referenced_values.contains(value) {
                    violations.push(Violation::new(
                        idx,
                        tuple.clone(),
                        col_name.clone(),
                        ViolationType::ForeignKeyViolation,
                        format!("Referenced value not found: {}", value),
                    ));
                }
            }
        }

        violations
    }
}

impl Default for ValidationEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, SchemaType};
    use crate::value::Value;

    fn make_user_schema() -> RelationSchema {
        RelationSchema::new("User")
            .with_column(
                ColumnSchema::new("id", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Primary)
                    .with_annotation(ColumnAnnotation::NotEmpty),
            )
            .with_column(ColumnSchema::new("name", SchemaType::Symbol))
            .with_column(
                ColumnSchema::new("age", SchemaType::Int)
                    .with_annotation(ColumnAnnotation::Range { min: 0, max: 120 }),
            )
            .with_column(
                ColumnSchema::new("email", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Pattern {
                        regex: r"^[^@]+@[^@]+\.[^@]+$".to_string(),
                    }),
            )
    }

    #[test]
    fn test_validate_valid_tuple() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
            Value::Int64(30),
            Value::string("alice@example.com"),
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_arity_mismatch() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
        ]); // Missing columns

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert_eq!(violations[0].violation_type, ViolationType::ArityMismatch);
        }
    }

    #[test]
    fn test_validate_type_mismatch() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
            Value::string("not a number"), // Wrong type
            Value::string("alice@example.com"),
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert!(violations.iter().any(|v| v.violation_type == ViolationType::TypeMismatch));
        }
    }

    #[test]
    fn test_validate_not_empty() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string(""), // Empty primary key
            Value::string("Alice"),
            Value::Int64(30),
            Value::string("alice@example.com"),
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert!(violations.iter().any(|v| v.violation_type == ViolationType::EmptyValue));
        }
    }

    #[test]
    fn test_validate_range() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
            Value::Int64(150), // Out of range
            Value::string("alice@example.com"),
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert!(violations.iter().any(|v| v.violation_type == ViolationType::RangeViolation));
        }
    }

    #[test]
    fn test_validate_pattern() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuple = Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
            Value::Int64(30),
            Value::string("invalid-email"), // Doesn't match pattern
        ]);

        let result = engine.validate_batch(&schema, &[tuple]);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert!(violations.iter().any(|v| v.violation_type == ViolationType::PatternViolation));
        }
    }

    #[test]
    fn test_validate_primary_key_uniqueness() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let existing = vec![Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
            Value::Int64(30),
            Value::string("alice@example.com"),
        ])];

        let new_tuples = vec![Tuple::new(vec![
            Value::string("u1"), // Duplicate PK
            Value::string("Bob"),
            Value::Int64(25),
            Value::string("bob@example.com"),
        ])];

        let result = engine.validate_batch_with_uniqueness(&schema, &new_tuples, &existing);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert!(violations.iter().any(|v| v.violation_type == ViolationType::PrimaryKeyViolation));
        }
    }

    #[test]
    fn test_validate_multiple_violations() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        // Tuple 1: valid
        let t1 = Tuple::new(vec![
            Value::string("u1"),
            Value::string("Alice"),
            Value::Int64(30),
            Value::string("alice@example.com"),
        ]);

        // Tuple 2: multiple violations
        let t2 = Tuple::new(vec![
            Value::string(""), // Empty PK
            Value::string("Bob"),
            Value::Int64(-5), // Negative age
            Value::string("bad"), // Bad email
        ]);

        let result = engine.validate_batch(&schema, &[t1, t2]);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            // Should have multiple violations for t2
            assert!(violations.len() >= 3);
            assert!(violations.iter().all(|v| v.tuple_index == 1));
        }
    }

    #[test]
    fn test_batch_all_or_nothing() {
        let schema = make_user_schema();
        let mut engine = ValidationEngine::new();

        let tuples = vec![
            Tuple::new(vec![
                Value::string("u1"),
                Value::string("Alice"),
                Value::Int64(30),
                Value::string("alice@example.com"),
            ]),
            Tuple::new(vec![
                Value::string("u2"),
                Value::string("Bob"),
                Value::Int64(200), // Invalid age - rejects entire batch
                Value::string("bob@example.com"),
            ]),
        ];

        let result = engine.validate_batch(&schema, &tuples);
        assert!(result.is_err());

        // Both tuples should be rejected (all-or-nothing)
        if let Err(ValidationError::BatchRejected { total_tuples, .. }) = result {
            assert_eq!(total_tuples, 2);
        }
    }

    #[test]
    fn test_unique_column() {
        let schema = RelationSchema::new("Data")
            .with_column(
                ColumnSchema::new("id", SchemaType::Int)
                    .with_annotation(ColumnAnnotation::Primary),
            )
            .with_column(
                ColumnSchema::new("email", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::Unique),
            );

        let mut engine = ValidationEngine::new();

        let existing = vec![Tuple::new(vec![
            Value::Int64(1),
            Value::string("alice@example.com"),
        ])];

        let new_tuples = vec![Tuple::new(vec![
            Value::Int64(2),
            Value::string("alice@example.com"), // Duplicate email
        ])];

        let result = engine.validate_batch_with_uniqueness(&schema, &new_tuples, &existing);
        assert!(result.is_err());
        if let Err(ValidationError::BatchRejected { violations, .. }) = result {
            assert!(violations.iter().any(|v| v.violation_type == ViolationType::UniqueViolation));
        }
    }

    #[test]
    fn test_foreign_key_validation() {
        let schema = RelationSchema::new("Order")
            .with_column(ColumnSchema::new("id", SchemaType::Int))
            .with_column(
                ColumnSchema::new("user_id", SchemaType::Symbol)
                    .with_annotation(ColumnAnnotation::ForeignKey {
                        relation: "User".to_string(),
                        column: "id".to_string(),
                    }),
            );

        let engine = ValidationEngine::new();

        let mut referenced_values = HashSet::new();
        referenced_values.insert(Value::string("u1"));
        referenced_values.insert(Value::string("u2"));

        let tuples = vec![
            Tuple::new(vec![Value::Int64(1), Value::string("u1")]), // Valid
            Tuple::new(vec![Value::Int64(2), Value::string("u99")]), // Invalid
        ];

        let violations = engine.validate_foreign_key(&schema, &tuples, 1, &referenced_values);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].tuple_index, 1);
        assert_eq!(violations[0].violation_type, ViolationType::ForeignKeyViolation);
    }
}
