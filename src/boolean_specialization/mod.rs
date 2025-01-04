//! # Boolean Specialization
//!
//! Selects the minimal semiring for each query: Boolean (set semantics, 1 byte)
//! when only existence matters, Counting (bag semantics, 8 bytes) when duplicates
//! or counts are needed, Min/Max for recursive min/max aggregation.
//!
//! Walks the IR tree, propagates constraints upward, and annotates each node.
//! For Boolean semiring, wraps Join/JoinFlatMap in Distinct to enforce set semantics.
//!
//! ```text
//! IRNode -> [Boolean Spec] -> Annotated IRNode -> Code Gen (with semiring info)
//! ```

use crate::ir::{IRNode, Predicate};
use std::collections::{HashMap, HashSet};

/// Semiring type for query execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SemiringType {
    /// Set semantics (presence only)
    #[default]
    Boolean,

    /// Bag semantics (tracks multiplicities)
    Counting,

    /// Finds minimum values (e.g. shortest path)
    Min,

    /// Finds maximum values (e.g. widest path)
    Max,
}

impl SemiringType {
    /// Check if this semiring is more general than another
    /// More general means it can express everything the other can
    pub fn is_more_general_than(&self, other: &SemiringType) -> bool {
        match (self, other) {
            // Everything is more general than Boolean
            (SemiringType::Counting, SemiringType::Boolean) => true,
            // Same type is equal, not more general
            (a, b) if a == b => false,
            // Default: not more general
            _ => false,
        }
    }

    /// Get the minimal semiring that satisfies both constraints
    pub fn meet(&self, other: &SemiringType) -> SemiringType {
        if self == other {
            return *self;
        }

        // If one is boolean and other is counting, need counting
        match (self, other) {
            (SemiringType::Boolean, SemiringType::Counting)
            | (SemiringType::Counting, SemiringType::Boolean) => SemiringType::Counting,
            // Min/Max don't combine well - default to counting
            (SemiringType::Min, _) | (_, SemiringType::Min) => SemiringType::Min,
            (SemiringType::Max, _) | (_, SemiringType::Max) => SemiringType::Max,
            _ => SemiringType::Counting,
        }
    }
}

/// Analysis result for a single IR node
#[derive(Debug, Clone)]
pub struct SemiringAnnotation {
    /// The selected semiring for this node
    pub semiring: SemiringType,
    /// Whether this node requires duplicate tracking
    pub needs_duplicates: bool,
    /// Whether this node is part of a recursive computation
    pub is_recursive: bool,
    /// Reason for semiring selection (for debugging)
    pub reason: String,
}

impl Default for SemiringAnnotation {
    fn default() -> Self {
        SemiringAnnotation {
            semiring: SemiringType::Boolean,
            needs_duplicates: false,
            is_recursive: false,
            reason: "default".to_string(),
        }
    }
}

/// Statistics about boolean specialization
#[derive(Debug, Clone, Default)]
