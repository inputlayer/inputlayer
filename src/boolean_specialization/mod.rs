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

