//! # Recursion Support
//!
//! Recursion detection, dependency graphs, SCC detection (Tarjan's), and stratification
//! for Datalog programs. Handles both positive recursion and stratified negation.
//!
//! A rule is recursive if its head relation appears in its body:
//! ```datalog
//! tc(x, z) :- tc(x, y), edge(y, z).
//! ```
//!
//! Stratification groups rules into evaluation layers so that negated relations
//! are fully computed before rules that negate them can execute.
//!
use crate::ast::{BodyPredicate, Program, Rule};
use std::collections::{HashMap, HashSet};

// Dependency Types for Stratification
/// Type of dependency between relations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyType {
    /// Positive dependency: head depends on relation via positive atom
    /// Can be in same stratum or higher
    Positive,
    /// Negative dependency: head depends on relation via negated atom
    /// Negated relation MUST be in strictly lower stratum
    Negative,
}

/// Extended dependency graph with positive/negative edges
///
/// This is essential for stratified negation:
/// - Positive edges: A -> B means A depends on B (can be same or higher stratum)
/// - Negative edges: A -/-> B means A negates B (B must be in lower stratum)
#[derive(Debug, Clone)]
pub struct DependencyGraph {
    /// Map from relation to its dependencies with types
    pub edges: HashMap<String, Vec<(String, DependencyType)>>,
    /// All relations in the graph
    pub relations: HashSet<String>,
}

