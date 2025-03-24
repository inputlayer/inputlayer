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

impl DependencyGraph {
    /// Create a new empty dependency graph
    pub fn new() -> Self {
        DependencyGraph {
            edges: HashMap::new(),
            relations: HashSet::new(),
        }
    }

    /// Add a dependency edge
    pub fn add_edge(&mut self, from: &str, to: &str, dep_type: DependencyType) {
        self.relations.insert(from.to_string());
        self.relations.insert(to.to_string());
        self.edges
            .entry(from.to_string())
            .or_default()
            .push((to.to_string(), dep_type));
    }

    /// Get positive dependencies for a relation
    pub fn positive_deps(&self, relation: &str) -> Vec<&str> {
        self.edges
            .get(relation)
            .map(|deps| {
                deps.iter()
                    .filter(|(_, t)| *t == DependencyType::Positive)
                    .map(|(r, _)| r.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get negative dependencies for a relation
    pub fn negative_deps(&self, relation: &str) -> Vec<&str> {
        self.edges
            .get(relation)
            .map(|deps| {
                deps.iter()
                    .filter(|(_, t)| *t == DependencyType::Negative)
                    .map(|(r, _)| r.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if a relation has any negative dependencies
    pub fn has_negative_deps(&self, relation: &str) -> bool {
        !self.negative_deps(relation).is_empty()
    }

    /// Convert to simple graph format for SCC detection (all edges, ignoring type)
    /// This is needed because cycles can form through BOTH positive and negative edges
    pub fn to_simple_graph(&self) -> HashMap<String, HashSet<String>> {
        let mut simple = HashMap::new();
        for (from, edges) in &self.edges {
            let deps: HashSet<String> = edges.iter().map(|(to, _)| to.clone()).collect();
            simple.insert(from.clone(), deps);
        }
        // Ensure all relations are in the graph even if they have no outgoing edges
        for rel in &self.relations {
            simple.entry(rel.clone()).or_insert_with(HashSet::new);
        }
        simple
    }

    /// Check if there's a negative edge within an SCC
    /// Returns the first negative edge found within the SCC, if any
    /// NOTE: Sorted for deterministic error messages
    pub fn has_negative_edge_in_scc(&self, scc: &[String]) -> Option<(String, String)> {
        let scc_set: HashSet<&String> = scc.iter().collect();
        // Sort for deterministic iteration order
        let mut sorted_scc: Vec<&String> = scc.iter().collect();
        sorted_scc.sort();
        for from in sorted_scc {
            if let Some(edges) = self.edges.get(from) {
                // Sort edges by target for deterministic order
                let mut sorted_edges: Vec<_> = edges.iter().collect();
                sorted_edges.sort_by(|a, b| a.0.cmp(&b.0));
                for (to, dep_type) in sorted_edges {
                    if *dep_type == DependencyType::Negative && scc_set.contains(to) {
                        return Some((from.clone(), to.clone()));
                    }
                }
            }
        }
        None
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

// Recursion Detection
/// Check if a single rule is recursive
///
/// A rule is recursive if its head relation appears in any positive body atom.
pub fn is_recursive_rule(rule: &Rule) -> bool {
    let head_relation = &rule.head.relation;

    for pred in &rule.body {
        if let BodyPredicate::Positive(atom) = pred {
            if &atom.relation == head_relation {
                return true;
            }
        }
    }

    false
}

/// Check if a program contains any recursive rules
pub fn has_recursion(program: &Program) -> bool {
    program.rules.iter().any(is_recursive_rule)
}

/// Build extended relation dependency graph with positive/negative edges
///
/// Returns: `DependencyGraph` with typed edges for stratification
pub fn build_extended_dependency_graph(program: &Program) -> DependencyGraph {
    let mut graph = DependencyGraph::new();

    for rule in &program.rules {
        let head_relation = &rule.head.relation;
        graph.relations.insert(head_relation.clone());

        for pred in &rule.body {
            match pred {
                BodyPredicate::Positive(atom) => {
                    graph.add_edge(head_relation, &atom.relation, DependencyType::Positive);
                }
                BodyPredicate::Negated(atom) => {
                    graph.add_edge(head_relation, &atom.relation, DependencyType::Negative);
                }
                BodyPredicate::Comparison(_, _, _) => {
                    // Comparisons don't add relation dependencies
                }
                BodyPredicate::HnswNearest { .. } => {
                    // HNSW nearest neighbor search doesn't add relation dependencies
                    // (it queries an index, not a relation)
                }
            }
        }
    }

    graph
}

/// Build relation dependency graph (positive dependencies only)
///
/// Returns: Map from relation name to set of relations it depends on
///
/// Note: For stratified negation, use `build_extended_dependency_graph` instead.
