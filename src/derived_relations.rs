//! Derived Relations Manager
//!
//! Manages materialized derived relations from persistent rules.
//! Enables HNSW indexing on rule outputs by maintaining persistent
//! materialized views that update incrementally when base data changes.
//!
//! ## Architecture
//!
//! ```text
//! Base Relations (DDComputation)
//!        |
//!        |--- edge(u, v)
//!        |--- embeddings(id, vec)
//!        `--- ...
//!              |
//!              ▼
//!     DerivedRelationsManager
//!        |
//!        |--- CompiledRule (IR + metadata)
//!        |         |
//!        |         ▼
//!        |--- MaterializedRelation (cached tuples + validity)
//!        |         |
//!        |         ▼
//!        `--- HNSW Index (future: on vector columns)
//! ```
//!
//! ## Key Concepts
//!
//! - CompiledRule: Parsed rule stored as IR, ready for execution
//! - MaterializedRelation: Cached rule output, invalidated on base changes
//! - Dependency Tracking: Maps base relations -> dependent derived relations
//!
//! ## Persistent vs Session Rules
//!
//! - Persistent Rules: Materialized here, results cached across queries
//! - Session Rules: NOT materialized, but CAN read from materialized persistent rules

use crate::ast::Rule;
use crate::ir::IRNode;
use crate::value::Tuple;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// Counter for materialization versions
static MATERIALIZATION_VERSION: AtomicU64 = AtomicU64::new(0);

/// A compiled rule ready for execution
#[derive(Debug, Clone)]
pub struct CompiledRule {
    /// Original rule name (head relation)
    pub name: String,

    /// All clauses of this rule compiled to IR
    /// Multiple clauses for recursive rules: path(X,Y) <- edge(X,Y). path(X,Z) <- path(X,Y), edge(Y,Z).
    pub clauses: Vec<CompiledClause>,

    /// Base relations this rule depends on (transitive)
    pub dependencies: HashSet<String>,

    /// Whether this rule is recursive (references itself in body)
    pub is_recursive: bool,

    /// Output schema (column names from head)
    pub output_schema: Vec<String>,

    /// Stratum level for stratified execution (higher = later)
    pub stratum: usize,
}

/// A single clause of a rule compiled to IR
#[derive(Debug, Clone)]
pub struct CompiledClause {
    /// The compiled IR tree for this clause
    pub ir: IRNode,

    /// Original AST rule for reference
    pub rule: Rule,

    /// Relations scanned by this clause (direct, not transitive)
    pub scanned_relations: HashSet<String>,
}

/// Materialized relation data with validity tracking
#[derive(Debug, Clone)]
pub struct MaterializedRelation {
    /// Cached tuple data
    pub tuples: Vec<Tuple>,

    /// Version when this was last materialized
    pub version: u64,

    /// Base data versions this materialization is based on
    /// Maps base_relation -> version_when_materialized
    pub base_versions: HashMap<String, u64>,

    /// Whether the materialization is currently valid
    pub valid: bool,

    /// Timestamp when materialized (for diagnostics)
    pub materialized_at: u64,
}

impl MaterializedRelation {
    /// Create a new materialized relation
    pub fn new(tuples: Vec<Tuple>, base_versions: HashMap<String, u64>) -> Self {
        let version = MATERIALIZATION_VERSION.fetch_add(1, Ordering::SeqCst);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);

        Self {
            tuples,
            version,
            base_versions,
            valid: true,
            materialized_at: now,
        }
    }

    /// Mark this materialization as invalid (needs recomputation)
    pub fn invalidate(&mut self) {
        self.valid = false;
    }

    /// Check if this materialization is still valid given current base versions
    pub fn is_valid_for(&self, current_base_versions: &HashMap<String, u64>) -> bool {
        if !self.valid {
            return false;
        }

        // Check if any base relation we depend on has been updated
        for (rel, our_version) in &self.base_versions {
            if let Some(current_version) = current_base_versions.get(rel) {
                if current_version > our_version {
                    return false;
                }
            }
        }

        true
    }
}

/// Manages derived relations from persistent rules
///
/// This is the core structure for rule materialization.
/// It tracks compiled rules, their dependencies, and cached results.
#[derive(Debug)]
pub struct DerivedRelationsManager {
    /// Compiled rules indexed by output relation name
    compiled_rules: HashMap<String, CompiledRule>,

    /// Materialized data for each derived relation
    materialized: HashMap<String, MaterializedRelation>,

    /// Forward dependency map: base_relation -> [derived_relations that depend on it]
    /// Used for invalidation when base data changes
    base_to_derived: HashMap<String, HashSet<String>>,

    /// Reverse dependency map: derived_relation -> [base_relations it depends on]
    /// Used to check validity and track what to rematerialize
    derived_to_base: HashMap<String, HashSet<String>>,

    /// Derived-to-derived dependencies for multi-level rules
    /// derived_relation -> [other derived relations it depends on]
    derived_to_derived: HashMap<String, HashSet<String>>,

    /// Current version of each base relation (for validity checking)
    base_versions: HashMap<String, u64>,

    /// Topologically sorted order of derived relations for safe materialization
    /// Earlier relations don't depend on later ones
    execution_order: Vec<String>,
}

impl Default for DerivedRelationsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DerivedRelationsManager {
    /// Create a new empty manager
    pub fn new() -> Self {
        Self {
            compiled_rules: HashMap::new(),
            materialized: HashMap::new(),
            base_to_derived: HashMap::new(),
            derived_to_base: HashMap::new(),
            derived_to_derived: HashMap::new(),
            base_versions: HashMap::new(),
            execution_order: Vec::new(),
        }
    }

    /// Register a compiled rule (not materialized until first read or explicit request).
    pub fn register_rule(&mut self, rule: CompiledRule) {
        let name = rule.name.clone();
        let deps = rule.dependencies.clone();

        // Track dependencies
        self.derived_to_base.insert(name.clone(), deps.clone());

        for base in &deps {
            self.base_to_derived
                .entry(base.clone())
                .or_default()
                .insert(name.clone());
        }

        // Store the compiled rule
        self.compiled_rules.insert(name.clone(), rule);

        // Recompute execution order
        self.update_execution_order();
    }

    /// Remove a rule and its materialization
    pub fn remove_rule(&mut self, name: &str) {
        self.compiled_rules.remove(name);
        self.materialized.remove(name);

        // Clean up dependency tracking
        if let Some(deps) = self.derived_to_base.remove(name) {
            for base in deps {
                if let Some(derived_set) = self.base_to_derived.get_mut(&base) {
                    derived_set.remove(name);
                }
            }
        }

        self.derived_to_derived.remove(name);
        self.execution_order.retain(|n| n != name);
    }

    /// Get a compiled rule by name
    pub fn get_rule(&self, name: &str) -> Option<&CompiledRule> {
        self.compiled_rules.get(name)
    }

    /// Check if a relation is a derived relation (has a rule)
    pub fn is_derived(&self, name: &str) -> bool {
        self.compiled_rules.contains_key(name)
    }

    /// Get materialized data (None if missing or invalid).
    pub fn get_materialized(&self, name: &str) -> Option<&MaterializedRelation> {
        self.materialized.get(name).filter(|m| m.valid)
    }

    /// Get materialized data, checking validity against current base versions
    pub fn get_materialized_if_valid(&self, name: &str) -> Option<&MaterializedRelation> {
        self.materialized
            .get(name)
            .filter(|m| m.is_valid_for(&self.base_versions))
    }

    /// Store materialized data for a derived relation
    pub fn set_materialized(&mut self, name: &str, tuples: Vec<Tuple>) {
        // Collect base versions for dependencies
        let deps = self.derived_to_base.get(name).cloned().unwrap_or_default();
        let base_versions: HashMap<String, u64> = deps
            .iter()
            .filter_map(|base| self.base_versions.get(base).map(|v| (base.clone(), *v)))
            .collect();

        self.materialized.insert(
            name.to_string(),
            MaterializedRelation::new(tuples, base_versions),
        );
    }

    /// Notify that a base relation has been updated
    ///
    /// This invalidates all derived relations that depend on it.
    /// Returns the names of relations that were invalidated.
    ///
    /// ## Atomicity
    ///
    /// This operation is atomic: all invalidations are computed first, then applied
    /// together. The version bump and all cascade invalidations happen as a single
    /// unit under the mutable borrow, ensuring no partial invalidation state is visible.
    pub fn notify_base_update(&mut self, base_relation: &str) -> Vec<String> {
        // Compute the full invalidation set BEFORE making any changes
        // This ensures atomicity - we know exactly what to invalidate before modifying state
        let to_invalidate = self.compute_invalidation_set(base_relation);

        // Bump version (atomic with apply step since we hold &mut self)
        let version = self
            .base_versions
            .entry(base_relation.to_string())
            .or_insert(0);
        *version += 1;

        // Apply all invalidations atomically
        for rel in &to_invalidate {
            if let Some(mat) = self.materialized.get_mut(rel) {
                mat.invalidate();
            }
        }

        to_invalidate
    }

    /// Compute the set of derived relations that would be invalidated by a base update
    ///
    /// This is a read-only operation that computes the full cascade without modifying state.
    /// Used by `notify_base_update` to ensure atomic invalidation.
    fn compute_invalidation_set(&self, base_relation: &str) -> Vec<String> {
        let mut to_invalidate = Vec::new();
        let mut seen = HashSet::new();

        // Find direct dependents of the base relation
        if let Some(derived_set) = self.base_to_derived.get(base_relation) {
            for derived in derived_set {
                if let Some(mat) = self.materialized.get(derived) {
                    if mat.valid && seen.insert(derived.clone()) {
                        to_invalidate.push(derived.clone());
                    }
                }
            }
        }

        // Cascade: find derived relations that depend on invalidated derived relations
        let mut i = 0;
        while i < to_invalidate.len() {
            let rel = to_invalidate[i].clone();
            if let Some(dependents) = self.derived_to_derived.get(&rel) {
                for dep in dependents {
                    if let Some(mat) = self.materialized.get(dep) {
                        if mat.valid && seen.insert(dep.clone()) {
                            to_invalidate.push(dep.clone());
                        }
                    }
                }
            }
            i += 1;
        }

        to_invalidate
    }

    /// Get all derived relations that need rematerialization
    pub fn get_invalid_relations(&self) -> Vec<String> {
        self.compiled_rules
            .keys()
            .filter(|name| {
                self.materialized.get(*name).is_none_or(|m| !m.valid) // Not yet materialized, or invalidated
            })
            .cloned()
            .collect()
    }

    /// Get derived relations in execution order (respects dependencies)
    pub fn get_execution_order(&self) -> &[String] {
        &self.execution_order
    }

    /// Get all base relations that a derived relation depends on
    pub fn get_base_dependencies(&self, derived: &str) -> Option<&HashSet<String>> {
        self.derived_to_base.get(derived)
    }

    /// Get all derived relations that depend on a base relation
    pub fn get_dependent_derived(&self, base: &str) -> Option<&HashSet<String>> {
        self.base_to_derived.get(base)
    }

    /// Update the topological execution order after rule changes
    fn update_execution_order(&mut self) {
        // Order rules by stratum (computed during rule registration)
        let mut rules: Vec<_> = self.compiled_rules.iter().collect();
        rules.sort_by_key(|(_, r)| r.stratum);
        self.execution_order = rules.into_iter().map(|(name, _)| name.clone()).collect();
    }

    /// Get statistics about the manager state
    pub fn stats(&self) -> DerivedRelationsStats {
        let total_rules = self.compiled_rules.len();
        let materialized_count = self.materialized.values().filter(|m| m.valid).count();
        let invalid_count = self.get_invalid_relations().len();
        let total_tuples: usize = self.materialized.values().map(|m| m.tuples.len()).sum();

        DerivedRelationsStats {
            total_rules,
            materialized_count,
            invalid_count,
            total_tuples,
        }
    }

    /// Get all valid materializations for snapshot integration
    ///
    /// Returns a map of relation_name -> tuples for all derived relations
    /// that have valid materializations. Used by `publish_snapshot()` to
    /// include materialized data in snapshots.
    pub fn get_all_valid_materializations(&self) -> HashMap<String, Vec<Tuple>> {
        self.materialized
            .iter()
            .filter(|(_, m)| m.valid)
            .map(|(name, m)| (name.clone(), m.tuples.clone()))
            .collect()
    }

    /// Get the names of all derived relations with valid materializations
    pub fn get_materialized_relation_names(&self) -> HashSet<String> {
        self.materialized
            .iter()
            .filter(|(_, m)| m.valid)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// Statistics about derived relations state
#[derive(Debug, Clone)]
pub struct DerivedRelationsStats {
    pub total_rules: usize,
    pub materialized_count: usize,
    pub invalid_count: usize,
    pub total_tuples: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn make_tuple(values: Vec<i32>) -> Tuple {
        Tuple::new(values.into_iter().map(|v| Value::Int32(v)).collect())
    }

    fn make_compiled_rule(name: &str, deps: Vec<&str>, stratum: usize) -> CompiledRule {
        CompiledRule {
            name: name.to_string(),
            clauses: vec![],
            dependencies: deps.into_iter().map(|s| s.to_string()).collect(),
            is_recursive: false,
            output_schema: vec![],
            stratum,
        }
    }

    #[test]
    fn test_register_rule() {
        let mut manager = DerivedRelationsManager::new();

        let rule = make_compiled_rule("path", vec!["edge"], 0);
        manager.register_rule(rule);

        assert!(manager.is_derived("path"));
        assert!(!manager.is_derived("edge"));
        assert!(manager.get_rule("path").is_some());
    }

    #[test]
    fn test_dependency_tracking() {
        let mut manager = DerivedRelationsManager::new();

        let rule = make_compiled_rule("reachable", vec!["edge", "start"], 0);
        manager.register_rule(rule);

        // Check forward dependencies
        assert!(manager
            .get_dependent_derived("edge")
            .unwrap()
            .contains("reachable"));
        assert!(manager
            .get_dependent_derived("start")
            .unwrap()
            .contains("reachable"));

        // Check reverse dependencies
        let deps = manager.get_base_dependencies("reachable").unwrap();
        assert!(deps.contains("edge"));
        assert!(deps.contains("start"));
    }

    #[test]
    fn test_materialization() {
        let mut manager = DerivedRelationsManager::new();

        let rule = make_compiled_rule("path", vec!["edge"], 0);
        manager.register_rule(rule);

        // Initially not materialized
        assert!(manager.get_materialized("path").is_none());

        // Materialize
        let tuples = vec![make_tuple(vec![1, 2]), make_tuple(vec![2, 3])];
        manager.set_materialized("path", tuples);

        // Now available
        let mat = manager.get_materialized("path").unwrap();
        assert_eq!(mat.tuples.len(), 2);
        assert!(mat.valid);
    }

    #[test]
    fn test_invalidation() {
        let mut manager = DerivedRelationsManager::new();

        let rule = make_compiled_rule("path", vec!["edge"], 0);
        manager.register_rule(rule);

        // Materialize
        manager.set_materialized("path", vec![make_tuple(vec![1, 2])]);
        assert!(manager.get_materialized("path").is_some());

        // Update base relation
        let invalidated = manager.notify_base_update("edge");
        assert_eq!(invalidated, vec!["path"]);

        // Now invalid
        assert!(manager.get_materialized("path").is_none());
    }

    #[test]
    fn test_cascading_invalidation() {
        let mut manager = DerivedRelationsManager::new();

        // path depends on edge
        let rule1 = make_compiled_rule("path", vec!["edge"], 0);
        manager.register_rule(rule1);

        // reachable depends on path (derived-to-derived)
        let mut rule2 = make_compiled_rule("reachable", vec!["path"], 1);
        rule2.dependencies.insert("edge".to_string()); // transitive
        manager.register_rule(rule2);

        // Add derived-to-derived dependency
        manager
            .derived_to_derived
            .entry("path".to_string())
            .or_default()
            .insert("reachable".to_string());

        // Materialize both
        manager.set_materialized("path", vec![make_tuple(vec![1, 2])]);
        manager.set_materialized("reachable", vec![make_tuple(vec![1])]);

        // Update edge - should invalidate both
        let invalidated = manager.notify_base_update("edge");

        assert!(invalidated.contains(&"path".to_string()));
        assert!(invalidated.contains(&"reachable".to_string()));
    }

    #[test]
    fn test_remove_rule() {
        let mut manager = DerivedRelationsManager::new();

        let rule = make_compiled_rule("path", vec!["edge"], 0);
        manager.register_rule(rule);
        manager.set_materialized("path", vec![make_tuple(vec![1, 2])]);

        assert!(manager.is_derived("path"));

        manager.remove_rule("path");

        assert!(!manager.is_derived("path"));
        assert!(manager.get_materialized("path").is_none());
    }

    #[test]
    fn test_execution_order() {
        let mut manager = DerivedRelationsManager::new();

        // Add rules in reverse stratum order
        let rule2 = make_compiled_rule("level2", vec!["level1"], 2);
        let rule0 = make_compiled_rule("level0", vec!["base"], 0);
        let rule1 = make_compiled_rule("level1", vec!["level0"], 1);

        manager.register_rule(rule2);
        manager.register_rule(rule0);
        manager.register_rule(rule1);

        // Execution order should be by stratum
        let order = manager.get_execution_order();
        assert_eq!(order[0], "level0");
        assert_eq!(order[1], "level1");
        assert_eq!(order[2], "level2");
    }

    #[test]
    fn test_stats() {
        let mut manager = DerivedRelationsManager::new();

        manager.register_rule(make_compiled_rule("a", vec!["base"], 0));
        manager.register_rule(make_compiled_rule("b", vec!["base"], 0));

        manager.set_materialized("a", vec![make_tuple(vec![1]), make_tuple(vec![2])]);

        let stats = manager.stats();
        assert_eq!(stats.total_rules, 2);
        assert_eq!(stats.materialized_count, 1);
        assert_eq!(stats.invalid_count, 1); // b is not materialized
        assert_eq!(stats.total_tuples, 2);
    }
}
