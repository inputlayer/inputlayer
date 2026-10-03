//! Lock-Free Snapshot System for Knowledge Graphs
//!
//! Provides immutable point-in-time snapshots of knowledge graph data
//! for lock-free read access. Uses arc-swap for instant atomic publishing.
//!
//! ## Design
//!
//! - `KnowledgeGraphSnapshot`: immutable; relations share tuples with the
//!   writer and with other snapshots (see [`Relation`]), so publishing after a
//!   write costs O(changed chunks), not O(KG)
//! - Persistent rules are parsed once per snapshot; a query is evaluated with
//!   only the rules and relations in its dependency closure
//! - Writers publish new snapshots atomically via `ArcSwap`
//! - Readers get consistent snapshots without holding locks

use crate::ast::dependencies::DependencyClosure;
use crate::ast::{Program, Rule};
use crate::execution::{TimingBreakdown, TimingMode};
use crate::index_manager::HnswSearchFn;
use crate::value::{Relation, RelationMap, Tuple};
use crate::{IQLEngine, OptimizationConfig};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// Counter for snapshot versioning
static SNAPSHOT_VERSION: AtomicU64 = AtomicU64::new(0);

/// Immutable point-in-time snapshot of knowledge graph data
///
/// Cloning a snapshot is O(1) - just incrementing reference counts.
#[derive(Clone)]
pub struct KnowledgeGraphSnapshot {
    /// Monotonically increasing version number
    pub version: u64,

    /// Timestamp when snapshot was created (microseconds since epoch)
    pub timestamp: u64,

    /// Base relation data, including valid materializations of derived relations.
    pub input_tuples: Arc<RelationMap>,

    /// Persistent rules (AST format)
    pub rules: Arc<Vec<Rule>>,

    /// Number of worker threads for parallel query execution
    pub num_workers: usize,

    /// Names of derived relations that have valid materializations
    ///
    /// Rules for these relations are skipped during execution since
    /// their data is already present in `input_tuples` as base facts.
    pub materialized_relations: Arc<HashSet<String>>,

    /// Non-materialized rules formatted as text, one per line.
    rule_prefix: Arc<String>,

    /// `rule_prefix` parsed once, as every query would parse it.
    prefix_rules: Arc<Result<Vec<Rule>, String>>,

    /// Maximum result rows returned per query (0 = unlimited)
    pub max_result_rows: usize,

    /// Maximum query cost score (0 = unlimited)
    pub max_query_cost: u64,

    /// Optimizer passes for engines built from this snapshot
    pub optimization: OptimizationConfig,

    /// HNSW search over the index views captured when this snapshot was
    /// published, so `hnsw_nearest` sees the same data as `input_tuples`.
    pub hnsw_search_fn: Option<HnswSearchFn>,
}

/// Whether the caller reads derived relations by their original names
/// (provenance). Constant specialization renames them, so it is off then.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Output {
    Result,
    WithDerived,
}

/// Rules a query is evaluated with.
#[derive(Clone, Copy)]
enum RuleSet {
    /// Only the rules in the query text.
    QueryOnly,
    /// The query plus the persistent rules in its dependency closure.
    WithPersistent,
}

impl KnowledgeGraphSnapshot {
    /// Create a new snapshot from knowledge graph data
    pub fn new<R: Into<Relation>>(input_tuples: HashMap<String, R>, rules: Vec<Rule>) -> Self {
        Self::new_with_workers(input_tuples, rules, 1)
    }

    /// Create a new snapshot with configurable worker count
    pub fn new_with_workers<R: Into<Relation>>(
        input_tuples: HashMap<String, R>,
        rules: Vec<Rule>,
        num_workers: usize,
    ) -> Self {
        Self::new_with_materializations(input_tuples, rules, num_workers, HashSet::new())
    }

    /// Create a new snapshot with materialized relations
    ///
    /// Materialized tuples must already be merged into `input_tuples` by the
    /// caller. `materialized_names` identifies them; their rules are skipped.
    pub fn new_with_materializations<R: Into<Relation>>(
        input_tuples: HashMap<String, R>,
        rules: Vec<Rule>,
        num_workers: usize,
        materialized_names: HashSet<String>,
    ) -> Self {
        let version = SNAPSHOT_VERSION.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64);

        let prefix = Self::build_rule_prefix(&rules, &materialized_names);
        let prefix_rules = crate::parser::parse_program(&prefix).map(|p| p.rules);
        let input_tuples: RelationMap = input_tuples
            .into_iter()
            .map(|(name, tuples)| (name, tuples.into()))
            .collect();

        Self {
            version,
            timestamp,
            input_tuples: Arc::new(input_tuples),
            rules: Arc::new(rules),
            num_workers,
            materialized_relations: Arc::new(materialized_names),
            rule_prefix: Arc::new(prefix),
            prefix_rules: Arc::new(prefix_rules),
            max_result_rows: 0,
            max_query_cost: 0,
            optimization: OptimizationConfig::default(),
            hnsw_search_fn: None,
        }
    }

    /// Create an empty snapshot
    pub fn empty() -> Self {
        Self::new(RelationMap::new(), Vec::new())
    }

    /// Build the formatted rule prefix text from rules, excluding materialized ones.
    fn build_rule_prefix(rules: &[Rule], materialized: &HashSet<String>) -> String {
        let mut prefix = String::new();
        for rule in rules {
            if materialized.contains(&rule.head.relation) {
                continue;
            }
            prefix.push_str(&super::format_rule(rule));
            prefix.push('\n');
        }
        prefix
    }

    /// Get the cached rule prefix (all non-materialized rules as text).
    pub fn rule_prefix(&self) -> &str {
        &self.rule_prefix
    }

    /// Build an engine and program for `program`: the query's rules, plus the
    /// persistent rules it depends on, over only the relations it can read.
    /// Session facts are layered on copies of the affected relations; the
    /// snapshot itself is never modified.
    fn prepare(
        &self,
        program: &str,
        rule_set: RuleSet,
        output: Output,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
    ) -> Result<(IQLEngine, Program), String> {
        let query = crate::parser::parse_program(program)?;
        let persistent: &[Rule] = match rule_set {
            RuleSet::QueryOnly => &[],
            RuleSet::WithPersistent => self.prefix_rules.as_ref().as_ref().map_err(Clone::clone)?,
        };

        let mut closure = DependencyClosure::default();
        for rule in &query.rules {
            closure.add_rule(rule);
        }
        closure.close_over(persistent);

        let mut combined = Program::new();
        combined.rules = persistent
            .iter()
            .filter(|rule| closure.contains(&rule.head.relation))
            .cloned()
            .chain(query.rules)
            .collect();

        let mut inputs: RelationMap = closure
            .relations()
            .filter_map(|name| {
                self.input_tuples
                    .get(name)
                    .map(|tuples| (name.to_string(), tuples.clone()))
            })
            .collect();
        for (relation, tuple) in session_facts {
            inputs.entry(relation).or_default().push(tuple);
        }

        let mut engine = self.new_engine();
        engine.set_timing_mode(timing_mode);
        if output == Output::WithDerived {
            let mut config = engine.config().clone();
            config.enable_constant_specialization = false;
            engine.set_config(config);
        }
        engine.set_inputs(inputs);
        Ok((engine, combined))
    }

    /// Build an engine with this snapshot's optimizer passes, limits, worker
    /// count and HNSW search. Callers load the input data.
    pub fn new_engine(&self) -> IQLEngine {
        let mut engine = IQLEngine::with_config(self.optimization.clone());
        engine.set_num_workers(self.num_workers);
        engine.set_max_result_rows(self.max_result_rows);
        engine.set_max_query_cost(self.max_query_cost);
        if let Some(ref search_fn) = self.hnsw_search_fn {
            engine.set_hnsw_search_fn(Arc::clone(search_fn));
        }
        engine
    }

    /// Evaluate `program`, returning the query result, all derived relations
    /// and the optional timing breakdown.
    fn run(
        &self,
        program: &str,
        rule_set: RuleSet,
        output: Output,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, RelationMap, Option<TimingBreakdown>), String> {
        let start = Instant::now();
        let session_fact_count = session_facts.len();
        let (mut engine, combined) =
            self.prepare(program, rule_set, output, session_facts, timing_mode)?;
        let rules = combined.rules.len();
        let result = engine.execute_program_profiled(combined);
        info!(
            program_len = program.len(),
            rules,
            session_facts = session_fact_count,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "snapshot_execute"
        );
        result
    }

    /// Execute a query (without persistent rules) returning binary tuples
    pub fn execute(&self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        Ok(Self::to_pairs(&self.execute_tuples(program)?))
    }

    /// Execute a query with persistent rules, returning binary tuples
    ///
    /// Rules for materialized relations are skipped - their data is already
    /// present in `input_tuples` as base facts (injected at snapshot creation).
    pub fn execute_with_rules(&self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        Ok(Self::to_pairs(&self.execute_with_rules_tuples(program)?))
    }

    fn to_pairs(tuples: &[Tuple]) -> Vec<(i32, i32)> {
        tuples.iter().filter_map(Tuple::to_pair).collect()
    }

    /// Execute a query (without persistent rules) returning arbitrary-arity tuples
    pub fn execute_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        self.run(
            program,
            RuleSet::QueryOnly,
            Output::Result,
            Vec::new(),
            TimingMode::Off,
        )
        .map(|(tuples, _, _)| tuples)
    }

    /// Execute a query with persistent rules, returning arbitrary-arity tuples
    pub fn execute_with_rules_tuples(&self, program: &str) -> Result<Vec<Tuple>, String> {
        self.run(
            program,
            RuleSet::WithPersistent,
            Output::Result,
            Vec::new(),
            TimingMode::Off,
        )
        .map(|(tuples, _, _)| tuples)
    }

    /// Persistent rules and base relations, as the backward chainer reads them.
    pub fn proof_inputs(&self) -> (Vec<Rule>, HashMap<String, Vec<Tuple>>) {
        (
            self.rules.as_ref().clone(),
            crate::value::relation::to_vec_map(&self.input_tuples),
        )
    }

    /// Execute a query with rules, returning tuples AND all derived relation data.
    ///
    /// Used by the provenance system to pass materialized derived tuples
    /// to the backward chainer, avoiding expensive re-derivation.
    pub fn execute_with_rules_tuples_and_derived(
        &self,
        program: &str,
    ) -> Result<(Vec<Tuple>, RelationMap), String> {
        self.run(
            program,
            RuleSet::WithPersistent,
            Output::WithDerived,
            Vec::new(),
            TimingMode::Off,
        )
        .map(|(tuples, derived, _)| (tuples, derived))
    }

    /// Execute a query with rules, returning tuples and optional timing breakdown.
    pub fn execute_with_rules_tuples_profiled(
        &self,
        program: &str,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, Option<TimingBreakdown>), String> {
        self.run(
            program,
            RuleSet::WithPersistent,
            Output::Result,
            Vec::new(),
            timing_mode,
        )
        .map(|(tuples, _, timing)| (tuples, timing))
    }

    /// Execute a query with rules, returning tuples, all derived relation data,
    /// and optional timing breakdown.
    pub fn execute_with_rules_tuples_profiled_full(
        &self,
        program: &str,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, RelationMap, Option<TimingBreakdown>), String> {
        self.run(
            program,
            RuleSet::WithPersistent,
            Output::WithDerived,
            Vec::new(),
            timing_mode,
        )
    }

    /// Execute a query with temporary session facts that don't affect the shared store
    ///
    /// Session facts are appended to request-local copies of the affected
    /// relations (copy-on-write per chunk), so concurrent queries never see
    /// each other's session facts.
    ///
    /// # Example
    /// ```text
    /// let snapshot = kg.snapshot();
    /// let result = snapshot.execute_with_session_facts(
    ///     "result(X) <- edge(X, Y), session_filter(Y)",
    ///     vec![("session_filter".to_string(), Tuple::from_pair(3, 0))],
    /// )?;
    /// // session_filter is only visible to THIS query, not other concurrent queries
    /// ```
    pub fn execute_with_session_facts(
        &self,
        program: &str,
        session_facts: Vec<(String, Tuple)>,
    ) -> Result<Vec<Tuple>, String> {
        self.run(
            program,
            RuleSet::WithPersistent,
            Output::Result,
            session_facts,
            TimingMode::Off,
        )
        .map(|(tuples, _, _)| tuples)
    }

    /// Execute a query with session facts, returning tuples and optional timing breakdown.
    pub fn execute_with_session_facts_profiled(
        &self,
        program: &str,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, Option<TimingBreakdown>), String> {
        self.run(
            program,
            RuleSet::WithPersistent,
            Output::Result,
            session_facts,
            timing_mode,
        )
        .map(|(tuples, _, timing)| (tuples, timing))
    }

    /// Get the number of relations in this snapshot
    pub fn relation_count(&self) -> usize {
        self.input_tuples.len()
    }

    /// Get the total number of tuples across all relations
    pub fn tuple_count(&self) -> usize {
        self.input_tuples.values().map(Relation::len).sum()
    }

    /// Check if this snapshot is empty (no data)
    pub fn is_empty(&self) -> bool {
        self.input_tuples.is_empty()
    }

    /// Get the number of materialized relations in this snapshot
    pub fn materialized_count(&self) -> usize {
        self.materialized_relations.len()
    }

    /// Check if a relation is materialized in this snapshot
    pub fn is_materialized(&self, relation: &str) -> bool {
        self.materialized_relations.contains(relation)
    }
}

impl std::fmt::Debug for KnowledgeGraphSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnowledgeGraphSnapshot")
            .field("version", &self.version)
            .field("timestamp", &self.timestamp)
            .field("relations", &self.relation_count())
            .field("tuples", &self.tuple_count())
            .field("rules", &self.rules.len())
            .field("materialized", &self.materialized_count())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_snapshot_creation() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        assert_eq!(snapshot.relation_count(), 1);
        assert_eq!(snapshot.tuple_count(), 2);
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn test_snapshot_execute() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        let results = snapshot.execute("result(X,Y) <- edge(X,Y)").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_snapshot_clone_is_cheap() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        let snapshot1 = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let snapshot2 = snapshot1.clone();

        // Both snapshots share the same underlying data (Arc)
        assert!(Arc::ptr_eq(
            &snapshot1.input_tuples,
            &snapshot2.input_tuples
        ));
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = KnowledgeGraphSnapshot::empty();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.relation_count(), 0);
        assert_eq!(snapshot.tuple_count(), 0);
    }

    // === Materialization Tests ===

    #[test]
    fn test_snapshot_with_materializations() {
        // Base relation
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        // Simulate materialized derived relation
        let mut materialized_names = HashSet::new();
        materialized_names.insert("path".to_string());

        // Add "path" tuples as if they were materialized
        input_tuples.insert(
            "path".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(1), Value::Int32(3)]), // transitive closure
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new_with_materializations(
            input_tuples,
            Vec::new(), // No rules needed since path is materialized
            1,
            materialized_names,
        );

        assert_eq!(snapshot.materialized_count(), 1);
        assert!(snapshot.is_materialized("path"));
        assert!(!snapshot.is_materialized("edge"));
    }

    #[test]
    fn test_snapshot_skips_materialized_rules() {
        use crate::ast::{Atom, BodyPredicate, Rule, Term};

        // Base relation
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        // Create a rule: path(X, Y) <- edge(X, Y)
        let rule = Rule {
            head: Atom {
                relation: "path".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "edge".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            })],
        };

        // Case 1: No materialization - rule is executed
        let snapshot_no_mat = KnowledgeGraphSnapshot::new_with_materializations(
            input_tuples.clone(),
            vec![rule.clone()],
            1,
            HashSet::new(),
        );

        // Query for path - should use the rule
        let results = snapshot_no_mat
            .execute_with_rules_tuples("result(X, Y) <- path(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 2); // edge has 2 tuples, so path has 2 tuples

        // Case 2: With materialization - rule is skipped, uses pre-computed data
        let mut mat_input_tuples = input_tuples.clone();
        mat_input_tuples.insert(
            "path".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(99), Value::Int32(100)]), // Extra tuple from "materialization"
            ],
        );

        let mut mat_names = HashSet::new();
        mat_names.insert("path".to_string());

        let snapshot_with_mat = KnowledgeGraphSnapshot::new_with_materializations(
            mat_input_tuples,
            vec![rule],
            1,
            mat_names,
        );

        // Query for path - should use materialized data (3 tuples, not 2)
        let results = snapshot_with_mat
            .execute_with_rules_tuples("result(X, Y) <- path(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 3); // Uses materialized data, not rule
    }

    #[test]
    fn test_snapshot_partial_materialization() {
        use crate::ast::{Atom, BodyPredicate, Rule, Term};

        // Base relation
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "base".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
        );

        // Two rules: derived1 and derived2
        let rule1 = Rule {
            head: Atom {
                relation: "derived1".to_string(),
                args: vec![Term::Variable("X".to_string())],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "base".to_string(),
                args: vec![Term::Variable("X".to_string())],
            })],
        };

        let rule2 = Rule {
            head: Atom {
                relation: "derived2".to_string(),
                args: vec![Term::Variable("X".to_string())],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "base".to_string(),
                args: vec![Term::Variable("X".to_string())],
            })],
        };

        // Only derived1 is materialized
        let mut mat_input_tuples = input_tuples.clone();
        mat_input_tuples.insert(
            "derived1".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(99)]), // Extra - proves we use materialized
            ],
        );

        let mut mat_names = HashSet::new();
        mat_names.insert("derived1".to_string());

        let snapshot = KnowledgeGraphSnapshot::new_with_materializations(
            mat_input_tuples,
            vec![rule1, rule2],
            1,
            mat_names,
        );

        // derived1 uses materialized data (3 tuples)
        let results1 = snapshot
            .execute_with_rules_tuples("result(X) <- derived1(X)")
            .unwrap();
        assert_eq!(results1.len(), 3);

        // derived2 uses rule (2 tuples)
        let results2 = snapshot
            .execute_with_rules_tuples("result(X) <- derived2(X)")
            .unwrap();
        assert_eq!(results2.len(), 2);
    }

    #[test]
    fn test_materialized_relations_in_debug() {
        let mut mat_names = HashSet::new();
        mat_names.insert("path".to_string());
        mat_names.insert("reachable".to_string());

        let snapshot = KnowledgeGraphSnapshot::new_with_materializations(
            RelationMap::new(),
            Vec::new(),
            1,
            mat_names,
        );

        let debug_str = format!("{snapshot:?}");
        assert!(debug_str.contains("materialized: 2"));
    }

    #[test]
    fn test_cached_rule_prefix() {
        use crate::ast::{Atom, BodyPredicate, Rule, Term};

        let rule = Rule {
            head: Atom {
                relation: "path".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "edge".to_string(),
                args: vec![
                    Term::Variable("X".to_string()),
                    Term::Variable("Y".to_string()),
                ],
            })],
        };

        let snapshot = KnowledgeGraphSnapshot::new(RelationMap::new(), vec![rule]);

        // Rule prefix should be cached at creation
        let prefix = snapshot.rule_prefix();
        assert!(!prefix.is_empty());
        assert!(prefix.contains("path"));
        assert!(prefix.contains("edge"));

        // Calling again should return the same cached string
        assert_eq!(prefix, snapshot.rule_prefix());
    }

    #[test]
    fn test_cached_rule_prefix_skips_materialized() {
        use crate::ast::{Atom, BodyPredicate, Rule, Term};

        let rule1 = Rule {
            head: Atom {
                relation: "derived1".to_string(),
                args: vec![Term::Variable("X".to_string())],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "base".to_string(),
                args: vec![Term::Variable("X".to_string())],
            })],
        };

        let rule2 = Rule {
            head: Atom {
                relation: "derived2".to_string(),
                args: vec![Term::Variable("X".to_string())],
            },
            body: vec![BodyPredicate::Positive(Atom {
                relation: "base".to_string(),
                args: vec![Term::Variable("X".to_string())],
            })],
        };

        let mut mat = HashSet::new();
        mat.insert("derived1".to_string());

        let snapshot = KnowledgeGraphSnapshot::new_with_materializations(
            RelationMap::new(),
            vec![rule1, rule2],
            1,
            mat,
        );

        let prefix = snapshot.rule_prefix();
        // derived1 is materialized → excluded from prefix
        assert!(!prefix.contains("derived1"));
        // derived2 is NOT materialized → included in prefix
        assert!(prefix.contains("derived2"));
    }

    // === Additional Coverage ===

    #[test]
    fn test_snapshot_with_workers() {
        let snapshot = KnowledgeGraphSnapshot::new_with_workers(RelationMap::new(), Vec::new(), 4);
        assert_eq!(snapshot.num_workers, 4);
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_snapshot_version_increases() {
        let s1 = KnowledgeGraphSnapshot::empty();
        let s2 = KnowledgeGraphSnapshot::empty();
        assert!(s2.version > s1.version);
    }

    #[test]
    fn test_snapshot_execute_tuples() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int64(1), Value::string("alice")]),
                Tuple::new(vec![Value::Int64(2), Value::string("bob")]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let results = snapshot
            .execute_tuples("result(N, Name) <- data(N, Name)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_snapshot_execute_with_session_facts() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        // Execute with session-scoped filter
        let results = snapshot
            .execute_with_session_facts(
                "result(X, Y) <- edge(X, Y), allowed(X)",
                vec![("allowed".to_string(), Tuple::new(vec![Value::Int32(1)]))],
            )
            .unwrap();

        // Only edge(1,2) matches because only 1 is in allowed
        assert_eq!(results.len(), 1);

        // Original snapshot is unaffected (no "allowed" in base data)
        assert!(!snapshot.input_tuples.contains_key("allowed"));
    }

    #[test]
    fn test_snapshot_execute_with_rules_no_rules() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        // No rules → execute_with_rules behaves like execute
        let results = snapshot
            .execute_with_rules("result(X, Y) <- edge(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_snapshot_debug_output() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let debug = format!("{snapshot:?}");
        assert!(debug.contains("KnowledgeGraphSnapshot"));
        assert!(debug.contains("relations: 1"));
        assert!(debug.contains("tuples: 1"));
    }

    #[test]
    fn test_empty_rules_empty_prefix() {
        let snapshot = KnowledgeGraphSnapshot::empty();
        assert!(snapshot.rule_prefix().is_empty());
    }

    // === Regression tests for production readiness fixes ===

    /// P1-8: Verify COW semantics - only relations with session facts are cloned.
    /// The original snapshot's data must remain untouched after session query.
    #[test]
    fn test_session_facts_cow_does_not_modify_snapshot() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );
        input_tuples.insert("node".to_string(), vec![Tuple::new(vec![Value::Int32(1)])]);

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        // Add session fact only to "edge"
        let _results = snapshot
            .execute_with_session_facts(
                "result(X, Y) <- edge(X, Y)",
                vec![(
                    "edge".to_string(),
                    Tuple::new(vec![Value::Int32(99), Value::Int32(100)]),
                )],
            )
            .unwrap();

        // Original snapshot must be unmodified
        assert_eq!(
            snapshot.input_tuples.get("edge").unwrap().len(),
            2,
            "Original edge relation must not be modified by session facts"
        );
        assert!(
            !snapshot.input_tuples.contains_key("allowed"),
            "Session-only relations must not leak into snapshot"
        );
    }

    /// P1-8: Verify session facts for NEW relations (not in base data).
    #[test]
    fn test_session_facts_new_relation() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());

        // Session introduces a brand-new relation "filter"
        let results = snapshot
            .execute_with_session_facts(
                "result(X, Y) <- edge(X, Y), filter(X)",
                vec![("filter".to_string(), Tuple::new(vec![Value::Int32(1)]))],
            )
            .unwrap();

        // Should find edge(1,2) because filter(1) matches
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            Tuple::new(vec![Value::Int32(1), Value::Int32(2)])
        );

        // Original snapshot must not have "filter"
        assert!(!snapshot.input_tuples.contains_key("filter"));
    }

    /// P1-8: Verify session facts with empty session facts is a no-op.
    #[test]
    fn test_session_facts_empty_is_noop() {
        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let results = snapshot
            .execute_with_session_facts("result(X, Y) <- edge(X, Y)", vec![])
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_snapshot_execute_profiled_summary() {
        use crate::execution::TimingMode;

        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::from_pair(1, 2),
                Tuple::from_pair(2, 3),
                Tuple::from_pair(3, 4),
            ],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let (results, timing) = snapshot
            .execute_with_rules_tuples_profiled("result(X, Y) <- edge(X, Y)", TimingMode::Summary)
            .unwrap();
        assert_eq!(results.len(), 3);

        // Summary mode should produce timing breakdown
        let tb = timing.expect("Summary mode should produce timing breakdown");
        assert!(tb.total_us > 0, "total_us should be > 0");
        // Rules array should be empty in Summary mode
        assert!(tb.rules.is_empty());
    }

    #[test]
    fn test_snapshot_execute_profiled_detailed() {
        use crate::execution::TimingMode;

        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![Tuple::from_pair(1, 2), Tuple::from_pair(2, 3)],
        );

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let (results, timing) = snapshot
            .execute_with_rules_tuples_profiled("result(X, Y) <- edge(X, Y)", TimingMode::Detailed)
            .unwrap();
        assert_eq!(results.len(), 2);

        let tb = timing.expect("Detailed mode should produce timing breakdown");
        assert!(tb.total_us > 0);
        // Detailed mode should have per-rule entries
        assert!(
            !tb.rules.is_empty(),
            "Detailed mode should have rule timings"
        );
        assert_eq!(tb.rules[0].rule_head, "result");
    }

    #[test]
    fn test_snapshot_execute_profiled_off() {
        use crate::execution::TimingMode;

        let mut input_tuples = HashMap::new();
        input_tuples.insert("edge".to_string(), vec![Tuple::from_pair(1, 2)]);

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, Vec::new());
        let (results, timing) = snapshot
            .execute_with_rules_tuples_profiled("result(X, Y) <- edge(X, Y)", TimingMode::Off)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(timing.is_none(), "Off mode should produce no timing");
    }
}
