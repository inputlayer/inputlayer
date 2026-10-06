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
//! - Persistent rules are stored definitions, recomputed on read: a snapshot
//!   holds only base facts, and a read that depends on a rule derives it from
//!   them in a fresh dataflow (nothing derived is kept between reads)
//! - Persistent rules are parsed once and shared by every snapshot until they
//!   change; a query is evaluated with only the rules and relations in its
//!   dependency closure
//! - Standing queries reuse their compiled plan across snapshots
//!   ([`KnowledgeGraphSnapshot::execute_with_rules_tuples_cached`]): plans
//!   live with the rules, so a rule change starts over with no plans
//! - Writers publish new snapshots atomically via `ArcSwap`
//! - Readers get consistent snapshots without holding locks

use super::precondition::ChangeLog;
use super::revisions::next_revision;
use crate::ast::dependencies::DependencyClosure;
use crate::ast::{Program, Rule};
use crate::execution::{TimingBreakdown, TimingMode};
use crate::index_manager::HnswSearchFn;
use crate::value::{Relation, RelationMap, Tuple};
use crate::{CompiledProgram, IQLEngine, OptimizationConfig};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

/// Immutable point-in-time snapshot of knowledge graph data
///
/// Cloning a snapshot is O(1) - just incrementing reference counts.
#[derive(Clone)]
pub struct KnowledgeGraphSnapshot {
    /// Revision of the knowledge graph state this snapshot holds: assigned
    /// when the snapshot is built, from one counter shared by every knowledge
    /// graph. A knowledge graph publishes its snapshots under its write lock,
    /// so each publish has a higher revision than the one before, even across
    /// a drop and re-create of the same name. A restarted engine continues
    /// above every revision its earlier runs issued (see
    /// `storage_engine::revisions`); the stream epoch
    /// ([`crate::protocol::notification_log::NotificationLog::epoch`]) tells
    /// runs apart.
    pub revision: u64,

    /// Timestamp when snapshot was created (microseconds since epoch)
    pub timestamp: u64,

    /// Base relation data.
    pub input_tuples: Arc<RelationMap>,

    /// Persistent rules (AST format)
    pub rules: Arc<Vec<Rule>>,

    /// Number of worker threads for parallel query execution
    pub num_workers: usize,

    /// The persistent rules queries are evaluated with, and the plans
    /// compiled against them. Shared with the previous snapshot when equal.
    persistent: Arc<PersistentRules>,

    /// Maximum result rows returned per query (0 = unlimited)
    pub max_result_rows: usize,

    /// Most rows one join of a query may be estimated to produce (0 = unlimited)
    pub max_query_cost: u64,

    /// Most fixpoint iterations a recursive evaluation may run (0 = unlimited)
    pub max_recursion_iterations: u32,

    /// Optimizer passes for engines built from this snapshot
    pub optimization: OptimizationConfig,

    /// HNSW search over the index views captured when this snapshot was
    /// published, so `hnsw_nearest` sees the same data as `input_tuples`.
    pub hnsw_search_fn: Option<HnswSearchFn>,

    /// When each relation and the rules last changed, as of this snapshot;
    /// shared with copies of it.
    changes: Arc<ChangeLog>,
}

/// The persistent rules a snapshot evaluates queries with, and the plans
/// compiled against them.
///
/// Snapshots published while the rules stay the same share one value, so a
/// plan compiled on one snapshot serves every later one; a rule change
/// publishes a new value with no plans. Lookups are lock-free: the plan map is
/// replaced, never modified, and a miss compiles outside any lock.
pub struct PersistentRules {
    /// The persistent rules formatted as text, one per line.
    prefix: String,
    /// `prefix` parsed once, as every query would parse it.
    rules: Result<Vec<Rule>, String>,
    plans: ArcSwap<HashMap<String, Arc<CachedPlan>>>,
}

/// How [`KnowledgeGraphSnapshot::execute_with_rules_tuples_cached`] ran.
#[derive(Debug, Clone, Copy, Default)]
pub struct CachedRun {
    /// Whether the plan came from the cache.
    pub plan_cached: bool,
    /// Time spent executing the plan, excluding compiling it.
    pub executing: Duration,
}

/// Plans kept per rule set; past this many, a new plan evicts an arbitrary one.
const MAX_CACHED_PLANS: usize = 256;

/// A query program compiled with the persistent rules it depends on.
struct CachedPlan {
    optimization: OptimizationConfig,
    compiled: CompiledProgram,
    /// The relations in the program's dependency closure: its inputs.
    relations: Vec<String>,
    /// Whether the plan evaluates persistent rules (see [`Combined`]).
    evaluates_rules: bool,
}

/// A query program with the persistent rules it depends on.
struct Combined {
    program: Program,
    /// The relations in the program's dependency closure: its inputs.
    relations: Vec<String>,
    /// Whether the closure holds a persistent rule, so running the program
    /// evaluates deployed rules from the base facts: one rule evaluation
    /// ([`crate::execution::ViewCounters::record_rule_evaluation`]) per run.
    evaluates_rules: bool,
}

impl PersistentRules {
    fn new(prefix: String) -> Self {
        let rules = crate::parser::parse_program(&prefix).map(|p| p.rules);
        Self {
            prefix,
            rules,
            plans: ArcSwap::default(),
        }
    }

    /// The plan for `program` under `optimization`, if one was compiled.
    fn plan(&self, program: &str, optimization: &OptimizationConfig) -> Option<Arc<CachedPlan>> {
        self.plans
            .load()
            .get(program)
            .filter(|plan| plan.optimization == *optimization)
            .cloned()
    }

    fn remember(&self, program: &str, plan: Arc<CachedPlan>) {
        self.plans.rcu(|plans| {
            let mut plans = HashMap::clone(plans);
            if plans.len() >= MAX_CACHED_PLANS && !plans.contains_key(program) {
                // Arbitrary, not oldest: more queries than slots still mostly hit.
                if let Some(evicted) = plans.keys().next().cloned() {
                    plans.remove(&evicted);
                }
            }
            plans.insert(program.to_string(), Arc::clone(&plan));
            plans
        });
    }

    /// Number of cached plans.
    pub fn cached_plans(&self) -> usize {
        self.plans.load().len()
    }
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
        Self::with_rules_after(input_tuples, rules, num_workers, None)
    }

    /// [`Self::new_with_workers`], sharing `previous`'s parsed rules and
    /// compiled plans when its rules are the same.
    pub fn with_rules_after<R: Into<Relation>>(
        input_tuples: HashMap<String, R>,
        rules: Vec<Rule>,
        num_workers: usize,
        previous: Option<&Self>,
    ) -> Self {
        let revision = next_revision();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64);

        let prefix = Self::build_rule_prefix(&rules);
        let persistent = match previous {
            Some(previous) if previous.persistent.prefix == prefix => {
                Arc::clone(&previous.persistent)
            }
            _ => Arc::new(PersistentRules::new(prefix)),
        };
        let input_tuples: RelationMap = input_tuples
            .into_iter()
            .map(|(name, tuples)| (name, tuples.into()))
            .collect();

        Self {
            revision,
            timestamp,
            input_tuples: Arc::new(input_tuples),
            rules: Arc::new(rules),
            num_workers,
            persistent,
            max_result_rows: 0,
            max_query_cost: 0,
            max_recursion_iterations: 0,
            optimization: OptimizationConfig::default(),
            hnsw_search_fn: None,
            changes: Arc::new(ChangeLog::starting_at(revision)),
        }
    }

    /// When each relation and the rules last changed, as of this snapshot.
    /// A published snapshot continues its predecessor's log; any other
    /// starts an empty one at its own revision.
    pub fn changes(&self) -> &ChangeLog {
        &self.changes
    }

    /// Set the change log of a snapshot about to be published.
    pub(super) fn set_changes(&mut self, changes: ChangeLog) {
        self.changes = Arc::new(changes);
    }

    /// Create an empty snapshot
    pub fn empty() -> Self {
        Self::new(RelationMap::new(), Vec::new())
    }

    /// Build the formatted rule prefix text from rules.
    fn build_rule_prefix(rules: &[Rule]) -> String {
        let mut prefix = String::new();
        for rule in rules {
            prefix.push_str(&super::format_rule(rule));
            prefix.push('\n');
        }
        prefix
    }

    /// Get the cached rule prefix (all persistent rules as text).
    pub fn rule_prefix(&self) -> &str {
        &self.persistent.prefix
    }

    /// The persistent rules and their compiled plans, shared with every
    /// snapshot of the same rules.
    pub fn persistent_rules(&self) -> &Arc<PersistentRules> {
        &self.persistent
    }

    /// `program`'s rules plus the persistent rules it depends on, and the
    /// relations in its dependency closure.
    fn combine(&self, query: Program, rule_set: RuleSet) -> Result<Combined, String> {
        let persistent: &[Rule] = match rule_set {
            RuleSet::QueryOnly => &[],
            RuleSet::WithPersistent => self.persistent.rules.as_ref().map_err(Clone::clone)?,
        };

        let mut closure = DependencyClosure::default();
        for rule in &query.rules {
            closure.add_rule(rule);
        }
        closure.close_over(persistent);

        let mut program = Program::new();
        program.rules = persistent
            .iter()
            .filter(|rule| closure.contains(&rule.head.relation))
            .cloned()
            .collect();
        let evaluates_rules = !program.rules.is_empty();
        program.rules.extend(query.rules);
        let relations = closure.relations().map(str::to_string).collect();
        Ok(Combined {
            program,
            relations,
            evaluates_rules,
        })
    }

    /// This snapshot's data for `relations` (those it has).
    fn inputs<'a>(&self, relations: impl IntoIterator<Item = &'a str>) -> RelationMap {
        relations
            .into_iter()
            .filter_map(|name| {
                self.input_tuples
                    .get(name)
                    .map(|tuples| (name.to_string(), tuples.clone()))
            })
            .collect()
    }

    /// Build an engine and program for `program`: the query's rules, plus the
    /// persistent rules it depends on, over only the relations it can read.
    /// Session facts are layered on copies of the affected relations; the
    /// snapshot itself is never modified.
    fn prepare(
        &self,
        query: Program,
        rule_set: RuleSet,
        output: Output,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
    ) -> Result<(IQLEngine, Combined), String> {
        let combined = self.combine(query, rule_set)?;
        let mut inputs = self.inputs(combined.relations.iter().map(String::as_str));
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
        engine.set_max_recursion_iterations(self.max_recursion_iterations);
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
        let query = crate::parser::parse_program(program)?;
        self.run_program(query, rule_set, output, session_facts, timing_mode)
    }

    /// `run` for an already parsed program.
    fn run_program(
        &self,
        query: Program,
        rule_set: RuleSet,
        output: Output,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, RelationMap, Option<TimingBreakdown>), String> {
        self.evaluate_program(query, rule_set, output, session_facts, timing_mode, true)
    }

    /// `run_program`, counting a rule evaluation only when `counted`: a
    /// read's internal re-run (the provenance baseline) is not another read.
    fn evaluate_program(
        &self,
        query: Program,
        rule_set: RuleSet,
        output: Output,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
        counted: bool,
    ) -> Result<(Vec<Tuple>, RelationMap, Option<TimingBreakdown>), String> {
        let start = Instant::now();
        let session_fact_count = session_facts.len();
        let (mut engine, combined) =
            self.prepare(query, rule_set, output, session_facts, timing_mode)?;
        let rules = combined.program.rules.len();
        if counted && combined.evaluates_rules {
            crate::execution::view_counters().record_rule_evaluation();
        }
        let result = engine.execute_program_profiled(combined.program);
        info!(
            rules,
            session_facts = session_fact_count,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "snapshot_execute"
        );
        result
    }

    /// Evaluate the parsed `query` with persistent rules, returning
    /// arbitrary-arity tuples. Callers that build or bind a program as a
    /// syntax tree evaluate it here, without writing it back into IQL text.
    pub fn execute_program_with_rules(&self, query: Program) -> Result<Vec<Tuple>, String> {
        self.run_program(
            query,
            RuleSet::WithPersistent,
            Output::Result,
            Vec::new(),
            TimingMode::Off,
        )
        .map(|(tuples, _, _)| tuples)
    }

    /// [`Self::execute_program_with_rules`] without counting a rule
    /// evaluation: for re-running part of a read already counted.
    pub fn execute_program_with_rules_uncounted(
        &self,
        query: Program,
    ) -> Result<Vec<Tuple>, String> {
        self.evaluate_program(
            query,
            RuleSet::WithPersistent,
            Output::Result,
            Vec::new(),
            TimingMode::Off,
            false,
        )
        .map(|(tuples, _, _)| tuples)
    }

    /// [`Self::execute_with_session_facts_profiled`] for a parsed `query`;
    /// no session facts evaluates with persistent rules only.
    pub fn execute_program_with_session_facts_profiled(
        &self,
        query: Program,
        session_facts: Vec<(String, Tuple)>,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, Option<TimingBreakdown>), String> {
        self.run_program(
            query,
            RuleSet::WithPersistent,
            Output::Result,
            session_facts,
            timing_mode,
        )
        .map(|(tuples, _, timing)| (tuples, timing))
    }

    /// Execute a query (without persistent rules) returning binary tuples
    pub fn execute(&self, program: &str) -> Result<Vec<(i32, i32)>, String> {
        Ok(Self::to_pairs(&self.execute_tuples(program)?))
    }

    /// Execute a query with persistent rules, returning binary tuples
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

    /// Execute a query with rules, returning tuples AND all derived relation data.
    ///
    /// Used by the provenance system to pass the derived tuples this
    /// evaluation computed to the backward chainer, avoiding a second
    /// derivation.
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

    /// [`Self::execute_with_rules_tuples_profiled`] reusing the plan compiled
    /// for `program` on any snapshot of the same rules, compiling and keeping
    /// it on a miss. For programs evaluated again and again (standing
    /// queries); a hit's timing breakdown has no compile stages. The
    /// [`CachedRun`] tells whether the plan came from the cache and how long
    /// executing it took, whatever the timing mode.
    pub fn execute_with_rules_tuples_cached(
        &self,
        program: &str,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, Option<TimingBreakdown>, CachedRun), String> {
        self.execute_cached(
            program,
            || crate::parser::parse_program(program),
            timing_mode,
        )
    }

    /// [`Self::execute_with_rules_tuples_cached`] for the parsed `query`,
    /// whose plan is kept under `key` (its text): a miss compiles `query`
    /// itself, never `key`.
    pub fn execute_program_with_rules_cached(
        &self,
        key: &str,
        query: Program,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, Option<TimingBreakdown>, CachedRun), String> {
        self.execute_cached(key, || Ok(query), timing_mode)
    }

    /// Run the plan kept under `key`, compiling `query` into it on a miss.
    fn execute_cached(
        &self,
        program: &str,
        query: impl FnOnce() -> Result<Program, String>,
        timing_mode: TimingMode,
    ) -> Result<(Vec<Tuple>, Option<TimingBreakdown>, CachedRun), String> {
        let start = Instant::now();
        let (plan, compiled_now) = match self.persistent.plan(program, &self.optimization) {
            Some(plan) => (plan, false),
            None => {
                let combined = self.combine(query()?, RuleSet::WithPersistent)?;
                let mut engine = self.new_engine();
                engine.set_timing_mode(timing_mode);
                let plan = Arc::new(CachedPlan {
                    optimization: self.optimization.clone(),
                    compiled: engine.compile_program(combined.program)?,
                    relations: combined.relations,
                    evaluates_rules: combined.evaluates_rules,
                });
                self.persistent.remember(program, Arc::clone(&plan));
                (plan, true)
            }
        };
        let executing = Instant::now();
        let mut engine = self.new_engine();
        engine.set_timing_mode(timing_mode);
        engine.set_inputs(self.inputs(plan.relations.iter().map(String::as_str)));
        if plan.evaluates_rules {
            crate::execution::view_counters().record_rule_evaluation();
        }
        let (tuples, _, mut timing) = engine.execute_compiled_profiled(&plan.compiled)?;
        let executing = executing.elapsed();
        if let (true, Some(timing)) = (compiled_now, timing.as_mut()) {
            let compile = plan.compiled.compile_timing();
            timing.parse_us = compile.parse_us;
            timing.sip_us = compile.sip_us;
            timing.magic_sets_us = compile.magic_sets_us;
            timing.ir_build_us = compile.ir_build_us;
            timing.optimize_us = compile.optimize_us;
            timing.total_us = start.elapsed().as_micros() as u64;
        }
        info!(
            program_len = program.len(),
            plan_cached = !compiled_now,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "snapshot_execute_cached"
        );
        let run = CachedRun {
            plan_cached: !compiled_now,
            executing,
        };
        Ok((tuples, timing, run))
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
}

impl std::fmt::Debug for KnowledgeGraphSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KnowledgeGraphSnapshot")
            .field("revision", &self.revision)
            .field("timestamp", &self.timestamp)
            .field("relations", &self.relation_count())
            .field("tuples", &self.tuple_count())
            .field("rules", &self.rules.len())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod plan_cache_tests;

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

    #[test]
    fn test_snapshot_evaluates_persistent_rules_on_read() {
        use crate::ast::{Atom, BodyPredicate, Rule, Term};

        let mut input_tuples = HashMap::new();
        input_tuples.insert(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
        );

        // path(X, Y) <- edge(X, Y)
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

        let snapshot = KnowledgeGraphSnapshot::new(input_tuples, vec![rule]);
        assert!(!snapshot.input_tuples.contains_key("path"));
        let results = snapshot
            .execute_with_rules_tuples("result(X, Y) <- path(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 2);
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

    // === Additional Coverage ===

    #[test]
    fn test_snapshot_with_workers() {
        let snapshot = KnowledgeGraphSnapshot::new_with_workers(RelationMap::new(), Vec::new(), 4);
        assert_eq!(snapshot.num_workers, 4);
        assert!(snapshot.is_empty());
    }

    #[test]
    fn test_snapshot_revision_increases() {
        let s1 = KnowledgeGraphSnapshot::empty();
        let s2 = KnowledgeGraphSnapshot::empty();
        assert!(s1.revision > 0);
        assert!(s2.revision > s1.revision);
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
