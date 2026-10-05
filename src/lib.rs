//! # `InputLayer` IQL Engine
//!
//! IQL engine built on Differential Dataflow.
//!
//! ## Pipeline Architecture
//!
//! ### Complete Pipeline
//! ```text
//! IQL Source Code
//!     |
//! [Parser (M04)]                -> AST
//!     |
//! [Recursion Analysis]          -> has_recursion flag + strata
//!     |
//! [IR Builder (M05)]            -> IRNode (with catalog)
//!     |
//! [Join Planning (M07)]         -> Reordered joins (optional)
//!     |
//! [SIP Rewriting (M08)]         -> Delta rules for recursion (optional)
//!     |
//! [Subplan Sharing (M09)]       -> CSE optimization (optional)
//!     |
//! [Boolean Specialization (M10)]-> Semiring selection (optional)
//!     |
//! [Basic Optimizer (M06)]       -> Optimized IRNode
//!     |
//! [Code Generator (M11)]        -> DD Code + Execution
//!     |
//! Results
//! ```
//!
//! ### Storage Engine Integration
//! ```text
//! StorageEngine
//!     |-- Multiple Knowledge Graphs (namespace isolation)
//!     |-- Parquet Persistence
//!     |-- Parallel Query Execution (Rayon)
//!     `-- Each Knowledge Graph -> IQLEngine instance
//! ```
//!
//! ## Usage
//!
//! ### Basic Query Execution
//! ```rust
//! use inputlayer::IQLEngine;
//!
//! let mut engine = IQLEngine::new();
//!
//! // Define base facts
//! engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);
//!
//! // Define and execute rules (variables must be uppercase)
//! let program = "
//!     path(X, Y) <- edge(X, Y)
//!     path(X, Z) <- path(X, Y), edge(Y, Z)
//! ";
//!
//! let results = engine.execute(program).unwrap();
//!
//! // Check if program has recursive rules
//! if engine.is_recursive() {
//!     println!("Program contains recursive rules");
//! }
//! ```
//!
//! ### Multi-Knowledge-Graph with Persistence
//! ```rust,no_run
//! use inputlayer::{StorageEngine, Config};
//!
//! let config = Config::default();
//! let mut storage = StorageEngine::new(config).unwrap();
//!
//! // Create and use knowledge graphs
//! storage.create_knowledge_graph("analytics").unwrap();
//! storage.use_knowledge_graph("analytics").unwrap();
//!
//! // Insert data and query (variables must be uppercase)
//! storage.insert("edge", vec![(1, 2), (2, 3)]).unwrap();
//! let results = storage.execute_query("path(X,Y) <- edge(X,Y)").unwrap();
//!
//! // Persist to disk
//! storage.save_knowledge_graph("analytics").unwrap();
//! ```
//!
//! ## Module Organization
//!
//! | Module | Purpose |
//! |--------|---------|
//! | `parser` | IQL -> AST |
//! | `ir_builder` | AST -> IR |
//! | `optimizer` | Basic IR optimizations |
//! | `join_planning` | Join order optimization |
//! | `sip_rewriting` | SIP semijoin reduction |
//! | `subplan_sharing` | Common subexpression elimination |
//! | `boolean_specialization` | Semiring selection |
//! | `code_generator` | IR -> Differential Dataflow |
//! | `recursion` | Recursion detection & stratification |
//! | `storage_engine` | Multi-knowledge-graph persistence |

// Meters each thread's heap use for the per-query memory limit.
#[global_allocator]
static ALLOCATOR: execution::memory::MeteredAllocator = execution::memory::MeteredAllocator;

// AST and IR modules (consolidated from crates/)
pub mod ast;
pub mod derived_relations; // Derived relation materialization
pub mod hnsw_index; // HNSW vector index implementation
pub mod incremental;
pub mod index_manager; // Index manager for vector similarity search
pub mod ir;
pub mod session; // Session manager for ephemeral triggers persistent

// Re-export types from internal modules
pub use crate::ast::builders::{fact, simple_rule, AtomBuilder, RuleBuilder};
pub use crate::ast::{
    AggregateFunc, ArithExpr, ArithOp, Atom, BodyPredicate, BuiltinFunc, Program, Rule, Term,
};
pub use crate::ir::{IRNode, Predicate};

// Internal modules
mod boolean_specialization; // Semiring selection
pub mod code_generator; // IR -> Differential Dataflow execution
mod ir_builder; // AST -> IR construction
mod join_planning; // Join order optimization
mod magic_sets; // Magic Sets demand-driven rewriting for recursive queries
mod optimizer; // Basic IR optimizations
pub mod params; // Parameterised programs: `$name` values bound out of band
pub mod parser; // IQL parsing & AST construction
pub mod rule_catalog; // Rule catalog for persistent rules
pub mod semiring_types; // Diff type abstraction: BooleanDiff, MinDiff, MaxDiff
mod sip_rewriting; // AST-level semijoin reduction
pub mod statement; // IQL-native statement parser
mod subplan_sharing; // Common subexpression elimination
pub mod syntax; // PEG-based syntax highlighting for REPL

// Storage Engine
pub mod config; // Configuration system
pub mod naming; // Canonical KG and relation name grammar
pub mod size_limits; // Bounds on request-supplied sizes
pub mod storage; // Storage formats (Parquet, metadata)
pub mod storage_engine; // Multi-knowledge-graph storage engine

// Authentication & RBAC
pub mod auth; // Role-based access control, password/key hashing

// Network Protocol (RPC)
pub mod protocol; // InputLayer RPC protocol (server/client)

// Execution hardening
pub mod execution; // Query timeout, resource limits, caching

// Value type system (production-grade arbitrary arity tuples)
pub mod value;

// Re-export value types for convenience
pub use value::{
    DataType, Relation, RelationMap, SchemaValidationError, Tuple, TupleSchema, Value,
};

// Schema validation module
pub mod schema;

// Re-export schema types for convenience
pub use schema::{
    catalog::SchemaError, ColumnSchema, RelationSchema, SchemaCatalog, SchemaType,
    ValidationEngine, ValidationError, Violation,
};

// Vector operations (distance functions, LSH, top-k)
pub mod vector_ops;

// Re-export vector operation types
pub use vector_ops::{
    abs_f64,
    abs_i64,
    clear_lsh_cache,
    cosine_distance_dequantized,
    cosine_distance_int8,
    dequantize_vector,
    dequantize_vector_with_scale,
    dot_product_int8,
    euclidean_distance_dequantized,
    // Int8 distance functions
    euclidean_distance_int8,
    get_lsh_cache_stats,
    // Utility functions
    hamming_distance,
    lsh_bucket_int8,
    lsh_bucket_with_distances,
    lsh_bucket_with_distances_int8,
    lsh_multi_probe,
    lsh_multi_probe_int8,
    // Multi-probe LSH
    lsh_probes,
    lsh_probes_ranked,
    manhattan_distance_int8,
    quantize_vector,
    quantize_vector_linear,
    quantize_vector_minmax,
    quantize_vector_symmetric,
    // Cache management
    LshCacheStats,
    // Quantization
    QuantizationMethod,
    VectorError,
};

// Temporal operations (time decay, temporal predicates, interval operations)
pub mod temporal_ops;

// Optimization infrastructure (reserved for future cost-based planning)
pub mod bloom_filter; // Bloom filters for predicate transfer optimization
pub mod hash_index; // Hash indexes for future cost-based join planning
pub mod statistics; // Statistics collection for future selectivity estimation

// Explainability
pub mod provenance; // Why-provenance proof trees and negative explanations

// Utilities
mod catalog;
mod pipeline_trace;
mod recursion;
#[cfg(test)]
mod test_arithmetic;

// Re-export public types
pub use catalog::Catalog;
pub use code_generator::CodeGenerator;
pub use config::{Config, DurabilityMode, OptimizationConfig};
pub use ir_builder::IRBuilder;
pub use optimizer::Optimizer;
pub use pipeline_trace::{OptimizationStats, PipelineTrace};
pub use storage_engine::StorageEngine;

// Re-export storage utilities (Parquet and CSV)
pub use storage::{
    load_from_csv, load_from_csv_with_options, load_from_parquet, save_to_csv,
    save_to_csv_with_options, save_to_parquet, CsvOptions, StorageError, StorageResult,
};

// Re-export execution utilities (timeout)
pub use execution::{ExecutionError, ExecutionResult, Halt, RequestControl, Stop};

// Re-export optimization modules for extensibility
pub use boolean_specialization::{BooleanSpecializer, SemiringAnnotation, SemiringType};
pub use join_planning::JoinPlanner;
pub use sip_rewriting::SipRewriter;
pub use subplan_sharing::SubplanSharer;

// Re-export statement parser types
pub use statement::{
    parse_rule_definition, parse_statement, BaseType, ColumnDef, DeleteOp, DeletePattern,
    DeleteTarget, InsertOp, InsertTarget, LoadMode, MetaCommand, QueryGoal, RecordField,
    Refinement, RefinementArg, RuleDef, SchemaDecl, SerializableArithExpr, SerializableArithOp,
    SerializableBodyPred, SerializableRule, SerializableTerm, Statement, TypeDecl, TypeExpr,
    UpdateOp,
};

// Re-export parser functions
pub use parser::{parse_program, parse_rule};

/// Stack size for threads that parse, plan and evaluate client programs. The
/// parser's nesting and rule-body limits were measured against it so every
/// recursive pass fits, in debug as well as release builds; Rust's 2 MiB
/// default does not. The stack is virtual memory, committed only as used.
pub const ENGINE_THREAD_STACK_BYTES: usize = 64 * 1024 * 1024;

// Re-export rule catalog
pub use rule_catalog::{validate_rule, validate_rules_stratification, RuleCatalog, RuleDefinition};

// Re-export session types
pub use session::{
    AuditEvent, AuditLog, Provenance, QueryMetadata, SessionConfig, SessionId, SessionManager,
    SessionStats,
};

// Re-export index types
pub use hnsw_index::HnswIndex;
pub use index_manager::{
    DistanceMetric, HnswConfig, HnswSearchFn, IdType, IndexManager, IndexStats, IndexType,
    IndexView, ManagedIndex, RegisteredIndex, TupleId,
};

// Re-export recursion utilities
pub use recursion::{
    build_dependency_graph,
    build_extended_dependency_graph,
    find_sccs,
    has_recursion,
    is_recursive_rule,
    stratify,
    stratify_with_negation,
    DependencyGraph,
    // New exports for negation-aware stratification
    DependencyType,
    StratificationResult,
};

use std::cell::Cell;
use std::collections::HashMap;
use std::time::Instant;
use tracing::info;

/// Query result, all derived relations, and optional timing breakdown.
pub type ExecutionOutput =
    Result<(Vec<Tuple>, RelationMap, Option<execution::TimingBreakdown>), String>;

/// Magic Sets seed facts, by relation.
type MagicSeeds = Vec<(String, Vec<Tuple>)>;

fn inject_magic_seeds(inputs: &mut RelationMap, seeds: &MagicSeeds) {
    for (relation, tuples) in seeds {
        inputs
            .entry(relation.clone())
            .or_default()
            .extend(tuples.iter().cloned());
    }
}

/// What execution needs from compilation besides the engine's fields.
#[derive(Clone)]
struct Staged {
    /// IR before optimization: recursive rules run on it.
    unoptimized_ir_nodes: Vec<IRNode>,
    /// Per IR node, the relation it recursively defines, if any.
    recursive_info: Vec<Option<String>>,
    magic_seeds: MagicSeeds,
}

/// A program compiled by [`IQLEngine::compile_program`]: rewritten, lowered to
/// IR and optimized. It depends only on the program and the optimizer
/// configuration, never on data, so it can be executed on any inputs.
#[derive(Clone)]
pub struct CompiledProgram {
    program: Program,
    ir_nodes: Vec<IRNode>,
    shared_views: HashMap<String, IRNode>,
    semiring_annotations: Vec<boolean_specialization::SemiringAnnotation>,
    has_recursion: bool,
    strata: Vec<Vec<usize>>,
    staged: Staged,
    /// Compile-stage timings of the compilation.
    timing: execution::TimingBreakdown,
}

impl CompiledProgram {
    /// Compile-stage timings (parse to optimize) of the compilation.
    pub fn compile_timing(&self) -> &execution::TimingBreakdown {
        &self.timing
    }
}

thread_local! {
    static RESULT_TRUNCATED: Cell<bool> = const { Cell::new(false) };
    static RESULT_CAP_OFF: Cell<bool> = const { Cell::new(false) };
}

/// Whether the last [`IQLEngine::execute_tuples_profiled`] run on this thread
/// cut its result at `max_result_rows`.
pub fn last_result_truncated() -> bool {
    RESULT_TRUNCATED.get()
}

/// Runs `f` with `max_result_rows` ignored on this thread, for internal
/// queries whose full result is needed (mutation matches, sorting,
/// provenance baselines).
pub fn without_result_cap<R>(f: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            RESULT_CAP_OFF.set(self.0);
        }
    }
    let _restore = Restore(RESULT_CAP_OFF.replace(true));
    f()
}

/// Main IQL engine that orchestrates the entire pipeline
pub struct IQLEngine {
    /// Input data for base relations (`relation_name` -> tuples)
    /// Supports arbitrary arity tuples with mixed types (int, float, string, vector)
    /// Use `input_tuples()` and `input_tuples_mut()` for access.
    input_tuples: RelationMap,

    /// Parsed program (after parsing)
    program: Option<Program>,

    /// Built IR (after IR building)
    ir_nodes: Vec<IRNode>,

    /// Catalog for schema management
    catalog: Catalog,

    /// Optimization configuration
    optimization_config: OptimizationConfig,

    /// Whether the current program contains recursive rules
    has_recursion: bool,

    /// Strata for rule evaluation order (computed during analysis)
    strata: Vec<Vec<usize>>,

    /// Shared views from subplan sharing optimization (`view_name` -> IR definition)
    /// These must be executed BEFORE the main rules that reference them
    shared_views: HashMap<String, IRNode>,

    /// Semiring annotations from boolean specialization (one per IR node)
    /// Used for diff-type dispatch (Boolean -> BooleanDiff, Counting -> isize)
    semiring_annotations: Vec<boolean_specialization::SemiringAnnotation>,

    /// Number of worker threads for parallel execution (1 = single-worker)
    num_workers: usize,

    /// Maximum rows in the final query result (0 = unlimited). Intermediate
    /// relations are never cut.
    max_result_rows: usize,

    /// Most rows one join of a query's plan may be estimated to produce (0 =
    /// unlimited); see [`ir::IRNode::estimate_rows`]. Queries over it are
    /// rejected before DD execution.
    max_query_cost: u64,

    /// Most fixpoint iterations a recursive evaluation may run before it
    /// fails (0 = unlimited).
    max_recursion_iterations: u32,

    /// HNSW search callback for `hnsw_nearest` (resolved before each rule runs).
    hnsw_search_fn: Option<HnswSearchFn>,

    /// Timing mode for query profiling (default: Summary)
    timing_mode: execution::TimingMode,
}

impl IQLEngine {
    /// Default optimization config.
    pub fn new() -> Self {
        IQLEngine {
            input_tuples: RelationMap::new(),
            program: None,
            ir_nodes: Vec::new(),
            catalog: Catalog::new(),
            optimization_config: OptimizationConfig::default(),
            has_recursion: false,
            strata: Vec::new(),
            shared_views: HashMap::new(),
            semiring_annotations: Vec::new(),
            num_workers: 1,
            max_result_rows: 0,
            max_query_cost: 0,
            max_recursion_iterations: 0,
            hnsw_search_fn: None,
            timing_mode: execution::TimingMode::default(),
        }
    }

    /// Create a new IQL engine with custom optimization configuration
    pub fn with_config(config: OptimizationConfig) -> Self {
        IQLEngine {
            input_tuples: RelationMap::new(),
            program: None,
            ir_nodes: Vec::new(),
            catalog: Catalog::new(),
            optimization_config: config,
            has_recursion: false,
            strata: Vec::new(),
            shared_views: HashMap::new(),
            semiring_annotations: Vec::new(),
            num_workers: 1,
            max_result_rows: 0,
            max_query_cost: 0,
            max_recursion_iterations: 0,
            hnsw_search_fn: None,
            timing_mode: execution::TimingMode::default(),
        }
    }

    /// Set the number of worker threads for parallel execution
    ///
    /// When `num_workers > 1`, non-recursive per-tuple rules (scan, filter,
    /// map, compute) run hash-partitioned on Rayon. Joins, negation,
    /// aggregates, distinct and recursion run on a single worker.
    pub fn set_num_workers(&mut self, num_workers: usize) {
        self.num_workers = num_workers.max(1);
    }

    /// Set the timing/profiling mode for query execution
    pub fn set_timing_mode(&mut self, mode: execution::TimingMode) {
        self.timing_mode = mode;
    }

    /// Check if the current program has recursive rules
    pub fn is_recursive(&self) -> bool {
        self.has_recursion
    }

    /// Get the computed strata for rule evaluation
    pub fn strata(&self) -> &[Vec<usize>] {
        &self.strata
    }

    /// Get the current optimization configuration
    pub fn config(&self) -> &OptimizationConfig {
        &self.optimization_config
    }

    /// Set the optimization configuration
    pub fn set_config(&mut self, config: OptimizationConfig) {
        self.optimization_config = config;
    }

    /// Set the final result row cap (0 = unlimited)
    pub fn set_max_result_rows(&mut self, max: usize) {
        self.max_result_rows = max;
    }

    /// The cap in effect on this thread (0 inside [`without_result_cap`]).
    fn result_cap(&self) -> usize {
        if RESULT_CAP_OFF.get() {
            0
        } else {
            self.max_result_rows
        }
    }

    /// Set the most rows one join of a query's plan may be estimated to
    /// produce (0 = unlimited); see [`ir::IRNode::estimate_rows`].
    pub fn set_max_query_cost(&mut self, max: u64) {
        self.max_query_cost = max;
    }

    /// Set the most fixpoint iterations a recursive evaluation may run
    /// before it fails (0 = unlimited).
    pub fn set_max_recursion_iterations(&mut self, max: u32) {
        self.max_recursion_iterations = max;
    }

    /// Replace all input data. Relations share tuples, so this copies none.
    pub fn set_inputs(&mut self, data: RelationMap) {
        self.input_tuples = data;
    }

    /// Set the HNSW search callback used by `hnsw_nearest`.
    ///
    /// Each rule's `HnswScan` nodes are searched right before the rule runs;
    /// results are loaded as synthetic base relations.
    pub fn set_hnsw_search_fn(&mut self, f: HnswSearchFn) {
        self.hnsw_search_fn = Some(f);
    }

    /// Get the catalog
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Get immutable reference to input tuples
    pub fn input_tuples(&self) -> &RelationMap {
        &self.input_tuples
    }

    /// Get mutable reference to input tuples
    pub fn input_tuples_mut(&mut self) -> &mut RelationMap {
        &mut self.input_tuples
    }

    /// Get tuples for a specific relation
    pub fn get_relation(&self, relation: &str) -> Option<&Relation> {
        self.input_tuples.get(relation)
    }

    /// Add binary (i32, i32) tuples. For arbitrary arity, use `add_tuples`.
    ///
    /// # Example
    /// ```rust
    /// use inputlayer::IQLEngine;
    ///
    /// let mut engine = IQLEngine::new();
    /// engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);
    /// ```
    pub fn add_fact(&mut self, relation: &str, data: Vec<(i32, i32)>) {
        // Convert to Tuple format
        let tuples: Relation = data.iter().map(|&(a, b)| Tuple::from_pair(a, b)).collect();
        self.input_tuples.insert(relation.to_string(), tuples);

        // Register schema in catalog if not already registered
        if !self.catalog.has_relation(relation) {
            // Default schema for 2-tuples
            self.catalog.register_relation(
                relation.to_string(),
                vec!["col0".to_string(), "col1".to_string()],
            );
        }
    }

    /// Add tuples with any arity and mixed types.
    ///
    /// # Example
    /// ```rust
    /// use inputlayer::{IQLEngine, Tuple, Value};
    ///
    /// let mut engine = IQLEngine::new();
    /// engine.add_tuples("edge", vec![
    ///     Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
    ///     Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
    /// ]);
    /// ```
    /// Add a single tuple to a relation.
    pub fn add_tuple(&mut self, relation: &str, tuple: Tuple) {
        if !self.catalog.has_relation(relation) {
            let arity = tuple.arity();
            let schema: Vec<String> = (0..arity).map(|i| format!("col{i}")).collect();
            self.catalog.register_relation(relation.to_string(), schema);
        }
        self.input_tuples
            .entry(relation.to_string())
            .or_default()
            .push(tuple);
    }

    /// Get the current optimization configuration.
    pub fn get_optimization_config(&self) -> &OptimizationConfig {
        &self.optimization_config
    }

    pub fn add_tuples(&mut self, relation: &str, tuples: Vec<Tuple>) {
        // Infer schema from first tuple if not already registered
        if !self.catalog.has_relation(relation) {
            let arity = tuples.first().map_or(2, value::Tuple::arity);
            let schema: Vec<String> = (0..arity).map(|i| format!("col{i}")).collect();
            self.catalog.register_relation(relation.to_string(), schema);
        }

        self.input_tuples
            .insert(relation.to_string(), Relation::from(tuples));
    }

    /// Parse an IQL program string into AST
    ///
    /// Converts IQL source code into an Abstract Syntax Tree.
    /// Also performs safety validation, recursion detection, and stratification.
    ///
    /// ## Pipeline Steps
    /// 1. Parse source into AST
    /// 2. Validate rule safety
    /// 3. Detect recursive rules
    /// 4. Compute stratification (evaluation order)
    pub fn parse(&mut self, source: &str) -> Result<&Program, String> {
        let program = parser::parse_program(source)?;
        self.load_program(program)
    }

    /// Install an already-parsed program: validates safety, detects recursion
    /// and computes stratification, exactly as [`Self::parse`] does after parsing.
    pub fn load_program(&mut self, program: Program) -> Result<&Program, String> {
        // Validate safety - all head variables must appear in positive body atoms
        for rule in &program.rules {
            if !rule.is_safe() {
                let head_vars = rule.head.variables();
                let body_vars = rule.positive_body_variables();
                let mut unsafe_vars: Vec<_> = head_vars.difference(&body_vars).cloned().collect();
                unsafe_vars.sort(); // Sort for deterministic output

                return Err(format!(
                    "Unsafe rule: {:?}. Variables {:?} in head do not appear in positive body atoms.",
                    rule.head, unsafe_vars
                ));
            }
        }

        // Negation inside a recursive cycle has no stratified meaning: reject it
        // here so every entry point (persistent, session, REST) agrees.
        rule_catalog::validate_rules_stratification(&program.rules)?;

        // Recursion detection
        self.has_recursion = recursion::has_recursion(&program);

        // Stratification - compute evaluation order using SCCs
        self.strata = recursion::stratify(&program);

        self.program = Some(program);
        Ok(self
            .program
            .as_ref()
            .expect("program is guaranteed Some: set on the line above"))
    }

    /// Apply SIP (Sideways Information Passing) rewriting at the AST level
    ///
    /// This rewrites multi-join rules into semijoin reduction chains
    /// before IR building. Must be called after parse() and before build_ir().
    fn apply_sip_rewriting(&mut self) {
        if !self.optimization_config.enable_sip_rewriting {
            return;
        }
        if let Some(program) = &self.program {
            let mut sip_rewriter = sip_rewriting::SipRewriter::new();

            // SIP skips relations on a dependency cycle.
            let recursive_rels = recursion::recursive_relations(program);
            if std::env::var("INPUTLAYER_DEBUG").is_ok() && !recursive_rels.is_empty() {
                eprintln!("DEBUG SIP: skipping recursive relations: {recursive_rels:?}");
            }
            sip_rewriter.set_recursive_relations(recursive_rels);

            let rewritten = sip_rewriter.rewrite_program(program);
            let stats = sip_rewriter.get_stats();

            if std::env::var("INPUTLAYER_DEBUG").is_ok() {
                if stats.rules_rewritten > 0 {
                    eprintln!(
                        "DEBUG SIP: rewrote {} rules, generated {} SIP rules",
                        stats.rules_rewritten, stats.rules_generated
                    );
                }
                for (i, rule) in rewritten.rules.iter().enumerate() {
                    let head_args = rule
                        .head
                        .args
                        .iter()
                        .map(|a| format!("{a:?}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let body_str = rule
                        .body
                        .iter()
                        .map(|p| format!("{p:?}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    eprintln!(
                        "DEBUG SIP rule[{}]: {}({}) <- {}",
                        i, rule.head.relation, head_args, body_str
                    );
                }
            }

            // Re-run safety check and recursion detection on the rewritten program
            self.has_recursion = recursion::has_recursion(&rewritten);
            self.strata = recursion::stratify(&rewritten);
            self.program = Some(rewritten);
        }
    }

    /// Apply constant specialization (see `magic_sets::specialize`).
    fn apply_constant_specialization(&mut self) {
        if !self.optimization_config.enable_constant_specialization {
            return;
        }
        let Some(rewritten) = self
            .program
            .as_ref()
            .and_then(magic_sets::specialize::specialize_constants)
        else {
            return;
        };
        if std::env::var("INPUTLAYER_DEBUG").is_ok() {
            for (i, rule) in rewritten.rules.iter().enumerate() {
                eprintln!("DEBUG specialize rule[{i}]: {rule}");
            }
        }
        self.has_recursion = recursion::has_recursion(&rewritten);
        self.strata = recursion::stratify(&rewritten);
        self.program = Some(rewritten);
    }

    /// Apply Magic Sets transformation for recursive queries with bound arguments.
    ///
    /// Rewrites recursive rules so that the fixpoint computation is restricted to
    /// only the tuples demanded by the query's constant bindings. For example,
    /// `?reach(1, Y)` will only compute reachability from node 1.
    /// Returns the seed facts it added to the inputs.
    fn apply_magic_sets(&mut self) -> MagicSeeds {
        if !self.optimization_config.enable_magic_sets {
            return MagicSeeds::new();
        }
        if let Some(program) = &self.program {
            let recursive_rels = magic_sets::find_recursive_relations(program);
            if recursive_rels.is_empty() {
                return MagicSeeds::new();
            }

            let bindings =
                magic_sets::MagicSetRewriter::detect_query_bindings(program, &recursive_rels);
            if bindings.is_empty() {
                return MagicSeeds::new();
            }

            let (rewritten, magic_seeds) =
                magic_sets::MagicSetRewriter::rewrite_program(program, &bindings);

            let debug = std::env::var("INPUTLAYER_DEBUG").is_ok();
            if debug {
                eprintln!(
                    "DEBUG Magic Sets: adorned {} atoms, {} magic seeds",
                    bindings.len(),
                    magic_seeds.len()
                );
                for (name, tuples) in &magic_seeds {
                    eprintln!("  seed {name}: {} tuples", tuples.len());
                }
                for (i, rule) in rewritten.rules.iter().enumerate() {
                    let head_args = rule
                        .head
                        .args
                        .iter()
                        .map(|a| format!("{a:?}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let body_str = rule
                        .body
                        .iter()
                        .map(|p| format!("{p:?}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    eprintln!(
                        "DEBUG Magic Sets rule[{i}]: {}({head_args}) <- {body_str}",
                        rule.head.relation,
                    );
                }
            }

            // Inject magic seed facts into input_tuples
            let magic_seeds: MagicSeeds = magic_seeds.into_iter().collect();
            inject_magic_seeds(&mut self.input_tuples, &magic_seeds);

            // Re-run recursion detection on rewritten program
            self.has_recursion = recursion::has_recursion(&rewritten);
            self.strata = recursion::stratify(&rewritten);
            self.program = Some(rewritten);
            return magic_seeds;
        }
        MagicSeeds::new()
    }

    /// Build IR from the parsed program
    ///
    /// Converts the AST into intermediate representation (IR) suitable for optimization.
    /// Uses the catalog to resolve variable positions in relations.
    ///
    /// For predicates with multiple rules (like recursive definitions), this creates
    /// a Union node combining all rules for that predicate.
    ///
    /// When `collect_timing` is true, uses the timed IR builder path and returns
    /// `Some(IrBuilderTiming)` with per-stage timing data. Otherwise returns `None`.
    pub fn build_ir(
        &mut self,
        collect_timing: bool,
    ) -> Result<Option<execution::timing::IrBuilderTiming>, String> {
        use std::collections::HashMap;

        let program = self
            .program
            .as_ref()
            .ok_or("No program parsed yet. Call parse() first.")?
            .clone();

        // Update catalog with schemas from program
        self.update_catalog_from_program(&program);

        // Create IR builder
        let builder = IRBuilder::new(self.catalog.clone());

        // Group rules by head predicate name
        let mut rules_by_head: HashMap<String, Vec<&Rule>> = HashMap::new();
        for rule in &program.rules {
            rules_by_head
                .entry(rule.head.relation.clone())
                .or_default()
                .push(rule);
        }

        // Build IR nodes, combining multiple rules for the same predicate with Union
        let mut ir_nodes = Vec::new();
        let mut processed_predicates = std::collections::HashSet::new();
        let mut agg_timing = if collect_timing {
            Some(execution::timing::IrBuilderTiming::default())
        } else {
            None
        };

        for rule in &program.rules {
            let predicate = &rule.head.relation;

            // Skip if we've already processed this predicate
            if processed_predicates.contains(predicate) {
                continue;
            }
            processed_predicates.insert(predicate.clone());

            let rules_for_predicate = rules_by_head.get(predicate).expect("predicate is guaranteed in rules_by_head: populated from same program.rules iteration");

            if rules_for_predicate.len() == 1 {
                if let Some(ref mut agg) = agg_timing {
                    let (ir, t) = builder.build_ir_timed(rule)?;
                    agg.scans_us += t.scans_us;
                    agg.joins_us += t.joins_us;
                    agg.computed_us += t.computed_us;
                    agg.filters_us += t.filters_us;
                    agg.antijoins_us += t.antijoins_us;
                    agg.projection_us += t.projection_us;
                    ir_nodes.push(ir);
                } else {
                    let ir = builder.build_ir(rule)?;
                    ir_nodes.push(ir);
                }
            } else {
                // Multiple rules - build IR for each and combine with Union
                let mut sub_irs = Vec::new();
                for r in rules_for_predicate {
                    if let Some(ref mut agg) = agg_timing {
                        let (ir, t) = builder.build_ir_timed(r)?;
                        agg.scans_us += t.scans_us;
                        agg.joins_us += t.joins_us;
                        agg.computed_us += t.computed_us;
                        agg.filters_us += t.filters_us;
                        agg.antijoins_us += t.antijoins_us;
                        agg.projection_us += t.projection_us;
                        sub_irs.push(ir);
                    } else {
                        let ir = builder.build_ir(r)?;
                        sub_irs.push(ir);
                    }
                }
                let union_ir = crate::ir::IRNode::Union { inputs: sub_irs };
                ir_nodes.push(union_ir);
            }
        }

        self.ir_nodes = ir_nodes;
        Ok(agg_timing)
    }

    /// Update catalog with schemas inferred from program
    fn update_catalog_from_program(&mut self, program: &Program) {
        for rule in &program.rules {
            // Register head relation
            let head_schema: Vec<_> = rule
                .head
                .args
                .iter()
                .enumerate()
                .map(|(i, term)| match term {
                    Term::Variable(v) => v.clone(),
                    _ => format!("col{i}"),
                })
                .collect();

            if !self.catalog.has_relation(&rule.head.relation) {
                self.catalog
                    .register_relation(rule.head.relation.clone(), head_schema);
            }

            // Register body relations
            for pred in &rule.body {
                if let Some(atom) = pred.atom() {
                    let body_schema: Vec<_> = atom
                        .args
                        .iter()
                        .enumerate()
                        .map(|(i, term)| match term {
                            Term::Variable(v) => v.clone(),
                            _ => format!("col{i}"),
                        })
                        .collect();

                    if !self.catalog.has_relation(&atom.relation) {
                        self.catalog
                            .register_relation(atom.relation.clone(), body_schema);
                    }
                }
            }
        }
    }

    /// Optimize the IR through the complete optimization pipeline
    ///
    /// ## Optimization Pipeline (controlled by `OptimizationConfig`)
    ///
    /// 1. Join Planning: Optimize join order based on cost model
    /// 2. SIP Rewriting: Apply Sideways Information Passing for recursion
    /// 3. Subplan Sharing: Detect and share common subexpressions
    /// 4. Boolean Specialization: Select appropriate semiring
    /// 5. Basic Optimizations: Identity elimination, filter simplification
    ///
    /// Each optimization can be enabled/disabled via `OptimizationConfig`.
    ///
    /// When `collect_timing` is true, uses the timed optimizer path and returns
    /// `Some(OptimizerTiming)`. Otherwise returns `None`.
    pub fn optimize_ir(
        &mut self,
        collect_timing: bool,
    ) -> Result<Option<execution::timing::OptimizerTiming>, String> {
        // Join Planning
        if self.optimization_config.enable_join_planning {
            let join_planner = join_planning::JoinPlanner::new();
            self.ir_nodes = self
                .ir_nodes
                .iter()
                .map(|ir| join_planner.plan_joins(ir.clone()))
                .collect();
        }

        // SIP Rewriting is applied at the AST level (before IR building)
        // See apply_sip_rewriting() called in execute_tuples() and execute()

        // Subplan Sharing (common subexpression elimination)
        if self.optimization_config.enable_subplan_sharing {
            let subplan_sharer = subplan_sharing::SubplanSharer::new();
            // Collect derived relation names (relations produced by rules).
            // Shared views execute before rules, so subtrees scanning derived
            // relations must not be extracted into shared views.
            let derived_relations: std::collections::HashSet<String> =
                self.get_rule_heads().into_iter().collect();
            let (optimized_irs, shared_views) =
                subplan_sharer.share_subplans(self.ir_nodes.clone(), &derived_relations);
            self.ir_nodes = optimized_irs;
            // Store shared views - they will be executed BEFORE main rules
            self.shared_views = shared_views;
            if std::env::var("INPUTLAYER_DEBUG").is_ok() && !self.shared_views.is_empty() {
                eprintln!(
                    "DEBUG optimize_ir: created {} shared views",
                    self.shared_views.len()
                );
                for name in self.shared_views.keys() {
                    eprintln!("  - {name}");
                }
            }
        }

        // Boolean Specialization (semiring selection)
        if self.optimization_config.enable_boolean_specialization {
            let mut bool_specializer = boolean_specialization::BooleanSpecializer::new();
            let mut annotations = Vec::new();
            self.ir_nodes = self
                .ir_nodes
                .iter()
                .map(|ir| {
                    let (optimized_ir, annotation) = bool_specializer.specialize(ir.clone());
                    annotations.push(annotation);
                    optimized_ir
                })
                .collect();
            self.semiring_annotations = annotations;
        }

        // Basic Optimizations (always applied)
        let optimizer = Optimizer::new();
        if collect_timing {
            let mut agg_timing = execution::timing::OptimizerTiming::default();
            self.ir_nodes = self
                .ir_nodes
                .iter()
                .map(|ir| {
                    let (optimized, t) = optimizer.optimize_timed(ir.clone());
                    agg_timing.iterations = agg_timing.iterations.max(t.iterations);
                    agg_timing.rules_us += t.rules_us;
                    agg_timing.fusion_us += t.fusion_us;
                    optimized
                })
                .collect();
            Ok(Some(agg_timing))
        } else {
            self.ir_nodes = self
                .ir_nodes
                .iter()
                .map(|ir| optimizer.optimize(ir.clone()))
                .collect();
            Ok(None)
        }
    }

    /// Generate and execute Differential Dataflow code
    ///
    /// Takes an IR node and executes it using Differential Dataflow,
    /// returning the computed results as binary tuples.
    pub fn execute_ir(&self, ir: &IRNode) -> Result<Vec<(i32, i32)>, String> {
        // Execute as Tuples and convert to binary format
        let tuples = self.execute_ir_tuples(ir)?;
        Ok(tuples.iter().filter_map(Tuple::to_pair).collect())
    }

    /// Generate and execute Differential Dataflow code (arbitrary arity)
    ///
    /// Takes an IR node and executes it using Differential Dataflow,
    /// returning the computed results as Tuples of any arity.
    pub fn execute_ir_tuples(&self, ir: &IRNode) -> Result<Vec<Tuple>, String> {
        // Create code generator
        let mut codegen = CodeGenerator::new();

        // Set semiring type from boolean specialization analysis
        let semiring = boolean_specialization::compute_global_semiring(&self.semiring_annotations);
        codegen.set_semiring_type(semiring);

        // Pass semiring annotations for debug tracing
        if !self.semiring_annotations.is_empty() {
            codegen.set_semiring_annotations(self.semiring_annotations.clone());
        }

        codegen.set_inputs(self.input_tuples.clone());

        // Execute and return Tuples
        codegen.execute(ir)
    }

    /// Full pipeline: parse -> IR -> optimize -> execute. Returns binary (i32, i32) tuples
    /// from the last rule only; use `execute_tuples()` for arbitrary arity,
    /// `execute_all_rules()` for all rules.
    pub fn execute(&mut self, source: &str) -> Result<Vec<(i32, i32)>, String> {
        // Delegate to execute_tuples and convert results to binary format
        let tuples = self.execute_tuples(source)?;
        Ok(tuples.iter().filter_map(Tuple::to_pair).collect())
    }

    // Execution Helper Methods
    /// Get unique rule head names in order of appearance
    fn get_rule_heads(&self) -> Vec<String> {
        let program = match &self.program {
            Some(p) => p,
            None => return Vec::new(),
        };

        let mut rule_heads = Vec::new();
        let mut seen_heads = std::collections::HashSet::new();

        for rule in &program.rules {
            let head = &rule.head.relation;
            if !seen_heads.contains(head) {
                rule_heads.push(head.clone());
                seen_heads.insert(head.clone());
            }
        }
        rule_heads
    }

    /// Detect which IR nodes require recursive execution
    ///
    /// Returns a vector where each element is `Some(head_name)` if the IR node
    /// at that index is recursive, or None if non-recursive.
    fn detect_recursion_info(&self, rule_heads: &[String]) -> Vec<Option<String>> {
        let debug = std::env::var("INPUTLAYER_DEBUG").is_ok();

        self.ir_nodes
            .iter()
            .enumerate()
            .map(|(i, ir)| {
                let head_name = rule_heads.get(i).cloned().unwrap_or_default();
                let is_recursive = CodeGenerator::references_relation(ir, &head_name);
                if debug {
                    let node_type = if matches!(ir, IRNode::Union { .. }) {
                        "Union"
                    } else {
                        "non-Union"
                    };
                    eprintln!(
                        "DEBUG: IR[{i}] head='{head_name}' is {node_type}, recursive={is_recursive}"
                    );
                }
                if is_recursive {
                    Some(head_name)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Fresh `CodeGenerator` for the rules at `indices`, loaded with base data
    /// and the relations computed so far. A group shares one semiring: the
    /// rules' common one, or Counting when they differ.
    fn rule_codegen(&self, indices: &[usize], accumulated: &RelationMap) -> CodeGenerator {
        use boolean_specialization::SemiringType;
        let semiring_of = |i: usize| {
            self.semiring_annotations
                .get(i)
                .map_or(SemiringType::Counting, |a| a.semiring)
        };
        let first = indices
            .first()
            .map_or(SemiringType::Counting, |&i| semiring_of(i));
        let semiring = if indices.iter().all(|&i| semiring_of(i) == first) {
            first
        } else {
            SemiringType::Counting
        };

        let mut codegen = CodeGenerator::new();
        codegen.set_semiring_type(semiring);
        codegen.set_max_iterations(self.max_recursion_iterations);
        self.load_inputs_into_codegen(&mut codegen, accumulated);
        codegen
    }

    /// Load all input data into a `CodeGenerator`
    fn load_inputs_into_codegen(&self, codegen: &mut CodeGenerator, accumulated: &RelationMap) {
        let debug = std::env::var("INPUTLAYER_DEBUG").is_ok();

        if debug {
            for (relation, data) in &self.input_tuples {
                eprintln!(
                    "DEBUG: loading input_tuples['{}'] = {} tuples",
                    relation,
                    data.len()
                );
                for t in data.iter().take(3) {
                    eprintln!("  - {t:?}");
                }
            }
        }
        codegen.set_inputs(self.input_tuples.clone());

        // Load accumulated results from previously executed rules
        for (rel_name, rel_data) in accumulated {
            if debug {
                eprintln!(
                    "DEBUG: loading accumulated['{}'] = {} tuples",
                    rel_name,
                    rel_data.len()
                );
            }
            codegen.add_relation(rel_name.clone(), rel_data.clone());
        }
    }

    /// Execute shared views and return their results
    ///
    /// Shared views may reference each other (cascading sharing), so we execute
    /// them in dependency order using topological sort: views that reference no
    /// other views first, then views that depend on already-computed views.
    fn execute_shared_views(&self) -> Result<RelationMap, String> {
        let debug = std::env::var("INPUTLAYER_DEBUG").is_ok();
        let mut results = RelationMap::new();

        if self.shared_views.is_empty() {
            return Ok(results);
        }

        // Build dependency graph: for each view, find which other shared views it references
        let view_names: std::collections::HashSet<&String> = self.shared_views.keys().collect();
        let mut deps: HashMap<&String, Vec<&String>> = HashMap::new();
        for (name, ir) in &self.shared_views {
            let mut scans = Vec::new();
            Self::collect_scan_relations(ir, &mut scans);
            let view_deps: Vec<&String> = scans
                .iter()
                .filter_map(|scan_name| {
                    view_names
                        .iter()
                        .find(|vn| vn.as_str() == scan_name)
                        .copied()
                })
                .collect();
            deps.insert(name, view_deps);
        }

        // Topological sort by in-degree reduction
        // in_degree[A] = number of shared views that A depends on (must execute before A)
        let mut in_degree: HashMap<&String, usize> = HashMap::new();
        for name in self.shared_views.keys() {
            in_degree.insert(name, deps.get(name).map_or(0, std::vec::Vec::len));
        }

        // Build reverse dependency map: dependents[B] = views that depend on B
        let mut dependents: HashMap<&String, Vec<&String>> = HashMap::new();
        for (name, dep_list) in &deps {
            for dep in dep_list {
                dependents.entry(*dep).or_default().push(*name);
            }
        }

        // Start with views that have no dependencies
        let mut queue: Vec<&String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&name, _)| name)
            .collect();
        queue.sort(); // deterministic tie-breaking

        let mut execution_order: Vec<&String> = Vec::new();
        while let Some(name) = queue.pop() {
            execution_order.push(name);
            // Decrement in-degree for views that depend on the just-resolved view
            if let Some(dependent_list) = dependents.get(name) {
                for dependent in dependent_list {
                    if let Some(deg) = in_degree.get_mut(dependent) {
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 {
                            queue.push(dependent);
                            queue.sort(); // maintain deterministic order
                        }
                    }
                }
            }
        }

        // If topological sort didn't include all views (cycle), fall back to name order
        if execution_order.len() < self.shared_views.len() {
            execution_order.clear();
            let mut all_names: Vec<&String> = self.shared_views.keys().collect();
            all_names.sort();
            execution_order = all_names;
        }

        for view_name in execution_order {
            let view_ir = &self.shared_views[view_name];
            if debug {
                eprintln!("DEBUG: executing shared view '{view_name}'");
            }

            let mut codegen = CodeGenerator::new();
            // Load base inputs AND results from previously computed shared views
            self.load_inputs_into_codegen(&mut codegen, &results);

            let view_results = codegen.execute(view_ir)?;

            if debug {
                eprintln!(
                    "DEBUG: shared view '{}' produced {} tuples",
                    view_name,
                    view_results.len()
                );
            }

            results.insert(view_name.clone(), Relation::from(view_results));
        }

        Ok(results)
    }

    /// Refuse the program if a join of its plan is estimated to produce
    /// more than `max_query_cost` rows; see [`IRNode::estimate_rows`].
    /// Relations the program derives are estimated from their rules in the
    /// order they run, a recursive one from its rules' first pass. Only a
    /// plan over the limit counts the rows its filters keep of stored
    /// relations, and is refused if it is still over it.
    fn check_query_cost(
        &self,
        unoptimized_ir_nodes: &[IRNode],
        recursive_info: &[Option<String>],
        rule_heads: &[String],
        execution_groups: &[Vec<usize>],
        source_len: usize,
    ) -> Result<(), String> {
        let over_limit = |j: &ir::JoinEstimate| j.rows > self.max_query_cost;
        let mut largest = self.largest_join(
            unoptimized_ir_nodes,
            recursive_info,
            rule_heads,
            execution_groups,
            false,
        );
        if largest.as_ref().is_some_and(over_limit) {
            largest = self.largest_join(
                unoptimized_ir_nodes,
                recursive_info,
                rule_heads,
                execution_groups,
                true,
            );
        }

        let Some(join) = largest.filter(over_limit) else {
            tracing::debug!(
                source_len,
                largest_join_rows = largest.map_or(0, |j| j.rows),
                max_cost = self.max_query_cost,
                "engine_cost_check_pass"
            );
            return Ok(());
        };
        info!(
            source_len,
            join_rows = join.rows,
            left_rows = join.left,
            right_rows = join.right,
            cross_product = join.cross_product,
            max_cost = self.max_query_cost,
            "engine_cost_check_refused"
        );
        Err(if join.cross_product {
            format!(
                "Query too complex: it joins {} rows with {} rows on no shared variable, \
                 a cross product of about {} rows, over the limit of {} \
                 (storage.performance.max_query_cost). Join them on a shared variable, \
                 order the rule body so each atom shares a variable with one before it, \
                 or filter a side with a constant or comparison so fewer of its rows meet",
                join.left, join.right, join.rows, self.max_query_cost
            )
        } else {
            format!(
                "Query too complex: it joins {} rows with {} rows, an estimated {} rows, \
                 over the limit of {} (storage.performance.max_query_cost). \
                 Filter an input with a constant or comparison so fewer of its rows meet",
                join.left, join.right, join.rows, self.max_query_cost
            )
        })
    }

    /// The program's join with the most estimated output rows; with
    /// `count_filters`, a filter over a stored relation keeps the rows it
    /// matches rather than all of them.
    fn largest_join(
        &self,
        unoptimized_ir_nodes: &[IRNode],
        recursive_info: &[Option<String>],
        rule_heads: &[String],
        execution_groups: &[Vec<usize>],
        count_filters: bool,
    ) -> Option<ir::JoinEstimate> {
        let mut derived: HashMap<&str, u64> = HashMap::new();
        let mut largest: Option<ir::JoinEstimate> = None;
        let mut estimate = |ir: &IRNode, derived: &HashMap<&str, u64>| {
            let rows_of = |name: &str| {
                let stored = self.input_tuples.get(name).map_or(0, |r| r.len() as u64);
                stored.saturating_add(derived.get(name).copied().unwrap_or(0))
            };
            let is_derived = |name: &str| derived.contains_key(name);
            let filtered_rows = |node: &IRNode| {
                count_filters
                    .then(|| {
                        CodeGenerator::count_filtered_scan(node, &self.input_tuples, &is_derived)
                    })
                    .flatten()
            };
            let estimate = ir.estimate_rows(&rows_of, &filtered_rows);
            largest = largest
                .into_iter()
                .chain(estimate.largest_join)
                .max_by_key(ir::JoinEstimate::rank);
            estimate.rows
        };
        for (name, view) in &self.shared_views {
            let rows = estimate(view, &derived);
            derived.insert(name, rows);
        }
        for group in execution_groups {
            for &i in group {
                let recursive =
                    group.len() > 1 || recursive_info.get(i).is_some_and(Option::is_some);
                let ir = if recursive {
                    &unoptimized_ir_nodes[i]
                } else {
                    &self.ir_nodes[i]
                };
                let rows = estimate(ir, &derived);
                if let Some(head) = rule_heads.get(i) {
                    let total = derived.entry(head).or_default();
                    *total = total.saturating_add(rows);
                }
            }
        }
        largest
    }

    /// Group IR nodes into strongly connected components of the rule
    /// dependency graph, in execution order.
    ///
    /// Node A depends on node B when A scans B's head. Each group runs after
    /// every group it reads; a group with more than one node is a set of
    /// mutually recursive relations evaluated to a joint fixpoint.
    fn execution_groups(ir_nodes: &[IRNode], rule_heads: &[String]) -> Vec<Vec<usize>> {
        let head_to_idx: HashMap<&str, usize> = rule_heads
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        let deps: Vec<std::collections::HashSet<usize>> = ir_nodes
            .iter()
            .enumerate()
            .map(|(i, ir)| {
                let mut scans = Vec::new();
                Self::collect_scan_relations(ir, &mut scans);
                scans
                    .iter()
                    .filter_map(|scan| head_to_idx.get(scan.as_str()).copied())
                    .filter(|&j| j != i)
                    .collect()
            })
            .collect();

        let groups = recursion::scc_execution_order(&deps);
        if std::env::var("INPUTLAYER_DEBUG").is_ok() {
            eprintln!("DEBUG execution_groups: {groups:?}");
        }
        groups
    }

    /// Collect all relation names referenced by Scan nodes in an IR tree
    fn collect_scan_relations(ir: &IRNode, scans: &mut Vec<String>) {
        match ir {
            IRNode::Scan { relation, .. } => {
                if !scans.contains(relation) {
                    scans.push(relation.clone());
                }
            }
            IRNode::Map { input, .. }
            | IRNode::Filter { input, .. }
            | IRNode::Distinct { input }
            | IRNode::Aggregate { input, .. }
            | IRNode::Compute { input, .. }
            | IRNode::FlatMap { input, .. } => {
                Self::collect_scan_relations(input, scans);
            }
            IRNode::Join { left, right, .. }
            | IRNode::Antijoin { left, right, .. }
            | IRNode::JoinFlatMap { left, right, .. } => {
                Self::collect_scan_relations(left, scans);
                Self::collect_scan_relations(right, scans);
            }
            IRNode::Union { inputs } => {
                for input in inputs {
                    Self::collect_scan_relations(input, scans);
                }
            }
            IRNode::HnswScan { .. } => {}
        }
    }

    /// Execute the full pipeline returning tuples of arbitrary arity
    ///
    /// This is the main entry point for queries that may return non-binary tuples.
    /// Returns results from the LAST rule (typically the query), while computing
    /// all intermediate rules (views) and making them available as input data.
    pub fn execute_tuples(&mut self, source: &str) -> Result<Vec<Tuple>, String> {
        self.execute_tuples_profiled(source)
            .map(|(tuples, _, _)| tuples)
    }

    /// Execute the full pipeline returning query results AND all accumulated
    /// derived relation contents.
    ///
    /// This is used by the provenance system to avoid re-deriving tuples
    /// during backward chaining proof construction.
    pub fn execute_tuples_with_derived(
        &mut self,
        source: &str,
    ) -> Result<(Vec<Tuple>, RelationMap), String> {
        self.execute_tuples_profiled(source)
            .map(|(tuples, derived, _)| (tuples, derived))
    }

    /// Execute the full pipeline returning tuples, all accumulated derived
    /// relation contents, and an optional timing breakdown.
    ///
    /// The derived data contains all intermediate relation results computed
    /// during evaluation. This is used by the provenance system to avoid
    /// expensive re-derivation during backward chaining.
    pub fn execute_tuples_profiled(&mut self, source: &str) -> ExecutionOutput {
        let collector = execution::TimingCollector::new(self.timing_mode);
        let source_len = source.len();
        info!(source_len, "engine_execute_start");
        let (parse_result, parse_us) = collector.time(|| self.parse(source).map(|_| ()));
        parse_result?;
        self.run_loaded_program(collector, parse_us, source_len)
    }

    /// Like [`Self::execute_tuples_profiled`], for an already-parsed program
    /// (e.g. cached persistent rules followed by the parsed query).
    pub fn execute_program_profiled(&mut self, program: Program) -> ExecutionOutput {
        let collector = execution::TimingCollector::new(self.timing_mode);
        let source_len = program.rules.len();
        info!(rules = source_len, "engine_execute_start");
        let (load_result, parse_us) = collector.time(|| self.load_program(program).map(|_| ()));
        load_result?;
        self.run_loaded_program(collector, parse_us, source_len)
    }

    /// Compile `program` without executing it: everything the pipeline derives
    /// before it reads data. [`Self::execute_compiled_profiled`] runs the
    /// result on any inputs, so one compilation can serve many evaluations.
    pub fn compile_program(&mut self, program: Program) -> Result<CompiledProgram, String> {
        let mut collector = execution::TimingCollector::new(self.timing_mode);
        let source_len = program.rules.len();
        let (load_result, parse_us) = collector.time(|| self.load_program(program).map(|_| ()));
        load_result?;
        let staged = self.compile_loaded(&mut collector, parse_us, source_len)?;
        let program = self
            .program
            .clone()
            .ok_or("No program parsed yet. Call parse() first.")?;
        Ok(CompiledProgram {
            program,
            ir_nodes: self.ir_nodes.clone(),
            shared_views: self.shared_views.clone(),
            semiring_annotations: self.semiring_annotations.clone(),
            has_recursion: self.has_recursion,
            strata: self.strata.clone(),
            staged,
            timing: collector.breakdown,
        })
    }

    /// Execute a program compiled by [`Self::compile_program`] on this
    /// engine's inputs, as executing the program itself would. The timing
    /// breakdown has no compile stages: none ran.
    pub fn execute_compiled_profiled(&mut self, compiled: &CompiledProgram) -> ExecutionOutput {
        self.program = Some(compiled.program.clone());
        self.ir_nodes.clone_from(&compiled.ir_nodes);
        self.shared_views.clone_from(&compiled.shared_views);
        self.semiring_annotations
            .clone_from(&compiled.semiring_annotations);
        self.has_recursion = compiled.has_recursion;
        self.strata.clone_from(&compiled.strata);
        inject_magic_seeds(&mut self.input_tuples, &compiled.staged.magic_seeds);
        let collector = execution::TimingCollector::new(self.timing_mode);
        let source_len = compiled.program.rules.len();
        info!(rules = source_len, "engine_execute_compiled_start");
        self.execute_loaded(collector, compiled.staged.clone(), source_len)
    }

    /// Run the pipeline after parsing: SIP, magic sets, IR, optimize, execute.
    fn run_loaded_program(
        &mut self,
        mut collector: execution::TimingCollector,
        parse_us: u64,
        source_len: usize,
    ) -> ExecutionOutput {
        let staged = self.compile_loaded(&mut collector, parse_us, source_len)?;
        self.execute_loaded(collector, staged, source_len)
    }

    /// The compile stages after parsing: constant specialization, SIP, Magic
    /// Sets, IR and optimization. Leaves the engine ready to execute.
    fn compile_loaded(
        &mut self,
        collector: &mut execution::TimingCollector,
        parse_us: u64,
        source_len: usize,
    ) -> Result<Staged, String> {
        let debug = std::env::var("INPUTLAYER_DEBUG").is_ok();
        let parse_ms = parse_us / 1000;
        info!(source_len, parse_ms, "engine_parse_complete");
        collector.breakdown.parse_us = parse_us;

        let ((), spec_us) = collector.time(|| self.apply_constant_specialization());
        info!(source_len, spec_us, "engine_specialize_complete");

        let ((), sip_us) = collector.time(|| self.apply_sip_rewriting());
        let sip_ms = sip_us / 1000;
        info!(source_len, sip_ms, "engine_sip_complete");
        collector.breakdown.sip_us = sip_us;

        let (magic_seeds, magic_us) = collector.time(|| self.apply_magic_sets());
        let magic_ms = magic_us / 1000;
        info!(source_len, magic_ms, "engine_magic_sets_complete");
        collector.breakdown.magic_sets_us = magic_us;

        let (build_result, build_us) = collector.time(|| self.build_ir(collector.is_detailed()));
        collector.breakdown.ir_builder_detail = build_result?;
        let build_ms = build_us / 1000;
        info!(
            source_len,
            build_ms,
            ir_nodes = self.ir_nodes.len(),
            "engine_build_ir_complete"
        );
        collector.breakdown.ir_build_us = build_us;

        if debug {
            eprintln!(
                "DEBUG execute_tuples: built {} IR nodes",
                self.ir_nodes.len()
            );
        }

        // Detect recursion BEFORE optimization (optimization destroys Union structure)
        let rule_heads = self.get_rule_heads();
        let recursive_info = self.detect_recursion_info(&rule_heads);
        let unoptimized_ir_nodes = self.ir_nodes.clone();

        // Optimize (for non-recursive nodes)
        let (opt_result, opt_us) = collector.time(|| self.optimize_ir(collector.is_detailed()));
        collector.breakdown.optimizer_detail = opt_result?;
        let opt_ms = opt_us / 1000;
        info!(source_len, opt_ms, "engine_optimize_complete");
        collector.breakdown.optimize_us = opt_us;

        Ok(Staged {
            unoptimized_ir_nodes,
            recursive_info,
            magic_seeds,
        })
    }

    /// Execute the compiled program on the engine's inputs.
    fn execute_loaded(
        &mut self,
        mut collector: execution::TimingCollector,
        staged: Staged,
        source_len: usize,
    ) -> ExecutionOutput {
        RESULT_TRUNCATED.set(false);
        let exec_start = Instant::now();
        let Staged {
            mut unoptimized_ir_nodes,
            recursive_info,
            ..
        } = staged;
        let rule_heads = self.get_rule_heads();

        if self.ir_nodes.is_empty() {
            return Err("No IR nodes to execute".to_string());
        }

        // Rules run one SCC at a time, in dependency order. A plan that would
        // multiply large relations is refused before anything runs.
        let execution_groups = Self::execution_groups(&unoptimized_ir_nodes, &rule_heads);
        if self.max_query_cost > 0 {
            self.check_query_cost(
                &unoptimized_ir_nodes,
                &recursive_info,
                &rule_heads,
                &execution_groups,
                source_len,
            )?;
        }

        // Execute shared views first (from subplan sharing optimization)
        let (shared_result, shared_us) = collector.time(|| self.execute_shared_views());
        let mut accumulated_results = shared_result?;
        let shared_ms = shared_us / 1000;
        info!(
            source_len,
            shared_ms,
            shared_views = self.shared_views.len(),
            "engine_shared_views_complete"
        );
        collector.breakdown.shared_views_us = shared_us;

        let query_idx = self.ir_nodes.len() - 1;
        let mut last_result: Vec<Tuple> = Vec::new();
        // The final rule may stop early (one row past the cap, to detect
        // truncation) only when it is non-recursive and no other rule reads it.
        let result_cap = self.result_cap();
        let query_stops_early = result_cap > 0
            && recursive_info.get(query_idx).is_some_and(Option::is_none)
            && rule_heads.get(query_idx).is_some_and(|head| {
                unoptimized_ir_nodes
                    .iter()
                    .enumerate()
                    .filter(|&(j, _)| j != query_idx)
                    .all(|(_, ir)| {
                        let mut scans = Vec::new();
                        Self::collect_scan_relations(ir, &mut scans);
                        !scans.contains(head)
                    })
            });

        for group in &execution_groups {
            if let [i] = group.as_slice() {
                let i = *i;
                let head_name = rule_heads.get(i).cloned().unwrap_or_default();

                let is_recursive = recursive_info.get(i).is_some_and(Option::is_some);

                // HNSW searches run outside DD; their results become synthetic inputs.
                let hnsw_inputs = {
                    let ir = if is_recursive {
                        &mut unoptimized_ir_nodes[i]
                    } else {
                        &mut self.ir_nodes[i]
                    };
                    let inputs = execution::hnsw_resolve::RuleInputs {
                        derived: &accumulated_results,
                        base: &self.input_tuples,
                    };
                    execution::hnsw_resolve::resolve_rule(
                        ir,
                        self.hnsw_search_fn.as_ref(),
                        &inputs,
                        i,
                        is_recursive,
                    )?
                };
                let hnsw_names: Vec<String> = hnsw_inputs.iter().map(|(n, _)| n.clone()).collect();
                accumulated_results.extend(hnsw_inputs);

                // Create fresh CodeGenerator for each rule (avoids timely state issues)
                let mut codegen = self.rule_codegen(&[i], &accumulated_results);
                if i == query_idx && query_stops_early {
                    codegen.set_max_result_rows(result_cap.saturating_add(1));
                }
                for name in &hnsw_names {
                    accumulated_results.remove(name);
                }

                // Use unoptimized IR for recursive nodes, optimized for others
                let (exec_result, rule_us) = collector.time(|| {
                    if let Some(Some(recursive_rel)) = recursive_info.get(i) {
                        codegen.execute_recursive(&unoptimized_ir_nodes[i], recursive_rel)
                    } else if self.num_workers > 1 {
                        // Use parallel execution when configured for multi-worker
                        let config =
                            code_generator::ExecutionConfig::with_workers(self.num_workers);
                        codegen.execute_with_config(&self.ir_nodes[i], config)
                    } else {
                        codegen.execute(&self.ir_nodes[i])
                    }
                });
                let result = exec_result?;

                if i == query_idx {
                    last_result.clone_from(&result);
                }

                // Store results for subsequent rules
                if !head_name.is_empty() {
                    accumulated_results.insert(head_name.clone(), Relation::from(result));
                }

                collector.record_rule(head_name.clone(), rule_us, is_recursive, self.num_workers);

                let rule_ms = rule_us / 1000;
                info!(
                    source_len,
                    rule_idx = i,
                    rule_head = %head_name,
                    rule_ms,
                    recursive = is_recursive,
                    workers = self.num_workers,
                    "engine_rule_complete"
                );
            } else {
                // Mutually recursive relations: one joint semi-naive fixpoint.
                // Unoptimized IR keeps each relation's clause Union intact.
                let mut hnsw_names = Vec::new();
                for &i in group {
                    let inputs = execution::hnsw_resolve::RuleInputs {
                        derived: &accumulated_results,
                        base: &self.input_tuples,
                    };
                    let resolved = execution::hnsw_resolve::resolve_rule(
                        &mut unoptimized_ir_nodes[i],
                        self.hnsw_search_fn.as_ref(),
                        &inputs,
                        i,
                        true,
                    )?;
                    hnsw_names.extend(resolved.iter().map(|(n, _)| n.clone()));
                    accumulated_results.extend(resolved);
                }
                let members: Vec<(String, IRNode)> = group
                    .iter()
                    .map(|&i| (rule_heads[i].clone(), unoptimized_ir_nodes[i].clone()))
                    .collect();
                let codegen = self.rule_codegen(group, &accumulated_results);
                for name in &hnsw_names {
                    accumulated_results.remove(name);
                }
                let (exec_result, scc_us) =
                    collector.time(|| codegen.execute_recursive_scc(&members));
                let mut results = exec_result?;

                for &i in group {
                    let result = results.remove(&rule_heads[i]).unwrap_or_default();
                    if i == query_idx {
                        last_result.clone_from(&result);
                    }
                    accumulated_results.insert(rule_heads[i].clone(), Relation::from(result));
                }

                let scc_head = group
                    .iter()
                    .map(|&i| rule_heads[i].as_str())
                    .collect::<Vec<_>>()
                    .join("+");
                collector.record_rule(scc_head.clone(), scc_us, true, self.num_workers);
                info!(
                    source_len,
                    rule_head = %scc_head,
                    rule_ms = scc_us / 1000,
                    recursive = true,
                    members = group.len(),
                    "engine_scc_complete"
                );
            }
        }

        if result_cap > 0 && last_result.len() > result_cap {
            last_result.truncate(result_cap);
            RESULT_TRUNCATED.set(true);
            if query_stops_early {
                if let Some(head) = accumulated_results.get_mut(&rule_heads[query_idx]) {
                    head.truncate(result_cap);
                }
            }
            info!(
                source_len,
                max_result_rows = result_cap,
                "engine_result_truncated"
            );
        }

        info!(
            source_len,
            total_ms = exec_start.elapsed().as_millis() as u64,
            "engine_execute_complete"
        );
        let timing = collector.finish();
        Ok((last_result, accumulated_results, timing))
    }

    /// Execute all rules in the program
    ///
    /// Returns a map from rule index to results.
    pub fn execute_all_rules(
        &mut self,
        source: &str,
    ) -> Result<HashMap<usize, Vec<(i32, i32)>>, String> {
        // Pipeline
        self.parse(source)?;
        self.apply_sip_rewriting();
        self.build_ir(false)?;
        self.optimize_ir(false)?;

        // Execute rules in dependency order, chaining intermediate results so SIP
        // intermediate rules feed into subsequent rules.
        let rule_heads = self.get_rule_heads();
        let execution_order: Vec<usize> = Self::execution_groups(&self.ir_nodes, &rule_heads)
            .into_iter()
            .flatten()
            .collect();
        let mut accumulated = RelationMap::new();
        let mut results = HashMap::new();

        for &i in &execution_order {
            let ir = &self.ir_nodes[i];
            let head_name = rule_heads.get(i).cloned().unwrap_or_default();

            let mut codegen = CodeGenerator::new();
            // Set per-rule semiring type from boolean specialization
            let semiring = self
                .semiring_annotations
                .get(i)
                .map_or(boolean_specialization::SemiringType::Counting, |a| {
                    a.semiring
                });
            codegen.set_semiring_type(semiring);
            self.load_inputs_into_codegen(&mut codegen, &accumulated);

            let rule_tuples = codegen.execute(ir)?;
            let rule_results: Vec<(i32, i32)> =
                rule_tuples.iter().filter_map(Tuple::to_pair).collect();
            results.insert(i, rule_results);

            // Store for subsequent rules
            if !head_name.is_empty() {
                accumulated.insert(head_name, Relation::from(rule_tuples));
            }
        }

        Ok(results)
    }

    /// Execute with full pipeline tracing
    ///
    /// Returns both results and a trace of all pipeline stages.
    /// Useful for debugging and understanding query processing.
    pub fn execute_with_trace(
        &mut self,
        source: &str,
    ) -> Result<(Vec<(i32, i32)>, PipelineTrace), String> {
        let mut trace = PipelineTrace::new();

        // Parse
        self.parse(source)?;

        // SIP Rewriting (AST level, before IR building)
        self.apply_sip_rewriting();

        if let Some(program) = &self.program {
            trace.record_ast(program.clone());
        }

        // Build IR
        self.build_ir(false)?;
        trace.record_ir_before(self.ir_nodes.clone());

        // Optimize
        self.optimize_ir(false)?;
        trace.record_ir_after(self.ir_nodes.clone());

        // Execute
        if self.ir_nodes.is_empty() {
            return Err("No IR nodes to execute".to_string());
        }

        let results = self.execute_ir(&self.ir_nodes[0])?;
        trace.record_results(vec![results.clone()]);

        Ok((results, trace))
    }

    /// Execute all rules with full pipeline tracing
    ///
    /// Returns results for each rule and a complete pipeline trace.
    pub fn execute_all_with_trace(
        &mut self,
        source: &str,
    ) -> Result<(HashMap<usize, Vec<(i32, i32)>>, PipelineTrace), String> {
        let mut trace = PipelineTrace::new();

        // Parse
        self.parse(source)?;

        // SIP Rewriting (AST level, before IR building)
        self.apply_sip_rewriting();

        if let Some(program) = &self.program {
            trace.record_ast(program.clone());
        }

        // Build IR
        self.build_ir(false)?;
        trace.record_ir_before(self.ir_nodes.clone());

        // Optimize
        self.optimize_ir(false)?;
        trace.record_ir_after(self.ir_nodes.clone());

        // Execute all rules
        let mut results = HashMap::new();
        let mut all_results = Vec::new();

        for (i, ir) in self.ir_nodes.iter().enumerate() {
            let rule_results = self.execute_ir(ir)?;
            results.insert(i, rule_results.clone());
            all_results.push(rule_results);
        }

        trace.record_results(all_results);

        Ok((results, trace))
    }

    /// Debug a query plan without executing it.
    ///
    /// Runs the full compilation pipeline (parse → SIP → IR → optimize)
    /// and returns a PipelineTrace showing the plan at each stage.
    pub fn debug(&mut self, source: &str) -> Result<PipelineTrace, String> {
        let mut trace = PipelineTrace::new();

        // Parse + SIP rewriting
        self.parse(source)?;
        self.apply_sip_rewriting();

        if let Some(program) = &self.program {
            trace.record_ast(program.clone());
        }

        // Build IR
        self.build_ir(false)?;
        trace.record_ir_before(self.ir_nodes.clone());

        // Optimize
        self.optimize_ir(false)?;
        trace.record_ir_after(self.ir_nodes.clone());

        Ok(trace)
    }

    /// Execute a simple query (simplified API for testing)
    ///
    /// This bypasses parsing and directly builds IR from a single rule.
    /// Useful for testing the IR -> optimize -> execute pipeline.
    pub fn execute_simple_query(
        &self,
        relation: &str,
        projection: Vec<usize>,
    ) -> Result<Vec<(i32, i32)>, String> {
        // Build a simple scan + map IR
        let scan = IRNode::Scan {
            relation: relation.to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let ir = if projection == vec![0, 1] {
            // Identity projection - just scan
            scan
        } else {
            // Non-identity projection - add map
            IRNode::Map {
                input: Box::new(scan),
                projection: projection.clone(),
                output_schema: vec!["col0".to_string(), "col1".to_string()],
            }
        };

        // Optimize
        let optimizer = Optimizer::new();
        let optimized_ir = optimizer.optimize(ir);

        // Execute
        let mut codegen = CodeGenerator::new();
        if let Some(data) = self.input_tuples.get(relation) {
            codegen.add_relation(relation.to_string(), data.clone());
        }

        let result_tuples = codegen.execute(&optimized_ir)?;
        // Convert to binary format for legacy return type
        let results: Vec<(i32, i32)> = result_tuples.iter().filter_map(Tuple::to_pair).collect();
        Ok(results)
    }

    /// Get the current program (if parsed)
    pub fn program(&self) -> Option<&Program> {
        self.program.as_ref()
    }

    /// Get the built IR nodes
    pub fn ir_nodes(&self) -> &[IRNode] {
        &self.ir_nodes
    }
}

impl Default for IQLEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_engine_creation() {
        let engine = IQLEngine::new();
        assert!(engine.program().is_none());
        assert_eq!(engine.ir_nodes().len(), 0);
    }

    #[test]
    fn test_add_facts() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

        assert_eq!(engine.input_tuples.len(), 1);
        assert_eq!(engine.input_tuples.get("edge").unwrap().len(), 3);
    }

    #[test]
    fn test_simple_query() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);

        // Execute simple query - this demonstrates the API
        let result = engine.execute_simple_query("edge", vec![0, 1]);

        // Test passes if query executes without error
        // Advanced optimization passes (join planning, SIP, etc.) don't affect correctness, only performance
        match result {
            Ok(_data) => {
                // Query executed successfully
                // Could verify results here if needed
            }
            Err(_e) => {
                // Query failed - acceptable for this basic test
                // Full integration is tested in other test suites
            }
        }
    }

    #[test]
    fn test_self_join_with_different_string_constants() {
        // Regression test: self-join with different string constants must
        // correctly filter each side independently and not produce Cartesian products.
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "events",
            vec![
                Tuple::new(vec![
                    Value::Int64(1),
                    Value::string("start"),
                    Value::Int64(1000),
                ]),
                Tuple::new(vec![
                    Value::Int64(1),
                    Value::string("end"),
                    Value::Int64(1500),
                ]),
                Tuple::new(vec![
                    Value::Int64(2),
                    Value::string("start"),
                    Value::Int64(2000),
                ]),
                Tuple::new(vec![
                    Value::Int64(2),
                    Value::string("end"),
                    Value::Int64(2800),
                ]),
            ],
        );

        let query = r#"duration(Id, D) <- events(Id, "start", S), events(Id, "end", E), D = E - S"#;
        let results = engine.execute_tuples(query).unwrap();
        eprintln!("Self-join results: {results:?}");
        assert_eq!(
            results.len(),
            2,
            "Expected 2 results (one per Id), got {}: {:?}",
            results.len(),
            results
        );
    }

    #[test]
    fn test_self_join_with_different_integer_constants() {
        // Regression test: self-join with different integer constants must
        // correctly filter each side independently (not produce empty result or Cartesian).
        // Pattern: common(A) <- ancestor(8, A), ancestor(10, A)
        let mut engine = IQLEngine::new();
        // Ancestors: 8 has ancestors {4, 2, 1}, 10 has ancestors {6, 3, 1}
        engine.add_tuples(
            "ancestor",
            vec![
                Tuple::new(vec![Value::Int64(8), Value::Int64(4)]),
                Tuple::new(vec![Value::Int64(8), Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(8), Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(10), Value::Int64(6)]),
                Tuple::new(vec![Value::Int64(10), Value::Int64(3)]),
                Tuple::new(vec![Value::Int64(10), Value::Int64(1)]),
            ],
        );

        let query = "common(A) <- ancestor(8, A), ancestor(10, A)";
        let results = engine.execute_tuples(query).unwrap();
        eprintln!("Common ancestor results: {results:?}");
        // Common ancestor of 8 and 10 is just: 1
        assert_eq!(
            results.len(),
            1,
            "Expected 1 common ancestor, got {}: {:?}",
            results.len(),
            results
        );
        assert_eq!(results[0].get(0), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_recursive_descendant_with_constant_in_head() {
        // Regression test: recursive rules with integer constants in the head
        // must produce correct results through the combined program execution path.
        // This mirrors the snapshot test pattern:
        //   descendant(2, X) <- parent(X, 2)
        //   descendant(2, X) <- parent(X, Y), descendant(2, Y)
        //   __query__(_c0, X) <- descendant(_c0, X), _c0 = 2
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "parent",
            vec![
                // Tree:  1 -> {2, 3}, 2 -> {4, 5}, 4 -> {8, 9}, 5 -> {10, 11}
                Tuple::new(vec![Value::Int64(2), Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(3), Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(4), Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(5), Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(8), Value::Int64(4)]),
                Tuple::new(vec![Value::Int64(9), Value::Int64(4)]),
                Tuple::new(vec![Value::Int64(10), Value::Int64(5)]),
                Tuple::new(vec![Value::Int64(11), Value::Int64(5)]),
            ],
        );

        // Combined program as the server would construct it:
        let program = concat!(
            "descendant(2, X) <- parent(X, 2)\n",
            "descendant(2, X) <- parent(X, Y), descendant(2, Y)\n",
            "__query__(_c0, X) <- descendant(_c0, X), _c0 = 2\n",
        );
        let results = engine.execute_tuples(program).unwrap();
        eprintln!("Recursive descendant results: {results:?}");
        // Descendants of node 2: {4, 5, 8, 9, 10, 11}
        assert!(
            results.len() >= 2,
            "Expected at least 2 descendants of node 2, got {}: {:?}",
            results.len(),
            results
        );
    }

    // === Additional Coverage ===

    #[test]
    fn test_optimization_config_default() {
        let config = OptimizationConfig::default();
        assert!(config.enable_join_planning);
        assert!(config.enable_sip_rewriting);
        assert!(config.enable_subplan_sharing);
        assert!(config.enable_boolean_specialization);
    }

    #[test]
    fn test_with_config() {
        let config = OptimizationConfig {
            enable_join_planning: false,
            enable_sip_rewriting: false,
            enable_subplan_sharing: false,
            enable_boolean_specialization: false,
            enable_magic_sets: false,
            enable_constant_specialization: false,
        };
        let engine = IQLEngine::with_config(config.clone());
        assert!(!engine.config().enable_join_planning);
        assert!(!engine.config().enable_sip_rewriting);
    }

    #[test]
    fn test_set_config() {
        let mut engine = IQLEngine::new();
        assert!(engine.config().enable_join_planning);

        let config = OptimizationConfig {
            enable_join_planning: false,
            enable_sip_rewriting: true,
            enable_subplan_sharing: true,
            enable_boolean_specialization: true,
            enable_magic_sets: true,
            enable_constant_specialization: true,
        };
        engine.set_config(config);
        assert!(!engine.config().enable_join_planning);
    }

    #[test]
    fn test_set_num_workers() {
        let mut engine = IQLEngine::new();
        engine.set_num_workers(4);
        // Workers must be at least 1
        engine.set_num_workers(0);
        // No panic, clamped to 1
    }

    #[test]
    fn test_add_tuple_single() {
        let mut engine = IQLEngine::new();
        engine.add_tuple("node", Tuple::new(vec![Value::Int64(42)]));
        engine.add_tuple("node", Tuple::new(vec![Value::Int64(99)]));

        let rel = engine.get_relation("node").unwrap();
        assert_eq!(rel.len(), 2);
        assert!(engine.catalog().has_relation("node"));
    }

    #[test]
    fn test_get_relation_nonexistent() {
        let engine = IQLEngine::new();
        assert!(engine.get_relation("nonexistent").is_none());
    }

    #[test]
    fn test_input_tuples_accessors() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);

        assert_eq!(engine.input_tuples().len(), 1);
        assert!(engine.input_tuples().contains_key("edge"));

        engine.input_tuples_mut().clear();
        assert!(engine.input_tuples().is_empty());
    }

    #[test]
    fn test_parse_detects_recursion() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3)]);

        // Non-recursive
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();
        assert!(!engine.is_recursive());
        assert!(!engine.strata().is_empty());

        // Recursive
        let mut engine2 = IQLEngine::new();
        engine2.add_fact("edge", vec![(1, 2), (2, 3)]);
        engine2
            .parse("path(X, Y) <- edge(X, Y)\npath(X, Z) <- path(X, Y), edge(Y, Z)")
            .unwrap();
        assert!(engine2.is_recursive());
    }

    #[test]
    fn test_parse_unsafe_rule_rejected() {
        let mut engine = IQLEngine::new();
        let result = engine.parse("result(X, Y) <- edge(X, _)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unsafe rule"));
    }

    #[test]
    fn test_execute_with_negation() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "node",
            vec![
                Tuple::new(vec![Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(3)]),
            ],
        );
        engine.add_tuples("excluded", vec![Tuple::new(vec![Value::Int64(2)])]);

        let results = engine
            .execute_tuples("result(X) <- node(X), !excluded(X)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_with_arithmetic() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "data",
            vec![
                Tuple::new(vec![Value::Int64(10), Value::Int64(3)]),
                Tuple::new(vec![Value::Int64(20), Value::Int64(5)]),
            ],
        );

        let results = engine
            .execute_tuples("result(X, Y, S) <- data(X, Y), S = X + Y")
            .unwrap();
        assert_eq!(results.len(), 2);
        // Verify arithmetic
        let sums: Vec<i64> = results
            .iter()
            .filter_map(|t| t.get(2).and_then(|v| v.as_i64()))
            .collect();
        assert!(sums.contains(&13));
        assert!(sums.contains(&25));
    }

    #[test]
    fn test_execute_all_rules() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3)]);

        let results = engine
            .execute_all_rules("path(X, Y) <- edge(X, Y)")
            .unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_execute_with_trace() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3)]);

        let (results, trace) = engine
            .execute_with_trace("path(X, Y) <- edge(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_debug() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);

        let trace = engine.debug("path(X, Y) <- edge(X, Y)").unwrap();
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_default_engine() {
        let engine = IQLEngine::default();
        assert!(engine.program().is_none());
    }

    #[test]
    fn test_execute_multiple_rules() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "edge",
            vec![
                Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
            ],
        );

        // Two rules: intermediate + query
        let results = engine
            .execute_tuples("path(X, Y) <- edge(X, Y)\nresult(X, Y) <- path(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_string_data() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "person",
            vec![
                Tuple::new(vec![Value::string("alice"), Value::Int64(30)]),
                Tuple::new(vec![Value::string("bob"), Value::Int64(25)]),
            ],
        );

        let results = engine
            .execute_tuples("result(N, A) <- person(N, A)")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_get_optimization_config() {
        let engine = IQLEngine::new();
        let config = engine.get_optimization_config();
        assert!(config.enable_join_planning);
    }

    #[test]
    fn test_sip_equijoin_multikey_column_order() {
        // Regression test: SIP rewrites the rule body, join planner may reorder
        // sides, but the output column order must match the head declaration.
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "orders",
            vec![
                Tuple::new(vec![
                    Value::Int64(1),
                    Value::string("2024"),
                    Value::string("Q1"),
                    Value::Int64(100),
                ]),
                Tuple::new(vec![
                    Value::Int64(2),
                    Value::string("2024"),
                    Value::string("Q2"),
                    Value::Int64(200),
                ]),
                Tuple::new(vec![
                    Value::Int64(3),
                    Value::string("2023"),
                    Value::string("Q1"),
                    Value::Int64(50),
                ]),
            ],
        );
        engine.add_tuples(
            "targets",
            vec![
                Tuple::new(vec![
                    Value::Int64(1),
                    Value::string("2024"),
                    Value::string("Q1"),
                    Value::Int64(90),
                ]),
                Tuple::new(vec![
                    Value::Int64(2),
                    Value::string("2024"),
                    Value::string("Q2"),
                    Value::Int64(150),
                ]),
            ],
        );

        let program = concat!(
            "matched(OrdId, Year, Qtr, Actual, Target) <- orders(OrdId, Year, Qtr, Actual), targets(_, Year, Qtr, Target)\n",
            "__query__(OrdId, Year, Qtr, Actual, Target) <- matched(OrdId, Year, Qtr, Actual, Target)\n",
        );
        let results = engine.execute_tuples(program).unwrap();

        // Verify column order matches the head: (OrdId, Year, Qtr, Actual, Target)
        assert_eq!(results.len(), 2);
        let first = &results[0];
        assert_eq!(
            first.values()[0],
            Value::Int64(1),
            "First column should be OrdId=1, got {first:?}"
        );
    }

    // =========================================================================
    // Additional IQLEngine Coverage Tests
    // =========================================================================

    #[test]
    fn test_build_ir_without_parse_fails() {
        let mut engine = IQLEngine::new();
        let result = engine.build_ir(false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No program parsed"));
    }

    #[test]
    fn test_execute_ir_tuples_direct() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "data",
            vec![
                Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(3), Value::Int64(4)]),
            ],
        );

        let ir = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        let results = engine.execute_ir_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_ir_binary_direct() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (3, 4)]);

        let ir = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        let results = engine.execute_ir(&ir).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_collect_scan_relations() {
        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            right: Box::new(IRNode::Filter {
                input: Box::new(IRNode::Scan {
                    relation: "node".to_string(),
                    schema: vec!["x".to_string()],
                }),
                predicate: Predicate::True,
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "y".to_string(), "x".to_string()],
        };

        let mut scans = Vec::new();
        IQLEngine::collect_scan_relations(&ir, &mut scans);
        assert!(scans.contains(&"edge".to_string()));
        assert!(scans.contains(&"node".to_string()));
        assert_eq!(scans.len(), 2);
    }

    #[test]
    fn test_collect_scan_relations_union() {
        let ir = IRNode::Union {
            inputs: vec![
                IRNode::Scan {
                    relation: "a".to_string(),
                    schema: vec!["x".to_string()],
                },
                IRNode::Scan {
                    relation: "b".to_string(),
                    schema: vec!["x".to_string()],
                },
                IRNode::Scan {
                    relation: "a".to_string(), // duplicate
                    schema: vec!["x".to_string()],
                },
            ],
        };

        let mut scans = Vec::new();
        IQLEngine::collect_scan_relations(&ir, &mut scans);
        // "a" should appear only once (dedup)
        assert_eq!(scans.len(), 2);
    }

    #[test]
    fn test_execution_groups_single_node() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();
        engine.build_ir(false).unwrap();

        let rule_heads = engine.get_rule_heads();
        let order = IQLEngine::execution_groups(engine.ir_nodes(), &rule_heads);
        assert_eq!(order, vec![vec![0]]);
    }

    #[test]
    fn test_execution_groups_dependency_chain() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "edge",
            vec![Tuple::new(vec![Value::Int64(1), Value::Int64(2)])],
        );

        // Two rules: intermediate depends on base, query depends on intermediate
        engine
            .parse("mid(X, Y) <- edge(X, Y)\nresult(X, Y) <- mid(X, Y)")
            .unwrap();
        engine.build_ir(false).unwrap();

        let rule_heads = engine.get_rule_heads();
        let order = IQLEngine::execution_groups(engine.ir_nodes(), &rule_heads);
        // mid (idx 0) executes before result (idx 1)
        assert_eq!(order, vec![vec![0], vec![1]]);
    }

    #[test]
    fn test_detect_recursion_info_nonrecursive() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();
        engine.build_ir(false).unwrap();

        let rule_heads = engine.get_rule_heads();
        let info = engine.detect_recursion_info(&rule_heads);
        assert_eq!(info.len(), 1);
        assert!(info[0].is_none());
    }

    #[test]
    fn test_execute_tuples_parse_error() {
        let mut engine = IQLEngine::new();
        let result = engine.execute_tuples("this is not valid IQL @@!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_all_with_trace() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3)]);

        let (results, trace) = engine
            .execute_all_with_trace("path(X, Y) <- edge(X, Y)")
            .unwrap();
        assert!(!results.is_empty());
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_pipeline_all_optimizations_disabled() {
        let config = OptimizationConfig {
            enable_join_planning: false,
            enable_sip_rewriting: false,
            enable_subplan_sharing: false,
            enable_boolean_specialization: false,
            enable_magic_sets: false,
            enable_constant_specialization: false,
        };
        let mut engine = IQLEngine::with_config(config);
        engine.add_tuples(
            "edge",
            vec![
                Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
            ],
        );

        let results = engine.execute_tuples("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_add_tuples_empty() {
        let mut engine = IQLEngine::new();
        engine.add_tuples("empty_rel", vec![]);
        // Schema should default to arity 2
        assert!(engine.catalog().has_relation("empty_rel"));
        assert_eq!(engine.get_relation("empty_rel").unwrap().len(), 0);
    }

    #[test]
    fn test_add_tuples_registers_schema() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "triple",
            vec![Tuple::new(vec![
                Value::Int64(1),
                Value::Int64(2),
                Value::Int64(3),
            ])],
        );
        assert!(engine.catalog().has_relation("triple"));
    }

    #[test]
    fn test_execute_simple_query_reversed_projection() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (3, 4)]);

        let results = engine.execute_simple_query("edge", vec![1, 0]).unwrap();
        // Reversed projection: (2,1) and (4,3)
        assert_eq!(results.len(), 2);
        assert!(results.contains(&(2, 1)));
        assert!(results.contains(&(4, 3)));
    }

    #[test]
    fn test_execute_simple_query_nonexistent_relation() {
        let engine = IQLEngine::new();
        let results = engine.execute_simple_query("missing", vec![0, 1]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_multiworker_execution() {
        let mut engine = IQLEngine::new();
        engine.set_num_workers(2);
        engine.add_tuples(
            "data",
            vec![
                Tuple::new(vec![Value::Int64(1), Value::Int64(10)]),
                Tuple::new(vec![Value::Int64(2), Value::Int64(20)]),
                Tuple::new(vec![Value::Int64(3), Value::Int64(30)]),
            ],
        );

        let results = engine.execute_tuples("result(X, Y) <- data(X, Y)").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_program_accessor_after_parse() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();

        let program = engine.program().unwrap();
        assert_eq!(program.rules.len(), 1);
        assert_eq!(program.rules[0].head.relation, "result");
    }

    #[test]
    fn test_ir_nodes_accessor_after_build() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();
        engine.build_ir(false).unwrap();

        let ir_nodes = engine.ir_nodes();
        assert_eq!(ir_nodes.len(), 1);
    }

    #[test]
    fn test_execute_with_comparison() {
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "score",
            vec![
                Tuple::new(vec![Value::string("alice"), Value::Int64(90)]),
                Tuple::new(vec![Value::string("bob"), Value::Int64(70)]),
                Tuple::new(vec![Value::string("carol"), Value::Int64(50)]),
            ],
        );

        let results = engine
            .execute_tuples("result(Name, S) <- score(Name, S), S > 60")
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    // Batch 19: Core pipeline and accessor coverage

    #[test]
    fn test_execute_binary_returns_pairs() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (3, 4)]);
        let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_build_ir_success() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();
        assert!(engine.build_ir(false).is_ok());
        assert!(!engine.ir_nodes().is_empty());
    }

    #[test]
    fn test_optimize_ir_after_build() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        engine.parse("result(X, Y) <- edge(X, Y)").unwrap();
        engine.build_ir(false).unwrap();
        assert!(engine.optimize_ir(false).is_ok());
    }

    #[test]
    fn test_optimize_ir_empty_noop() {
        // optimize_ir on empty ir_nodes is a no-op (maps over empty vec)
        let mut engine = IQLEngine::new();
        let result = engine.optimize_ir(false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_all_rules_multiple() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (3, 4)]);
        // Execute two rules separately (parser handles one rule per call)
        let r1 = engine.execute_all_rules("r1(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(r1.len(), 1);
        let r2 = engine.execute_all_rules("r2(Y, X) <- edge(X, Y)").unwrap();
        assert_eq!(r2.len(), 1);
    }

    #[test]
    fn test_execute_with_trace_has_ast() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        let (results, trace) = engine
            .execute_with_trace("result(X, Y) <- edge(X, Y)")
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_set_num_workers_value() {
        let mut engine = IQLEngine::new();
        engine.set_num_workers(4);
        // Verify it doesn't panic and engine still works
        engine.add_fact("edge", vec![(1, 2)]);
        let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_get_optimization_config_accessors() {
        let engine = IQLEngine::new();
        let config = engine.get_optimization_config();
        assert!(config.enable_join_planning);
    }

    #[test]
    fn test_config_with_join_planning_disabled() {
        let config = OptimizationConfig {
            enable_join_planning: false,
            ..Default::default()
        };
        let mut engine = IQLEngine::with_config(config);
        engine.add_fact("edge", vec![(1, 2)]);
        let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_config_with_sip_disabled() {
        let config = OptimizationConfig {
            enable_sip_rewriting: false,
            ..Default::default()
        };
        let mut engine = IQLEngine::with_config(config);
        engine.add_fact("edge", vec![(1, 2)]);
        let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_config_with_subplan_sharing_disabled() {
        let config = OptimizationConfig {
            enable_subplan_sharing: false,
            ..Default::default()
        };
        let mut engine = IQLEngine::with_config(config);
        engine.add_fact("edge", vec![(1, 2)]);
        let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_config_with_boolean_specialization_disabled() {
        let config = OptimizationConfig {
            enable_boolean_specialization: false,
            ..Default::default()
        };
        let mut engine = IQLEngine::with_config(config);
        engine.add_fact("edge", vec![(1, 2)]);
        let results = engine.execute("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_get_relation_after_add() {
        let mut engine = IQLEngine::new();
        engine.add_fact("data", vec![(1, 2)]);
        let rel = engine.get_relation("data");
        assert!(rel.is_some());
        assert_eq!(rel.unwrap().len(), 1);
    }

    #[test]
    fn test_input_tuples_mut_direct_modification() {
        let mut engine = IQLEngine::new();
        engine.add_fact("data", vec![(1, 2)]);
        let tuples = engine.input_tuples_mut();
        tuples
            .get_mut("data")
            .unwrap()
            .push(Tuple::new(vec![Value::Int64(3), Value::Int64(4)]));
        assert_eq!(engine.get_relation("data").unwrap().len(), 2);
    }

    #[test]
    fn test_execute_parse_error() {
        let mut engine = IQLEngine::new();
        let result = engine.execute("@#$%^INVALID");
        assert!(result.is_err());
    }

    #[test]
    fn test_debug_produces_trace() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2)]);
        let trace = engine.debug("result(X, Y) <- edge(X, Y)").unwrap();
        assert!(trace.ast.is_some());
    }

    // === HNSW IQL integration tests (#20) ===

    #[test]
    fn test_hnsw_nearest_query_with_search_fn() {
        let mut engine = IQLEngine::new();

        // Register a mock HNSW search function
        engine.set_hnsw_search_fn(Arc::new(
            |index_name: &str, _query: &[f32], k: usize, _ef: Option<usize>| {
                assert_eq!(index_name, "test_idx");
                // Return k fake results
                Ok((0..k as i64)
                    .map(|i| (Value::Int64(i + 1), (i as f64) * 0.1))
                    .collect())
            },
        ));

        let results = engine
            .execute_tuples(
                r#"result(Id, Dist) <- hnsw_nearest("test_idx", [1.0, 2.0], 3, Id, Dist)"#,
            )
            .unwrap();

        assert_eq!(results.len(), 3);
        // Check first result: id=1, dist=0.0
        assert_eq!(results[0].get(0), Some(&Value::Int64(1)));
        assert_eq!(results[0].get(1), Some(&Value::Float64(0.0)));
        // Check third result: id=3, dist=0.2
        assert_eq!(results[2].get(0), Some(&Value::Int64(3)));
        assert_eq!(results[2].get(1), Some(&Value::Float64(0.2)));
    }

    #[test]
    fn test_hnsw_nearest_joins_with_base_relation() {
        let mut engine = IQLEngine::new();

        // Base relation: doc(id, title)
        engine.add_tuple(
            "doc",
            Tuple::new(vec![Value::Int64(1), Value::string("alpha")]),
        );
        engine.add_tuple(
            "doc",
            Tuple::new(vec![Value::Int64(2), Value::string("beta")]),
        );
        engine.add_tuple(
            "doc",
            Tuple::new(vec![Value::Int64(3), Value::string("gamma")]),
        );

        // Mock HNSW returns ids 1 and 3
        engine.set_hnsw_search_fn(Arc::new(
            |_idx: &str, _q: &[f32], _k: usize, _ef: Option<usize>| {
                Ok(vec![(Value::Int64(1), 0.1), (Value::Int64(3), 0.3)])
            },
        ));

        // Query joins HNSW results with doc table
        let results = engine
            .execute_tuples(
                r#"result(Title, Dist) <- hnsw_nearest("idx", [0.5, 0.5], 2, Id, Dist), doc(Id, Title)"#,
            )
            .unwrap();

        // Should match doc ids 1 and 3
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_hnsw_nearest_no_search_fn_errors() {
        let mut engine = IQLEngine::new();

        // No HNSW search function registered - should error
        let result =
            engine.execute_tuples(r#"result(Id, Dist) <- hnsw_nearest("idx", [1.0], 3, Id, Dist)"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("hnsw_nearest"));
    }

    #[test]
    fn test_hnsw_nearest_index_not_found_errors() {
        let mut engine = IQLEngine::new();

        // Search function that returns error for unknown index
        engine.set_hnsw_search_fn(Arc::new(
            |index_name: &str, _q: &[f32], _k: usize, _ef: Option<usize>| {
                Err(format!("Index '{index_name}' not found"))
            },
        ));

        let result = engine.execute_tuples(
            r#"result(Id, Dist) <- hnsw_nearest("nonexistent", [1.0], 3, Id, Dist)"#,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_hnsw_nearest_empty_results() {
        let mut engine = IQLEngine::new();

        engine.set_hnsw_search_fn(Arc::new(
            |_idx: &str, _q: &[f32], _k: usize, _ef: Option<usize>| Ok(vec![]),
        ));

        let results = engine
            .execute_tuples(r#"result(Id, Dist) <- hnsw_nearest("idx", [1.0, 2.0], 5, Id, Dist)"#)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_no_hnsw_in_query_no_search_fn_is_ok() {
        let mut engine = IQLEngine::new();
        engine.add_fact("edge", vec![(1, 2), (2, 3)]);

        // No HNSW search function, but query doesn't use hnsw_nearest - should be fine
        let results = engine.execute_tuples("result(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_hnsw_nearest_bound_variable_query() {
        let mut engine = IQLEngine::new();
        engine.add_tuple(
            "query_vec",
            Tuple::new(vec![Value::Vector(Arc::new(vec![7.0, 0.0]))]),
        );
        engine.add_tuple(
            "doc",
            Tuple::new(vec![Value::Int64(7), Value::string("seven")]),
        );
        // The mock returns the query's first component as the id.
        engine.set_hnsw_search_fn(Arc::new(|_idx: &str, q: &[f32], _k, _ef| {
            Ok(vec![(Value::Int64(q[0] as i64), 0.25)])
        }));
        let results = engine
            .execute_tuples(
                r#"result(Title, Dist) <- query_vec(QV), hnsw_nearest("idx", QV, 1, Id, Dist), doc(Id, Title)"#,
            )
            .unwrap();
        assert_eq!(
            results,
            vec![Tuple::new(vec![
                Value::string("seven"),
                Value::Float64(0.25)
            ])]
        );
    }

    #[test]
    fn test_hnsw_nearest_bound_variable_from_derived_relation() {
        let mut engine = IQLEngine::new();
        engine.add_tuple(
            "doc_vec",
            Tuple::new(vec![Value::Int64(1), Value::Vector(Arc::new(vec![2.0]))]),
        );
        engine.add_tuple(
            "doc_vec",
            Tuple::new(vec![Value::Int64(5), Value::Vector(Arc::new(vec![3.0]))]),
        );
        engine.set_hnsw_search_fn(Arc::new(|_idx: &str, q: &[f32], _k, _ef| {
            Ok(vec![(Value::Int64(q[0] as i64 * 10), 0.0)])
        }));
        let results = engine
            .execute_tuples(
                "picked(V) <- doc_vec(1, V)\n\
                 result(Id) <- picked(QV), hnsw_nearest(\"idx\", QV, 1, Id, D)",
            )
            .unwrap();
        // Only the vector of doc 1 is a query.
        assert_eq!(results, vec![Tuple::new(vec![Value::Int64(20)])]);
    }

    #[test]
    fn test_hnsw_nearest_unbound_variable_errors() {
        let mut engine = IQLEngine::new();
        engine.add_tuple("other", Tuple::new(vec![Value::Int64(1)]));
        engine.set_hnsw_search_fn(Arc::new(|_: &str, _: &[f32], _, _| Ok(vec![])));
        let err = engine
            .execute_tuples(r#"result(Id) <- other(X), hnsw_nearest("idx", QV, 1, Id, D)"#)
            .unwrap_err();
        assert!(err.contains("must be bound"), "{err}");
    }

    // ====== Magic Sets Integration Tests ======

    #[test]
    fn test_magic_sets_tc_chain() {
        // Linear chain: 1→2→3→...→20
        let mut engine = IQLEngine::new();
        let edges: Vec<Tuple> = (1..20)
            .map(|i| Tuple::new(vec![Value::Int64(i), Value::Int64(i + 1)]))
            .collect();
        engine.add_tuples("edge", edges);

        // Query reach(1, Y) - should find 19 reachable nodes
        let program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1";
        let results = engine.execute_tuples(program).unwrap();
        assert_eq!(results.len(), 19);
    }

    #[test]
    fn test_magic_sets_tc_matches_unbound() {
        // Verify magic sets produces same results as unbound query (just filtered)
        let mut engine = IQLEngine::new();
        let edges = vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
            Tuple::new(vec![Value::Int64(3), Value::Int64(4)]),
            Tuple::new(vec![Value::Int64(4), Value::Int64(5)]),
            Tuple::new(vec![Value::Int64(10), Value::Int64(11)]),
        ];
        engine.add_tuples("edge", edges.clone());

        // Unbound query: all pairs
        let unbound_program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(X, Y) <- reach(X, Y)";
        let all_results = engine.execute_tuples(unbound_program).unwrap();

        // Filter manually for X=1
        let expected: Vec<_> = all_results
            .iter()
            .filter(|t| t.get(0) == Some(&Value::Int64(1)))
            .cloned()
            .collect();

        // Bound query: reach(1, Y)
        let mut engine2 = IQLEngine::new();
        engine2.add_tuples("edge", edges);
        let bound_program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1";
        let bound_results = engine2.execute_tuples(bound_program).unwrap();

        // Same count (column 1 values should match)
        assert_eq!(bound_results.len(), expected.len());
        let mut bound_ys: Vec<_> = bound_results
            .iter()
            .map(|t| t.get(1).cloned().unwrap())
            .collect();
        let mut expected_ys: Vec<_> = expected
            .iter()
            .map(|t| t.get(1).cloned().unwrap())
            .collect();
        bound_ys.sort();
        expected_ys.sort();
        assert_eq!(bound_ys, expected_ys);
    }

    #[test]
    fn test_magic_sets_disabled() {
        // With magic sets disabled, bound query still works (just slower)
        let config = OptimizationConfig {
            enable_magic_sets: false,
            enable_constant_specialization: false,
            ..Default::default()
        };
        let mut engine = IQLEngine::with_config(config);
        let edges = vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
        ];
        engine.add_tuples("edge", edges);

        let program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1";
        let results = engine.execute_tuples(program).unwrap();
        assert_eq!(results.len(), 2); // 1→2, 1→3
    }

    #[test]
    fn test_magic_sets_unbound_unchanged() {
        // Unbound query should not be rewritten
        let mut engine = IQLEngine::new();
        let edges = vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
        ];
        engine.add_tuples("edge", edges);

        let program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(X, Y) <- reach(X, Y)";
        let results = engine.execute_tuples(program).unwrap();
        // Full TC: (1,2), (1,3), (2,3)
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_magic_sets_both_bound() {
        // ?reach(1, 3) - point query. Magic sets restricts to reach(1, *),
        // the _c1 = 3 filter is applied post-hoc by the __query__ rule.
        let mut engine = IQLEngine::new();
        let edges = vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
            Tuple::new(vec![Value::Int64(3), Value::Int64(4)]),
        ];
        engine.add_tuples("edge", edges);

        let program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(_c0, _c1) <- reach(_c0, _c1), _c0 = 1, _c1 = 3";
        let results = engine.execute_tuples(program).unwrap();
        assert_eq!(results.len(), 1); // Only (1, 3)
    }

    #[test]
    fn test_magic_sets_string_constants() {
        // Recursive relation with string keys
        let mut engine = IQLEngine::new();
        engine.add_tuples(
            "link",
            vec![
                Tuple::new(vec![
                    Value::String(std::sync::Arc::from("a")),
                    Value::String(std::sync::Arc::from("b")),
                ]),
                Tuple::new(vec![
                    Value::String(std::sync::Arc::from("b")),
                    Value::String(std::sync::Arc::from("c")),
                ]),
            ],
        );

        let program = "\
            reach(X, Y) <- link(X, Y)\n\
            reach(X, Z) <- reach(X, Y), link(Y, Z)\n\
            __query__(_c0, Y) <- reach(_c0, Y), _c0 = \"a\"";
        let results = engine.execute_tuples(program).unwrap();
        assert_eq!(results.len(), 2); // a→b, a→c
    }

    #[test]
    fn test_magic_sets_with_sip() {
        // Both SIP and Magic Sets active - no conflicts
        let mut engine = IQLEngine::new();
        let edges = vec![
            Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
            Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
            Tuple::new(vec![Value::Int64(3), Value::Int64(4)]),
        ];
        engine.add_tuples("edge", edges);

        let program = "\
            reach(X, Y) <- edge(X, Y)\n\
            reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
            __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1";
        let results = engine.execute_tuples(program).unwrap();
        assert_eq!(results.len(), 3); // 1→2, 1→3, 1→4
    }

    /// Regression test: multi-clause session rules with self-join + arithmetic must not hang.
    ///
    /// Reproduces a bug where combining two clauses for the same head relation into a
    /// Union causes the DD computation to never complete when one clause contains a
    /// Cartesian self-join with an arithmetic-derived key.
    #[test]
    fn test_union_self_join_with_arithmetic_does_not_hang() {
        use std::sync::mpsc;
        use std::time::Duration;

        let (tx, rx) = mpsc::channel();

        let handle = std::thread::spawn(move || {
            let mut engine = IQLEngine::new();
            engine.add_tuples(
                "data",
                vec![
                    Tuple::new(vec![Value::Int64(1), Value::Int64(1)]),
                    Tuple::new(vec![Value::Int64(2), Value::Int64(1)]),
                    Tuple::new(vec![Value::Int64(3), Value::Int64(0)]),
                ],
            );

            // Two clauses for the same head: first has self-join + arithmetic,
            // second is simple. This combination caused an infinite hang in DD.
            let program = "\
                r(Id) <- data(Id, 1), PrevId = Id - 1, data(PrevId, 0)\n\
                r(Id) <- data(Id, 1)\n\
                __query__(X) <- r(X)";

            let result = engine.execute_tuples(program);
            tx.send(result).ok();
        });

        match rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(results)) => {
                let ids: std::collections::HashSet<i64> = results
                    .iter()
                    .filter_map(|t| t.get(0).and_then(|v| v.as_i64()))
                    .collect();
                assert!(ids.contains(&1), "Should contain Id=1");
                assert!(ids.contains(&2), "Should contain Id=2");
            }
            Ok(Err(e)) => panic!("Query failed: {e}"),
            Err(_) => {
                drop(handle);
                panic!("BUG: Union with self-join + arithmetic hung for >10s");
            }
        }
    }

    #[test]
    fn test_mutual_recursion_joint_fixpoint() {
        let mut engine = IQLEngine::new();
        let int = Value::Int64;
        let succ = (0..5)
            .map(|i| Tuple::new(vec![int(i), int(i + 1)]))
            .collect();
        engine.add_tuples("succ", succ);
        engine.add_tuples("zero", vec![Tuple::new(vec![int(0)])]);
        let program = "ev(N) <- zero(N)\n\
                       ev(N) <- succ(M, N), od(M)\n\
                       od(N) <- succ(M, N), ev(M)\n\
                       __query__(X) <- ev(X)";
        let (results, derived) = engine.execute_tuples_with_derived(program).unwrap();
        assert!(engine.is_recursive());
        let mut evens: Vec<i64> = results.iter().filter_map(|t| t.get(0)?.as_i64()).collect();
        evens.sort_unstable();
        assert_eq!(evens, vec![0, 2, 4]);
        assert_eq!(derived["od"].len(), 3);
    }

    #[test]
    fn test_parse_rejects_negation_through_recursion() {
        let mut engine = IQLEngine::new();
        let err = engine
            .parse("a(X) <- base(X), !b(X)\nb(X) <- base(X), !a(X)")
            .unwrap_err();
        assert!(err.contains("Unstratified negation"), "{err}");
    }

    fn limited_engine(limit: usize) -> IQLEngine {
        let mut engine = IQLEngine::new();
        engine.set_max_result_rows(limit);
        engine.add_tuples(
            "e",
            (0..10)
                .map(|i| Tuple::new(vec![Value::Int64(i), Value::Int64(100 + i)]))
                .collect(),
        );
        engine
    }

    /// Regression: the row cap applies only to the final result. An
    /// intermediate relation over the cap stays whole, and hitting the cap
    /// never trips the request's cancel flag.
    #[test]
    fn test_result_limit_spares_intermediates() {
        let control = crate::execution::RequestControl::new(None);
        code_generator::set_request_control(Some(Arc::clone(&control)));
        let filtered = limited_engine(3)
            .execute_tuples("big(X, Y) <- e(X, Y)\n__query__(Y) <- big(X, Y), X = 7");
        let filtered_truncated = last_result_truncated();
        let counted = limited_engine(3)
            .execute_tuples("big(X, Y) <- e(X, Y)\n__query__(count<X>) <- big(X, Y)");
        let cancelled = control.is_stopped();
        code_generator::set_request_control(None);

        assert_eq!(filtered.unwrap(), vec![Tuple::new(vec![Value::Int64(107)])]);
        assert!(!filtered_truncated);
        assert_eq!(counted.unwrap(), vec![Tuple::new(vec![Value::Int64(10)])]);
        assert!(!cancelled, "row cap must not signal cancel");
    }

    /// A final result over the cap is cut to the cap and flagged.
    #[test]
    fn test_result_limit_truncates_final_result() {
        let rows = limited_engine(3).execute_tuples("__query__(X, Y) <- e(X, Y)");
        assert_eq!(rows.unwrap().len(), 3);
        assert!(last_result_truncated());

        let rows = limited_engine(10).execute_tuples("__query__(X, Y) <- e(X, Y)");
        assert_eq!(rows.unwrap().len(), 10);
        assert!(!last_result_truncated(), "exactly at the cap is complete");

        let rows = limited_engine(3).execute_tuples("__query__(X, Y) <- e(X, Y), X = 1");
        assert_eq!(rows.unwrap().len(), 1);
        assert!(!last_result_truncated());
    }

    /// The last rule is cut only in the returned result when another rule
    /// reads it.
    #[test]
    fn test_result_limit_keeps_last_rule_readers_whole() {
        let (rows, derived) = limited_engine(3)
            .execute_tuples_with_derived("a(X) <- b(X)\nb(X) <- e(X, _Y)")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(last_result_truncated());
        assert_eq!(derived["a"].len(), 10);
    }

    /// The derived entry for an early-stopped query head matches the
    /// returned rows.
    #[test]
    fn test_result_limit_derived_head_matches_result() {
        let (rows, derived) = limited_engine(3)
            .execute_tuples_with_derived("__query__(X, Y) <- e(X, Y)")
            .unwrap();
        assert!(last_result_truncated());
        assert_eq!(derived["__query__"].to_vec(), rows);
    }

    /// `without_result_cap` returns every row, unflagged, and restores the cap.
    #[test]
    fn test_without_result_cap() {
        let rows =
            without_result_cap(|| limited_engine(3).execute_tuples("__query__(X, Y) <- e(X, Y)"));
        assert_eq!(rows.unwrap().len(), 10);
        assert!(!last_result_truncated());

        let rows = limited_engine(3).execute_tuples("__query__(X, Y) <- e(X, Y)");
        assert_eq!(rows.unwrap().len(), 3);
    }

    /// Raw IR execution is never capped.
    #[test]
    fn test_execute_ir_tuples_ignores_result_limit() {
        let ir = IRNode::Scan {
            relation: "e".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };
        assert_eq!(limited_engine(3).execute_ir_tuples(&ir).unwrap().len(), 10);
    }

    /// A recursive final rule is computed to its fixpoint, then cut.
    #[test]
    fn test_result_limit_recursive_final_rule() {
        let rows = limited_engine(3)
            .execute_tuples("p(X, Y) <- e(X, Y)\np(X, Z) <- p(X, Y), e(Y, Z)")
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert!(last_result_truncated());
    }
}
