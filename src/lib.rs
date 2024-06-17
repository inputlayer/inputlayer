//! # `InputLayer` Datalog Engine
//!
//! Datalog engine built on Differential Dataflow.
//!
//! ## Pipeline Architecture
//!
//! ### Complete Pipeline
//! ```text
//! Datalog Source Code
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
//!     `-- Each Knowledge Graph -> DatalogEngine instance
//! ```
//!
//! ## Usage
//!
//! ### Basic Query Execution
//! ```rust
//! use inputlayer::DatalogEngine;
//!
//! let mut engine = DatalogEngine::new();
//!
//! // Define base facts
//! engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);
//!
//! // Define and execute rules (variables must be uppercase)
//! let program = "
//!     path(X, Y) :- edge(X, Y).
//!     path(X, Z) :- path(X, Y), edge(Y, Z).
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
//! let results = storage.execute_query("path(X,Y) :- edge(X,Y).").unwrap();
//!
//! // Persist to disk
//! storage.save_knowledge_graph("analytics").unwrap();
//! ```
//!
//! ## Module Organization
//!
//! | Module | Purpose |
//! |--------|---------|
//! | `parser` | Datalog -> AST |
//! | `ir_builder` | AST -> IR |
//! | `optimizer` | Basic IR optimizations |
//! | `join_planning` | Join order optimization |
//! | `sip_rewriting` | SIP semijoin reduction |
//! | `subplan_sharing` | Common subexpression elimination |
//! | `boolean_specialization` | Semiring selection |
//! | `code_generator` | IR -> Differential Dataflow |
//! | `recursion` | Recursion detection & stratification |
//! | `storage_engine` | Multi-knowledge-graph persistence |

// AST and IR modules (consolidated from crates/)
pub mod ast;
pub mod dd_computation;
pub mod derived_relations; // Derived relation materialization
pub mod hnsw_index; // HNSW vector index implementation
pub mod index_manager; // Index manager for vector similarity search
pub mod ir;

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
mod optimizer; // Basic IR optimizations
pub mod parser; // Datalog parsing & AST construction
pub mod rule_catalog; // Rule catalog for persistent rules
pub mod semiring_types; // Diff type abstraction: BooleanDiff, MinDiff, MaxDiff
mod sip_rewriting; // AST-level semijoin reduction
pub mod statement; // Datalog-native statement parser
mod subplan_sharing; // Common subexpression elimination

// Storage Engine
pub mod config; // Configuration system
pub mod storage; // Storage formats (Parquet, metadata)
pub mod storage_engine; // Multi-knowledge-graph storage engine

// Network Protocol (RPC)
pub mod protocol; // InputLayer RPC protocol (server/client)

// Execution hardening
pub mod execution; // Query timeout, resource limits, caching

// Value type system (production-grade arbitrary arity tuples)
pub mod value;

// Re-export value types for convenience
pub use value::{DataType, SchemaValidationError, Tuple, TupleSchema, Value};

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

// Utilities
mod catalog;
mod pipeline_trace;
mod recursion;
#[cfg(test)]
mod test_arithmetic;

// Re-export public types
pub use catalog::Catalog;
pub use code_generator::CodeGenerator;
pub use config::{Config, DurabilityMode};
pub use ir_builder::IRBuilder;
pub use optimizer::Optimizer;
pub use pipeline_trace::{OptimizationStats, PipelineTrace};
pub use storage_engine::StorageEngine;

// Re-export storage utilities (Parquet and CSV)
pub use storage::{
    load_from_csv, load_from_csv_with_options, load_from_parquet, save_to_csv,
    save_to_csv_with_options, save_to_parquet, CsvOptions, StorageError, StorageResult,
};

// Re-export execution utilities (timeout, limits, caching)
pub use execution::{
    CacheEntry, CacheStats, CancelHandle, ExecutionConfig, ExecutionError, ExecutionResult,
    MemoryTracker, QueryCache, QueryTimeout, ResourceError, ResourceLimits, TimeoutError,
};

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

// Re-export rule catalog
pub use rule_catalog::{validate_rule, validate_rules_stratification, RuleCatalog, RuleDefinition};

// Re-export index types
pub use hnsw_index::HnswIndex;
pub use index_manager::{
    DistanceMetric, HnswConfig, Index, IndexManager, IndexStats, IndexType, MaterializedIndex,
    RegisteredIndex, TupleId,
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

use std::collections::HashMap;

/// Configuration for advanced optimizations
#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    /// Enable join spanning tree planning
    pub enable_join_planning: bool,

    /// Enable SIP rewriting (semijoin reduction)
    pub enable_sip_rewriting: bool,

    /// Enable subplan sharing (common subexpression elimination.clone())
    pub enable_subplan_sharing: bool,

    /// Enable boolean specialization (semiring selection)
    pub enable_boolean_specialization: bool,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        OptimizationConfig {
            // Join planning is enabled - the code generator supports arbitrary arity
            // tuples (N-tuples) via the Value/Tuple type system. Join planning optimizes
            // join order using Maximum Spanning Tree algorithm.
            enable_join_planning: true,
            // SIP (Sideways Information Passing) - semijoin reduction.
            // Rewrites multi-join rules into chains of semijoin reduction rules that
            // filter intermediate results early. Skipped for recursive rules.
            enable_sip_rewriting: true,
            // Subplan sharing extracts common subexpressions into shared views.
            // The shared views are executed before main rules to materialize their data.
            enable_subplan_sharing: true,
            enable_boolean_specialization: true,
        }
    }
}


/// Main Datalog engine that orchestrates the entire pipeline
pub struct DatalogEngine {
    /// Input data for base relations (`relation_name` -> tuples)
    /// Supports arbitrary arity tuples with mixed types (int, float, string, vector.clone())
    /// Use `input_tuples()` and `input_tuples_mut()` for access.
    input_tuples: HashMap<String, Vec<Tuple>>,

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

    /// Semiring annotations from boolean specialization (one per IR node.clone())
    /// Used for diff-type dispatch (Boolean -> BooleanDiff, Counting -> isize)
    semiring_annotations: Vec<boolean_specialization::SemiringAnnotation>,

    /// Number of worker threads for parallel execution (1 = single-worker)
    num_workers: usize,
}

impl DatalogEngine {
    /// Default optimization config.
    pub fn new() -> Self {
        DatalogEngine {
            input_tuples: HashMap::new(),
            program: None,
            ir_nodes: Vec::new(),
            catalog: Catalog::new(),
            optimization_config: OptimizationConfig::default(),
            has_recursion: false,
            strata: Vec::new(),
            shared_views: HashMap::new(),
            semiring_annotations: Vec::new(),
            num_workers: 1,
        }
    }

    /// Create a new Datalog engine with custom optimization configuration
    pub fn with_config(config: OptimizationConfig) -> Self {
        DatalogEngine {
            input_tuples: HashMap::new(),
            program: None,
            ir_nodes: Vec::new(),
            catalog: Catalog::new(),
            optimization_config: config,
            has_recursion: false,
            strata: Vec::new(),
            shared_views: HashMap::new(),
            semiring_annotations: Vec::new(),
            num_workers: 1,
        }
    }

    /// Set the number of worker threads for parallel execution
    ///
    /// When `num_workers > 1`, non-recursive queries without joins use
    /// Rayon-based parallel execution with data partitioning.
    /// Recursive and join-containing queries always use single-worker
    /// DD execution for correctness.
    pub fn set_num_workers(&mut self, num_workers: usize) {
        self.num_workers = num_workers.max(1);
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

    /// Get the catalog
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Get immutable reference to input tuples
    pub fn input_tuples(&self) -> &HashMap<String, Vec<Tuple>> {
        &self.input_tuples
    }

    /// Get mutable reference to input tuples
    pub fn input_tuples_mut(&mut self) -> &mut HashMap<String, Vec<Tuple>> {
        &mut self.input_tuples
    }

    /// Get tuples for a specific relation
    pub fn get_relation(&self, relation: &str) -> Option<&Vec<Tuple>> {
        self.input_tuples.get(relation)
    }

    /// Add binary (i32, i32) tuples. For arbitrary arity, use `add_tuples`.
    ///
    /// # Example
    /// ```rust
    /// use inputlayer::DatalogEngine;
    ///
    /// let mut engine = DatalogEngine::new();
    /// engine.add_fact("edge", vec![(1, 2), (2, 3), (3, 4)]);
    /// ```
    pub fn add_fact(&mut self, relation: &str, data: Vec<(i32, i32)>) {
        // Convert to Tuple format
        let tuples: Vec<Tuple> = data.iter().map(|&(a, b)| Tuple::from_pair(a, b)).collect();
        self.input_tuples.insert(format!("{}", relation), tuples);

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
    /// use inputlayer::{DatalogEngine, Tuple, Value};
    ///
    /// let mut engine = DatalogEngine::new();
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

        self.input_tuples.insert(relation.to_string(), tuples);
    }

    /// Parse a Datalog program string into AST
    ///
    /// Converts Datalog source code into an Abstract Syntax Tree.
    /// Also performs safety validation, recursion detection, and stratification.
    ///
    /// ## Pipeline Steps
    /// 1. Parse source into AST
    /// 2. Validate rule safety
    /// 3. Detect recursive rules
    /// 4. Compute stratification (evaluation order)
    pub fn parse(&mut self, source: &str) -> Result<&Program, String> {
        // Parse source into AST
        let program = parser::parse_program(source)?;

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

        // Recursion detection
        self.has_recursion = recursion::has_recursion(&program);

        // Stratification - compute evaluation order using SCCs
        self.strata = recursion::stratify(&program);

        self.program = Some(program);
        Ok(self.program.as_ref().unwrap())
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

            // Compute recursive relations so SIP skips them.
            // A relation is recursive if it's in an SCC with a cycle.
            let dep_graph = recursion::build_dependency_graph(program);
            let sccs = recursion::find_sccs(&dep_graph);
            let recursive_rels: std::collections::HashSet<String> = sccs
                .iter()
                .filter(|scc| {
                    scc.len() > 1
                        || (scc.len() == 1
                            && dep_graph
                                .get(&scc[0])
                                .is_some_and(|deps| deps.contains(&scc[0])))
                })
                .flat_map(|scc| scc.iter().cloned())
                .collect();
            if std::env::var("IL_DEBUG").is_ok() && !recursive_rels.is_empty() {
                eprintln!("DEBUG SIP: skipping recursive relations: {recursive_rels:?}");
            }
            sip_rewriter.set_recursive_relations(recursive_rels);

            let rewritten = sip_rewriter.rewrite_program(program);
            let stats = sip_rewriter.get_stats();

            if std::env::var("IL_DEBUG").is_ok() {
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
                        "DEBUG SIP rule[{}]: {}({}) :- {}",
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

    /// Build IR from the parsed program
    ///
    /// Converts the AST into intermediate representation (IR) suitable for optimization.
    /// Uses the catalog to resolve variable positions in relations.
    ///
    /// For predicates with multiple rules (like recursive definitions), this creates
    /// a Union node combining all rules for that predicate.
