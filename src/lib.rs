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

    /// Enable subplan sharing (common subexpression elimination)
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
    /// Supports arbitrary arity tuples with mixed types (int, float, string, vector)
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

    /// Semiring annotations from boolean specialization (one per IR node)
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
    pub fn build_ir(&mut self) -> Result<(), String> {
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

        for rule in &program.rules {
            let predicate = &rule.head.relation;

            // Skip if we've already processed this predicate
            if processed_predicates.contains(predicate) {
                continue;
            }
            processed_predicates.insert(predicate.clone());

            let rules_for_predicate = rules_by_head.get(predicate).unwrap();

            if rules_for_predicate.len() == 1 {
                // Single rule - build IR directly
                let ir = builder.build_ir(rule)?;
                ir_nodes.push(ir);
            } else {
                // Multiple rules - build IR for each and combine with Union
                let mut sub_irs = Vec::new();
                for r in rules_for_predicate {
                    let ir = builder.build_ir(r)?;
                    sub_irs.push(ir);
                }
                let union_ir = crate::ir::IRNode::Union { inputs: sub_irs };
                ir_nodes.push(union_ir);
            }
        }

        self.ir_nodes = ir_nodes;
        Ok(())
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
    pub fn optimize_ir(&mut self) -> Result<(), String> {
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
            if std::env::var("IL_DEBUG").is_ok() && !self.shared_views.is_empty() {
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
        self.ir_nodes = self
            .ir_nodes
            .iter()
            .map(|ir| optimizer.optimize(ir.clone()))
            .collect();

        Ok(())
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

        // Load input tuples
        for (relation, data) in &self.input_tuples {
            codegen.add_input(relation.clone(), data.clone());
        }

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
        let debug = std::env::var("IL_DEBUG").is_ok();

        self.ir_nodes
            .iter()
            .enumerate()
            .map(|(i, ir)| {
                let head_name = rule_heads.get(i).cloned().unwrap_or_default();
                if let IRNode::Union { inputs } = ir {
                    let is_recursive = CodeGenerator::references_relation(ir, &head_name);
                    if debug {
                        eprintln!(
                            "DEBUG: IR[{}] head='{}' is Union with {} inputs, recursive={}",
                            i,
                            head_name,
                            inputs.len(),
                            is_recursive
                        );
                    }
                    if is_recursive {
                        Some(head_name)
                    } else {
                        None
                    }
                } else {
                    if debug {
                        eprintln!("DEBUG: IR[{i}] head='{head_name}' is not Union");
                    }
                    None
                }
            })
            .collect()
    }

    /// Load all input data into a `CodeGenerator`
    fn load_inputs_into_codegen(
        &self,
        codegen: &mut CodeGenerator,
        accumulated: &HashMap<String, Vec<Tuple>>,
    ) {
        let debug = std::env::var("IL_DEBUG").is_ok();

        // Load input tuples
        for (relation, data) in &self.input_tuples {
            if debug {
                eprintln!(
                    "DEBUG: loading input_tuples['{}'] = {} tuples",
                    relation,
                    data.len()
                );
                for t in data.iter().take(3) {
                    eprintln!("  - {t:?}");
                }
            }
            codegen.add_input(relation.clone(), data.clone());
        }

        // Load accumulated results from previously executed rules
        for (rel_name, rel_data) in accumulated {
            if debug {
                eprintln!(
                    "DEBUG: loading accumulated['{}'] = {} tuples",
                    rel_name,
                    rel_data.len()
                );
            }
            codegen.add_input(rel_name.clone(), rel_data.clone());
        }
    }

    /// Execute shared views and return their results
    ///
    /// Shared views may reference each other (cascading sharing), so we execute
    /// them in dependency order using topological sort: views that reference no
    /// other views first, then views that depend on already-computed views.
    fn execute_shared_views(&self) -> Result<HashMap<String, Vec<Tuple>>, String> {
        let debug = std::env::var("IL_DEBUG").is_ok();
        let mut results: HashMap<String, Vec<Tuple>> = HashMap::new();

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

            results.insert(view_name.clone(), view_results);
        }

        Ok(results)
    }

    /// Collect all relation names referenced by Scan nodes in an IR tree
    /// Topologically sort IR nodes by their scan dependencies.
    ///
    /// If node A scans a relation produced by node B, then B must execute before A.
    /// For cycles (recursive mutual dependencies), nodes are kept in their original
    /// order. The last node always stays last (it's the query).
    fn topological_sort_ir_nodes(&self, rule_heads: &[String]) -> Vec<usize> {
        let n = self.ir_nodes.len();
        if n <= 1 {
            return (0..n).collect();
        }

        // Build name->index map for rule heads
        let head_to_idx: HashMap<&str, usize> = rule_heads
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        // Build dependency graph: deps[i] = set of indices that must execute before i
        let mut deps: Vec<std::collections::HashSet<usize>> =
            vec![std::collections::HashSet::new(); n];
        for (i, ir) in self.ir_nodes.iter().enumerate() {
            let mut scans = Vec::new();
            Self::collect_scan_relations(ir, &mut scans);
            for scan_name in &scans {
                if let Some(&j) = head_to_idx.get(scan_name.as_str()) {
                    if j != i {
                        deps[i].insert(j);
                    }
                }
            }
        }

        // Topological sort by in-degree reduction
        let mut in_degree: Vec<usize> = deps.iter().map(std::collections::HashSet::len).collect();
        let mut reverse_deps: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (i, dep_set) in deps.iter().enumerate() {
            for &j in dep_set {
                reverse_deps[j].push(i);
            }
        }

        // Start with nodes that have no dependencies
        // Use a BinaryHeap with Reverse to process lower indices first (deterministic)
        let mut queue: std::collections::BinaryHeap<std::cmp::Reverse<usize>> =
            std::collections::BinaryHeap::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                queue.push(std::cmp::Reverse(i));
            }
        }

        let mut order: Vec<usize> = Vec::with_capacity(n);
        while let Some(std::cmp::Reverse(i)) = queue.pop() {
            order.push(i);
            for &dependent in &reverse_deps[i] {
                in_degree[dependent] = in_degree[dependent].saturating_sub(1);
                if in_degree[dependent] == 0 {
                    queue.push(std::cmp::Reverse(dependent));
                }
            }
        }

        // If cycle detected (not all nodes included), add remaining in original order
        if order.len() < n {
            let in_order: std::collections::HashSet<usize> = order.iter().copied().collect();
            for i in 0..n {
                if !in_order.contains(&i) {
                    order.push(i);
                }
            }
        }

        // Ensure the last IR node (the query) stays last in execution order.
        // The query is always the last parsed rule and must execute after all others.
        let last_idx = n - 1;
        if let Some(pos) = order.iter().position(|&i| i == last_idx) {
            if pos != order.len() - 1 {
                order.remove(pos);
                order.push(last_idx);
            }
        }

        if std::env::var("IL_DEBUG").is_ok() {
            eprintln!("DEBUG topological_sort_ir_nodes: execution order = {order:?}");
        }

        order
    }

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
        let debug = std::env::var("IL_DEBUG").is_ok();
        if debug {
            eprintln!("DEBUG execute_tuples: starting");
        }

        // Parse, apply SIP rewriting, and build IR
        self.parse(source)?;
        self.apply_sip_rewriting();
        self.build_ir()?;

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
        self.optimize_ir()?;

        if self.ir_nodes.is_empty() {
            return Err("No IR nodes to execute".to_string());
        }

        // Execute shared views first (from subplan sharing optimization)
        let mut accumulated_results = self.execute_shared_views()?;

        // Execute main rules in dependency order (topological sort)
        let execution_order = self.topological_sort_ir_nodes(&rule_heads);
        let mut last_result: Vec<Tuple> = Vec::new();

        for &i in &execution_order {
            let head_name = rule_heads.get(i).cloned().unwrap_or_default();

            // Create fresh CodeGenerator for each rule (avoids timely state issues)
            let mut codegen = CodeGenerator::new();
            // Set per-rule semiring type from boolean specialization
            let semiring = self
                .semiring_annotations
                .get(i)
                .map_or(boolean_specialization::SemiringType::Counting, |a| {
                    a.semiring
                });
            codegen.set_semiring_type(semiring);
            self.load_inputs_into_codegen(&mut codegen, &accumulated_results);

            // Use unoptimized IR for recursive nodes, optimized for others
            let result = if let Some(Some(recursive_rel)) = recursive_info.get(i) {
                codegen.execute_recursive(&unoptimized_ir_nodes[i], recursive_rel)?
            } else if self.num_workers > 1 {
                // Use parallel execution when configured for multi-worker
                let config = code_generator::ExecutionConfig::with_workers(self.num_workers);
                codegen.execute_with_config(&self.ir_nodes[i], config)?
            } else {
                codegen.execute(&self.ir_nodes[i])?
            };

            last_result.clone_from(&result);

            // Store results for subsequent rules
            if !head_name.is_empty() {
                accumulated_results.insert(head_name, result);
            }
        }

        Ok(last_result)
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
        self.build_ir()?;
        self.optimize_ir()?;

        // Execute rules in dependency order, chaining intermediate results so SIP
        // intermediate rules feed into subsequent rules.
        let rule_heads = self.get_rule_heads();
        let execution_order = self.topological_sort_ir_nodes(&rule_heads);
        let mut accumulated: HashMap<String, Vec<Tuple>> = HashMap::new();
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
            // Load base facts
            for (relation, data) in &self.input_tuples {
                codegen.add_input(relation.clone(), data.clone());
            }
            // Load accumulated intermediate results
            for (rel, data) in &accumulated {
                codegen.add_input(rel.clone(), data.clone());
            }

            let rule_tuples = codegen.execute(ir)?;
            let rule_results: Vec<(i32, i32)> =
                rule_tuples.iter().filter_map(Tuple::to_pair).collect();
            results.insert(i, rule_results);

            // Store for subsequent rules
            if !head_name.is_empty() {
                accumulated.insert(head_name, rule_tuples);
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
        self.build_ir()?;
        trace.record_ir_before(self.ir_nodes.clone());

        // Optimize
        self.optimize_ir()?;
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
        self.build_ir()?;
        trace.record_ir_before(self.ir_nodes.clone());

        // Optimize
        self.optimize_ir()?;
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

    /// Explain a query plan without executing it.
    ///
    /// Runs the full compilation pipeline (parse → SIP → IR → optimize)
    /// and returns a PipelineTrace showing the plan at each stage.
    pub fn explain(&mut self, source: &str) -> Result<PipelineTrace, String> {
        let mut trace = PipelineTrace::new();

        // Parse + SIP rewriting
        self.parse(source)?;
        self.apply_sip_rewriting();

        if let Some(program) = &self.program {
            trace.record_ast(program.clone());
        }

        // Build IR
        self.build_ir()?;
        trace.record_ir_before(self.ir_nodes.clone());

        // Optimize
        self.optimize_ir()?;
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
            codegen.add_input(relation.to_string(), data.clone());
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

