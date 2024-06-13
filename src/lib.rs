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
// TODO: verify this condition
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

