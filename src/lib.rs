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
