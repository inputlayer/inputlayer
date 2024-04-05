//! # Code Generator
//!
//! Converts IR to Differential Dataflow code and executes it, returning results.
//!
//! ## Pipeline Position
//!
//! ```text
//! Optimized IRNode -> [Code Generator] -> DD Execution -> Results
//! ```
//!
//! ## Capabilities
//!
//! - Arbitrary arity tuples with multiple data types
//! - Complex joins with multi-column keys
//! - Generic projections (any column reordering or selection)
//! - Recursive evaluation via `.iterative()` scopes with `SemigroupVariable`
//! - Semi-naive evaluation for efficient fixpoint computation

use crate::boolean_specialization::SemiringType;
use crate::ir::{AggregateFunction, ArithOp, BuiltinFunction, IRExpression, IRNode, Predicate};
use crate::semiring_types::{BooleanDiff, DiffType};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::operators::join::{Join, JoinCore};
use differential_dataflow::operators::{Reduce, Threshold};
use differential_dataflow::Collection;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use timely::dataflow::operators::{Inspect, Map, Probe, ToStream};
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::order::Product;

use crate::temporal_ops;
use crate::value::{Tuple, Value};
use crate::vector_ops;

/// Iteration counter type for recursive scopes
pub type Iter = u32;

/// Configuration for multi-worker execution
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Number of worker threads (default: 1)
    pub num_workers: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        ExecutionConfig { num_workers: 1 }
    }
}

impl ExecutionConfig {
    /// Create a configuration with the specified number of workers
    pub fn with_workers(num_workers: usize) -> Self {
        ExecutionConfig { num_workers }
    }

    /// Create a single-worker configuration
    pub fn single_worker() -> Self {
        Self::default()
    }

    /// Create a configuration with the number of workers equal to the number of CPU cores
    pub fn all_cores() -> Self {
        ExecutionConfig {
            num_workers: num_cpus::get(),
        }
    }
}

/// Executes IR trees using Differential Dataflow.
pub struct CodeGenerator {
    /// Input data for base relations
    input_tuples: HashMap<String, Vec<Tuple>>,
    /// Semiring annotations (debug tracing only).
    #[allow(dead_code)]
    semiring_annotations: Vec<crate::boolean_specialization::SemiringAnnotation>,
    /// Diff type dispatch: Boolean -> BooleanDiff(i8), Counting/Min/Max -> isize.
    semiring_type: SemiringType,
}

impl CodeGenerator {
    /// Create a new code generator
    pub fn new() -> Self {
        CodeGenerator {
            input_tuples: HashMap::new(),
            semiring_annotations: Vec::new(),
            semiring_type: SemiringType::Counting, // safe default
        }
    }

    /// Set the semiring type for diff-type dispatch.
    /// Boolean -> BooleanDiff(i8), anything else -> isize.
    pub fn set_semiring_type(&mut self, st: SemiringType) {
        self.semiring_type = st;
    }

    /// Set semiring annotations from boolean specialization
    pub fn set_semiring_annotations(
        &mut self,
        annotations: Vec<crate::boolean_specialization::SemiringAnnotation>,
    ) {
        // TODO: verify this condition
        if std::env::var("IL_DEBUG").is_ok() && !annotations.is_empty() {
            for (i, ann) in annotations.iter().enumerate() {
                eprintln!(
                    "DEBUG CodeGen semiring[{}]: {:?} ({})",
                    i, ann.semiring, ann.reason
                );
            }
        }
        self.semiring_annotations = annotations;
    }

    /// Add input data for a relation
    pub fn add_input(&mut self, relation: String, data: Vec<Tuple>) {
        self.input_tuples.insert(relation, data);
    }

    /// Add input data for a relation (alias for `add_input`)
    pub fn add_input_tuples(&mut self, relation: String, data: Vec<Tuple>) {
        self.add_input(relation, data);
    }

    /// Execute IR and return results
    ///
    /// For recursive queries, use `execute_recursive` which handles
    /// fixpoint iteration. This method always executes a single pass.
    /// Dispatches to `BooleanDiff` or `isize` based on the semiring type.
    pub fn execute(&self, ir: &IRNode) -> Result<Vec<Tuple>, String> {
        if std::env::var("IL_DEBUG").is_ok() {
            eprintln!(
                "DEBUG CodeGen::execute: semiring={:?}, diff_type={}",
                self.semiring_type,
                if self.semiring_type == SemiringType::Boolean {
                    "BooleanDiff(i8)"
                } else {
                    "isize"
                }
            );
        }
        match self.semiring_type {
            SemiringType::Boolean => self.execute_single_pass_typed::<BooleanDiff>(ir),
            _ => self.execute_single_pass_typed::<isize>(ir),
        }
    }

    /// Alias for execute (for backward compatibility during migration)
    pub fn generate_and_execute_tuples(&self, ir: &IRNode) -> Result<Vec<Tuple>, String> {
        self.execute(ir)
    }

    /// Execute a single-pass (non-recursive) query, generic over the diff type.
    fn execute_single_pass_typed<R: DiffType>(&self, ir: &IRNode) -> Result<Vec<Tuple>, String> {
        // Shared results vector
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        // Clone data for move into closure
        let input_data = self.input_tuples.clone();
        let ir_clone = ir.clone();

        // Execute DD computation
        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();

            worker.dataflow::<(), _, _>(|scope| {
                // Generate collection from IR
                let collection =
                    Self::generate_collection_tuples::<_, R>(scope, &ir_clone, &input_data, None);

                // distinct_core::<R> gives set semantics while preserving diff type R
                collection
                    .distinct_core::<R>()
                    .inner
                    .inspect(move |(data, _time, _diff)| {
                        results_clone.lock().push(data.clone());
                    })
                    .probe_with(&mut probe);
            });

            // Wait for computation to complete
            while !probe.done() {
                worker.step();
            }
        });

        // Extract results from Arc<Mutex<>>
        // parking_lot::Mutex never poisons, so into_inner() returns the value directly
        let final_results = Arc::try_unwrap(results)
            .map_err(|_| "Failed to extract results")?
            .into_inner();

        Ok(final_results)
    }

    /// Recursive query via DD's `.iterative()` scope (semi-naive fixpoint).
