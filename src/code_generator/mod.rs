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
    pub fn execute_recursive_fixpoint_tuples(
        &self,
        ir: &IRNode,
        recursive_rel: &str,
    ) -> Result<Vec<Tuple>, String> {
        let inputs = match ir {
            IRNode::Union { inputs } => inputs,
            _ => return self.execute(ir),
        };

        // Partition inputs into base cases and recursive cases
        let (base_indices, recursive_indices) = if let Some((_, base, rec)) =
            Self::detect_recursive_union_for_relation(inputs, Some(recursive_rel))
        {
            if std::env::var("IL_DEBUG").is_ok() {
                eprintln!(
                    "DEBUG: recursive fixpoint: base_indices={base:?}, recursive_indices={rec:?}"
                );
            }
            (base, rec)
        } else {
            if std::env::var("IL_DEBUG").is_ok() {
                eprintln!(
                    "DEBUG: falling back to single_pass - detect_recursive_union returned None"
                );
            }
            return self.execute(ir);
        };

        let base_inputs: Vec<IRNode> = base_indices.iter().map(|&i| inputs[i].clone()).collect();
        let recursive_inputs: Vec<IRNode> = recursive_indices
            .iter()
            .map(|&i| inputs[i].clone())
            .collect();

        // Try to detect if this is a simple transitive closure pattern
        // If so, use the optimized DD iterative implementation
        if let Some(edge_relation) =
            Self::detect_transitive_closure_pattern(&base_inputs, &recursive_inputs, recursive_rel)
        {
            if std::env::var("IL_DEBUG").is_ok() {
                eprintln!(
                    "DEBUG: detected transitive closure pattern with edge relation '{edge_relation}'"
                );
            }
            return self.execute_transitive_closure_optimized(&edge_relation, recursive_rel);
        }

        // For complex patterns, use the general DD iterative approach.
        // For Min/Max semiring, we still use isize as the DD diff type but apply
        // early min/max aggregation inside the fixpoint loop to prune non-optimal
        // paths.
        match self.semiring_type {
            SemiringType::Boolean => self.execute_recursive_dd_iterative_typed::<BooleanDiff>(
                &base_inputs,
                &recursive_inputs,
                recursive_rel,
            ),
            _ => self.execute_recursive_dd_iterative_typed::<isize>(
                &base_inputs,
                &recursive_inputs,
                recursive_rel,
            ),
        }
    }

    /// Detect if the recursive pattern is a simple BINARY transitive closure
    ///
    /// Pattern must be EXACTLY:
    ///   base: tc(X, Y) :- edge(X, Y)
    ///   recursive: tc(X, Z) :- edge(X, Y), tc(Y, Z)
    ///
    /// This matches standard transitive closure where:
    /// - edge is on the LEFT side of the join, keyed by column 1
    /// - tc (recursive) is on the RIGHT side of the join, keyed by column 0
    ///
    /// Other patterns like `subordinate(Mgr, Emp) :- reports_to(Emp, Mid), subordinate(Mgr, Mid)`
    /// have different join key columns and must use the general recursive handler.
    fn detect_transitive_closure_pattern(
        base_inputs: &[IRNode],
        recursive_inputs: &[IRNode],
        recursive_rel: &str,
    ) -> Option<String> {
        // Check base case: should be a simple scan of some relation with exactly 2 columns
        if base_inputs.len() != 1 {
            return None;
        }

        let (edge_relation, schema_len) = match &base_inputs[0] {
            IRNode::Scan { relation, schema } => (relation.clone(), schema.len()),
            IRNode::Map {
                input, projection, ..
            } => {
                // For Map, check if output is binary
                if projection.len() != 2 {
                    return None;
                }
                match input.as_ref() {
                    IRNode::Scan { relation, .. } => (relation.clone(), 2),
                    _ => return None,
                }
            }
            _ => return None,
        };

        // CRITICAL: Only optimize binary relations (exactly 2 columns)
        if schema_len != 2 {
            return None;
        }

        // Check recursive case: must be a single rule
        if recursive_inputs.len() != 1 {
            return None;
        }

        // The recursive case must be a Join with specific structure:
        // - Left side scans edge relation, keyed by column 1
        // - Right side scans recursive relation, keyed by column 0
        match &recursive_inputs[0] {
            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                ..
            } => {
                // Check left side scans edge relation
                let left_scans_edge = match left.as_ref() {
                    IRNode::Scan { relation, .. } => relation == &edge_relation,
                    IRNode::Map { input, .. } => match input.as_ref() {
                        IRNode::Scan { relation, .. } => relation == &edge_relation,
                        _ => false,
                    },
                    _ => false,
                };

                // Check right side scans recursive relation
                let right_scans_recursive = match right.as_ref() {
                    IRNode::Scan { relation, .. } => relation == recursive_rel,
                    IRNode::Map { input, .. } => match input.as_ref() {
                        IRNode::Scan { relation, .. } => relation == recursive_rel,
                        _ => false,
                    },
                    _ => false,
                };

                // Check join keys: edge.col1 = recursive.col0
                let correct_keys = left_keys == &[1] && right_keys == &[0];

                if left_scans_edge && right_scans_recursive && correct_keys {
                    Some(edge_relation)
                } else {
                    None
                }
            }
            // Also handle Map over Join (for projections)
            IRNode::Map { input, .. } => match input.as_ref() {
                IRNode::Join {
                    left,
                    right,
                    left_keys,
                    right_keys,
                    ..
                } => {
                    let left_scans_edge = match left.as_ref() {
                        IRNode::Scan { relation, .. } => relation == &edge_relation,
                        _ => false,
                    };
                    let right_scans_recursive = match right.as_ref() {
                        IRNode::Scan { relation, .. } => relation == recursive_rel,
                        _ => false,
                    };
                    let correct_keys = left_keys == &[1] && right_keys == &[0];

                    if left_scans_edge && right_scans_recursive && correct_keys {
                        Some(edge_relation)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Optimized transitive closure using DD's native .`iterative()` scope
    ///
    /// This is O(n) for chain graphs vs O(n²) for naive iteration.
    /// Uses `SemigroupVariable` for proper semi-naive evaluation.
    fn execute_transitive_closure_optimized(
        &self,
        edge_relation: &str,
        recursive_rel: &str,
    ) -> Result<Vec<Tuple>, String> {
        match self.semiring_type {
            SemiringType::Boolean => {
                self.execute_transitive_closure_typed::<BooleanDiff>(edge_relation, recursive_rel)
            }
            _ => self.execute_transitive_closure_typed::<isize>(edge_relation, recursive_rel),
        }
    }

    fn execute_transitive_closure_typed<R: DiffType>(
        &self,
        edge_relation: &str,
        _recursive_rel: &str,
    ) -> Result<Vec<Tuple>, String> {
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        // Get edge data
        let edges: Vec<Tuple> = self
            .input_tuples
            .get(edge_relation)
            .cloned()
            .unwrap_or_default();

        if edges.is_empty() {
            return Ok(Vec::new());
        }

        let edge_data = edges.clone();

        // Execute DD computation with TRUE recursion using .iterative()
        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();

            worker.dataflow::<(), _, _>(|scope| {
                // Load edge data as base collection
                let edge_collection: Collection<_, Tuple, R> = Collection::new(
                    edge_data
                        .clone()
                        .to_stream(scope)
                        .map(|x| (x, (), R::one())),
                );

                // Use iterative scope for efficient semi-naive recursion
                let tc_result = scope.iterative::<Iter, _, _>(|inner| {
                    // Create SemigroupVariable for transitive closure
                    let variable: SemigroupVariable<_, Tuple, R> =
                        SemigroupVariable::new(inner, Product::new((), 1));

                    // Enter edge collection into iterative scope
                    let edges_in_scope = edge_collection.enter(inner);

                    // Recursive case: tc(x, z) :- tc(x, y), edge(y, z)
                    // Key tc by second column (y) for join with edge(y, z)
                    let tc_keyed = variable.map(|tuple| {
                        let x = tuple.get(0).cloned().unwrap_or(Value::Null);
                        let y = tuple.get(1).cloned().unwrap_or(Value::Null);
                        (Tuple::new(vec![y]), x) // Key by y, value is x
                    });

                    // Key edges by first column (y) for join
                    let edges_keyed = edges_in_scope.map(|tuple| {
                        let y = tuple.get(0).cloned().unwrap_or(Value::Null);
                        let z = tuple.get(1).cloned().unwrap_or(Value::Null);
                        (Tuple::new(vec![y]), z) // Key by y, value is z
                    });

                    // Join: tc(x, y) JOIN edge(y, z) -> tc(x, z)
                    let recursive = tc_keyed
                        .join(&edges_keyed)
                        .map(|(_y_key, (x, z))| Tuple::new(vec![x, z]));

                    // Combine base case and recursive case
                    let next = edges_in_scope.concat(&recursive).distinct_core::<R>();

                    // Set variable for next iteration
                    variable.set(&next);

                    // Leave scope with final result
                    next.leave()
                });

                // Capture results
                tc_result
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

        // Extract results
        // parking_lot::Mutex never poisons, so into_inner() returns the value directly
        let final_results = Arc::try_unwrap(results)
            .map_err(|_| "Failed to extract results")?
            .into_inner();

        Ok(final_results)
    }

    /// General recursive execution using DD's `.iterative()` scope
    ///
    /// Uses DD's native semi-naive evaluation via `SemigroupVariable` for proper
    /// incremental fixpoint computation. This handles arbitrary recursive patterns
    /// (not just transitive closure) by routing the recursive relation through a
    /// live collection in the iterative scope.
    /// Extract min/max aggregation info from a single recursive input IR node.
    /// Returns (group_by_indices, agg_col_index, is_min) if the top-level node
    /// is an Aggregate with a single Min or Max function.
    fn extract_minmax_aggregation(ir: &IRNode) -> Option<(Vec<usize>, usize, bool)> {
        match ir {
            IRNode::Aggregate {
                group_by,
                aggregations,
                ..
            } => {
                if aggregations.len() != 1 {
                    return None;
                }
                match &aggregations[0] {
                    (AggregateFunction::Min, col) => Some((group_by.clone(), *col, true)),
                    (AggregateFunction::Max, col) => Some((group_by.clone(), *col, false)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// Strip the top-level Aggregate node from an IR, returning the inner input.
    /// Used when we want to apply aggregation at a different point (e.g., in the
    /// fixpoint loop instead of inside the recursive body).
    fn strip_top_aggregate(ir: &IRNode) -> &IRNode {
        match ir {
            IRNode::Aggregate { input, .. } => input,
            _ => ir,
        }
    }

    fn execute_recursive_dd_iterative_typed<R: DiffType>(
        &self,
        base_inputs: &[IRNode],
        recursive_inputs: &[IRNode],
        recursive_rel: &str,
    ) -> Result<Vec<Tuple>, String> {
        // Check if we can use aggregation-in-loop optimization for min/max.
        // If ALL recursive inputs have a top-level min or max aggregate with the
        // same group_by and agg_col, we strip the aggregate from the recursive body
        // and apply it to the combined (base + recursive) result in the loop.
        // This prunes non-optimal paths early, reducing intermediate data.
        let agg_in_loop = if recursive_inputs.len() == 1 {
            Self::extract_minmax_aggregation(&recursive_inputs[0])
        } else {
            // For multiple recursive inputs, check if ALL have the same aggregation
            let first = Self::extract_minmax_aggregation(&recursive_inputs[0]);
            if let Some(ref first_agg) = first {
                let all_same = recursive_inputs[1..].iter().all(|ri| {
                    Self::extract_minmax_aggregation(ri).is_some_and(|a| a == *first_agg)
                });
                if all_same {
                    first
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Build the recursive IR, optionally stripping the aggregate
        let effective_recursive_inputs: Vec<IRNode> = if agg_in_loop.is_some() {
            recursive_inputs
                .iter()
                .map(|ri| Self::strip_top_aggregate(ri).clone())
                .collect()
        } else {
            recursive_inputs.to_vec()
        };

        let base_ir = if base_inputs.len() == 1 {
            base_inputs[0].clone()
        } else {
            IRNode::Union {
                inputs: base_inputs.to_vec(),
            }
        };
        let recursive_ir = if effective_recursive_inputs.len() == 1 {
            effective_recursive_inputs[0].clone()
        } else {
            IRNode::Union {
                inputs: effective_recursive_inputs,
            }
        };

        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);
        let input_data = self.input_tuples.clone();
        let rec_rel = recursive_rel.to_string();

        if std::env::var("IL_DEBUG").is_ok() {
            if let Some(ref agg) = agg_in_loop {
                eprintln!(
                    "DEBUG: recursive min/max aggregation-in-loop: group_by={:?}, agg_col={}, is_min={}",
                    agg.0, agg.1, agg.2
                );
            }
        }

        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();

            worker.dataflow::<(), _, _>(|scope| {
                // Generate base case collection from static input data
                let base_collection =
                    Self::generate_collection_tuples::<_, R>(scope, &base_ir, &input_data, None);

                // Use DD's iterative scope for proper semi-naive fixpoint evaluation
                let result = scope.iterative::<Iter, _, _>(|inner| {
                    // Create SemigroupVariable for the recursive relation
                    let variable: SemigroupVariable<_, Tuple, R> =
                        SemigroupVariable::new(inner, Product::new((), 1));

                    // Build live collections map:
                    // - All base relations entered into the iterative scope
                    // - The recursive relation backed by the SemigroupVariable
                    let mut live: HashMap<String, Collection<_, Tuple, R>> = HashMap::new();
                    for (name, tuples) in &input_data {
                        let coll: Collection<_, Tuple, R> = Collection::new(
                            tuples
                                .clone()
                                .to_stream(inner)
                                .map(|x| (x, Product::default(), R::one())),
                        );
                        live.insert(name.clone(), coll);
                    }
                    // Override the recursive relation with the SemigroupVariable
                    live.insert(rec_rel.clone(), (*variable).clone());

                    // Generate recursive body using live collections
                    // The code generator will use the SemigroupVariable's collection
                    // when scanning the recursive relation.
                    // Pass input_data so antijoin's eager collection can read base relations.
                    let recursive_result = Self::generate_collection_tuples::<_, R>(
                        inner,
                        &recursive_ir,
                        &input_data,
                        Some(&live),
                    );

                    // Enter base case into iterative scope
                    let base_in_scope = base_collection.enter(inner);

                    // Combine base + recursive results
                    let combined = base_in_scope.concat(&recursive_result);

                    // Apply deduplication strategy based on aggregation mode
                    let next = if let Some((ref group_by, agg_col, is_min)) = agg_in_loop {
                        // Min/Max aggregation-in-loop: instead of distinct(), apply
                        // reduce() with min/max logic. This prunes non-optimal paths
                        // at each iteration, reducing intermediate data volume.
                        let group_by = group_by.clone();
                        combined
                            .map(move |tuple| {
                                let key: Vec<Value> = group_by
                                    .iter()
                                    .map(|&i| tuple.get(i).cloned().unwrap_or(Value::Null))
                                    .collect();
                                (Tuple::new(key), tuple)
                            })
                            .reduce(move |_key, input, output| {
                                // Find the tuple with min/max value at agg_col
                                let best = if is_min {
                                    input.iter().min_by(|(a, _), (b, _)| {
                                        let va = a.get(agg_col).cloned().unwrap_or(Value::Null);
                                        let vb = b.get(agg_col).cloned().unwrap_or(Value::Null);
                                        va.cmp(&vb)
                                    })
                                } else {
                                    input.iter().max_by(|(a, _), (b, _)| {
                                        let va = a.get(agg_col).cloned().unwrap_or(Value::Null);
                                        let vb = b.get(agg_col).cloned().unwrap_or(Value::Null);
                                        va.cmp(&vb)
                                    })
                                };
                                if let Some((tuple, _count)) = best {
                                    output.push(((*tuple).clone(), R::one()));
                                }
                            })
                            .map(|(_key, tuple)| tuple)
                    } else {
                        // Standard deduplication with distinct
                        combined.distinct_core::<R>()
                    };

                    // Set variable for next iteration
                    variable.set(&next);

                    // Leave scope with final result
                    next.leave()
                });

                // Capture results
                result
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

        let final_results = Arc::try_unwrap(results)
            .map_err(|_| "Failed to extract results")?
            .into_inner();

        Ok(final_results)
    }

    /// Execute a recursive query using fixpoint iteration
    ///
    /// This is the public API for recursive execution from lib.rs.
    /// Dispatches to `BooleanDiff` or `isize` based on the semiring type.
    pub fn execute_recursive(
        &self,
        ir: &IRNode,
        recursive_rel: &str,
    ) -> Result<Vec<Tuple>, String> {
        self.execute_recursive_fixpoint_tuples(ir, recursive_rel)
    }

    /// Execute with Rayon-based parallelism. Falls back to single-worker for joins
    /// (data must be co-located). Scan/filter/map queries partition data across workers.
    pub fn execute_with_config(
        &self,
        ir: &IRNode,
        config: ExecutionConfig,
    ) -> Result<Vec<Tuple>, String> {
        use rayon::prelude::*;
        use std::collections::HashSet;

        if config.num_workers == 1 {
            // Fall back to direct execution for single worker
            return self.generate_and_execute_tuples(ir);
        }

        // Check if the IR contains joins - if so, use single-worker for correctness
        // Joins require coordinated data exchange which our simple partitioning doesn't handle
        if Self::contains_join(ir) {
            return self.generate_and_execute_tuples(ir);
        }

        // For queries without joins, we can partition and process in parallel
        let num_workers = config.num_workers;

        // Partition input data across workers
        let partitioned_inputs: Vec<HashMap<String, Vec<Tuple>>> = (0..num_workers)
            .map(|worker_idx| {
                Self::partition_data_for_worker(&self.input_tuples, worker_idx, num_workers)
            })
            .collect();

        let ir_clone = ir.clone();
        let semiring_type = self.semiring_type;

        // Execute in parallel using Rayon
        let all_results: Vec<Vec<Tuple>> = partitioned_inputs
            .into_par_iter()
            .map(|partition| {
                // Create a temporary code generator with this partition
                let mut temp_codegen = CodeGenerator::new();
                temp_codegen.set_semiring_type(semiring_type);
                for (relation, tuples) in partition {
                    temp_codegen.add_input_tuples(relation, tuples);
                }
                temp_codegen
                    .generate_and_execute_tuples(&ir_clone)
                    .unwrap_or_default()
            })
            .collect();

        // Merge and deduplicate results
        let mut combined: HashSet<Tuple> = HashSet::new();
        for results in all_results {
            combined.extend(results);
        }

        Ok(combined.into_iter().collect())
    }

    /// Check if IR tree contains any join operations
    fn contains_join(ir: &IRNode) -> bool {
        match ir {
            IRNode::Scan { .. } => false,
            IRNode::HnswScan { .. } => false,
            IRNode::Map { input, .. } => Self::contains_join(input),
            IRNode::Filter { input, .. } => Self::contains_join(input),
            IRNode::Join { .. } => true,
            IRNode::Distinct { input } => Self::contains_join(input),
            IRNode::Union { inputs } => inputs.iter().any(Self::contains_join),
            IRNode::Aggregate { input, .. } => Self::contains_join(input),
            IRNode::Antijoin { .. } => true, // Antijoin is also a join-like operation
            IRNode::Compute { input, .. } => Self::contains_join(input),
            IRNode::FlatMap { input, .. } => Self::contains_join(input),
            IRNode::JoinFlatMap { .. } => true,
        }
    }

    /// Execute with the number of workers equal to CPU cores
    pub fn execute_parallel(&self, ir: &IRNode) -> Result<Vec<Tuple>, String> {
        self.execute_with_config(ir, ExecutionConfig::all_cores())
    }

    /// Partition input data for a specific worker
    ///
    /// Each worker gets tuples where `hash(tuple) % num_workers == worker_index`
    fn partition_data_for_worker(
        input_data: &HashMap<String, Vec<Tuple>>,
        worker_index: usize,
        num_workers: usize,
    ) -> HashMap<String, Vec<Tuple>> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        input_data
            .iter()
            .map(|(relation, tuples)| {
                let partitioned: Vec<Tuple> = tuples
                    .iter()
                    .filter(|tuple| {
                        let mut hasher = DefaultHasher::new();
                        tuple.hash(&mut hasher);
                        let hash = hasher.finish() as usize;
                        hash % num_workers == worker_index
                    })
                    .cloned()
                    .collect();
                (relation.clone(), partitioned)
            })
            .collect()
    }

    /// Generate DD collection from IR (production: Tuple)
    fn generate_collection_tuples<G, R: DiffType>(
        scope: &mut G,
        ir: &IRNode,
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        match ir {
            IRNode::Scan { relation, .. } => {
                Self::generate_scan_tuples::<G, R>(scope, relation, input_data, live)
            }

            IRNode::Map {
                input, projection, ..
            } => Self::generate_map_tuples::<G, R>(scope, input, projection, input_data, live),

            IRNode::Filter { input, predicate } => {
                Self::generate_filter_tuples::<G, R>(scope, input, predicate, input_data, live)
            }

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                if std::env::var("IL_DEBUG").is_ok() {
                    eprintln!("DEBUG IRNode::Join: left schema={:?} right schema={:?} left_keys={:?} right_keys={:?} output_schema={:?}",
                             left.output_schema(), right.output_schema(), left_keys, right_keys, output_schema);
                }
                Self::generate_join_tuples::<G, R>(
                    scope,
                    left,
                    right,
                    left_keys,
                    right_keys,
                    output_schema,
                    input_data,
                    live,
                )
            }

            IRNode::Distinct { input } => {
                Self::generate_distinct_tuples::<G, R>(scope, input, input_data, live)
            }

            IRNode::Union { inputs } => {
                Self::generate_union_tuples::<G, R>(scope, inputs, input_data, live)
            }

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                ..
            } => Self::generate_aggregate_tuples::<G, R>(
                scope,
                input,
                group_by,
                aggregations,
                input_data,
                live,
            ),

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                ..
            } => Self::generate_antijoin_tuples::<G, R>(
                scope, left, right, left_keys, right_keys, input_data, live,
            ),

            IRNode::Compute { input, expressions } => {
                Self::generate_compute_tuples::<G, R>(scope, input, expressions, input_data, live)
            }

            IRNode::HnswScan { .. } => {
                // HNSW queries are resolved by the IndexManager before reaching
                // the DD pipeline. This IR node exists for completeness but the
                // actual nearest-neighbor search runs outside Differential Dataflow.
                // Returns an empty collection since this path is not reachable
                // during normal query execution.
                Collection::new(
                    Vec::<Tuple>::new()
                        .to_stream(scope)
                        .map(|x| (x, Default::default(), R::one())),
                )
            }

            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                ..
            } => {
                // Fused Map+Filter: uses flat_map() to apply projection + optional filter
                // in a single DD operator, eliminating intermediate collection
                let input_coll =
                    Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
                let projection = projection.clone();
                let pred_fn = filter_predicate
                    .as_ref()
                    .map(|p| Self::predicate_to_tuple_fn(p));

                input_coll.flat_map(move |tuple| {
                    let projected = tuple.project(&projection);
                    match &pred_fn {
                        Some(f) => {
                            if f(&projected) {
                                Some(projected)
                            } else {
                                None
                            }
                        }
                        None => Some(projected),
                    }
                })
            }

            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                ..
            } => {
                // Fused Join+Map+Filter using DD's join_map/join_core to avoid
                // materializing an intermediate (key, (left, right)) collection.
                let left_coll =
                    Self::generate_collection_tuples::<G, R>(scope, left, input_data, live);
                let right_coll =
                    Self::generate_collection_tuples::<G, R>(scope, right, input_data, live);

                let left_key_indices = left_keys.clone();
                let right_key_indices = right_keys.clone();
                let projection = projection.clone();

                // Key left by join columns
                let left_keyed = left_coll.map(move |tuple| {
                    let key = Tuple::new(
                        left_key_indices
                            .iter()
                            .map(|&i| tuple.values()[i].clone())
                            .collect(),
                    );
                    (key, tuple)
                });

                // Key right by join columns
                let right_keyed = right_coll.map(move |tuple| {
                    let key = Tuple::new(
                        right_key_indices
                            .iter()
                            .map(|&i| tuple.values()[i].clone())
                            .collect(),
                    );
                    (key, tuple)
                });

                if filter_predicate.is_none() {
                    // No filter: use join_map to fuse join + projection in one operator
                    left_keyed.join_map(&right_keyed, move |_key, left_tuple, right_tuple| {
                        let combined = left_tuple.concat(right_tuple);
                        combined.project(&projection)
                    })
                } else {
                    // With filter: use arrange_by_key + join_core which supports
                    // returning Option (skipping non-matching tuples)
                    let pred_fn = filter_predicate
                        .as_ref()
                        .map(|p| Self::predicate_to_tuple_fn(p));
                    let left_arranged = left_keyed.arrange_by_key();
                    let right_arranged = right_keyed.arrange_by_key();
                    left_arranged.join_core(
                        &right_arranged,
                        move |_key, left_tuple, right_tuple| {
                            let combined = left_tuple.concat(right_tuple);
                            let projected = combined.project(&projection);
                            match &pred_fn {
                                Some(f) if !f(&projected) => None,
                                _ => Some(projected),
                            }
                        },
                    )
                }
            }
        }
    }

    /// Generate scan node (production)
    ///
    /// If `live` collections are provided and contain the relation, returns a clone
    /// of the live collection (used in iterative scopes for recursive relations).
    /// Otherwise falls back to creating a collection from static `input_data`.
    fn generate_scan_tuples<G, R: DiffType>(
        scope: &mut G,
        relation: &str,
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        // Check live collections first (for recursive relations in iterative scopes)
        if let Some(live_map) = live {
            if let Some(collection) = live_map.get(relation) {
                if std::env::var("IL_DEBUG").is_ok() {
                    eprintln!("DEBUG Scan '{relation}': using live collection");
                }
                return collection.clone();
            }
        }

        let data = input_data.get(relation).cloned().unwrap_or_default();
        if std::env::var("IL_DEBUG").is_ok() {
            eprintln!("DEBUG Scan '{}': {} tuples", relation, data.len());
            for t in &data {
                eprintln!("DEBUG Scan '{}': {:?}", relation, t.values());
            }
        }
        Collection::new(
            data.to_stream(scope)
                .map(|x| (x, Default::default(), R::one())),
        )
    }

    /// Generate map node (production: arbitrary projection)
    fn generate_map_tuples<G, R: DiffType>(
        scope: &mut G,
        input: &IRNode,
        projection: &[usize],
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        let input_coll = Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
        let projection = projection.to_vec();

        input_coll.map(move |tuple| tuple.project(&projection))
    }

    /// Generate filter node (production)
    fn generate_filter_tuples<G, R: DiffType>(
        scope: &mut G,
        input: &IRNode,
        predicate: &Predicate,
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        let input_coll = Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
        let pred_fn = Self::predicate_to_tuple_fn(predicate);
        input_coll.filter(move |tuple| pred_fn(tuple))
    }

    /// Convert predicate to filter function (production: Tuple)
    fn predicate_to_tuple_fn(
        predicate: &Predicate,
    ) -> Box<dyn Fn(&Tuple) -> bool + Send + Sync + 'static> {
        match predicate.clone() {
            // Integer comparisons (with float fallback for mixed numeric types)
            Predicate::ColumnEqConst(col, val) => Box::new(move |tuple: &Tuple| {
                if let Some(v) = tuple.get(col) {
                    // Try integer first
                    if let Some(i) = v.as_i64() {
                        return i == val;
                    }
                    // Fall back to float comparison for Float64 values
                    if let Some(f) = v.as_f64() {
                        return (f - (val as f64)).abs() < f64::EPSILON;
                    }
                }
                false
            }),
            Predicate::ColumnNeConst(col, val) => Box::new(move |tuple: &Tuple| {
                if let Some(v) = tuple.get(col) {
                    // Try integer first
                    if let Some(i) = v.as_i64() {
                        return i != val;
                    }
                    // Fall back to float comparison for Float64 values
                    if let Some(f) = v.as_f64() {
                        return (f - (val as f64)).abs() >= f64::EPSILON;
                    }
                }
                true
            }),
            Predicate::ColumnGtConst(col, val) => Box::new(move |tuple: &Tuple| {
                if let Some(v) = tuple.get(col) {
                    // Try integer first
                    if let Some(i) = v.as_i64() {
                        return i > val;
                    }
                    // Fall back to float comparison for Float64 values
                    if let Some(f) = v.as_f64() {
                        return f > (val as f64);
                    }
                }
                false
            }),
            Predicate::ColumnLtConst(col, val) => Box::new(move |tuple: &Tuple| {
                if let Some(v) = tuple.get(col) {
                    // Try integer first
                    if let Some(i) = v.as_i64() {
                        return i < val;
                    }
                    // Fall back to float comparison for Float64 values
                    if let Some(f) = v.as_f64() {
                        return f < (val as f64);
                    }
                }
                false
            }),
            Predicate::ColumnGeConst(col, val) => Box::new(move |tuple: &Tuple| {
                if let Some(v) = tuple.get(col) {
                    // Try integer first
                    if let Some(i) = v.as_i64() {
                        return i >= val;
                    }
                    // Fall back to float comparison for Float64 values
                    if let Some(f) = v.as_f64() {
                        return f >= (val as f64);
                    }
                }
                false
            }),
            Predicate::ColumnLeConst(col, val) => Box::new(move |tuple: &Tuple| {
                if let Some(v) = tuple.get(col) {
                    // Try integer first
                    if let Some(i) = v.as_i64() {
                        return i <= val;
                    }
                    // Fall back to float comparison for Float64 values
                    if let Some(f) = v.as_f64() {
                        return f <= (val as f64);
                    }
                }
                false
            }),
            // String comparisons
            Predicate::ColumnEqStr(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s == val)
            }),
            Predicate::ColumnNeStr(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(|v| v.as_str())
                    .is_none_or(|s| s != val)
            }),
            Predicate::ColumnLtStr(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s < val.as_str())
            }),
            Predicate::ColumnGtStr(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s > val.as_str())
            }),
            Predicate::ColumnLeStr(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s <= val.as_str())
            }),
            Predicate::ColumnGeStr(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| s >= val.as_str())
            }),
            // Float comparisons
            Predicate::ColumnEqFloat(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(super::value::Value::as_f64)
                    .is_some_and(|f| (f - val).abs() < f64::EPSILON)
            }),
            Predicate::ColumnNeFloat(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(super::value::Value::as_f64)
                    .is_none_or(|f| (f - val).abs() >= f64::EPSILON)
            }),
            Predicate::ColumnGtFloat(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(super::value::Value::as_f64)
                    .is_some_and(|f| f > val)
            }),
            Predicate::ColumnLtFloat(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(super::value::Value::as_f64)
                    .is_some_and(|f| f < val)
            }),
            Predicate::ColumnGeFloat(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(super::value::Value::as_f64)
                    .is_some_and(|f| f >= val)
            }),
            Predicate::ColumnLeFloat(col, val) => Box::new(move |tuple: &Tuple| {
                tuple
                    .get(col)
                    .and_then(super::value::Value::as_f64)
                    .is_some_and(|f| f <= val)
            }),
            // Column comparisons
            Predicate::ColumnsEq(left, right) => Box::new(move |tuple: &Tuple| {
                let lv = tuple.get(left);
                let rv = tuple.get(right);
                lv == rv
            }),
            Predicate::ColumnsNe(left, right) => Box::new(move |tuple: &Tuple| {
                let lv = tuple.get(left);
                let rv = tuple.get(right);
                lv != rv
            }),
            // Column-to-column ordering comparisons
            Predicate::ColumnsLt(left, right) => Box::new(move |tuple: &Tuple| {
                match (tuple.get(left), tuple.get(right)) {
                    (Some(lv), Some(rv)) => {
                        // Try integer comparison first
                        if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                            return li < ri;
                        }
                        // Fall back to float comparison
                        if let (Some(lf), Some(rf)) = (lv.as_f64(), rv.as_f64()) {
                            return lf < rf;
                        }
                        false
                    }
                    _ => false,
                }
            }),
            Predicate::ColumnsGt(left, right) => {
                Box::new(
                    move |tuple: &Tuple| match (tuple.get(left), tuple.get(right)) {
                        (Some(lv), Some(rv)) => {
                            if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                                return li > ri;
                            }
                            if let (Some(lf), Some(rf)) = (lv.as_f64(), rv.as_f64()) {
                                return lf > rf;
                            }
                            false
                        }
                        _ => false,
                    },
                )
            }
            Predicate::ColumnsLe(left, right) => {
                Box::new(
                    move |tuple: &Tuple| match (tuple.get(left), tuple.get(right)) {
                        (Some(lv), Some(rv)) => {
                            if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                                return li <= ri;
                            }
                            if let (Some(lf), Some(rf)) = (lv.as_f64(), rv.as_f64()) {
                                return lf <= rf;
                            }
                            false
                        }
                        _ => false,
                    },
                )
            }
            Predicate::ColumnsGe(left, right) => {
                Box::new(
                    move |tuple: &Tuple| match (tuple.get(left), tuple.get(right)) {
                        (Some(lv), Some(rv)) => {
                            if let (Some(li), Some(ri)) = (lv.as_i64(), rv.as_i64()) {
                                return li >= ri;
                            }
                            if let (Some(lf), Some(rf)) = (lv.as_f64(), rv.as_f64()) {
                                return lf >= rf;
                            }
                            false
                        }
                        _ => false,
                    },
                )
            }
            // Logical combinations
            Predicate::And(p1, p2) => {
                let f1 = Self::predicate_to_tuple_fn(&p1);
                let f2 = Self::predicate_to_tuple_fn(&p2);
                Box::new(move |tuple| f1(tuple) && f2(tuple))
            }
            Predicate::Or(p1, p2) => {
                let f1 = Self::predicate_to_tuple_fn(&p1);
                let f2 = Self::predicate_to_tuple_fn(&p2);
                Box::new(move |tuple| f1(tuple) || f2(tuple))
            }
            // Runtime arithmetic comparison
            Predicate::ColumnCompareArith(col, cmp_op, arith_expr, var_map) => {
                Box::new(move |tuple: &Tuple| {
                    // Evaluate the arithmetic expression with runtime values
                    let arith_val = Self::eval_arith_runtime(&arith_expr, tuple, &var_map);
                    let Some(arith_val) = arith_val else {
                        return false; // Could not evaluate
                    };

                    // Get the column value to compare against
                    let Some(col_val) = tuple.get(col) else {
                        return false;
                    };

                    // Try integer comparison
                    if let Some(col_i) = col_val.as_i64() {
                        return match cmp_op {
                            crate::ast::ComparisonOp::Equal => col_i == arith_val,
                            crate::ast::ComparisonOp::NotEqual => col_i != arith_val,
                            crate::ast::ComparisonOp::LessThan => col_i < arith_val,
                            crate::ast::ComparisonOp::LessOrEqual => col_i <= arith_val,
                            crate::ast::ComparisonOp::GreaterThan => col_i > arith_val,
                            crate::ast::ComparisonOp::GreaterOrEqual => col_i >= arith_val,
                        };
                    }

                    // Fall back to float comparison
                    if let Some(col_f) = col_val.as_f64() {
                        let arith_f = arith_val as f64;
                        return match cmp_op {
                            crate::ast::ComparisonOp::Equal => {
                                (col_f - arith_f).abs() < f64::EPSILON
                            }
                            crate::ast::ComparisonOp::NotEqual => {
                                (col_f - arith_f).abs() >= f64::EPSILON
                            }
                            crate::ast::ComparisonOp::LessThan => col_f < arith_f,
                            crate::ast::ComparisonOp::LessOrEqual => col_f <= arith_f,
                            crate::ast::ComparisonOp::GreaterThan => col_f > arith_f,
                            crate::ast::ComparisonOp::GreaterOrEqual => col_f >= arith_f,
                        };
                    }

                    false
                })
            }
            // Runtime arithmetic compared to constant
            Predicate::ArithCompareConst(arith_expr, cmp_op, const_val, var_map) => {
                Box::new(move |tuple: &Tuple| {
                    let Some(arith_val) = Self::eval_arith_runtime(&arith_expr, tuple, &var_map)
                    else {
                        return false;
                    };
                    match cmp_op {
                        crate::ast::ComparisonOp::Equal => arith_val == const_val,
                        crate::ast::ComparisonOp::NotEqual => arith_val != const_val,
                        crate::ast::ComparisonOp::LessThan => arith_val < const_val,
                        crate::ast::ComparisonOp::LessOrEqual => arith_val <= const_val,
                        crate::ast::ComparisonOp::GreaterThan => arith_val > const_val,
                        crate::ast::ComparisonOp::GreaterOrEqual => arith_val >= const_val,
                    }
                })
            }
            Predicate::True => Box::new(|_| true),
            Predicate::False => Box::new(|_| false),
        }
    }

    /// Evaluate an arithmetic expression at runtime using tuple values
    fn eval_arith_runtime(
        expr: &crate::ast::ArithExpr,
        tuple: &Tuple,
        var_map: &std::collections::HashMap<String, usize>,
    ) -> Option<i64> {
        use crate::ast::{ArithExpr, ArithOp};
        match expr {
            ArithExpr::Constant(val) => Some(*val),
            ArithExpr::FloatConstant(bits) => Some(f64::from_bits(*bits) as i64),
            ArithExpr::Variable(name) => {
                let col_idx = var_map.get(name)?;
                tuple.get(*col_idx)?.as_i64()
            }
            ArithExpr::Binary { op, left, right } => {
                let left_val = Self::eval_arith_runtime(left, tuple, var_map)?;
                let right_val = Self::eval_arith_runtime(right, tuple, var_map)?;
                match op {
                    ArithOp::Add => Some(left_val + right_val),
                    ArithOp::Sub => Some(left_val - right_val),
                    ArithOp::Mul => Some(left_val * right_val),
                    ArithOp::Div if right_val != 0 => Some(left_val / right_val),
                    ArithOp::Mod if right_val != 0 => Some(left_val % right_val),
                    _ => None, // Division by zero
                }
            }
        }
    }

    /// Generate join node (production: multi-column keys)
    ///
    /// Output schema follows IR builder convention:
    /// - All columns from left in their original order
    /// - Non-key columns from right
    fn generate_join_tuples<G, R: DiffType>(
        scope: &mut G,
        left: &IRNode,
        right: &IRNode,
        left_keys: &[usize],
        right_keys: &[usize],
        _output_schema: &[String],
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        let left_coll = Self::generate_collection_tuples::<G, R>(scope, left, input_data, live);
        let right_coll = Self::generate_collection_tuples::<G, R>(scope, right, input_data, live);

        // CARTESIAN PRODUCT FIX: When both key arrays are empty, we need a
        // Cartesian product (cross join). Using empty tuples as keys causes
        // issues in Differential Dataflow, so we use a sentinel value instead.
        let is_cartesian = left_keys.is_empty() && right_keys.is_empty();

        if is_cartesian {
            // All tuples keyed by the same constant = full Cartesian product
            let sentinel = Tuple::new(vec![Value::Int64(0)]);

            let left_keyed = left_coll.map(move |tuple| (sentinel.clone(), tuple));

            let sentinel2 = Tuple::new(vec![Value::Int64(0)]);
            let right_keyed = right_coll.map(move |tuple| (sentinel2.clone(), tuple));

            // For Cartesian product, concatenate ALL columns from both sides
            left_keyed
                .join(&right_keyed)
                .map(|(_key, (left_tuple, right_tuple))| left_tuple.concat(&right_tuple))
        } else {
            // Normal join with actual keys
            let left_keys = left_keys.to_vec();
            let right_keys = right_keys.to_vec();
            let right_keys_clone = right_keys.clone();

            // Map to (key, full_tuple) format - keep full tuples for correct reconstruction
            let left_keyed = left_coll.map(move |tuple| {
                let key = tuple.from_indices(&left_keys);
                (key, tuple)
            });

            let right_keyed = right_coll.map(move |tuple| {
                let key = tuple.from_indices(&right_keys_clone);
                (key, tuple)
            });

            // Join and reconstruct: all of left + non-key columns of right
            let right_keys_for_map = right_keys.clone();
            left_keyed
                .join(&right_keyed)
                .map(move |(_key, (left_tuple, right_tuple))| {
                    // Output schema: all columns from left, then non-key columns from right
                    let right_non_keys = right_tuple.excluding_indices(&right_keys_for_map);
                    left_tuple.concat(&right_non_keys)
                })
        }
    }

    /// Generate antijoin node (negation): Left - (Left JOIN Right)
    ///
    /// Implements stratified negation by computing:
    /// - All tuples from left that do NOT have a matching tuple in right
    ///
    /// ## DD Implementation
    ///
    /// For stratified negation, the right side is always fully computed before
    /// the antijoin. We collect all right keys into a set, then filter the left
    /// collection to exclude tuples whose keys are in the right set.
    ///
    /// The key insight is that for stratified negation, we can safely collect
    /// the right side into memory first since it's already materialized.
    ///
    /// ## Example
    /// ```text
    /// unreachable(x) :- node(x), !reach(x).
    ///
    /// left (node):  [(1,), (2,), (3,), (4,)]
    /// right (reach): [(1,), (2,)]
    /// antijoin:      [(3,), (4,)]  // nodes not in reach
    /// ```
    fn generate_antijoin_tuples<G, R: DiffType>(
        scope: &mut G,
        left: &IRNode,
        right: &IRNode,
        left_keys: &[usize],
        right_keys: &[usize],
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        use std::collections::HashSet;

        // For stratified negation, collect right keys eagerly
        // This is correct because the right side is already fully computed
        let right_keys_set: HashSet<Tuple> = {
            let mut set = HashSet::new();
            // Recursively collect all tuples from the right IR node
            Self::collect_tuples_from_ir(right, input_data, right_keys, &mut set);
            set
        };

        let left_coll = Self::generate_collection_tuples::<G, R>(scope, left, input_data, live);
        let left_keys_vec = left_keys.to_vec();

        // Filter left to only keep tuples whose key is NOT in right set
        left_coll.filter(move |tuple| {
            let key = tuple.from_indices(&left_keys_vec);
            !right_keys_set.contains(&key)
        })
    }

    /// Helper function to recursively collect tuples from an IR node into a `HashSet`
    fn collect_tuples_from_ir(
        node: &IRNode,
        input_data: &HashMap<String, Vec<Tuple>>,
        key_indices: &[usize],
        result: &mut std::collections::HashSet<Tuple>,
    ) {
        match node {
            IRNode::Scan { relation, .. } => {
                if let Some(tuples) = input_data.get(relation) {
                    for tuple in tuples {
                        let key = tuple.from_indices(key_indices);
                        result.insert(key);
                    }
                }
            }
            IRNode::Filter { input, .. } => {
                // For filter, we need to generate and filter tuples
                // This is a simplified version - full implementation would evaluate the filter
                Self::collect_tuples_from_ir(input, input_data, key_indices, result);
            }
            IRNode::Map { input, .. } => {
                Self::collect_tuples_from_ir(input, input_data, key_indices, result);
            }
            IRNode::Distinct { input } => {
                Self::collect_tuples_from_ir(input, input_data, key_indices, result);
            }
            IRNode::Union { inputs } => {
                for input in inputs {
                    Self::collect_tuples_from_ir(input, input_data, key_indices, result);
                }
            }
            // For complex nodes like Join, we fall back to executing the dataflow
            // This is needed for computed views
            _ => {
                // Execute the node to get its tuples
                let tuples = Self::execute_subquery_for_antijoin(node, input_data);
                for tuple in tuples {
                    let key = tuple.from_indices(key_indices);
                    result.insert(key);
                }
            }
        }
    }

    /// Execute a subquery to collect tuples for antijoin
    fn execute_subquery_for_antijoin(
        node: &IRNode,
        input_data: &HashMap<String, Vec<Tuple>>,
    ) -> Vec<Tuple> {
        // Use timely to execute the subquery and collect results
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);
        let node_clone = node.clone();
        let input_data_clone = input_data.clone();

        timely::execute_directly(move |worker| {
            worker.dataflow::<(), _, _>(|scope| {
                let coll = Self::generate_collection_tuples::<_, isize>(
                    scope,
                    &node_clone,
                    &input_data_clone,
                    None,
                );
                let results_ref = Arc::clone(&results_clone);
                coll.inner.inspect(move |(tuple, _time, diff)| {
                    if *diff > 0 {
                        results_ref.lock().push(tuple.clone());
                    }
                });
            });
            // Step until complete
            while worker.step() {}
        });

        // Safely extract results from Arc<Mutex<Vec<Tuple>>>
        // parking_lot::Mutex never poisons, so into_inner() returns the value directly
        match Arc::try_unwrap(results) {
            Ok(mutex) => mutex.into_inner(),
            Err(arc) => {
                // Arc still has references (shouldn't happen, but handle gracefully)
                arc.lock().clone()
            }
        }
    }

    /// Generate distinct node (production)
    fn generate_distinct_tuples<G, R: DiffType>(
        scope: &mut G,
        input: &IRNode,
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        let input_coll = Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
        input_coll.distinct_core::<R>()
    }

    /// Generate union node (production)
    ///
    /// Note: This handles simple unions by concatenation. Recursive queries
    /// (transitive closure, etc.) are handled at a higher level via iterative
    /// execution in `generate_and_execute_recursive`.
    fn generate_union_tuples<G, R: DiffType>(
        scope: &mut G,
        inputs: &[IRNode],
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        if inputs.is_empty() {
            return Collection::new(
                Vec::<Tuple>::new()
                    .to_stream(scope)
                    .map(|x| (x, Default::default(), R::one())),
            );
        }

        let mut result =
            Self::generate_collection_tuples::<G, R>(scope, &inputs[0], input_data, live);

        for input in &inputs[1..] {
            let coll = Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
            result = result.concat(&coll);
        }

        result
    }

    /// Check if an IR node references (scans) a particular relation
    pub fn references_relation(ir: &IRNode, relation: &str) -> bool {
        match ir {
            IRNode::Scan { relation: rel, .. } => rel == relation,
            IRNode::HnswScan { index_name, .. } => index_name == relation,
            IRNode::Map { input, .. }
            | IRNode::Filter { input, .. }
            | IRNode::Distinct { input }
            | IRNode::Aggregate { input, .. }
            | IRNode::Compute { input, .. }
            | IRNode::FlatMap { input, .. } => Self::references_relation(input, relation),
            IRNode::Join { left, right, .. }
            | IRNode::Antijoin { left, right, .. }
            | IRNode::JoinFlatMap { left, right, .. } => {
                Self::references_relation(left, relation)
                    || Self::references_relation(right, relation)
            }
            IRNode::Union { inputs } => inputs
                .iter()
                .any(|inp| Self::references_relation(inp, relation)),
        }
    }

    /// Detect recursive relations in a Union node
    ///
    /// Returns the name of the recursive relation if found, along with
    /// partitioned inputs (base cases vs recursive cases)
    pub fn detect_recursive_union(inputs: &[IRNode]) -> Option<(String, Vec<usize>, Vec<usize>)> {
        Self::detect_recursive_union_for_relation(inputs, None)
    }

    /// Detect recursion for a specific relation in a Union node
    ///
    /// If `expected_relation` is Some, only that relation is considered for recursion.
    /// This is used when we know the head relation of the Union (e.g., from the rule head).
    ///
    /// Returns partitioned inputs (base cases vs recursive cases)
    pub fn detect_recursive_union_for_relation(
        inputs: &[IRNode],
        expected_relation: Option<&str>,
    ) -> Option<(String, Vec<usize>, Vec<usize>)> {
        // Find all relations that are scanned by each input
        let mut scan_relations: HashMap<String, Vec<usize>> = HashMap::new();

        fn collect_scans(ir: &IRNode, scans: &mut Vec<String>) {
            match ir {
                IRNode::Scan { relation, .. } => {
                    scans.push(relation.clone());
                }
                IRNode::HnswScan { index_name, .. } => {
                    scans.push(index_name.clone());
                }
                IRNode::Map { input, .. }
                | IRNode::Filter { input, .. }
                | IRNode::Distinct { input }
                | IRNode::Aggregate { input, .. }
                | IRNode::Compute { input, .. }
                | IRNode::FlatMap { input, .. } => {
                    collect_scans(input, scans);
                }
                IRNode::Join { left, right, .. }
                | IRNode::Antijoin { left, right, .. }
                | IRNode::JoinFlatMap { left, right, .. } => {
                    collect_scans(left, scans);
                    collect_scans(right, scans);
                }
                IRNode::Union { inputs } => {
                    for inp in inputs {
                        collect_scans(inp, scans);
                    }
                }
            }
        }

        for (i, input) in inputs.iter().enumerate() {
            let mut scans = Vec::new();
            collect_scans(input, &mut scans);
            for rel in scans {
                scan_relations.entry(rel).or_default().push(i);
            }
        }

        // DEBUG: Log what we found
        if std::env::var("IL_DEBUG").is_ok() {
            eprintln!(
                "DEBUG detect_recursive_union: {} inputs, scan_relations = {:?}",
                inputs.len(),
                scan_relations
            );
            if let Some(expected) = expected_relation {
                eprintln!("DEBUG: expected_relation = {expected}");
            }
        }

        // If we have an expected relation, only check that one
        if let Some(expected) = expected_relation {
            if let Some(indices) = scan_relations.get(expected) {
                let appears_in = indices.len();
                if appears_in > 0 && appears_in < inputs.len() {
                    // This is the recursive relation
                    let base_indices: Vec<usize> =
                        (0..inputs.len()).filter(|i| !indices.contains(i)).collect();
                    let recursive_indices = indices.clone();
                    return Some((expected.to_string(), base_indices, recursive_indices));
                }
            }
            return None;
        }

        // A recursive relation is scanned by some but not all inputs
        // (the base case doesn't scan it, recursive cases do)
        for (rel, indices) in &scan_relations {
            let appears_in = indices.len();
            if appears_in > 0 && appears_in < inputs.len() {
                // This might be recursive - the inputs that don't scan it are base cases
                let base_indices: Vec<usize> =
                    (0..inputs.len()).filter(|i| !indices.contains(i)).collect();
                let recursive_indices = indices.clone();
                return Some((rel.clone(), base_indices, recursive_indices));
            }
        }

        None
    }

    /// Generate aggregate node (production)
    ///
    /// Implements GROUP BY with aggregation functions (count, sum, min, max, avg).
    /// Uses Differential Dataflow's reduce operator for efficient aggregation.
