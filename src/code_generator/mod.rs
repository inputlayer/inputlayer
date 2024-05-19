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
    fn generate_aggregate_tuples<G, R: DiffType>(
        scope: &mut G,
        input: &IRNode,
        group_by: &[usize],
        aggregations: &[(AggregateFunction, usize)],
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        let input_coll = Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
        let group_by = group_by.to_vec();
        let aggregations = aggregations.to_vec();

        // Map to (group_key, value_tuple) pairs
        let keyed = input_coll.map(move |tuple| {
            // Extract group-by columns as key
            let key = tuple.project(&group_by);
            // Keep entire tuple as value for aggregation
            (key, tuple)
        });

        // Use reduce to compute aggregations
        // reduce returns Collection<G, (K, V)> so we need to extract just the result tuple
        let aggs_clone = aggregations.clone();

        keyed.reduce(move |key, input, output| {
            // input: slice of (&Tuple, R) pairs
            // We need to compute aggregations over all tuples with this key

            // Collect all tuples (accounting for multiplicities)
            let mut tuples: Vec<&Tuple> = Vec::new();
            for (tuple, count) in input {
                for _ in 0..count.to_count() {
                    tuples.push(*tuple);
                }
            }

            if tuples.is_empty() {
                return;
            }

            // Check for ranking aggregates (TopK, TopKThreshold, WithinRadius)
            // These return multiple rows per group instead of a single aggregate value
            let has_ranking_agg = aggs_clone.iter().any(|(func, _)| {
                matches!(func,
                    AggregateFunction::TopK { .. } |
                    AggregateFunction::TopKThreshold { .. } |
                    AggregateFunction::WithinRadius { .. }
                )
            });

            if has_ranking_agg {
                // Handle ranking aggregates - output multiple rows per group
                // Only one ranking aggregate should be present per query
                for (func, _col_idx) in &aggs_clone {
                    match func {
                        AggregateFunction::TopK { k, order_col, descending } => {
                            // O(n log k) heap-based top-k selection
                            use std::cmp::Reverse;
                            use std::collections::BinaryHeap;

                            // Local OrdF64 wrapper for heap ordering
                            #[derive(Clone, Copy, PartialEq)]
                            struct OrdF64(f64);
                            impl Eq for OrdF64 {}
                            impl PartialOrd for OrdF64 {
                                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                                    Some(self.cmp(other))
                                }
                            }
                            impl Ord for OrdF64 {
                                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                                    self.0.partial_cmp(&other.0).unwrap_or_else(|| {
                                        match (self.0.is_nan(), other.0.is_nan()) {
                                            (true, true) => std::cmp::Ordering::Equal,
                                            (true, false) => std::cmp::Ordering::Less,
                                            (false, true) => std::cmp::Ordering::Greater,
                                            (false, false) => unreachable!(),
                                        }
                                    })
                                }
                            }

                            if *descending {
                                // Top k largest: use min-heap via Reverse
                                let mut heap: BinaryHeap<Reverse<(OrdF64, &Tuple)>> = BinaryHeap::with_capacity(*k + 1);

                                for t in &tuples {
                                    let score = OrdF64(t.get(*order_col).map_or(f64::NEG_INFINITY, super::value::Value::to_f64));
                                    if heap.len() < *k {
                                        heap.push(Reverse((score, *t)));
                                    } else if let Some(&Reverse((min_score, _))) = heap.peek() {
                                        if score > min_score {
                                            heap.pop();
                                            heap.push(Reverse((score, *t)));
                                        }
                                    }
                                }

                                // Extract, sort descending, and output
                                let mut result: Vec<_> = heap.into_iter().map(|Reverse((score, t))| (score, t)).collect();
                                result.sort_by(|a, b| b.0.cmp(&a.0));
                                for (_, tuple) in result {
                                    output.push((tuple.clone(), R::one()));
                                }
                            } else {
                                // Top k smallest: use max-heap
                                let mut heap: BinaryHeap<(OrdF64, &Tuple)> = BinaryHeap::with_capacity(*k + 1);

                                for t in &tuples {
                                    let score = OrdF64(t.get(*order_col).map_or(f64::INFINITY, super::value::Value::to_f64));
                                    if heap.len() < *k {
                                        heap.push((score, *t));
                                    } else if let Some(&(max_score, _)) = heap.peek() {
                                        if score < max_score {
                                            heap.pop();
                                            heap.push((score, *t));
                                        }
                                    }
                                }

                                // Extract, sort ascending, and output
                                let mut result: Vec<_> = heap.into_iter().collect();
                                result.sort_by(|a, b| a.0.cmp(&b.0));
                                for (_, tuple) in result {
                                    output.push((tuple.clone(), R::one()));
                                }
                            }
                        }
                        AggregateFunction::TopKThreshold { k, order_col, threshold, descending } => {
                            // O(n log k) heap-based top-k selection with threshold filtering
                            use std::cmp::Reverse;
                            use std::collections::BinaryHeap;

                            // Local OrdF64 wrapper for heap ordering
                            #[derive(Clone, Copy, PartialEq)]
                            struct OrdF64(f64);
                            impl Eq for OrdF64 {}
                            impl PartialOrd for OrdF64 {
                                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                                    Some(self.cmp(other))
                                }
                            }
                            impl Ord for OrdF64 {
                                fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                                    self.0.partial_cmp(&other.0).unwrap_or_else(|| {
                                        match (self.0.is_nan(), other.0.is_nan()) {
                                            (true, true) => std::cmp::Ordering::Equal,
                                            (true, false) => std::cmp::Ordering::Less,
                                            (false, true) => std::cmp::Ordering::Greater,
                                            (false, false) => unreachable!(),
                                        }
                                    })
                                }
                            }

                            if *descending {
                                // Top k largest with threshold: use min-heap via Reverse
                                let mut heap: BinaryHeap<Reverse<(OrdF64, &Tuple)>> = BinaryHeap::with_capacity(*k + 1);

                                for t in &tuples {
                                    let score_val = t.get(*order_col).map_or(f64::NEG_INFINITY, super::value::Value::to_f64);
                                    // Filter: keep if score >= threshold
                                    if score_val < *threshold {
                                        continue;
                                    }
                                    let score = OrdF64(score_val);
                                    if heap.len() < *k {
                                        heap.push(Reverse((score, *t)));
                                    } else if let Some(&Reverse((min_score, _))) = heap.peek() {
                                        if score > min_score {
                                            heap.pop();
                                            heap.push(Reverse((score, *t)));
                                        }
                                    }
                                }

                                // Extract, sort descending, and output
                                let mut result: Vec<_> = heap.into_iter().map(|Reverse((score, t))| (score, t)).collect();
                                result.sort_by(|a, b| b.0.cmp(&a.0));
                                for (_, tuple) in result {
                                    output.push((tuple.clone(), R::one()));
                                }
                            } else {
                                // Top k smallest with threshold: use max-heap
                                let mut heap: BinaryHeap<(OrdF64, &Tuple)> = BinaryHeap::with_capacity(*k + 1);

                                for t in &tuples {
                                    let score_val = t.get(*order_col).map_or(f64::INFINITY, super::value::Value::to_f64);
                                    // Filter: keep if score <= threshold
                                    if score_val > *threshold {
                                        continue;
                                    }
                                    let score = OrdF64(score_val);
                                    if heap.len() < *k {
                                        heap.push((score, *t));
                                    } else if let Some(&(max_score, _)) = heap.peek() {
                                        if score < max_score {
                                            heap.pop();
                                            heap.push((score, *t));
                                        }
                                    }
                                }

                                // Extract, sort ascending, and output
                                let mut result: Vec<_> = heap.into_iter().collect();
                                result.sort_by(|a, b| a.0.cmp(&b.0));
                                for (_, tuple) in result {
                                    output.push((tuple.clone(), R::one()));
                                }
                            }
                        }
                        AggregateFunction::WithinRadius { distance_col, max_distance } => {
                            // Keep all tuples where distance_col <= max_distance
                            for tuple in &tuples {
                                let dist = tuple.get(*distance_col)
                                    .map_or(f64::INFINITY, super::value::Value::to_f64);
                                if dist <= *max_distance {
                                    output.push(((*tuple).clone(), R::one()));
                                }
                            }
                        }
                        _ => {} // Standard aggregates handled below
                    }
                }
            } else {
                // Standard aggregation - output one row per group
                let mut agg_values: Vec<Value> = Vec::new();

                for (func, col_idx) in &aggs_clone {
                    let agg_result = match func {
                        AggregateFunction::Count => {
                            Value::Int64(tuples.len() as i64)
                        }
                        AggregateFunction::CountDistinct => {
                            // Count unique values in the column
                            let unique_values: std::collections::HashSet<_> = tuples
                                .iter()
                                .filter_map(|t| t.get(*col_idx).cloned())
                                .collect();
                            Value::Int64(unique_values.len() as i64)
                        }
                        AggregateFunction::Sum => {
                            // Use saturating arithmetic to handle overflow safely
                            // This processes ALL tuples and saturates at boundaries
                            let mut sum: i64 = 0;
                            let mut overflow = false;
                            for t in &tuples {
                                let val = t.get(*col_idx).map_or(0, super::value::Value::to_i64);
                                let old_sum = sum;
                                sum = sum.saturating_add(val);
                                // Detect if saturation occurred
                                if !overflow && sum != old_sum.wrapping_add(val) {
                                    overflow = true;
                                }
                            }
                            if overflow {
                                // Log warning but continue with saturated value
                                // This matches SQL behavior for overflow
                                eprintln!("Warning: Integer overflow in SUM aggregation, result saturated");
                            }
                            Value::Int64(sum)
                        }
                        AggregateFunction::Min => {
                            let min = tuples
                                .iter()
                                .filter_map(|t| t.get(*col_idx))
                                .min()
                                .cloned()
                                .unwrap_or(Value::Null);
                            min
                        }
                        AggregateFunction::Max => {
                            let max = tuples
                                .iter()
                                .filter_map(|t| t.get(*col_idx))
                                .max()
                                .cloned()
                                .unwrap_or(Value::Null);
                            max
                        }
                        AggregateFunction::Avg => {
                            let count = tuples.len() as f64;
                            if count == 0.0 {
                                Value::Null // Guard against division by zero
                            } else {
                                let sum: f64 = tuples
                                    .iter()
                                    .map(|t| t.get(*col_idx).map_or(0.0, super::value::Value::to_f64))
                                    .sum();
                                Value::Float64(sum / count)
                            }
                        }
                        // Ranking aggregates handled above
                        _ => continue,
                    };
                    agg_values.push(agg_result);
                }

                // Build output tuple: group key columns + aggregate values
                let mut result_values: Vec<Value> = key.values().to_vec();
                result_values.extend(agg_values);
                let result = Tuple::new(result_values);

                output.push((result, R::one()));
            }
        })
        // Extract just the result tuple from (key, result) pairs
        .map(|(_key, result)| result)
    }

    /// Generate compute node (production: vector functions and expressions)
    ///
    /// Computes new columns from expressions and appends them to input tuples.
    fn generate_compute_tuples<G, R: DiffType>(
        scope: &mut G,
        input: &IRNode,
        expressions: &[(String, IRExpression)],
        input_data: &HashMap<String, Vec<Tuple>>,
        live: Option<&HashMap<String, Collection<G, Tuple, R>>>,
    ) -> Collection<G, Tuple, R>
    where
        G: Scope,
        G::Timestamp: Lattice + Ord + Default,
    {
        let input_coll = Self::generate_collection_tuples::<G, R>(scope, input, input_data, live);
        let expressions = expressions.to_vec();

        input_coll.map(move |tuple| {
            // Evaluate each expression and append to tuple
            // Use a growing tuple so chained computed columns work:
            // e.g., Q = quantize(V), D = dequantize(Q) - D needs to see Q
            let mut current_tuple = tuple.clone();

            if std::env::var("IL_DEBUG").is_ok() {
                eprintln!("DEBUG Compute: input tuple = {:?}", current_tuple.values());
            }

            for (name, expr) in &expressions {
                let value = Self::evaluate_expression(expr, &current_tuple);
                if std::env::var("IL_DEBUG").is_ok() {
                    eprintln!("DEBUG Compute: expr='{name}' => {value:?}");
                }
                // Extend the current tuple with the computed value
                // so subsequent expressions can reference it
                let mut values: Vec<Value> = current_tuple.values().to_vec();
                values.push(value);
                current_tuple = Tuple::new(values);
            }

            if std::env::var("IL_DEBUG").is_ok() {
                eprintln!("DEBUG Compute: output = {:?}", current_tuple.values());
            }

            current_tuple
        })
    }

    /// Evaluate an IR expression against a tuple
    fn evaluate_expression(expr: &IRExpression, tuple: &Tuple) -> Value {
        match expr {
            IRExpression::Column(idx) => tuple.get(*idx).cloned().unwrap_or(Value::Null),
            IRExpression::IntConstant(val) => Value::Int64(*val),
            IRExpression::FloatConstant(val) => Value::Float64(*val),
            IRExpression::StringConstant(s) => Value::String(s.clone().into()),
            IRExpression::VectorLiteral(vals) => Value::vector(vals.clone()),
            IRExpression::FunctionCall(func, args) => Self::evaluate_function(func, args, tuple),
            IRExpression::Arithmetic { op, left, right } => {
                let left_val = Self::evaluate_expression(left, tuple);
                let right_val = Self::evaluate_expression(right, tuple);
                Self::evaluate_arithmetic(*op, &left_val, &right_val)
            }
        }
    }

    /// Evaluate a built-in function call
    fn evaluate_function(func: &BuiltinFunction, args: &[IRExpression], tuple: &Tuple) -> Value {
        // Evaluate arguments
        let arg_values: Vec<Value> = args
            .iter()
            .map(|arg| Self::evaluate_expression(arg, tuple))
            .collect();

        match func {
            BuiltinFunction::Euclidean => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) =
                        (arg_values[0].as_vector(), arg_values[1].as_vector())
                    {
                        let dist = vector_ops::euclidean_distance(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }
            BuiltinFunction::Cosine => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) =
                        (arg_values[0].as_vector(), arg_values[1].as_vector())
                    {
                        let dist = vector_ops::cosine_distance(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }
            BuiltinFunction::DotProduct => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) =
                        (arg_values[0].as_vector(), arg_values[1].as_vector())
                    {
                        let dot = vector_ops::dot_product(v1, v2);
                        return Value::Float64(dot);
                    }
                }
                Value::Null
            }
            BuiltinFunction::Manhattan => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) =
                        (arg_values[0].as_vector(), arg_values[1].as_vector())
                    {
                        let dist = vector_ops::manhattan_distance(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }
            BuiltinFunction::Hamming => {
                // hamming(a, b) - Hamming distance between two integers
                if arg_values.len() >= 2 {
                    let a = arg_values[0].to_i64();
                    let b = arg_values[1].to_i64();
                    return Value::Int64(vector_ops::hamming_distance(a, b));
                }
                Value::Null
            }
            BuiltinFunction::LshBucket => {
                // lsh_bucket(vector, table_idx, num_hyperplanes)
                if arg_values.len() >= 3 {
                    if let Some(v) = arg_values[0].as_vector() {
                        let table_idx = arg_values[1].to_i64();
                        let num_hyperplanes = arg_values[2].to_i64() as usize;
                        let bucket = vector_ops::lsh_bucket(v, table_idx, num_hyperplanes);
                        return Value::Int64(bucket);
                    }
                }
                Value::Null
            }
            BuiltinFunction::VecNormalize => {
                if let Some(v) = arg_values.first().and_then(|v| v.as_vector()) {
                    let normalized = vector_ops::normalize(v);
                    return Value::vector(normalized);
                }
                Value::Null
            }
            BuiltinFunction::VecDim => {
                if let Some(v) = arg_values.first().and_then(|v| v.as_vector()) {
                    return Value::Int64(v.len() as i64);
                }
                Value::Null
            }
            BuiltinFunction::VecAdd => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) =
                        (arg_values[0].as_vector(), arg_values[1].as_vector())
                    {
                        if v1.len() == v2.len() {
                            let result: Vec<f32> =
                                v1.iter().zip(v2.iter()).map(|(a, b)| a + b).collect();
                            return Value::vector(result);
                        }
                    }
                }
                Value::Null
            }
            BuiltinFunction::VecScale => {
                // vec_scale(vector, scalar)
                if arg_values.len() >= 2 {
                    if let Some(v) = arg_values[0].as_vector() {
                        let scalar = arg_values[1].to_f64() as f32;
                        let result: Vec<f32> = v.iter().map(|x| x * scalar).collect();
                        return Value::vector(result);
                    }
                }
                Value::Null
            }

            // Int8 quantization functions
            BuiltinFunction::QuantizeLinear => {
                if let Some(v) = arg_values.first().and_then(|v| v.as_vector()) {
                    let quantized = vector_ops::quantize_vector_linear(v);
                    return Value::vector_int8(quantized);
                }
                Value::Null
            }
            BuiltinFunction::QuantizeSymmetric => {
                if let Some(v) = arg_values.first().and_then(|v| v.as_vector()) {
                    let quantized = vector_ops::quantize_vector_symmetric(v);
                    return Value::vector_int8(quantized);
                }
                Value::Null
            }
            BuiltinFunction::Dequantize => {
                if let Some(v) = arg_values.first().and_then(|v| v.as_vector_int8()) {
                    let dequantized = vector_ops::dequantize_vector(v);
                    return Value::vector(dequantized);
                }
                Value::Null
            }
            BuiltinFunction::DequantizeScaled => {
                // dequantize_scaled(vector_int8, scale)
                if arg_values.len() >= 2 {
                    if let Some(v) = arg_values[0].as_vector_int8() {
                        let scale = arg_values[1].to_f64() as f32;
                        let dequantized = vector_ops::dequantize_vector_with_scale(v, scale);
                        return Value::vector(dequantized);
                    }
                }
                Value::Null
            }

            // Int8 distance functions (native, fast)
            BuiltinFunction::EuclideanInt8 => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) = (
                        arg_values[0].as_vector_int8(),
                        arg_values[1].as_vector_int8(),
                    ) {
                        let dist = vector_ops::euclidean_distance_int8(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }
            BuiltinFunction::CosineInt8 => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) = (
                        arg_values[0].as_vector_int8(),
                        arg_values[1].as_vector_int8(),
                    ) {
                        let dist = vector_ops::cosine_distance_int8(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }
            BuiltinFunction::DotProductInt8 => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) = (
                        arg_values[0].as_vector_int8(),
                        arg_values[1].as_vector_int8(),
                    ) {
                        let dot = vector_ops::dot_product_int8(v1, v2);
                        return Value::Float64(dot);
                    }
                }
                Value::Null
            }
            BuiltinFunction::ManhattanInt8 => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) = (
                        arg_values[0].as_vector_int8(),
                        arg_values[1].as_vector_int8(),
                    ) {
                        let dist = vector_ops::manhattan_distance_int8(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }

            // Int8 distance functions (dequantized, accurate)
            BuiltinFunction::EuclideanDequantized => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) = (
                        arg_values[0].as_vector_int8(),
                        arg_values[1].as_vector_int8(),
                    ) {
                        let dist = vector_ops::euclidean_distance_dequantized(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }
            BuiltinFunction::CosineDequantized => {
                if arg_values.len() >= 2 {
                    if let (Some(v1), Some(v2)) = (
                        arg_values[0].as_vector_int8(),
                        arg_values[1].as_vector_int8(),
                    ) {
                        let dist = vector_ops::cosine_distance_dequantized(v1, v2);
                        return Value::Float64(dist);
                    }
                }
                Value::Null
            }

            // Int8 LSH
            BuiltinFunction::LshBucketInt8 => {
                // lsh_bucket_int8(vector_int8, table_idx, num_hyperplanes)
                if arg_values.len() >= 3 {
                    if let Some(v) = arg_values[0].as_vector_int8() {
                        let table_idx = arg_values[1].to_i64();
                        let num_hyperplanes = arg_values[2].to_i64() as usize;
                        let bucket = vector_ops::lsh_bucket_int8(v, table_idx, num_hyperplanes);
                        return Value::Int64(bucket);
                    }
                }
                Value::Null
            }

            // Multi-probe LSH
            BuiltinFunction::LshProbes => {
                // lsh_probes(bucket, num_hyperplanes, num_probes) -> Vec<Int64>
                if arg_values.len() >= 3 {
                    let bucket = arg_values[0].to_i64();
                    let num_hyperplanes = arg_values[1].to_i64() as usize;
                    let num_probes = arg_values[2].to_i64() as usize;
                    let probes = vector_ops::lsh_probes(bucket, num_hyperplanes, num_probes);
                    // Return as a vector of f32 (since we don't have Vec<i64> Value type)
                    // The caller can cast as needed
                    let probes_f32: Vec<f32> = probes.iter().map(|&p| p as f32).collect();
                    return Value::vector(probes_f32);
                }
                Value::Null
            }
            BuiltinFunction::LshBucketWithDistances => {
                // lsh_bucket_with_distances(vector, table_idx, num_hyperplanes) -> (bucket, distances)
                // Returns bucket as Int64; distances need separate handling
                // Returns the bucket; use lsh_multi_probe for full multi-probe functionality
                if arg_values.len() >= 3 {
                    if let Some(v) = arg_values[0].as_vector() {
                        let table_idx = arg_values[1].to_i64();
                        let num_hyperplanes = arg_values[2].to_i64() as usize;
                        let (bucket, _distances) =
                            vector_ops::lsh_bucket_with_distances(v, table_idx, num_hyperplanes);
                        return Value::Int64(bucket);
                    }
                }
                Value::Null
            }
            BuiltinFunction::LshProbesRanked => {
                // lsh_probes_ranked(bucket, distances_vec, num_probes) -> Vec<Int64>
                // Note: distances are provided as a Vector (f32) since that's our available type
                if arg_values.len() >= 3 {
                    let bucket = arg_values[0].to_i64();
                    let num_probes = arg_values[2].to_i64() as usize;
                    if let Some(distances_f32) = arg_values[1].as_vector() {
                        let distances: Vec<f64> =
                            distances_f32.iter().map(|&d| f64::from(d)).collect();
                        let probes = vector_ops::lsh_probes_ranked(bucket, &distances, num_probes);
                        let probes_f32: Vec<f32> = probes.iter().map(|&p| p as f32).collect();
                        return Value::vector(probes_f32);
                    }
                }
                Value::Null
            }
            BuiltinFunction::LshMultiProbe => {
                // lsh_multi_probe(vector, table_idx, num_hyperplanes, num_probes) -> Vec<Int64>
                if arg_values.len() >= 4 {
                    if let Some(v) = arg_values[0].as_vector() {
                        let table_idx = arg_values[1].to_i64();
                        let num_hyperplanes = arg_values[2].to_i64() as usize;
                        let num_probes = arg_values[3].to_i64() as usize;
                        let probes =
                            vector_ops::lsh_multi_probe(v, table_idx, num_hyperplanes, num_probes);
                        let probes_f32: Vec<f32> = probes.iter().map(|&p| p as f32).collect();
                        return Value::vector(probes_f32);
                    }
                }
                Value::Null
            }
            BuiltinFunction::LshMultiProbeInt8 => {
                // lsh_multi_probe_int8(vector_int8, table_idx, num_hyperplanes, num_probes) -> Vec<Int64>
                if arg_values.len() >= 4 {
                    if let Some(v) = arg_values[0].as_vector_int8() {
                        let table_idx = arg_values[1].to_i64();
                        let num_hyperplanes = arg_values[2].to_i64() as usize;
                        let num_probes = arg_values[3].to_i64() as usize;
                        let probes = vector_ops::lsh_multi_probe_int8(
                            v,
                            table_idx,
                            num_hyperplanes,
                            num_probes,
                        );
                        let probes_f32: Vec<f32> = probes.iter().map(|&p| p as f32).collect();
                        return Value::vector(probes_f32);
                    }
                }
                Value::Null
            }

            // Int8 vector utilities
            BuiltinFunction::VecDimInt8 => {
                if let Some(v) = arg_values.first().and_then(|v| v.as_vector_int8()) {
                    return Value::Int64(v.len() as i64);
                }
                Value::Null
            }

            // Temporal functions
            BuiltinFunction::TimeNow => Value::Timestamp(temporal_ops::time_now()),
            BuiltinFunction::TimeDiff => {
                if arg_values.len() >= 2 {
                    if let (Some(t1), Some(t2)) =
                        (arg_values[0].as_timestamp(), arg_values[1].as_timestamp())
                    {
                        return Value::Int64(temporal_ops::time_diff(t1, t2));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeAdd => {
                if arg_values.len() >= 2 {
                    if let (Some(ts), Some(dur)) =
                        (arg_values[0].as_timestamp(), arg_values[1].as_i64())
                    {
                        return Value::Timestamp(temporal_ops::time_add(ts, dur));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeSub => {
                if arg_values.len() >= 2 {
                    if let (Some(ts), Some(dur)) =
                        (arg_values[0].as_timestamp(), arg_values[1].as_i64())
                    {
                        return Value::Timestamp(temporal_ops::time_sub(ts, dur));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeDecay => {
                if arg_values.len() >= 3 {
                    if let (Some(ts), Some(now), Some(half_life)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_i64(),
                    ) {
                        return Value::Float64(temporal_ops::time_decay(ts, now, half_life));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeDecayLinear => {
                if arg_values.len() >= 3 {
                    if let (Some(ts), Some(now), Some(max_age)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_i64(),
                    ) {
                        return Value::Float64(temporal_ops::time_decay_linear(ts, now, max_age));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeBefore => {
                if arg_values.len() >= 2 {
                    if let (Some(t1), Some(t2)) =
                        (arg_values[0].as_timestamp(), arg_values[1].as_timestamp())
                    {
                        return Value::Bool(temporal_ops::time_before(t1, t2));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeAfter => {
                if arg_values.len() >= 2 {
                    if let (Some(t1), Some(t2)) =
                        (arg_values[0].as_timestamp(), arg_values[1].as_timestamp())
                    {
                        return Value::Bool(temporal_ops::time_after(t1, t2));
                    }
                }
                Value::Null
            }
            BuiltinFunction::TimeBetween => {
                if arg_values.len() >= 3 {
                    if let (Some(ts), Some(start), Some(end)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_timestamp(),
                    ) {
                        return Value::Bool(temporal_ops::time_between(ts, start, end));
                    }
                }
                Value::Null
            }
            BuiltinFunction::WithinLast => {
                if arg_values.len() >= 3 {
                    if let (Some(ts), Some(now), Some(dur)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_i64(),
                    ) {
                        return Value::Bool(temporal_ops::within_last(ts, now, dur));
                    }
                }
                Value::Null
            }
            BuiltinFunction::IntervalsOverlap => {
                if arg_values.len() >= 4 {
                    if let (Some(s1), Some(e1), Some(s2), Some(e2)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_timestamp(),
                        arg_values[3].as_timestamp(),
                    ) {
                        return Value::Bool(temporal_ops::intervals_overlap(s1, e1, s2, e2));
                    }
                }
                Value::Null
            }
            BuiltinFunction::IntervalContains => {
                if arg_values.len() >= 4 {
                    if let (Some(s1), Some(e1), Some(s2), Some(e2)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_timestamp(),
                        arg_values[3].as_timestamp(),
                    ) {
                        return Value::Bool(temporal_ops::interval_contains(s1, e1, s2, e2));
                    }
                }
                Value::Null
            }
            BuiltinFunction::IntervalDuration => {
                if arg_values.len() >= 2 {
                    if let (Some(start), Some(end)) =
                        (arg_values[0].as_timestamp(), arg_values[1].as_timestamp())
                    {
                        return Value::Int64(temporal_ops::interval_duration(start, end));
                    }
                }
                Value::Null
            }
            BuiltinFunction::PointInInterval => {
                if arg_values.len() >= 3 {
                    if let (Some(ts), Some(start), Some(end)) = (
                        arg_values[0].as_timestamp(),
                        arg_values[1].as_timestamp(),
                        arg_values[2].as_timestamp(),
                    ) {
                        return Value::Bool(temporal_ops::point_in_interval(ts, start, end));
                    }
                }
                Value::Null
            }

            // Math utility functions
            BuiltinFunction::AbsInt64 => {
                if let Some(x) = arg_values.first().and_then(super::value::Value::as_i64) {
                    return Value::Int64(vector_ops::abs_i64(x));
                }
                // Also handle timestamp as i64
                if let Some(x) = arg_values
                    .first()
                    .and_then(super::value::Value::as_timestamp)
                {
                    return Value::Int64(vector_ops::abs_i64(x));
                }
                Value::Null
            }
            BuiltinFunction::AbsFloat64 => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Float64(vector_ops::abs_f64(x))
            }

            // Generic math functions
            BuiltinFunction::Abs => {
                if let Some(val) = arg_values.first() {
                    // Handle both int and float
                    if let Some(x) = val.as_i64() {
                        return Value::Int64(x.abs());
                    }
                    let x = val.to_f64();
                    return Value::Float64(x.abs());
                }
                Value::Null
            }
            BuiltinFunction::Sqrt => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                if x < 0.0 {
                    Value::Null
                } else {
                    Value::Float64(x.sqrt())
                }
            }
            BuiltinFunction::Pow => {
                if arg_values.len() >= 2 {
                    let base = arg_values[0].to_f64();
                    let exp = arg_values[1].to_f64();
                    return Value::Float64(base.powf(exp));
                }
                Value::Null
            }
            BuiltinFunction::Log => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                if x <= 0.0 {
                    Value::Null
                } else {
                    Value::Float64(x.ln())
                }
            }
            BuiltinFunction::Exp => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Float64(x.exp())
            }
            BuiltinFunction::Sin => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Float64(x.sin())
            }
            BuiltinFunction::Cos => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Float64(x.cos())
            }
            BuiltinFunction::Tan => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Float64(x.tan())
            }
            BuiltinFunction::Floor => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Int64(x.floor() as i64)
            }
            BuiltinFunction::Ceil => {
                let x = arg_values.first().map_or(0.0, super::value::Value::to_f64);
                Value::Int64(x.ceil() as i64)
            }
            BuiltinFunction::Sign => {
                if let Some(val) = arg_values.first() {
                    if let Some(x) = val.as_i64() {
                        return Value::Int64(x.signum());
                    }
                    let x = val.to_f64();
                    if x == 0.0 {
                        return Value::Int64(0);
                    } else if x > 0.0 {
                        return Value::Int64(1);
                    } else {
                        return Value::Int64(-1);
                    }
                }
                Value::Null
            }

            // String functions
            BuiltinFunction::Len => {
                if let Some(s) = arg_values.first().and_then(super::value::Value::as_str) {
                    return Value::Int64(s.len() as i64);
                }
                Value::Null
            }
            BuiltinFunction::Upper => {
                if let Some(s) = arg_values.first().and_then(super::value::Value::as_str) {
                    return Value::String(s.to_uppercase().into());
                }
                Value::Null
            }
            BuiltinFunction::Lower => {
                if let Some(s) = arg_values.first().and_then(super::value::Value::as_str) {
                    return Value::String(s.to_lowercase().into());
                }
                Value::Null
            }
            BuiltinFunction::Trim => {
                if let Some(s) = arg_values.first().and_then(super::value::Value::as_str) {
                    return Value::String(s.trim().into());
                }
                Value::Null
            }
            BuiltinFunction::Substr => {
                if arg_values.len() >= 3 {
                    if let Some(s) = arg_values[0].as_str() {
                        let start = arg_values[1].as_i64().unwrap_or(0) as usize;
                        let len = arg_values[2].as_i64().unwrap_or(0) as usize;
                        // Handle bounds safely
                        if start <= s.len() {
                            let end = (start + len).min(s.len());
                            return Value::String(s[start..end].into());
                        }
                        return Value::String("".into());
                    }
                }
                Value::Null
            }
            BuiltinFunction::Replace => {
                if arg_values.len() >= 3 {
                    if let (Some(s), Some(find), Some(replacement)) = (
                        arg_values[0].as_str(),
                        arg_values[1].as_str(),
                        arg_values[2].as_str(),
                    ) {
                        return Value::String(s.replace(find, replacement).into());
                    }
                }
                Value::Null
            }
            BuiltinFunction::Concat => {
                let mut result = String::new();
                for v in &arg_values {
                    match v {
                        Value::String(s) => result.push_str(s),
                        Value::Int64(n) => result.push_str(&n.to_string()),
                        Value::Float64(f) => result.push_str(&f.to_string()),
                        _ => result.push_str(&format!("{v}")),
                    }
                }
                Value::String(result.into())
            }
            BuiltinFunction::MinVal => {
                if arg_values.len() >= 2 {
                    let a = &arg_values[0];
                    let b = &arg_values[1];
                    match (a, b) {
                        (Value::Int64(x), Value::Int64(y)) => Value::Int64(*x.min(y)),
                        (Value::Float64(x), Value::Float64(y)) => Value::Float64(x.min(*y)),
                        (Value::Int64(x), Value::Float64(y)) => Value::Float64((*x as f64).min(*y)),
                        (Value::Float64(x), Value::Int64(y)) => Value::Float64(x.min(*y as f64)),
                        (Value::String(x), Value::String(y)) => {
                            if x <= y {
                                a.clone()
                            } else {
                                b.clone()
                            }
                        }
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
            BuiltinFunction::MaxVal => {
                if arg_values.len() >= 2 {
                    let a = &arg_values[0];
                    let b = &arg_values[1];
                    match (a, b) {
                        (Value::Int64(x), Value::Int64(y)) => Value::Int64(*x.max(y)),
                        (Value::Float64(x), Value::Float64(y)) => Value::Float64(x.max(*y)),
                        (Value::Int64(x), Value::Float64(y)) => Value::Float64((*x as f64).max(*y)),
                        (Value::Float64(x), Value::Int64(y)) => Value::Float64(x.max(*y as f64)),
                        (Value::String(x), Value::String(y)) => {
                            if x >= y {
                                a.clone()
                            } else {
                                b.clone()
                            }
                        }
                        _ => Value::Null,
                    }
                } else {
                    Value::Null
                }
            }
        }
    }

    /// Evaluate arithmetic operation
    fn evaluate_arithmetic(op: ArithOp, left: &Value, right: &Value) -> Value {
        let l = left.to_f64();
        let r = right.to_f64();

        let result = match op {
            ArithOp::Add => l + r,
            ArithOp::Sub => l - r,
            ArithOp::Mul => l * r,
            ArithOp::Div => {
                if r == 0.0 {
                    return Value::Null;
                }
                l / r
            }
            ArithOp::Mod => {
                if r == 0.0 {
                    return Value::Null;
                }
                l % r
            }
        };

        // Return Int64 if both inputs were integers and result is finite
        if matches!(left, Value::Int32(_) | Value::Int64(_))
            && matches!(right, Value::Int32(_) | Value::Int64(_))
            && matches!(
                op,
                ArithOp::Add | ArithOp::Sub | ArithOp::Mul | ArithOp::Mod
            )
        {
            // Check for NaN/Infinity before casting to avoid undefined behavior
            if !result.is_finite() {
                return Value::Null;
            }
            Value::Int64(result as i64)
        } else {
            Value::Float64(result)
        }
    }

    // Recursive Query Execution
    /// Execute transitive closure query using iterative materialization
    ///
    /// This is a convenience method for the common pattern:
    /// tc(x, y) :- edge(x, y).
    /// tc(x, z) :- tc(x, y), edge(y, z).
    ///
    /// Takes edge relation name and computes transitive closure.
    /// Uses iterative materialization for reliable fixpoint computation.
    pub fn execute_transitive_closure(&self, edge_relation: &str) -> Result<Vec<Tuple>, String> {
        use std::collections::{HashMap as StdHashMap, HashSet};

        // Get edges from input_tuples, extract first two i64 values
        let edges: Vec<(i64, i64)> = self
            .input_tuples
            .get(edge_relation)
            .map(|tuples| {
                tuples
                    .iter()
                    .filter_map(|t| {
                        let a = t.get(0).and_then(super::value::Value::as_i64)?;
                        let b = t.get(1).and_then(super::value::Value::as_i64)?;
                        Some((a, b))
                    })
                    .collect()
            })
            .unwrap_or_default();

        if edges.is_empty() {
            return Ok(Vec::new());
        }

        // Build adjacency list for efficient lookups
        let mut adj: StdHashMap<i64, Vec<i64>> = StdHashMap::new();
        for &(x, y) in &edges {
            adj.entry(x).or_default().push(y);
        }

        // Initialize with base case (all direct edges)
        let mut tc: HashSet<(i64, i64)> = edges.iter().copied().collect();
        let mut changed = true;

        // Iterate until fixpoint
        while changed {
            changed = false;
            let current: Vec<(i64, i64)> = tc.iter().copied().collect();

            for (x, y) in current {
                // For each (x, y) in tc, look for edges (y, z) to create (x, z)
                if let Some(neighbors) = adj.get(&y) {
                    for &z in neighbors {
                        if tc.insert((x, z)) {
                            changed = true;
                        }
                    }
                }
            }
        }

        // Convert to Vec<Tuple>
        Ok(tc.into_iter().map(|(a, b)| Tuple::pair(a, b)).collect())
    }

    /// Execute reachability query from a set of source nodes
    ///
    /// Pattern:
    /// reach(x) :- source(x).
    /// reach(y) :- reach(x), edge(x, y).
    ///
    /// Returns nodes reachable from any source node.
    pub fn execute_reachability(
        &self,
        source_relation: &str,
        edge_relation: &str,
    ) -> Result<Vec<i64>, String> {
        use std::collections::{HashMap as StdHashMap, HashSet, VecDeque};

        // Get source nodes (first column only)
        let sources: Vec<i64> = self
            .input_tuples
            .get(source_relation)
            .map(|tuples| {
                tuples
                    .iter()
                    .filter_map(|t| t.get(0).and_then(super::value::Value::as_i64))
                    .collect()
            })
            .unwrap_or_default();

        // Get edges from input_tuples
        let edges: Vec<(i64, i64)> = self
            .input_tuples
            .get(edge_relation)
            .map(|tuples| {
                tuples
                    .iter()
                    .filter_map(|t| {
                        let a = t.get(0).and_then(super::value::Value::as_i64)?;
                        let b = t.get(1).and_then(super::value::Value::as_i64)?;
                        Some((a, b))
                    })
                    .collect()
            })
            .unwrap_or_default();

        if sources.is_empty() {
            return Ok(Vec::new());
        }

        // Build adjacency list
        let mut adj: StdHashMap<i64, Vec<i64>> = StdHashMap::new();
        for &(x, y) in &edges {
            adj.entry(x).or_default().push(y);
        }

        // BFS from all sources
        let mut reachable: HashSet<i64> = sources.iter().copied().collect();
        let mut queue: VecDeque<i64> = sources.iter().copied().collect();

        while let Some(node) = queue.pop_front() {
            if let Some(neighbors) = adj.get(&node) {
                for &neighbor in neighbors {
                    if reachable.insert(neighbor) {
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        Ok(reachable.into_iter().collect())
    }

    // True DD Recursion (Production Implementation)
    /// Execute transitive closure using TRUE Differential Dataflow recursion
    ///
    /// This is the production-grade implementation that uses DD's `.iterative()` scope
    /// with `SemigroupVariable` for proper semi-naive evaluation.
    ///
    /// ## DD Pattern
    ///
    /// ```text
    /// scope.iterative::<Iter, _, _>(|inner| {
    ///     // Create SemigroupVariable for recursive IDB
    ///     let variable = SemigroupVariable::new(inner, Product::new((), 1));
    ///
    ///     // Enter base case into scope
    ///     let base = edges.enter(inner);
    ///
    ///     // Recursive step: tc(x,z) :- tc(x,y), edge(y,z)
    ///     let recursive = variable.join(&edges_keyed).map(|(y, (x, z))| (x, z));
    ///
    ///     // Combine base and recursive, set variable
    ///     let next = base.concat(&recursive).distinct();
    ///     variable.set(&next);
    ///
    ///     // Leave scope
    ///     next.leave()
    /// })
    /// ```
    ///
    /// ## Why This Matters
    ///
    /// Unlike the manual while-loop implementation, this:
    /// - Leverages DD's incremental computation
    /// - Only processes NEW tuples each iteration (semi-naive)
    /// - Properly handles timestamps and convergence
    /// - Is the same pattern used in production `InputLayer`
    pub fn execute_transitive_closure_dd(&self, edge_relation: &str) -> Result<Vec<Tuple>, String> {
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

        // Execute DD computation with TRUE recursion
        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();

            worker.dataflow::<(), _, _>(|scope| {
                // Load edge data as base collection
                let edge_collection: Collection<_, Tuple, isize> =
                    Collection::new(edge_data.clone().to_stream(scope).map(|x| (x, (), 1)));

                // Use iterative scope for recursion
                let tc_result = scope.iterative::<Iter, _, _>(|inner| {
                    // Create SemigroupVariable for transitive closure
                    let variable: SemigroupVariable<_, Tuple, isize> =
                        SemigroupVariable::new(inner, Product::new((), 1));

                    // Enter edge collection into iterative scope
                    let edges_in_scope = edge_collection.enter(inner);

                    // Base case: tc(x, y) :- edge(x, y)
                    // (already have edges in scope)

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
                    let next = edges_in_scope.concat(&recursive).distinct();

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

    /// Execute reachability using TRUE Differential Dataflow recursion
    ///
    /// Pattern:
    /// reach(x) :- source(x).
    /// reach(y) :- reach(x), edge(x, y).
    ///
    /// Uses DD's `.iterative()` scope for proper semi-naive evaluation.
    pub fn execute_reachability_dd(
        &self,
        source_relation: &str,
        edge_relation: &str,
    ) -> Result<Vec<Tuple>, String> {
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        // Get source nodes
        let sources: Vec<Tuple> = self
            .input_tuples
            .get(source_relation)
            .cloned()
            .unwrap_or_default();

        // Get edges
        let edges: Vec<Tuple> = self
            .input_tuples
            .get(edge_relation)
            .cloned()
            .unwrap_or_default();

        if sources.is_empty() {
            return Ok(Vec::new());
        }

        let source_data = sources.clone();
        let edge_data = edges.clone();

        // Execute DD computation with TRUE recursion
        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();

            worker.dataflow::<(), _, _>(|scope| {
                // Load source and edge collections
                let source_collection: Collection<_, Tuple, isize> =
                    Collection::new(source_data.clone().to_stream(scope).map(|x| (x, (), 1)));
                let edge_collection: Collection<_, Tuple, isize> =
                    Collection::new(edge_data.clone().to_stream(scope).map(|x| (x, (), 1)));

                // Use iterative scope for recursion
                let reach_result = scope.iterative::<Iter, _, _>(|inner| {
                    // Create SemigroupVariable for reachable nodes
                    let variable: SemigroupVariable<_, Tuple, isize> =
                        SemigroupVariable::new(inner, Product::new((), 1));

                    // Enter collections into iterative scope
                    let sources_in_scope = source_collection.enter(inner);
                    let edges_in_scope = edge_collection.enter(inner);

                    // Base case: reach(x) :- source(x)
                    // (sources_in_scope is the base case)

                    // Recursive case: reach(y) :- reach(x), edge(x, y)
                    // Key reach by its value (x) for join with edge(x, y)
                    let reach_keyed = variable.map(|tuple| {
                        let x = tuple.get(0).cloned().unwrap_or(Value::Null);
                        (Tuple::new(vec![x.clone()]), x) // Key and value are both x
                    });

                    // Key edges by first column (x) for join
                    let edges_keyed = edges_in_scope.map(|tuple| {
                        let x = tuple.get(0).cloned().unwrap_or(Value::Null);
                        let y = tuple.get(1).cloned().unwrap_or(Value::Null);
                        (Tuple::new(vec![x]), y) // Key by x, value is y
                    });

                    // Join: reach(x) JOIN edge(x, y) -> reach(y)
                    let recursive = reach_keyed
                        .join(&edges_keyed)
                        .map(|(_x_key, (_x, y))| Tuple::new(vec![y]));

                    // Combine base case and recursive case
                    let next = sources_in_scope.concat(&recursive).distinct();

                    // Set variable for next iteration
                    variable.set(&next);

                    // Leave scope with final result
                    next.leave()
                });

                // Capture results
                reach_result
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
}

impl Default for CodeGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to add edges from (i64, i64) tuples
    fn edges(pairs: &[(i64, i64)]) -> Vec<Tuple> {
        pairs.iter().map(|&(a, b)| Tuple::pair(a, b)).collect()
    }

    #[test]
    fn test_simple_scan() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input(
            "edge".to_string(),
            vec![Tuple::pair(1, 2), Tuple::pair(2, 3), Tuple::pair(3, 4)],
        );

        let ir = IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        let results = codegen.execute(&ir).unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.contains(&Tuple::pair(1, 2)));
        assert!(results.contains(&Tuple::pair(2, 3)));
        assert!(results.contains(&Tuple::pair(3, 4)));
    }

    #[test]
    fn test_map_swap() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input(
            "edge".to_string(),
            vec![Tuple::pair(1, 2), Tuple::pair(2, 3)],
        );

        let ir = IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![1, 0], // Swap columns
            output_schema: vec!["y".to_string(), "x".to_string()],
        };

        let results = codegen.execute(&ir).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&Tuple::pair(2, 1)));
        assert!(results.contains(&Tuple::pair(3, 2)));
    }

    #[test]
    fn test_filter() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input(
            "edge".to_string(),
            vec![Tuple::pair(1, 2), Tuple::pair(5, 10), Tuple::pair(3, 4)],
        );

        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::ColumnGtConst(0, 3),
        };

        let results = codegen.execute(&ir).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&Tuple::pair(5, 10)));
    }

    #[test]
    fn test_distinct() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input(
            "edge".to_string(),
            vec![Tuple::pair(1, 2), Tuple::pair(1, 2), Tuple::pair(2, 3)],
        );

        let ir = IRNode::Distinct {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
        };

        let results = codegen.execute(&ir).unwrap();
        assert_eq!(results.len(), 2); // Duplicates removed
    }

    // Production Tests (Arbitrary Arity)
    #[test]
    fn test_tuple_scan() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![
                    Value::Int32(1),
                    Value::string("a"),
                    Value::Float64(1.0),
                ]),
                Tuple::new(vec![
                    Value::Int32(2),
                    Value::string("b"),
                    Value::Float64(2.0),
                ]),
            ],
        );

        let ir = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["id".to_string(), "name".to_string(), "score".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_tuple_projection() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(5), Value::Int32(6)]),
            ],
        );

        // Project to [2, 0] - third column, then first column
        let ir = IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            }),
            projection: vec![2, 0],
            output_schema: vec!["c".to_string(), "a".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);

        // First tuple: (1,2,3) projected to (3,1)
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int32(3)) && t.get(1) == Some(&Value::Int32(1))));
        // Second tuple: (4,5,6) projected to (6,4)
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int32(6)) && t.get(1) == Some(&Value::Int32(4))));
    }

    #[test]
    fn test_tuple_filter() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(10)]),
                Tuple::new(vec![Value::Int32(5), Value::Int32(50)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(30)]),
            ],
        );

        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::ColumnGtConst(0, 2),
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);
        // Should contain (5, 50) and (3, 30), not (1, 10)
        assert!(results
            .iter()
            .all(|t| t.get(0).and_then(|v| v.as_i32()).unwrap_or(0) > 2));
    }

    #[test]
    fn test_tuple_join() {
        let mut codegen = CodeGenerator::new();

        // Relation R(x, y)
        codegen.add_input_tuples(
            "r".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(10)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(20)]),
            ],
        );

        // Relation S(y, z) - join on y column
        codegen.add_input_tuples(
            "s".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(10), Value::Int32(100)]),
                Tuple::new(vec![Value::Int32(20), Value::Int32(200)]),
                Tuple::new(vec![Value::Int32(30), Value::Int32(300)]), // No match
            ],
        );

        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "r".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "s".to_string(),
                schema: vec!["y".to_string(), "z".to_string()],
            }),
            left_keys: vec![1],  // R.y
            right_keys: vec![0], // S.y
            output_schema: vec!["x".to_string(), "y".to_string(), "z".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);
    }

    // Cartesian Product (Cross Join) Tests
    #[test]
    fn test_cartesian_product_basic() {
        // Test: 2x2 Cartesian product (no shared join keys)
        let mut codegen = CodeGenerator::new();

        // Relation A(x)
        codegen.add_input_tuples(
            "a".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
        );

        // Relation B(y)
        codegen.add_input_tuples(
            "b".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(10)]),
                Tuple::new(vec![Value::Int32(20)]),
            ],
        );

        // Join with empty keys = Cartesian product
        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["y".to_string()],
            }),
            left_keys: vec![],  // Empty keys
            right_keys: vec![], // Empty keys
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // 2 * 2 = 4 results
        assert_eq!(
            results.len(),
            4,
            "Expected 4 results from 2x2 Cartesian product"
        );

        // Check all combinations are present
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int32(1)) && t.get(1) == Some(&Value::Int32(10))));
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int32(1)) && t.get(1) == Some(&Value::Int32(20))));
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int32(2)) && t.get(1) == Some(&Value::Int32(10))));
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int32(2)) && t.get(1) == Some(&Value::Int32(20))));
    }

    #[test]
    fn test_cartesian_product_self_join() {
        // Test: Self Cartesian product (3x3 = 9 results)
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "items".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(3)]),
            ],
        );

        // Self-join with empty keys = all pairs
        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "items".to_string(),
                schema: vec!["a".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "items".to_string(),
                schema: vec!["b".to_string()],
            }),
            left_keys: vec![],
            right_keys: vec![],
            output_schema: vec!["a".to_string(), "b".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // 3 * 3 = 9 results (including self-pairs)
        assert_eq!(
            results.len(),
            9,
            "Expected 9 results from 3x3 self Cartesian product"
        );
    }

    #[test]
    fn test_cartesian_product_with_filter() {
        // Test: Cartesian product followed by filter (A < B)
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "items".to_string(),
            vec![
                Tuple::new(vec![Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(2)]),
                Tuple::new(vec![Value::Int64(3)]),
            ],
        );

        // Self Cartesian product, then filter for A < B
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Join {
                left: Box::new(IRNode::Scan {
                    relation: "items".to_string(),
                    schema: vec!["a".to_string()],
                }),
                right: Box::new(IRNode::Scan {
                    relation: "items".to_string(),
                    schema: vec!["b".to_string()],
                }),
                left_keys: vec![],
                right_keys: vec![],
                output_schema: vec!["a".to_string(), "b".to_string()],
            }),
            predicate: Predicate::ColumnsLt(0, 1), // a < b
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // Pairs where a < b: (1,2), (1,3), (2,3) = 3 results
        assert_eq!(results.len(), 3, "Expected 3 results where a < b");

        // Verify the pairs
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int64(1)) && t.get(1) == Some(&Value::Int64(2))));
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int64(1)) && t.get(1) == Some(&Value::Int64(3))));
        assert!(results
            .iter()
            .any(|t| t.get(0) == Some(&Value::Int64(2)) && t.get(1) == Some(&Value::Int64(3))));
    }

    #[test]
    fn test_cartesian_product_empty_relation() {
        // Test: Cartesian product with empty relation = empty result
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "a".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
        );

        codegen.add_input_tuples("b".to_string(), vec![]); // Empty relation

        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["y".to_string()],
            }),
            left_keys: vec![],
            right_keys: vec![],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(
            results.len(),
            0,
            "Cartesian product with empty relation should be empty"
        );
    }

    #[test]
    fn test_cartesian_product_single_elements() {
        // Test: 1x1 Cartesian product
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples("a".to_string(), vec![Tuple::new(vec![Value::Int32(42)])]);

        codegen.add_input_tuples("b".to_string(), vec![Tuple::new(vec![Value::Int32(99)])]);

        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["y".to_string()],
            }),
            left_keys: vec![],
            right_keys: vec![],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(
            results.len(),
            1,
            "1x1 Cartesian product should have 1 result"
        );
        assert_eq!(results[0].get(0), Some(&Value::Int32(42)));
        assert_eq!(results[0].get(1), Some(&Value::Int32(99)));
    }

    #[test]
    fn test_cartesian_product_with_vectors() {
        // Test: Cartesian product with vector columns (for pairwise similarity)
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "embedding".to_string(),
            vec![
                Tuple::new(vec![
                    Value::Int32(1),
                    Value::Vector(std::sync::Arc::new(vec![1.0f32, 0.0, 0.0])),
                ]),
                Tuple::new(vec![
                    Value::Int32(2),
                    Value::Vector(std::sync::Arc::new(vec![0.0f32, 1.0, 0.0])),
                ]),
            ],
        );

        // Self Cartesian product with filter Id1 < Id2
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Join {
                left: Box::new(IRNode::Scan {
                    relation: "embedding".to_string(),
                    schema: vec!["id1".to_string(), "v1".to_string()],
                }),
                right: Box::new(IRNode::Scan {
                    relation: "embedding".to_string(),
                    schema: vec!["id2".to_string(), "v2".to_string()],
                }),
                left_keys: vec![],
                right_keys: vec![],
                output_schema: vec![
                    "id1".to_string(),
                    "v1".to_string(),
                    "id2".to_string(),
                    "v2".to_string(),
                ],
            }),
            predicate: Predicate::ColumnsLt(0, 2), // id1 < id2
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // Only 1 pair: (1, v1, 2, v2)
        assert_eq!(
            results.len(),
            1,
            "Expected 1 result for pairs where id1 < id2"
        );

        // Verify the pair has vectors
        let result = &results[0];
        assert_eq!(result.get(0), Some(&Value::Int32(1)));
        assert!(matches!(result.get(1), Some(Value::Vector(_))));
        assert_eq!(result.get(2), Some(&Value::Int32(2)));
        assert!(matches!(result.get(3), Some(Value::Vector(_))));
    }

    #[test]
    fn test_3tuple_operations() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "triple".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(5), Value::Int32(6)]),
            ],
        );

        // Test that we can handle 3-tuples through the entire pipeline
        let ir = IRNode::Distinct {
            input: Box::new(IRNode::Filter {
                input: Box::new(IRNode::Scan {
                    relation: "triple".to_string(),
                    schema: vec!["a".to_string(), "b".to_string(), "c".to_string()],
                }),
                predicate: Predicate::ColumnGtConst(0, 0),
            }),
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|t| t.arity() == 3));
    }

    #[test]
    fn test_union_tuples() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "r1".to_string(),
            vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])],
        );
        codegen.add_input_tuples(
            "r2".to_string(),
            vec![Tuple::new(vec![Value::Int32(3), Value::Int32(4)])],
        );

        let ir = IRNode::Union {
            inputs: vec![
                IRNode::Scan {
                    relation: "r1".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                },
                IRNode::Scan {
                    relation: "r2".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                },
            ],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);
    }

    // Recursive Query Tests
    #[test]
    fn test_transitive_closure() {
        let mut codegen = CodeGenerator::new();

        // Graph: 1 -> 2 -> 3 -> 4
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 4)]));

        let results = codegen.execute_transitive_closure("edge").unwrap();

        // Should contain:
        // Direct: (1,2), (2,3), (3,4)
        // 2-hop: (1,3), (2,4)
        // 3-hop: (1,4)
        assert!(
            results.len() >= 6,
            "Expected at least 6 paths, got {}",
            results.len()
        );
        assert!(results.contains(&Tuple::pair(1, 2)), "Missing (1,2)");
        assert!(results.contains(&Tuple::pair(2, 3)), "Missing (2,3)");
        assert!(results.contains(&Tuple::pair(3, 4)), "Missing (3,4)");
        assert!(
            results.contains(&Tuple::pair(1, 3)),
            "Missing (1,3) - 2-hop path"
        );
        assert!(
            results.contains(&Tuple::pair(2, 4)),
            "Missing (2,4) - 2-hop path"
        );
        assert!(
            results.contains(&Tuple::pair(1, 4)),
            "Missing (1,4) - 3-hop path"
        );
    }

    #[test]
    fn test_transitive_closure_with_cycle() {
        let mut codegen = CodeGenerator::new();

        // Graph with cycle: 1 -> 2 -> 3 -> 1
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 1)]));

        let results = codegen.execute_transitive_closure("edge").unwrap();

        // With cycle, everyone can reach everyone
        // From 1: can reach 2, 3, 1
        // From 2: can reach 3, 1, 2
        // From 3: can reach 1, 2, 3
        // Total should be 9 paths (or 6 if self-loops excluded from base)
        assert!(
            results.len() >= 6,
            "Expected at least 6 paths, got {}",
            results.len()
        );

        // All paths should eventually exist
        assert!(results.contains(&Tuple::pair(1, 2)));
        assert!(results.contains(&Tuple::pair(2, 3)));
        assert!(results.contains(&Tuple::pair(3, 1)));
    }

    #[test]
    fn test_reachability() {
        let mut codegen = CodeGenerator::new();

        // Graph: 1 -> 2 -> 3 -> 4
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 4)]));

        // Source: node 1 (use Int64 to match edge data)
        codegen.add_input(
            "source".to_string(),
            vec![Tuple::new(vec![Value::Int64(1)])],
        );

        let results = codegen.execute_reachability("source", "edge").unwrap();

        // From source 1, we can reach: 1, 2, 3, 4
        assert!(
            results.len() >= 4,
            "Expected at least 4 reachable nodes, got {}",
            results.len()
        );

        assert!(results.contains(&1i64), "Source 1 should be reachable");
        assert!(results.contains(&2i64), "Node 2 should be reachable from 1");
        assert!(results.contains(&3i64), "Node 3 should be reachable from 1");
        assert!(results.contains(&4i64), "Node 4 should be reachable from 1");
    }

    #[test]
    fn test_reachability_multiple_sources() {
        let mut codegen = CodeGenerator::new();

        // Two disconnected components: 1 -> 2 and 3 -> 4
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (3, 4)]));

        // Sources: nodes 1 and 3 (use Int64 to match edge data)
        codegen.add_input(
            "source".to_string(),
            vec![
                Tuple::new(vec![Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(3)]),
            ],
        );

        let results = codegen.execute_reachability("source", "edge").unwrap();

        // From sources 1 and 3, we can reach: 1, 2, 3, 4
        assert!(results.contains(&1i64));
        assert!(results.contains(&2i64));
        assert!(results.contains(&3i64));
        assert!(results.contains(&4i64));
    }

    // True DD Recursion Tests (Using SemigroupVariable + .iterative())
    #[test]
    fn test_transitive_closure_dd_linear() {
        let mut codegen = CodeGenerator::new();

        // Graph: 1 -> 2 -> 3 -> 4 (linear chain)
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 4)]));

        let results = codegen.execute_transitive_closure_dd("edge").unwrap();

        // Should contain:
        // Direct: (1,2), (2,3), (3,4)
        // 2-hop: (1,3), (2,4)
        // 3-hop: (1,4)
        assert!(
            results.len() >= 6,
            "Expected at least 6 paths, got {}",
            results.len()
        );

        // Check all expected paths using Tuple
        let expected_pairs: Vec<(i64, i64)> = vec![(1, 2), (2, 3), (3, 4), (1, 3), (2, 4), (1, 4)];
        for (x, y) in expected_pairs {
            assert!(
                results.contains(&Tuple::pair(x, y)),
                "Missing path ({}, {})",
                x,
                y
            );
        }
    }

    #[test]
    fn test_transitive_closure_dd_branching() {
        let mut codegen = CodeGenerator::new();

        // Tree: 1 -> 2, 1 -> 3, 2 -> 4, 3 -> 5
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (1, 3), (2, 4), (3, 5)]));

        let results = codegen.execute_transitive_closure_dd("edge").unwrap();

        // Direct: (1,2), (1,3), (2,4), (3,5)
        // 2-hop: (1,4), (1,5)
        assert!(
            results.len() >= 6,
            "Expected at least 6 paths, got {}",
            results.len()
        );

        // All paths from node 1
        assert!(results.contains(&Tuple::pair(1, 2)), "Missing (1,2)");
        assert!(results.contains(&Tuple::pair(1, 3)), "Missing (1,3)");
        assert!(results.contains(&Tuple::pair(1, 4)), "Missing (1,4)");
        assert!(results.contains(&Tuple::pair(1, 5)), "Missing (1,5)");
    }

    #[test]
    fn test_transitive_closure_dd_cycle() {
        let mut codegen = CodeGenerator::new();

        // Cycle: 1 -> 2 -> 3 -> 1
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 1)]));

        let results = codegen.execute_transitive_closure_dd("edge").unwrap();

        // With cycle, everyone can reach everyone (including themselves via cycle)
        // All 9 pairs should be reachable
        assert!(
            results.len() >= 9,
            "Expected at least 9 paths in cycle, got {}",
            results.len()
        );

        // Check that 1 can reach all nodes
        assert!(results.contains(&Tuple::pair(1, 2)), "Missing (1,2)");
        assert!(results.contains(&Tuple::pair(1, 3)), "Missing (1,3)");
        assert!(
            results.contains(&Tuple::pair(1, 1)),
            "Missing (1,1) - cycle!"
        );
    }

    #[test]
    fn test_transitive_closure_dd_diamond() {
        let mut codegen = CodeGenerator::new();

        // Diamond: 1 -> 2, 1 -> 3, 2 -> 4, 3 -> 4
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (1, 3), (2, 4), (3, 4)]));

        let results = codegen.execute_transitive_closure_dd("edge").unwrap();

        // Direct: (1,2), (1,3), (2,4), (3,4)
        // 2-hop: (1,4) via both paths (but distinct)
        assert!(
            results.len() >= 5,
            "Expected at least 5 paths, got {}",
            results.len()
        );

        // 1 can reach 4 (via two different paths, but result is same)
        assert!(results.contains(&Tuple::pair(1, 4)), "Missing (1,4)");
    }

    #[test]
    fn test_transitive_closure_dd_empty() {
        let mut codegen = CodeGenerator::new();

        // No edges
        codegen.add_input("edge".to_string(), vec![]);

        let results = codegen.execute_transitive_closure_dd("edge").unwrap();
        assert!(results.is_empty(), "Expected empty result for empty graph");
    }

    #[test]
    fn test_transitive_closure_dd_self_loop() {
        let mut codegen = CodeGenerator::new();

        // Self-loop: 1 -> 1
        codegen.add_input("edge".to_string(), edges(&[(1, 1)]));

        let results = codegen.execute_transitive_closure_dd("edge").unwrap();

        // Should contain just (1, 1)
        assert!(results.len() >= 1, "Expected at least 1 path");
        assert!(results.contains(&Tuple::pair(1, 1)), "Missing (1,1)");
    }

    #[test]
    fn test_reachability_dd_linear() {
        let mut codegen = CodeGenerator::new();

        // Graph: 1 -> 2 -> 3 -> 4
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 4)]));

        // Source: node 1 (use Int64 to match edge data)
        codegen.add_input(
            "source".to_string(),
            vec![Tuple::new(vec![Value::Int64(1)])],
        );

        let results = codegen.execute_reachability_dd("source", "edge").unwrap();

        // From source 1, we can reach: 1, 2, 3, 4
        assert!(
            results.len() >= 4,
            "Expected at least 4 reachable nodes, got {}",
            results.len()
        );

        let reachable_ints: Vec<i64> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i64()))
            .collect();

        assert!(reachable_ints.contains(&1), "Source 1 should be reachable");
        assert!(
            reachable_ints.contains(&2),
            "Node 2 should be reachable from 1"
        );
        assert!(
            reachable_ints.contains(&3),
            "Node 3 should be reachable from 1"
        );
        assert!(
            reachable_ints.contains(&4),
            "Node 4 should be reachable from 1"
        );
    }

    #[test]
    fn test_reachability_dd_multiple_sources() {
        let mut codegen = CodeGenerator::new();

        // Two disconnected components: 1 -> 2 and 10 -> 20
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (10, 20)]));

        // Sources: nodes 1 and 10 (use Int64 to match edge data)
        codegen.add_input(
            "source".to_string(),
            vec![
                Tuple::new(vec![Value::Int64(1)]),
                Tuple::new(vec![Value::Int64(10)]),
            ],
        );

        let results = codegen.execute_reachability_dd("source", "edge").unwrap();

        let reachable_ints: Vec<i64> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i64()))
            .collect();

        // From sources 1 and 10, we can reach: 1, 2, 10, 20
        assert!(reachable_ints.contains(&1), "Source 1 should be reachable");
        assert!(reachable_ints.contains(&2), "Node 2 should be reachable");
        assert!(
            reachable_ints.contains(&10),
            "Source 10 should be reachable"
        );
        assert!(reachable_ints.contains(&20), "Node 20 should be reachable");
    }

    #[test]
    fn test_reachability_dd_unreachable() {
        let mut codegen = CodeGenerator::new();

        // Graph: 1 -> 2, 10 -> 20 (disconnected)
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (10, 20)]));

        // Source: only node 1 (use Int64 to match edge data)
        codegen.add_input(
            "source".to_string(),
            vec![Tuple::new(vec![Value::Int64(1)])],
        );

        let results = codegen.execute_reachability_dd("source", "edge").unwrap();

        let reachable_ints: Vec<i64> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i64()))
            .collect();

        // From source 1, we can reach: 1, 2 (but NOT 10, 20)
        assert!(reachable_ints.contains(&1), "Source 1 should be reachable");
        assert!(reachable_ints.contains(&2), "Node 2 should be reachable");
        assert!(
            !reachable_ints.contains(&10),
            "Node 10 should NOT be reachable"
        );
        assert!(
            !reachable_ints.contains(&20),
            "Node 20 should NOT be reachable"
        );
    }

    #[test]
    fn test_reachability_dd_cycle() {
        let mut codegen = CodeGenerator::new();

        // Cycle: 1 -> 2 -> 3 -> 1
        codegen.add_input("edge".to_string(), edges(&[(1, 2), (2, 3), (3, 1)]));

        // Source: node 1 (use Int64 to match edge data)
        codegen.add_input(
            "source".to_string(),
            vec![Tuple::new(vec![Value::Int64(1)])],
        );

        let results = codegen.execute_reachability_dd("source", "edge").unwrap();

        let reachable_ints: Vec<i64> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i64()))
            .collect();

        // All nodes in cycle should be reachable
        assert!(reachable_ints.contains(&1), "Node 1 should be reachable");
        assert!(reachable_ints.contains(&2), "Node 2 should be reachable");
        assert!(reachable_ints.contains(&3), "Node 3 should be reachable");
    }

    // Antijoin (Negation) Tests
    #[test]
    fn test_antijoin_simple() {
        // Test: unreachable(x) :- node(x), !reach(x)
        // Nodes: 1, 2, 3, 4, 5
        // Reachable: 1, 2
        // Expected unreachable: 3, 4, 5
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "node".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(4)]),
                Tuple::new(vec![Value::Int32(5)]),
            ],
        );

        codegen.add_input_tuples(
            "reach".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
        );

        // Build IR: node(x) antijoin reach(x)
        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "node".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "reach".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // Should get nodes 3, 4, 5 (not in reach)
        assert_eq!(results.len(), 3, "Expected 3 unreachable nodes");

        let result_ints: Vec<i32> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i32()))
            .collect();

        assert!(result_ints.contains(&3), "Node 3 should be unreachable");
        assert!(result_ints.contains(&4), "Node 4 should be unreachable");
        assert!(result_ints.contains(&5), "Node 5 should be unreachable");
        assert!(!result_ints.contains(&1), "Node 1 should NOT be in result");
        assert!(!result_ints.contains(&2), "Node 2 should NOT be in result");
    }

    #[test]
    fn test_antijoin_empty_right() {
        // When right side is empty, all left tuples pass through
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "left".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(3)]),
            ],
        );

        codegen.add_input_tuples(
            "right".to_string(),
            vec![], // Empty!
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "left".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "right".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // All left tuples should pass through
        assert_eq!(results.len(), 3, "All left tuples should remain");
    }

    #[test]
    fn test_antijoin_full_filter() {
        // When right contains all keys from left, result is empty
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "left".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
        );

        codegen.add_input_tuples(
            "right".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(3)]), // Extra, doesn't matter
            ],
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "left".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "right".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // All left tuples have matches, so result is empty
        assert_eq!(results.len(), 0, "All left tuples were filtered out");
    }

    #[test]
    fn test_antijoin_multi_column_left() {
        // Antijoin with multi-column left tuples, single-column key
        // left: (x, name)
        // right: (x)
        // Result: left tuples where x is NOT in right
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "person".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::String("Alice".into())]),
                Tuple::new(vec![Value::Int32(2), Value::String("Bob".into())]),
                Tuple::new(vec![Value::Int32(3), Value::String("Carol".into())]),
            ],
        );

        codegen.add_input_tuples(
            "banned".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(2)]), // Bob is banned
            ],
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "person".to_string(),
                schema: vec!["id".to_string(), "name".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "banned".to_string(),
                schema: vec!["id".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["id".to_string(), "name".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // Alice and Carol should remain (not banned)
        assert_eq!(results.len(), 2, "Expected 2 non-banned people");

        let names: Vec<&str> = results
            .iter()
            .filter_map(|t| t.get(1).and_then(|v| v.as_str()))
            .collect();

        assert!(names.contains(&"Alice"), "Alice should be in result");
        assert!(names.contains(&"Carol"), "Carol should be in result");
        assert!(!names.contains(&"Bob"), "Bob should NOT be in result");
    }

    #[test]
    fn test_antijoin_multi_column_key() {
        // Antijoin on multiple join columns
        // left: (x, y, data)
        // right: (x, y)
        // Join on both x and y
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "left".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(1), Value::Int32(100)]),
                Tuple::new(vec![Value::Int32(1), Value::Int32(2), Value::Int32(200)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(1), Value::Int32(300)]),
            ],
        );

        codegen.add_input_tuples(
            "right".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(1)]), // Matches first row
            ],
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "left".to_string(),
                schema: vec!["x".to_string(), "y".to_string(), "data".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "right".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            left_keys: vec![0, 1],
            right_keys: vec![0, 1],
            output_schema: vec!["x".to_string(), "y".to_string(), "data".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // (1,1,100) is filtered out, (1,2,200) and (2,1,300) remain
        assert_eq!(results.len(), 2, "Expected 2 rows after antijoin");

        let data_values: Vec<i32> = results
            .iter()
            .filter_map(|t| t.get(2).and_then(|v| v.as_i32()))
            .collect();

        assert!(
            data_values.contains(&200),
            "Row with data 200 should remain"
        );
        assert!(
            data_values.contains(&300),
            "Row with data 300 should remain"
        );
        assert!(
            !data_values.contains(&100),
            "Row with data 100 should be filtered"
        );
    }

    #[test]
    fn test_antijoin_with_filter() {
        // Antijoin combined with filter
        // First filter left, then antijoin
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(10)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(20)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(30)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(40)]),
            ],
        );

        codegen.add_input_tuples(
            "excluded".to_string(),
            vec![Tuple::new(vec![Value::Int32(2)])],
        );

        // Filter: x > 1, then antijoin to remove excluded
        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Filter {
                input: Box::new(IRNode::Scan {
                    relation: "data".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                predicate: Predicate::ColumnGtConst(0, 1), // x > 1
            }),
            right: Box::new(IRNode::Scan {
                relation: "excluded".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // After filter: (2,20), (3,30), (4,40)
        // After antijoin (remove 2): (3,30), (4,40)
        assert_eq!(results.len(), 2, "Expected 2 rows");

        let x_values: Vec<i32> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i32()))
            .collect();

        assert!(x_values.contains(&3), "Row with x=3 should remain");
        assert!(x_values.contains(&4), "Row with x=4 should remain");
    }

    #[test]
    fn test_antijoin_empty_left() {
        // When left is empty, result is empty
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "left".to_string(),
            vec![], // Empty!
        );

        codegen.add_input_tuples(
            "right".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "left".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "right".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        assert_eq!(results.len(), 0, "Empty left produces empty result");
    }

    #[test]
    fn test_antijoin_duplicates_in_right() {
        // Duplicates in right should not affect result
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "left".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(3)]),
            ],
        );

        codegen.add_input_tuples(
            "right".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(1)]), // Duplicate!
                Tuple::new(vec![Value::Int32(1)]), // Another duplicate!
            ],
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "left".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "right".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // Only 1 is filtered, 2 and 3 remain
        assert_eq!(results.len(), 2, "Expected 2 rows");

        let values: Vec<i32> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i32()))
            .collect();

        assert!(values.contains(&2), "2 should remain");
        assert!(values.contains(&3), "3 should remain");
        assert!(!values.contains(&1), "1 should be filtered");
    }

    #[test]
    fn test_antijoin_new_edges() {
        // Pattern: new_edge(x,y) :- candidate(x,y), !edge(x,y)
        // Find edges that are candidates but not already in graph
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "candidate".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(5)]),
            ],
        );

        codegen.add_input_tuples(
            "edge".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]), // Already exists
                Tuple::new(vec![Value::Int32(3), Value::Int32(4)]), // Already exists
            ],
        );

        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "candidate".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            left_keys: vec![0, 1],
            right_keys: vec![0, 1],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // New edges: (2,3) and (4,5)
        assert_eq!(results.len(), 2, "Expected 2 new edges");

        let pairs: Vec<(i32, i32)> = results.iter().filter_map(|t| t.to_pair()).collect();

        assert!(pairs.contains(&(2, 3)), "Edge (2,3) should be new");
        assert!(pairs.contains(&(4, 5)), "Edge (4,5) should be new");
    }

    // Multi-Worker Execution Tests
    #[test]
    fn test_multi_worker_simple_scan() {
        // Test that multi-worker execution produces same results as single-worker
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(10)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(20)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(30)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(40)]),
            ],
        );

        let ir = IRNode::Scan {
            relation: "data".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        };

        // Single worker
        let single_results = codegen.generate_and_execute_tuples(&ir).unwrap();

        // Multi-worker (2 workers)
        let config = ExecutionConfig::with_workers(2);
        let multi_results = codegen.execute_with_config(&ir, config).unwrap();

        // Results should have same length
        assert_eq!(
            single_results.len(),
            multi_results.len(),
            "Multi-worker should produce same number of results"
        );

        // Sort both for comparison (order may differ)
        let mut sorted_single: Vec<_> = single_results.iter().collect();
        let mut sorted_multi: Vec<_> = multi_results.iter().collect();
        sorted_single.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));
        sorted_multi.sort_by(|a, b| format!("{:?}", a).cmp(&format!("{:?}", b)));

        assert_eq!(sorted_single, sorted_multi, "Results should match");
    }

    #[test]
    fn test_multi_worker_filter() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(10)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(20)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(30)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(40)]),
                Tuple::new(vec![Value::Int32(5), Value::Int32(50)]),
                Tuple::new(vec![Value::Int32(6), Value::Int32(60)]),
            ],
        );

        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::ColumnGtConst(0, 3), // x > 3
        };

        // Multi-worker (2 workers)
        let config = ExecutionConfig::with_workers(2);
        let results = codegen.execute_with_config(&ir, config).unwrap();

        // Should have 3 results: (4,40), (5,50), (6,60)
        assert_eq!(results.len(), 3, "Expected 3 rows where x > 3");

        let x_values: Vec<i32> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i32()))
            .collect();

        for x in [4, 5, 6] {
            assert!(x_values.contains(&x), "Row with x={} should be present", x);
        }
    }

    #[test]
    fn test_multi_worker_join() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "left".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(100)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(200)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(300)]),
            ],
        );

        codegen.add_input_tuples(
            "right".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(2), Value::Int32(20)]),
                Tuple::new(vec![Value::Int32(3), Value::Int32(30)]),
                Tuple::new(vec![Value::Int32(4), Value::Int32(40)]),
            ],
        );

        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "left".to_string(),
                schema: vec!["x".to_string(), "a".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "right".to_string(),
                schema: vec!["x".to_string(), "b".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "a".to_string(), "b".to_string()],
        };

        // Multi-worker (2 workers)
        let config = ExecutionConfig::with_workers(2);
        let results = codegen.execute_with_config(&ir, config).unwrap();

        // Should have 2 results: keys 2 and 3 match
        assert_eq!(results.len(), 2, "Expected 2 join results");
    }

    #[test]
    fn test_execution_config_defaults() {
        let config = ExecutionConfig::default();
        assert_eq!(config.num_workers, 1);

        let config = ExecutionConfig::with_workers(4);
        assert_eq!(config.num_workers, 4);

        let config = ExecutionConfig::single_worker();
        assert_eq!(config.num_workers, 1);

        let config = ExecutionConfig::all_cores();
        assert!(config.num_workers >= 1);
    }

    // Vector Search Integration Tests
    #[test]
    fn test_compute_euclidean_distance() {
        let mut codegen = CodeGenerator::new();

        // Add vectors as input data
        codegen.add_input_tuples(
            "vectors".to_string(),
            vec![
                Tuple::new(vec![
                    Value::Int32(1),
                    Value::vector(vec![0.0, 0.0]),
                    Value::vector(vec![3.0, 4.0]),
                ]),
                Tuple::new(vec![
                    Value::Int32(2),
                    Value::vector(vec![1.0, 1.0]),
                    Value::vector(vec![2.0, 2.0]),
                ]),
            ],
        );

        // IR: Scan vectors, compute euclidean distance between columns 1 and 2
        let ir = IRNode::Compute {
            input: Box::new(IRNode::Scan {
                relation: "vectors".to_string(),
                schema: vec!["id".to_string(), "v1".to_string(), "v2".to_string()],
            }),
            expressions: vec![(
                "dist".to_string(),
                IRExpression::FunctionCall(
                    BuiltinFunction::Euclidean,
                    vec![IRExpression::Column(1), IRExpression::Column(2)],
                ),
            )],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);

        // First tuple: distance between (0,0) and (3,4) = 5.0
        let first = &results[0];
        assert_eq!(first.get(0), Some(&Value::Int32(1)));
        let dist1 = first.get(3).unwrap().to_f64();
        assert!(
            (dist1 - 5.0).abs() < 0.001,
            "Expected dist 5.0, got {}",
            dist1
        );

        // Second tuple: distance between (1,1) and (2,2) = sqrt(2)
        let second = &results[1];
        let dist2 = second.get(3).unwrap().to_f64();
        let expected = (2.0_f64).sqrt();
        assert!(
            (dist2 - expected).abs() < 0.001,
            "Expected dist {}, got {}",
            expected,
            dist2
        );
    }

    #[test]
    fn test_compute_cosine_distance() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "vectors".to_string(),
            vec![
                Tuple::new(vec![
                    Value::Int32(1),
                    Value::vector(vec![1.0, 0.0]),
                    Value::vector(vec![2.0, 0.0]), // Same direction
                ]),
                Tuple::new(vec![
                    Value::Int32(2),
                    Value::vector(vec![1.0, 0.0]),
                    Value::vector(vec![0.0, 1.0]), // Orthogonal
                ]),
            ],
        );

        let ir = IRNode::Compute {
            input: Box::new(IRNode::Scan {
                relation: "vectors".to_string(),
                schema: vec!["id".to_string(), "v1".to_string(), "v2".to_string()],
            }),
            expressions: vec![(
                "cos_dist".to_string(),
                IRExpression::FunctionCall(
                    BuiltinFunction::Cosine,
                    vec![IRExpression::Column(1), IRExpression::Column(2)],
                ),
            )],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);

        // Same direction: cosine distance = 0
        let dist1 = results[0].get(3).unwrap().to_f64();
        assert!(
            dist1.abs() < 0.001,
            "Expected cosine dist ~0, got {}",
            dist1
        );

        // Orthogonal: cosine distance = 1
        let dist2 = results[1].get(3).unwrap().to_f64();
        assert!(
            (dist2 - 1.0).abs() < 0.001,
            "Expected cosine dist ~1, got {}",
            dist2
        );
    }

    #[test]
    fn test_compute_lsh_bucket() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "vectors".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::vector(vec![1.0, 0.0, 0.0])]),
                Tuple::new(vec![
                    Value::Int32(2),
                    Value::vector(vec![0.99, 0.01, 0.0]), // Similar to first
                ]),
                Tuple::new(vec![
                    Value::Int32(3),
                    Value::vector(vec![-1.0, 0.0, 0.0]), // Opposite direction
                ]),
            ],
        );

        // Compute LSH bucket with 4 hyperplanes (16 possible buckets)
        let ir = IRNode::Compute {
            input: Box::new(IRNode::Scan {
                relation: "vectors".to_string(),
                schema: vec!["id".to_string(), "vec".to_string()],
            }),
            expressions: vec![(
                "bucket".to_string(),
                IRExpression::FunctionCall(
                    BuiltinFunction::LshBucket,
                    vec![
                        IRExpression::Column(1),
                        IRExpression::IntConstant(0), // table_idx
                        IRExpression::IntConstant(4), // num_hyperplanes
                    ],
                ),
            )],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 3);

        // All buckets should be valid (0-15 for 4 hyperplanes)
        for result in &results {
            let bucket = result.get(2).unwrap().to_i64();
            assert!(bucket >= 0 && bucket < 16, "Invalid bucket: {}", bucket);
        }

        // Similar vectors (1 and 2) might have same bucket (not guaranteed but likely)
        // This is probabilistic, so we don't assert equality
    }

    #[test]
    fn test_top_k_aggregate() {
        let mut codegen = CodeGenerator::new();

        // Input: items with scores
        codegen.add_input_tuples(
            "items".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Float64(5.0)]),
                Tuple::new(vec![Value::Int32(2), Value::Float64(3.0)]),
                Tuple::new(vec![Value::Int32(3), Value::Float64(8.0)]),
                Tuple::new(vec![Value::Int32(4), Value::Float64(1.0)]),
                Tuple::new(vec![Value::Int32(5), Value::Float64(7.0)]),
            ],
        );

        // Top 3 by score (descending)
        let ir = IRNode::Aggregate {
            input: Box::new(IRNode::Scan {
                relation: "items".to_string(),
                schema: vec!["id".to_string(), "score".to_string()],
            }),
            group_by: vec![], // No grouping - global top-k
            aggregations: vec![(
                AggregateFunction::TopK {
                    k: 3,
                    order_col: 1,
                    descending: true,
                },
                0,
            )],
            output_schema: vec!["id".to_string(), "score".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 3, "Expected top 3 results");

        // Verify we got the top 3 scores (8.0, 7.0, 5.0)
        let scores: Vec<f64> = results.iter().map(|t| t.get(1).unwrap().to_f64()).collect();
        assert!(scores.contains(&8.0), "Missing score 8.0");
        assert!(scores.contains(&7.0), "Missing score 7.0");
        assert!(scores.contains(&5.0), "Missing score 5.0");
    }

    #[test]
    fn test_top_k_threshold_aggregate() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "items".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Float64(2.0)]), // Below threshold
                Tuple::new(vec![Value::Int32(2), Value::Float64(5.0)]), // Above
                Tuple::new(vec![Value::Int32(3), Value::Float64(8.0)]), // Above
                Tuple::new(vec![Value::Int32(4), Value::Float64(1.0)]), // Below threshold
            ],
        );

        // Top 3 with threshold 4.0 (only scores >= 4.0)
        let ir = IRNode::Aggregate {
            input: Box::new(IRNode::Scan {
                relation: "items".to_string(),
                schema: vec!["id".to_string(), "score".to_string()],
            }),
            group_by: vec![],
            aggregations: vec![(
                AggregateFunction::TopKThreshold {
                    k: 3,
                    order_col: 1,
                    threshold: 4.0,
                    descending: true,
                },
                0,
            )],
            output_schema: vec!["id".to_string(), "score".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2, "Expected 2 results above threshold");

        // Both should be above threshold (5.0 and 8.0)
        for result in &results {
            let score = result.get(1).unwrap().to_f64();
            assert!(score >= 4.0, "Score {} is below threshold 4.0", score);
        }
    }

    #[test]
    fn test_within_radius_aggregate() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "items".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Float64(0.1)]), // Within
                Tuple::new(vec![Value::Int32(2), Value::Float64(0.5)]), // Within
                Tuple::new(vec![Value::Int32(3), Value::Float64(1.5)]), // Outside
                Tuple::new(vec![Value::Int32(4), Value::Float64(0.3)]), // Within
            ],
        );

        // All items within distance 0.5
        let ir = IRNode::Aggregate {
            input: Box::new(IRNode::Scan {
                relation: "items".to_string(),
                schema: vec!["id".to_string(), "dist".to_string()],
            }),
            group_by: vec![],
            aggregations: vec![(
                AggregateFunction::WithinRadius {
                    distance_col: 1,
                    max_distance: 0.5,
                },
                0,
            )],
            output_schema: vec!["id".to_string(), "dist".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 3, "Expected 3 items within radius");

        // All should have distance <= 0.5
        for result in &results {
            let dist = result.get(1).unwrap().to_f64();
            assert!(dist <= 0.5, "Distance {} is outside radius 0.5", dist);
        }
    }

    #[test]
    fn test_vector_search_pipeline() {
        // Full vector search pipeline:
        // 1. Compute distances
        // 2. Filter by threshold
        // 3. Return top-k

        let mut codegen = CodeGenerator::new();

        // Database vectors
        codegen.add_input_tuples(
            "db_vectors".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::vector(vec![1.0, 0.0])]),
                Tuple::new(vec![Value::Int32(2), Value::vector(vec![0.9, 0.1])]), // Close to query
                Tuple::new(vec![Value::Int32(3), Value::vector(vec![0.0, 1.0])]), // Far from query
                Tuple::new(vec![Value::Int32(4), Value::vector(vec![0.8, 0.2])]), // Close
                Tuple::new(vec![Value::Int32(5), Value::vector(vec![-1.0, 0.0])]), // Very far
            ],
        );

        // Query vector (will be a constant in the expression)
        let query_vec = vec![1.0, 0.0];

        // Compute distances to query vector
        let with_distances = IRNode::Compute {
            input: Box::new(IRNode::Scan {
                relation: "db_vectors".to_string(),
                schema: vec!["id".to_string(), "vec".to_string()],
            }),
            expressions: vec![(
                "dist".to_string(),
                IRExpression::FunctionCall(
                    BuiltinFunction::Euclidean,
                    vec![
                        IRExpression::Column(1),
                        IRExpression::VectorLiteral(query_vec),
                    ],
                ),
            )],
        };

        // Get top 2 closest (ascending by distance)
        let ir = IRNode::Aggregate {
            input: Box::new(with_distances),
            group_by: vec![],
            aggregations: vec![(
                AggregateFunction::TopK {
                    k: 2,
                    order_col: 2,
                    descending: false,
                },
                0,
            )],
            output_schema: vec!["id".to_string(), "vec".to_string(), "dist".to_string()],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2, "Expected top 2 closest vectors");

        // The closest should be id=1 (distance 0) and id=2 (very close)
        let ids: Vec<i32> = results
            .iter()
            .filter_map(|t| t.get(0).and_then(|v| v.as_i32()))
            .collect();
        assert!(ids.contains(&1), "ID 1 should be in top 2 (exact match)");
        // ID 2 or 4 should be second (both are close)
    }

    #[test]
    fn test_arithmetic_expression() {
        let mut codegen = CodeGenerator::new();

        codegen.add_input_tuples(
            "data".to_string(),
            vec![
                Tuple::new(vec![Value::Int64(10), Value::Int64(3)]),
                Tuple::new(vec![Value::Int64(20), Value::Int64(5)]),
            ],
        );

        // Compute: a + b, a - b, a * b, a / b
        let ir = IRNode::Compute {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["a".to_string(), "b".to_string()],
            }),
            expressions: vec![
                (
                    "sum".to_string(),
                    IRExpression::Arithmetic {
                        op: ArithOp::Add,
                        left: Box::new(IRExpression::Column(0)),
                        right: Box::new(IRExpression::Column(1)),
                    },
                ),
                (
                    "diff".to_string(),
                    IRExpression::Arithmetic {
                        op: ArithOp::Sub,
                        left: Box::new(IRExpression::Column(0)),
                        right: Box::new(IRExpression::Column(1)),
                    },
                ),
                (
                    "prod".to_string(),
                    IRExpression::Arithmetic {
                        op: ArithOp::Mul,
                        left: Box::new(IRExpression::Column(0)),
                        right: Box::new(IRExpression::Column(1)),
                    },
                ),
            ],
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2);

        // First row: 10 + 3 = 13, 10 - 3 = 7, 10 * 3 = 30
        let first = &results[0];
        assert_eq!(first.get(2).unwrap().to_i64(), 13); // sum
        assert_eq!(first.get(3).unwrap().to_i64(), 7); // diff
        assert_eq!(first.get(4).unwrap().to_i64(), 30); // prod

        // Second row: 20 + 5 = 25, 20 - 5 = 15, 20 * 5 = 100
        let second = &results[1];
        assert_eq!(second.get(2).unwrap().to_i64(), 25);
        assert_eq!(second.get(3).unwrap().to_i64(), 15);
        assert_eq!(second.get(4).unwrap().to_i64(), 100);
    }

    // String and Float Predicate Tests
    #[test]
    fn test_string_equality_filter() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "person".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::string("alice")]),
                Tuple::new(vec![Value::Int32(2), Value::string("bob")]),
                Tuple::new(vec![Value::Int32(3), Value::string("alice")]),
                Tuple::new(vec![Value::Int32(4), Value::string("charlie")]),
            ],
        );

        // Filter: name = "alice"
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "person".to_string(),
                schema: vec!["id".to_string(), "name".to_string()],
            }),
            predicate: Predicate::ColumnEqStr(1, "alice".to_string()),
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2, "Expected 2 rows with name='alice'");

        // All results should have name = "alice"
        for tuple in &results {
            assert_eq!(
                tuple.get(1).and_then(|v| v.as_str()),
                Some("alice"),
                "All results should have name='alice'"
            );
        }
    }

    #[test]
    fn test_string_inequality_filter() {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "person".to_string(),
            vec![
                Tuple::new(vec![Value::Int32(1), Value::string("alice")]),
                Tuple::new(vec![Value::Int32(2), Value::string("bob")]),
                Tuple::new(vec![Value::Int32(3), Value::string("alice")]),
                Tuple::new(vec![Value::Int32(4), Value::string("charlie")]),
            ],
        );

        // Filter: name != "alice"
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "person".to_string(),
                schema: vec!["id".to_string(), "name".to_string()],
            }),
            predicate: Predicate::ColumnNeStr(1, "alice".to_string()),
        };

        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), 2, "Expected 2 rows with name!='alice'");

        // No results should have name = "alice"
        for tuple in &results {
            assert_ne!(
                tuple.get(1).and_then(|v| v.as_str()),
                Some("alice"),
                "No results should have name='alice'"
            );
        }
    }

    fn run_float_filter_test(
        data: Vec<(i32, f64)>,
        predicate: Predicate,
        expected_count: usize,
        label: &str,
    ) {
        let mut codegen = CodeGenerator::new();
        codegen.add_input_tuples(
            "measurement".to_string(),
            data.iter()
                .map(|(id, val)| Tuple::new(vec![Value::Int32(*id), Value::Float64(*val)]))
                .collect(),
        );
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "measurement".to_string(),
                schema: vec!["id".to_string(), "value".to_string()],
            }),
            predicate,
        };
        let results = codegen.generate_and_execute_tuples(&ir).unwrap();
        assert_eq!(results.len(), expected_count, "{label}");
    }

    #[test]
    fn test_float_comparison_filters() {
        let ordered = vec![(1, 1.0), (2, 2.5), (3, 3.0), (4, 4.5)];
        let with_dups = vec![(1, 3.14), (2, 2.71), (3, 3.14), (4, 1.41)];

        run_float_filter_test(
            with_dups.clone(),
            Predicate::ColumnEqFloat(1, 3.14),
            2,
            "eq 3.14",
        );
        run_float_filter_test(
            ordered.clone(),
            Predicate::ColumnGtFloat(1, 2.0),
            3,
            "gt 2.0",
        );
        run_float_filter_test(
            ordered.clone(),
            Predicate::ColumnLtFloat(1, 3.0),
            2,
            "lt 3.0",
        );
        run_float_filter_test(
            ordered.clone(),
            Predicate::ColumnGeFloat(1, 2.5),
            3,
            "ge 2.5",
        );
        run_float_filter_test(ordered, Predicate::ColumnLeFloat(1, 2.5), 2, "le 2.5");
        run_float_filter_test(
            vec![(1, 3.14), (2, 2.71), (3, 3.14)],
            Predicate::ColumnNeFloat(1, 3.14),
            1,
            "ne 3.14",
        );
    }

