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
