//! # IR Optimizer
//!
//! Algebraic optimizations on IR trees, applied to fixpoint:
//!
//! - Map fusion: `Map(Map(x, p1), p2)` -> `Map(x, p1 compose p2)`
//! - Filter fusion: `Filter(Filter(x, p1), p2)` -> `Filter(x, p1 && p2)`
//! - Filter pushdown: `Filter(Join(A, B), pred)` -> `Join(Filter(A, pred), B)`
//! - Identity elimination: `Map(x, id)` -> `x`, `Filter(x, True)` -> `x`
//! - Logic fusion: `Filter(Map(x, proj), pred)` -> `FlatMap(x, proj, pred)`
//!
//! ```text
//! IRNode (from IR Builder) -> [Optimizer] -> Optimized IRNode -> Code Gen
//! ```

use crate::ir::{IRNode, Predicate};

/// IR Optimizer with fixpoint iteration
pub struct Optimizer {
    /// Maximum number of optimization passes
    max_iterations: usize,
}

impl Optimizer {
    /// Create a new optimizer with default max iterations
    pub fn new() -> Self {
        Optimizer { max_iterations: 10 }
    }

    /// Create optimizer with custom max iterations
    pub fn with_max_iterations(max_iterations: usize) -> Self {
        Optimizer { max_iterations }
    }

    /// Optimize an IR tree to fixpoint
    ///
    /// Applies optimization rules repeatedly until the IR stops changing
    /// or `max_iterations` is reached. Then applies logic fusion as a
    /// final single pass (fusion creates FlatMap/JoinFlatMap which are
    /// terminal forms - no further optimization needed).
    pub fn optimize(&self, ir: IRNode) -> IRNode {
        let mut current = ir;

        for _iteration in 0..self.max_iterations {
            let optimized = self.apply_all_rules(current.clone());

            // Check if we reached fixpoint
            if Self::ir_equals(&optimized, &current) {
                #[cfg(test)]
                println!("Optimizer reached fixpoint at iteration {}", _iteration);
                break;
            }

            current = optimized;
        }

        // Final pass: Logic Fusion (FlatMap, JoinFlatMap)
        // Applied once after fixpoint since fused nodes are terminal forms
        let current = self.fuse_to_flatmap(current);
        self.fuse_to_join_flatmap(current)
    }

    /// Apply all optimization rules once
    fn apply_all_rules(&self, ir: IRNode) -> IRNode {
        // Identity elimination
        let ir = self.eliminate_identity_maps(ir);
        let ir = self.eliminate_always_true_filters(ir);
        let ir = self.eliminate_always_false_filters(ir);

        // Fusion optimizations
        let ir = self.fuse_consecutive_maps(ir);
        let ir = self.fuse_consecutive_filters(ir);

        // Filter pushdown
        let ir = self.pushdown_filters(ir);

        // Dead code elimination
        self.eliminate_empty_unions(ir)
    }

    /// Rule: Fuse consecutive Map nodes
    ///
    /// Map(Map(input, p1), p2) -> Map(input, p1 compose p2)
    /// Composition: `new_projection`[i] = p1[p2[i]]
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn fuse_consecutive_maps(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Map {
                input,
                projection: outer_projection,
                output_schema: outer_schema,
            } => {
                // First, recursively optimize the input
                let optimized_input = self.fuse_consecutive_maps(*input);

                // Check if input is also a Map
                if let IRNode::Map {
                    input: inner_input,
                    projection: inner_projection,
                    output_schema: _,
                } = optimized_input
                {
                    // Compose projections: outer_projection indexes into inner_projection
                    let composed_projection: Vec<usize> = outer_projection
                        .iter()
                        .map(|&outer_idx| inner_projection[outer_idx])
                        .collect();

                    IRNode::Map {
                        input: inner_input,
                        projection: composed_projection,
                        output_schema: outer_schema,
                    }
                } else {
                    IRNode::Map {
                        input: Box::new(optimized_input),
                        projection: outer_projection,
                        output_schema: outer_schema,
                    }
                }
            }

            IRNode::Filter { input, predicate } => IRNode::Filter {
                input: Box::new(self.fuse_consecutive_maps(*input)),
                predicate,
            },

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Join {
                left: Box::new(self.fuse_consecutive_maps(*left)),
                right: Box::new(self.fuse_consecutive_maps(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Antijoin {
                left: Box::new(self.fuse_consecutive_maps(*left)),
                right: Box::new(self.fuse_consecutive_maps(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.fuse_consecutive_maps(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.fuse_consecutive_maps(ir))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.fuse_consecutive_maps(*input)),
                group_by,
                aggregations,
                output_schema,
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.fuse_consecutive_maps(*input)),
                expressions,
            },

            other => other,
        }
    }

    /// Rule: Fuse consecutive Filter nodes
    ///
    /// Filter(Filter(input, p1), p2) -> Filter(input, And(p1, p2))
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn fuse_consecutive_filters(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Filter {
                input,
                predicate: outer_predicate,
            } => {
                // First, recursively optimize the input
                let optimized_input = self.fuse_consecutive_filters(*input);

                // Check if input is also a Filter
                if let IRNode::Filter {
                    input: inner_input,
                    predicate: inner_predicate,
                } = optimized_input
                {
                    // Combine predicates with And
                    let combined_predicate =
                        Predicate::And(Box::new(inner_predicate), Box::new(outer_predicate));

                    IRNode::Filter {
                        input: inner_input,
                        predicate: combined_predicate,
                    }
                } else {
                    IRNode::Filter {
                        input: Box::new(optimized_input),
                        predicate: outer_predicate,
                    }
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.fuse_consecutive_filters(*input)),
                projection,
                output_schema,
            },

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Join {
                left: Box::new(self.fuse_consecutive_filters(*left)),
                right: Box::new(self.fuse_consecutive_filters(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Antijoin {
                left: Box::new(self.fuse_consecutive_filters(*left)),
                right: Box::new(self.fuse_consecutive_filters(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.fuse_consecutive_filters(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.fuse_consecutive_filters(ir))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.fuse_consecutive_filters(*input)),
                group_by,
                aggregations,
                output_schema,
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.fuse_consecutive_filters(*input)),
                expressions,
            },

            other => other,
        }
    }

    /// Rule: Push filters down through joins
    ///
    /// Filter(Join(A, B), pred) -> Join(Filter(A, pred), B)
    ///   when pred only references columns from A
    ///
    /// This reduces the size of intermediate results by filtering early.
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn pushdown_filters(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Filter { input, predicate } => {
                let optimized_input = self.pushdown_filters(*input);

                match optimized_input {
                    IRNode::Join {
                        left,
                        right,
                        left_keys,
                        right_keys,
                        output_schema,
                    } => {
                        let left_schema = left.output_schema();
                        let left_cols = left_schema.len();

                        // Analyze which side(s) the predicate references
                        let pred_cols = Self::get_predicate_columns(&predicate);
                        let refs_left = pred_cols.iter().any(|&c| c < left_cols);
                        let refs_right = pred_cols.iter().any(|&c| c >= left_cols);

                        if refs_left && !refs_right {
                            // Predicate only references left side - push down to left
                            IRNode::Join {
                                left: Box::new(IRNode::Filter {
                                    input: left,
                                    predicate,
                                }),
                                right,
                                left_keys,
                                right_keys,
                                output_schema,
                            }
                        } else if refs_right && !refs_left {
                            // Predicate only references right side - push down to right
                            // Need to adjust column indices
                            let adjusted_predicate =
                                Self::adjust_predicate_columns(&predicate, -(left_cols as i32));
                            IRNode::Join {
                                left,
                                right: Box::new(IRNode::Filter {
                                    input: right,
                                    predicate: adjusted_predicate,
                                }),
                                left_keys,
                                right_keys,
                                output_schema,
                            }
                        } else {
                            // Predicate references both sides - cannot push down
                            IRNode::Filter {
                                input: Box::new(IRNode::Join {
                                    left,
                                    right,
                                    left_keys,
                                    right_keys,
                                    output_schema,
                                }),
                                predicate,
                            }
                        }
                    }
                    other => IRNode::Filter {
                        input: Box::new(other),
                        predicate,
                    },
                }
            }

