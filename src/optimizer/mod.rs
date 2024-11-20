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

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.pushdown_filters(*input)),
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
                left: Box::new(self.pushdown_filters(*left)),
                right: Box::new(self.pushdown_filters(*right)),
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
                left: Box::new(self.pushdown_filters(*left)),
                right: Box::new(self.pushdown_filters(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.pushdown_filters(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.pushdown_filters(ir))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.pushdown_filters(*input)),
                group_by,
                aggregations,
                output_schema,
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.pushdown_filters(*input)),
                expressions,
            },

            other => other,
        }
    }

    /// Extract column indices referenced by a predicate
    fn get_predicate_columns(predicate: &Predicate) -> Vec<usize> {
        match predicate {
            Predicate::ColumnEqConst(col, _)
            | Predicate::ColumnNeConst(col, _)
            | Predicate::ColumnLtConst(col, _)
            | Predicate::ColumnLeConst(col, _)
            | Predicate::ColumnGtConst(col, _)
            | Predicate::ColumnGeConst(col, _)
            | Predicate::ColumnEqStr(col, _)
            | Predicate::ColumnNeStr(col, _)
            | Predicate::ColumnLtStr(col, _)
            | Predicate::ColumnGtStr(col, _)
            | Predicate::ColumnLeStr(col, _)
            | Predicate::ColumnGeStr(col, _)
            | Predicate::ColumnEqFloat(col, _)
            | Predicate::ColumnNeFloat(col, _)
            | Predicate::ColumnGtFloat(col, _)
            | Predicate::ColumnLtFloat(col, _)
            | Predicate::ColumnGeFloat(col, _)
            | Predicate::ColumnLeFloat(col, _) => {
                vec![*col]
            }
            Predicate::ColumnsEq(col1, col2)
            | Predicate::ColumnsNe(col1, col2)
            | Predicate::ColumnsLt(col1, col2)
            | Predicate::ColumnsGt(col1, col2)
            | Predicate::ColumnsLe(col1, col2)
            | Predicate::ColumnsGe(col1, col2) => {
                vec![*col1, *col2]
            }
            Predicate::ColumnCompareArith(col, _, _, var_map) => {
                let mut cols = vec![*col];
                cols.extend(var_map.values().copied());
                cols
            }
            Predicate::ArithCompareConst(_, _, _, var_map) => var_map.values().copied().collect(),
            Predicate::And(left, right) | Predicate::Or(left, right) => {
                let mut cols = Self::get_predicate_columns(left);
                cols.extend(Self::get_predicate_columns(right));
                cols
            }
            Predicate::True | Predicate::False => vec![],
        }
    }

    /// Adjust column indices in a predicate by an offset
    fn adjust_predicate_columns(predicate: &Predicate, offset: i32) -> Predicate {
        let adjust = |col: usize| -> usize { ((col as i32) + offset) as usize };

        match predicate {
            Predicate::ColumnEqConst(col, val) => Predicate::ColumnEqConst(adjust(*col), *val),
            Predicate::ColumnNeConst(col, val) => Predicate::ColumnNeConst(adjust(*col), *val),
            Predicate::ColumnLtConst(col, val) => Predicate::ColumnLtConst(adjust(*col), *val),
            Predicate::ColumnLeConst(col, val) => Predicate::ColumnLeConst(adjust(*col), *val),
            Predicate::ColumnGtConst(col, val) => Predicate::ColumnGtConst(adjust(*col), *val),
            Predicate::ColumnGeConst(col, val) => Predicate::ColumnGeConst(adjust(*col), *val),
            // String predicates
            Predicate::ColumnEqStr(col, val) => Predicate::ColumnEqStr(adjust(*col), val.clone()),
            Predicate::ColumnNeStr(col, val) => Predicate::ColumnNeStr(adjust(*col), val.clone()),
            Predicate::ColumnLtStr(col, val) => Predicate::ColumnLtStr(adjust(*col), val.clone()),
            Predicate::ColumnGtStr(col, val) => Predicate::ColumnGtStr(adjust(*col), val.clone()),
            Predicate::ColumnLeStr(col, val) => Predicate::ColumnLeStr(adjust(*col), val.clone()),
            Predicate::ColumnGeStr(col, val) => Predicate::ColumnGeStr(adjust(*col), val.clone()),
            // Float predicates
            Predicate::ColumnEqFloat(col, val) => Predicate::ColumnEqFloat(adjust(*col), *val),
            Predicate::ColumnNeFloat(col, val) => Predicate::ColumnNeFloat(adjust(*col), *val),
            Predicate::ColumnGtFloat(col, val) => Predicate::ColumnGtFloat(adjust(*col), *val),
            Predicate::ColumnLtFloat(col, val) => Predicate::ColumnLtFloat(adjust(*col), *val),
            Predicate::ColumnGeFloat(col, val) => Predicate::ColumnGeFloat(adjust(*col), *val),
            Predicate::ColumnLeFloat(col, val) => Predicate::ColumnLeFloat(adjust(*col), *val),
            Predicate::ColumnsEq(col1, col2) => Predicate::ColumnsEq(adjust(*col1), adjust(*col2)),
            Predicate::ColumnsNe(col1, col2) => Predicate::ColumnsNe(adjust(*col1), adjust(*col2)),
            Predicate::ColumnsLt(col1, col2) => Predicate::ColumnsLt(adjust(*col1), adjust(*col2)),
            Predicate::ColumnsGt(col1, col2) => Predicate::ColumnsGt(adjust(*col1), adjust(*col2)),
            Predicate::ColumnsLe(col1, col2) => Predicate::ColumnsLe(adjust(*col1), adjust(*col2)),
            Predicate::ColumnsGe(col1, col2) => Predicate::ColumnsGe(adjust(*col1), adjust(*col2)),
            Predicate::ColumnCompareArith(col, op, expr, var_map) => {
                let new_var_map: std::collections::HashMap<String, usize> = var_map
                    .iter()
                    .map(|(name, idx)| (name.clone(), adjust(*idx)))
                    .collect();
                Predicate::ColumnCompareArith(adjust(*col), op.clone(), expr.clone(), new_var_map)
            }
            Predicate::ArithCompareConst(expr, op, val, var_map) => {
                let new_var_map: std::collections::HashMap<String, usize> = var_map
                    .iter()
                    .map(|(name, idx)| (name.clone(), adjust(*idx)))
                    .collect();
                Predicate::ArithCompareConst(expr.clone(), op.clone(), *val, new_var_map)
            }
            Predicate::And(left, right) => Predicate::And(
                Box::new(Self::adjust_predicate_columns(left, offset)),
                Box::new(Self::adjust_predicate_columns(right, offset)),
            ),
            Predicate::Or(left, right) => Predicate::Or(
                Box::new(Self::adjust_predicate_columns(left, offset)),
                Box::new(Self::adjust_predicate_columns(right, offset)),
            ),
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
        }
    }

    /// Rule: Eliminate empty unions from the tree
    ///
    /// Union([]) appearing anywhere should be propagated up
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn eliminate_empty_unions(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Union { inputs } => {
                // Filter out empty unions from inputs
                let non_empty: Vec<IRNode> = inputs
                    .into_iter()
                    .map(|i| self.eliminate_empty_unions(i))
                    .filter(|i| !matches!(i, IRNode::Union { inputs } if inputs.is_empty()))
                    .collect();

                if non_empty.is_empty() {
                    IRNode::Union { inputs: vec![] }
                } else if non_empty.len() == 1 {
                    non_empty.into_iter().next().unwrap()
                } else {
                    IRNode::Union { inputs: non_empty }
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                let input = self.eliminate_empty_unions(*input);
                if matches!(&input, IRNode::Union { inputs } if inputs.is_empty()) {
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Map {
                        input: Box::new(input),
                        projection,
                        output_schema,
                    }
                }
            }

            IRNode::Filter { input, predicate } => {
                let input = self.eliminate_empty_unions(*input);
                if matches!(&input, IRNode::Union { inputs } if inputs.is_empty()) {
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Filter {
                        input: Box::new(input),
                        predicate,
                    }
                }
            }

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                let left = self.eliminate_empty_unions(*left);
                let right = self.eliminate_empty_unions(*right);

                // If either side is empty, the join is empty
                if matches!(&left, IRNode::Union { inputs } if inputs.is_empty())
                    || matches!(&right, IRNode::Union { inputs } if inputs.is_empty())
                {
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Join {
                        left: Box::new(left),
                        right: Box::new(right),
                        left_keys,
                        right_keys,
                        output_schema,
                    }
                }
            }

            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                let left = self.eliminate_empty_unions(*left);
                let right = self.eliminate_empty_unions(*right);

                // If left is empty, antijoin is empty
                if matches!(&left, IRNode::Union { inputs } if inputs.is_empty()) {
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Antijoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        left_keys,
                        right_keys,
                        output_schema,
                    }
                }
            }

            IRNode::Distinct { input } => {
                let input = self.eliminate_empty_unions(*input);
                if matches!(&input, IRNode::Union { inputs } if inputs.is_empty()) {
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Distinct {
                        input: Box::new(input),
                    }
                }
            }

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => {
                let input = self.eliminate_empty_unions(*input);
                IRNode::Aggregate {
                    input: Box::new(input),
                    group_by,
                    aggregations,
                    output_schema,
                }
            }

            IRNode::Compute { input, expressions } => {
                let input = self.eliminate_empty_unions(*input);
                if matches!(&input, IRNode::Union { inputs } if inputs.is_empty()) {
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Compute {
                        input: Box::new(input),
                        expressions,
                    }
                }
            }

            other => other,
        }
    }

    /// Rule: Remove identity Map nodes
    ///
    /// Map(input, [0, 1, ..., n]) where projection is identity -> input
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn eliminate_identity_maps(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                let input = Box::new(self.eliminate_identity_maps(*input));
                let input_schema = input.output_schema();

                // Check if projection is identity
                let is_identity = projection.iter().enumerate().all(|(i, &p)| i == p)
                    && projection.len() == input_schema.len();

                if is_identity {
                    *input
                } else {
                    IRNode::Map {
                        input,
                        projection,
                        output_schema,
                    }
                }
            }

            IRNode::Filter { input, predicate } => IRNode::Filter {
                input: Box::new(self.eliminate_identity_maps(*input)),
                predicate,
            },

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Join {
                left: Box::new(self.eliminate_identity_maps(*left)),
                right: Box::new(self.eliminate_identity_maps(*right)),
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
                left: Box::new(self.eliminate_identity_maps(*left)),
                right: Box::new(self.eliminate_identity_maps(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.eliminate_identity_maps(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.eliminate_identity_maps(ir))
                    .collect(),
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.eliminate_identity_maps(*input)),
                expressions,
            },

            other => other,
        }
    }

    /// Rule: Remove always-true filters
    ///
    /// Filter(input, True) -> input
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn eliminate_always_true_filters(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Filter { input, predicate } => {
                let input = Box::new(self.eliminate_always_true_filters(*input));

                if predicate.is_always_true() {
                    *input
                } else {
                    IRNode::Filter { input, predicate }
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.eliminate_always_true_filters(*input)),
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
                left: Box::new(self.eliminate_always_true_filters(*left)),
                right: Box::new(self.eliminate_always_true_filters(*right)),
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
                left: Box::new(self.eliminate_always_true_filters(*left)),
                right: Box::new(self.eliminate_always_true_filters(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.eliminate_always_true_filters(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.eliminate_always_true_filters(ir))
                    .collect(),
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.eliminate_always_true_filters(*input)),
                expressions,
            },

            other => other,
        }
    }

    /// Rule: Remove always-false filters
    ///
    /// Filter(input, False) -> Empty
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn eliminate_always_false_filters(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Filter { input, predicate } => {
                if predicate.is_always_false() {
                    // Return an empty union (represents empty set)
                    IRNode::Union { inputs: vec![] }
                } else {
                    IRNode::Filter {
                        input: Box::new(self.eliminate_always_false_filters(*input)),
                        predicate,
                    }
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.eliminate_always_false_filters(*input)),
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
                left: Box::new(self.eliminate_always_false_filters(*left)),
                right: Box::new(self.eliminate_always_false_filters(*right)),
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
                left: Box::new(self.eliminate_always_false_filters(*left)),
                right: Box::new(self.eliminate_always_false_filters(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.eliminate_always_false_filters(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.eliminate_always_false_filters(ir))
                    .collect(),
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.eliminate_always_false_filters(*input)),
                expressions,
            },

            other => other,
        }
    }

    /// Logic Fusion: Fuse Map+Filter into FlatMap
    ///
    /// Patterns recognized:
    /// - `Filter(Map(input, proj), pred)` -> `FlatMap(input, proj, Some(pred))`
    /// - `Map(input, proj)` with no filter -> `FlatMap(input, proj, None)` (not fused; only Filter+Map is)
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn fuse_to_flatmap(&self, ir: IRNode) -> IRNode {
        match ir {
            IRNode::Filter {
                input, predicate, ..
            } => {
                let optimized_input = self.fuse_to_flatmap(*input);
                match optimized_input {
                    IRNode::Map {
                        input: inner_input,
                        projection,
                        output_schema,
                    } => {
                        // Filter(Map(input, proj), pred) -> FlatMap(input, proj, Some(pred))
                        IRNode::FlatMap {
                            input: inner_input,
                            projection,
                            filter_predicate: Some(predicate),
                            output_schema,
                        }
                    }
                    other => IRNode::Filter {
                        input: Box::new(other),
                        predicate,
                    },
                }
            }

            IRNode::Map {
                input,
                projection,
                output_schema,
            } => IRNode::Map {
                input: Box::new(self.fuse_to_flatmap(*input)),
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
                left: Box::new(self.fuse_to_flatmap(*left)),
                right: Box::new(self.fuse_to_flatmap(*right)),
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
                left: Box::new(self.fuse_to_flatmap(*left)),
                right: Box::new(self.fuse_to_flatmap(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.fuse_to_flatmap(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.fuse_to_flatmap(ir))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.fuse_to_flatmap(*input)),
                group_by,
                aggregations,
                output_schema,
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.fuse_to_flatmap(*input)),
                expressions,
            },

            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::FlatMap {
                input: Box::new(self.fuse_to_flatmap(*input)),
                projection,
                filter_predicate,
                output_schema,
            },

            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::JoinFlatMap {
                left: Box::new(self.fuse_to_flatmap(*left)),
                right: Box::new(self.fuse_to_flatmap(*right)),
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            },

            other => other,
        }
    }

    /// Logic Fusion: Fuse Join+Map into JoinFlatMap
    ///
    /// Patterns recognized:
    /// - `Map(Join(L, R, lk, rk), proj)` -> `JoinFlatMap(L, R, lk, rk, proj, None)`
    /// - `Filter(Map(Join(L, R, lk, rk), proj), pred)` -> `JoinFlatMap(L, R, lk, rk, proj, Some(pred))`
    ///   (handled by fuse_to_flatmap first converting to FlatMap, then this converts FlatMap-over-Join)
    /// Remap projection indices from Join output space to JoinFlatMap concat space.
    ///
    /// Join output: `left_cols + right_non_key_cols` (right_keys columns excluded)
    /// JoinFlatMap concat: `left_cols + ALL_right_cols` (everything included)
    ///
    /// For indices in the left portion (< left_width), no change needed.
    /// For indices in the right portion (>= left_width), we need to map back to
    /// the original right column index, accounting for the excluded key columns.
    fn remap_projection_for_join_flatmap(
        projection: &[usize],
        left_width: usize,
        right_keys: &[usize],
    ) -> Vec<usize> {
        // Build a mapping from "right non-key position" to "right absolute position"
        // right_keys are the indices that were excluded from the Join output
        let mut sorted_keys = right_keys.to_vec();
        sorted_keys.sort_unstable();

        projection
            .iter()
            .map(|&idx| {
                if idx < left_width {
                    // Left-side index: unchanged
                    idx
                } else {
                    // Right-side index in Join output: idx - left_width is the
                    // position in right_non_key_cols. We need to find the actual
                    // right column index by reinserting the excluded key positions.
                    let right_non_key_pos = idx - left_width;
                    let mut actual_right_idx = right_non_key_pos;
                    for &key_idx in &sorted_keys {
                        if key_idx <= actual_right_idx {
                            actual_right_idx += 1;
                        }
                    }
                    left_width + actual_right_idx
                }
            })
            .collect()
    }

    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
    fn fuse_to_join_flatmap(&self, ir: IRNode) -> IRNode {
        match ir {
            // Map(Join(...), proj) -> JoinFlatMap
            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                let optimized_input = self.fuse_to_join_flatmap(*input);
                match optimized_input {
                    IRNode::Join {
                        left,
                        right,
                        left_keys,
                        right_keys,
                        ..
                    } => {
                        // Remap projection indices: Join output schema excludes right_keys
                        // columns, but JoinFlatMap concat includes ALL columns from both sides.
                        let left_width = left.output_schema().len();
                        let remapped = Self::remap_projection_for_join_flatmap(
                            &projection,
                            left_width,
                            &right_keys,
                        );
                        IRNode::JoinFlatMap {
                            left,
                            right,
                            left_keys,
                            right_keys,
                            projection: remapped,
                            filter_predicate: None,
                            output_schema,
                        }
                    }
                    other => IRNode::Map {
                        input: Box::new(other),
                        projection,
                        output_schema,
                    },
                }
            }

            // FlatMap(Join(...), proj, pred) -> JoinFlatMap
            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => {
                let optimized_input = self.fuse_to_join_flatmap(*input);
                match optimized_input {
                    IRNode::Join {
                        left,
                        right,
                        left_keys,
                        right_keys,
                        ..
                    } => {
                        let left_width = left.output_schema().len();
                        let remapped = Self::remap_projection_for_join_flatmap(
                            &projection,
                            left_width,
                            &right_keys,
                        );
                        IRNode::JoinFlatMap {
                            left,
                            right,
                            left_keys,
                            right_keys,
                            projection: remapped,
                            filter_predicate,
                            output_schema,
                        }
                    }
                    other => IRNode::FlatMap {
                        input: Box::new(other),
                        projection,
                        filter_predicate,
                        output_schema,
                    },
                }
            }

            IRNode::Filter { input, predicate } => IRNode::Filter {
                input: Box::new(self.fuse_to_join_flatmap(*input)),
                predicate,
            },

            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => IRNode::Join {
                left: Box::new(self.fuse_to_join_flatmap(*left)),
                right: Box::new(self.fuse_to_join_flatmap(*right)),
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
                left: Box::new(self.fuse_to_join_flatmap(*left)),
                right: Box::new(self.fuse_to_join_flatmap(*right)),
                left_keys,
                right_keys,
                output_schema,
            },

            IRNode::Distinct { input } => IRNode::Distinct {
                input: Box::new(self.fuse_to_join_flatmap(*input)),
            },

            IRNode::Union { inputs } => IRNode::Union {
                inputs: inputs
                    .into_iter()
                    .map(|ir| self.fuse_to_join_flatmap(ir))
                    .collect(),
            },

            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => IRNode::Aggregate {
                input: Box::new(self.fuse_to_join_flatmap(*input)),
                group_by,
                aggregations,
                output_schema,
            },

            IRNode::Compute { input, expressions } => IRNode::Compute {
                input: Box::new(self.fuse_to_join_flatmap(*input)),
                expressions,
            },

            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } => IRNode::JoinFlatMap {
                left: Box::new(self.fuse_to_join_flatmap(*left)),
                right: Box::new(self.fuse_to_join_flatmap(*right)),
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            },

            other => other,
        }
    }

    /// Check if two IR trees are structurally equal
    ///
    /// Used for fixpoint detection
    fn ir_equals(a: &IRNode, b: &IRNode) -> bool {
        match (a, b) {
            (
                IRNode::Scan {
                    relation: r1,
                    schema: s1,
                },
                IRNode::Scan {
                    relation: r2,
                    schema: s2,
                },
            ) => r1 == r2 && s1 == s2,

            (
                IRNode::Map {
                    input: i1,
                    projection: p1,
                    output_schema: s1,
                },
                IRNode::Map {
                    input: i2,
                    projection: p2,
                    output_schema: s2,
                },
            ) => p1 == p2 && s1 == s2 && Self::ir_equals(i1, i2),

            (
                IRNode::Filter {
                    input: i1,
                    predicate: p1,
                },
                IRNode::Filter {
                    input: i2,
                    predicate: p2,
                },
            ) => Self::predicate_equals(p1, p2) && Self::ir_equals(i1, i2),

            (
                IRNode::Join {
                    left: l1,
                    right: r1,
                    left_keys: lk1,
                    right_keys: rk1,
                    output_schema: s1,
                },
                IRNode::Join {
                    left: l2,
                    right: r2,
                    left_keys: lk2,
                    right_keys: rk2,
                    output_schema: s2,
                },
            ) => {
                lk1 == lk2
                    && rk1 == rk2
                    && s1 == s2
                    && Self::ir_equals(l1, l2)
                    && Self::ir_equals(r1, r2)
            }

            (IRNode::Distinct { input: i1 }, IRNode::Distinct { input: i2 }) => {
                Self::ir_equals(i1, i2)
            }

            (IRNode::Union { inputs: in1 }, IRNode::Union { inputs: in2 }) => {
                in1.len() == in2.len()
                    && in1
                        .iter()
                        .zip(in2.iter())
                        .all(|(a, b)| Self::ir_equals(a, b))
            }

            (
                IRNode::Antijoin {
                    left: l1,
                    right: r1,
                    left_keys: lk1,
                    right_keys: rk1,
                    output_schema: s1,
                },
                IRNode::Antijoin {
                    left: l2,
                    right: r2,
                    left_keys: lk2,
                    right_keys: rk2,
                    output_schema: s2,
                },
            ) => {
                lk1 == lk2
                    && rk1 == rk2
                    && s1 == s2
                    && Self::ir_equals(l1, l2)
                    && Self::ir_equals(r1, r2)
            }

            (
                IRNode::FlatMap {
                    input: i1,
                    projection: p1,
                    filter_predicate: f1,
                    output_schema: s1,
                },
                IRNode::FlatMap {
                    input: i2,
                    projection: p2,
                    filter_predicate: f2,
                    output_schema: s2,
                },
            ) => {
                p1 == p2
                    && s1 == s2
                    && format!("{f1:?}") == format!("{f2:?}")
                    && Self::ir_equals(i1, i2)
            }

            (
                IRNode::JoinFlatMap {
                    left: l1,
                    right: r1,
                    left_keys: lk1,
                    right_keys: rk1,
                    projection: p1,
                    filter_predicate: f1,
                    output_schema: s1,
                },
                IRNode::JoinFlatMap {
                    left: l2,
                    right: r2,
                    left_keys: lk2,
                    right_keys: rk2,
                    projection: p2,
                    filter_predicate: f2,
                    output_schema: s2,
                },
            ) => {
                lk1 == lk2
                    && rk1 == rk2
                    && p1 == p2
                    && s1 == s2
                    && format!("{f1:?}") == format!("{f2:?}")
                    && Self::ir_equals(l1, l2)
                    && Self::ir_equals(r1, r2)
            }

            _ => false,
        }
    }

    /// Check if two predicates are equal
    fn predicate_equals(a: &Predicate, b: &Predicate) -> bool {
        // Simple structural equality
        // For more complex predicates, would need deeper comparison
        format!("{a:?}") == format!("{b:?}")
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eliminate_identity_map() {
        let optimizer = Optimizer::new();

        // Map with identity projection
        let ir = IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![0, 1], // Identity
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let optimized = optimizer.eliminate_identity_maps(ir);

        // Should be reduced to just the scan
        assert!(optimized.is_scan());
    }

    #[test]
    fn test_eliminate_always_true_filter() {
        let optimizer = Optimizer::new();

        // Filter with always-true predicate
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::True,
        };

        let optimized = optimizer.eliminate_always_true_filters(ir);

        // Should be reduced to just the scan
        assert!(optimized.is_scan());
    }

    #[test]
    fn test_eliminate_always_false_filter() {
        let optimizer = Optimizer::new();

        // Filter with always-false predicate
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: Predicate::False,
        };

        let optimized = optimizer.eliminate_always_false_filters(ir);

        // Should be reduced to empty union
        match optimized {
            IRNode::Union { inputs } => assert_eq!(inputs.len(), 0),
            _ => panic!("Expected empty union"),
        }
    }

    #[test]
    fn test_fixpoint_optimization() {
        let optimizer = Optimizer::new();

        // Nested identity maps
        let ir = IRNode::Map {
            input: Box::new(IRNode::Map {
                input: Box::new(IRNode::Scan {
                    relation: "edge".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                projection: vec![0, 1],
                output_schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![0, 1],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };

        let optimized = optimizer.optimize(ir);

        // Both identity maps should be eliminated
        assert!(optimized.is_scan());
    }

    #[test]
    fn test_fuse_consecutive_maps() {
        let optimizer = Optimizer::new();

        // Map(Map(Scan, [1, 0]), [0]) -> Map(Scan, [1])
        let ir = IRNode::Map {
            input: Box::new(IRNode::Map {
                input: Box::new(IRNode::Scan {
                    relation: "edge".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                projection: vec![1, 0], // swap columns
                output_schema: vec!["y".to_string(), "x".to_string()],
            }),
            projection: vec![0], // project first column (which was y)
            output_schema: vec!["y".to_string()],
        };

        let optimized = optimizer.fuse_consecutive_maps(ir);

        // Should fuse into single Map with projection [1]
        match optimized {
            IRNode::Map { projection, .. } => {
                assert_eq!(projection, vec![1]);
            }
            _ => panic!("Expected fused Map"),
        }
    }

