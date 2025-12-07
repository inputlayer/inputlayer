//! # IR Optimizer (Module 06)
//!
//! **Course Context**: Students implement this module in **Module 06: Basic IR Optimizations**.
//!
//! This module teaches:
//! - Identifying optimization opportunities in IR trees
//! - Implementing algebraic optimization rules
//! - Pattern matching on IR structures
//! - Recursive tree transformations
//! - Fixpoint iteration for multi-pass optimization
//!
//! ## Learning Objectives (Module 06)
//!
//! Students learn to:
//! 1. **Map Fusion**: Combine consecutive Map nodes
//!    - `Map(Map(input, p1), p2)` → `Map(input, p1 ∘ p2)`
//! 2. **Filter Fusion**: Combine consecutive Filter nodes
//!    - `Filter(Filter(input, p1), p2)` → `Filter(input, p1 ∧ p2)`
//! 3. **Filter Pushdown**: Move filters closer to scans
//!    - `Filter(Join(A, B), pred)` → `Join(Filter(A, pred), B)`
//! 4. **Identity Elimination**: Remove no-op operations
//!    - `Map(input, identity)` → `input`
//!    - `Filter(input, True)` → `input`
//! 5. **Fixpoint Iteration**: Apply rules until no changes
//!
//! ## Key Concepts
//!
//! - **Algebraic Optimizations**: Transformations that preserve semantics
//! - **Pattern Matching**: Identifying optimization patterns
//! - **Fixpoint**: Iterating until IR stops changing
//! - **Cost Model** (basic): Prefer fewer operators
//!
//! ## Pipeline Position
//!
//! ```text
//! IRNode (from IR Builder) → [Optimizer] → Optimized IRNode → Code Gen
//! ```
//!
//! ---
//!
//! # Implementation
//!
//! Full implementation of basic IR optimizations with fixpoint iteration.

use datalog_ir::{IRNode, Predicate};

/// IR Optimizer with fixpoint iteration
pub struct Optimizer {
    /// Maximum number of optimization passes
    max_iterations: usize,
}

impl Optimizer {
    /// Create a new optimizer with default max iterations
    pub fn new() -> Self {
        Optimizer {
            max_iterations: 10,
        }
    }

    /// Create optimizer with custom max iterations
    pub fn with_max_iterations(max_iterations: usize) -> Self {
        Optimizer { max_iterations }
    }

    /// Optimize an IR tree to fixpoint
    ///
    /// Applies optimization rules repeatedly until the IR stops changing
    /// or max_iterations is reached.
    pub fn optimize(&self, ir: IRNode) -> IRNode {
        let mut current = ir;

        for iteration in 0..self.max_iterations {
            let optimized = self.apply_all_rules(current.clone());

            // Check if we reached fixpoint
            if Self::ir_equals(&optimized, &current) {
                #[cfg(test)]
                println!("Optimizer reached fixpoint at iteration {}", iteration);
                break;
            }

            current = optimized;
        }

        current
    }

    /// Apply all optimization rules once
    fn apply_all_rules(&self, ir: IRNode) -> IRNode {
        // Phase 1: Identity elimination
        let ir = self.eliminate_identity_maps(ir);
        let ir = self.eliminate_always_true_filters(ir);
        let ir = self.eliminate_always_false_filters(ir);

        // Phase 2: Fusion optimizations
        let ir = self.fuse_consecutive_maps(ir);
        let ir = self.fuse_consecutive_filters(ir);

        // Phase 3: Pushdown optimizations
        let ir = self.pushdown_filters(ir);

        // Phase 4: Dead code elimination
        let ir = self.eliminate_empty_unions(ir);

        ir
    }

    /// Rule: Fuse consecutive Map nodes
    ///
    /// Map(Map(input, p1), p2) → Map(input, p1 ∘ p2)
    /// Composition: new_projection[i] = p1[p2[i]]
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
    /// Filter(Filter(input, p1), p2) → Filter(input, And(p1, p2))
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
    /// Filter(Join(A, B), pred) → Join(Filter(A, pred), B)
    ///   when pred only references columns from A
    ///
    /// This reduces the size of intermediate results by filtering early.
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
                        let right_schema = right.output_schema();
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
            | Predicate::ColumnEqFloat(col, _)
            | Predicate::ColumnNeFloat(col, _)
            | Predicate::ColumnGtFloat(col, _)
            | Predicate::ColumnLtFloat(col, _)
            | Predicate::ColumnGeFloat(col, _)
            | Predicate::ColumnLeFloat(col, _) => {
                vec![*col]
            }
            Predicate::ColumnsEq(col1, col2) | Predicate::ColumnsNe(col1, col2) => {
                vec![*col1, *col2]
            }
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
        let adjust = |col: usize| -> usize {
            ((col as i32) + offset) as usize
        };

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
            // Float predicates
            Predicate::ColumnEqFloat(col, val) => Predicate::ColumnEqFloat(adjust(*col), *val),
            Predicate::ColumnNeFloat(col, val) => Predicate::ColumnNeFloat(adjust(*col), *val),
            Predicate::ColumnGtFloat(col, val) => Predicate::ColumnGtFloat(adjust(*col), *val),
            Predicate::ColumnLtFloat(col, val) => Predicate::ColumnLtFloat(adjust(*col), *val),
            Predicate::ColumnGeFloat(col, val) => Predicate::ColumnGeFloat(adjust(*col), *val),
            Predicate::ColumnLeFloat(col, val) => Predicate::ColumnLeFloat(adjust(*col), *val),
            Predicate::ColumnsEq(col1, col2) => Predicate::ColumnsEq(adjust(*col1), adjust(*col2)),
            Predicate::ColumnsNe(col1, col2) => Predicate::ColumnsNe(adjust(*col1), adjust(*col2)),
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
    /// Map(input, [0, 1, ..., n]) where projection is identity → input
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
                let is_identity = projection
                    .iter()
                    .enumerate()
                    .all(|(i, &p)| i == p)
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
    /// Filter(input, True) → input
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
    /// Filter(input, False) → Empty
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

            _ => false,
        }
    }

    /// Check if two predicates are equal
    fn predicate_equals(a: &Predicate, b: &Predicate) -> bool {
        // Simple structural equality
        // For more complex predicates, would need deeper comparison
        format!("{:?}", a) == format!("{:?}", b)
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

        // Map(Map(Scan, [1, 0]), [0]) → Map(Scan, [1])
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

    #[test]
    fn test_fuse_consecutive_filters() {
        let optimizer = Optimizer::new();

        // Filter(Filter(Scan, p1), p2) → Filter(Scan, And(p1, p2))
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Filter {
                input: Box::new(IRNode::Scan {
                    relation: "edge".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                predicate: Predicate::ColumnGtConst(0, 5),
            }),
            predicate: Predicate::ColumnLtConst(0, 10),
        };

        let optimized = optimizer.fuse_consecutive_filters(ir);

        // Should fuse into single Filter with And predicate
        match optimized {
            IRNode::Filter { predicate, .. } => {
                assert!(matches!(predicate, Predicate::And(_, _)));
            }
            _ => panic!("Expected fused Filter"),
        }
    }

    #[test]
    fn test_pushdown_filter_to_left() {
        let optimizer = Optimizer::new();

        // Filter(Join(A, B), pred_on_A) → Join(Filter(A, pred), B)
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Join {
                left: Box::new(IRNode::Scan {
                    relation: "r".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                right: Box::new(IRNode::Scan {
                    relation: "s".to_string(),
                    schema: vec!["y".to_string(), "z".to_string()],
                }),
                left_keys: vec![1],
                right_keys: vec![0],
                output_schema: vec![
                    "x".to_string(),
                    "y".to_string(),
                    "y".to_string(),
                    "z".to_string(),
                ],
            }),
            predicate: Predicate::ColumnGtConst(0, 5), // x > 5, only references left side
        };

        let optimized = optimizer.pushdown_filters(ir);

        // Should push filter down to left side of join
        match optimized {
            IRNode::Join { left, .. } => {
                assert!(matches!(*left, IRNode::Filter { .. }));
            }
            _ => panic!("Expected Join with Filter on left"),
        }
    }

    #[test]
    fn test_pushdown_filter_to_right() {
        let optimizer = Optimizer::new();

        // Filter(Join(A, B), pred_on_B) → Join(A, Filter(B, adjusted_pred))
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Join {
                left: Box::new(IRNode::Scan {
                    relation: "r".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                right: Box::new(IRNode::Scan {
                    relation: "s".to_string(),
                    schema: vec!["y".to_string(), "z".to_string()],
                }),
                left_keys: vec![1],
                right_keys: vec![0],
                output_schema: vec![
                    "x".to_string(),
                    "y".to_string(),
                    "y".to_string(),
                    "z".to_string(),
                ],
            }),
            predicate: Predicate::ColumnLtConst(3, 100), // z < 100, only references right side (col 3)
        };

        let optimized = optimizer.pushdown_filters(ir);

        // Should push filter down to right side of join
        match optimized {
            IRNode::Join { right, .. } => {
                assert!(matches!(*right, IRNode::Filter { .. }));
            }
            _ => panic!("Expected Join with Filter on right"),
        }
    }

    #[test]
    fn test_no_pushdown_for_cross_reference() {
        let optimizer = Optimizer::new();

        // Filter referencing both sides cannot be pushed down
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Join {
                left: Box::new(IRNode::Scan {
                    relation: "r".to_string(),
                    schema: vec!["x".to_string(), "y".to_string()],
                }),
                right: Box::new(IRNode::Scan {
                    relation: "s".to_string(),
                    schema: vec!["y".to_string(), "z".to_string()],
                }),
                left_keys: vec![1],
                right_keys: vec![0],
                output_schema: vec![
                    "x".to_string(),
                    "y".to_string(),
                    "y".to_string(),
                    "z".to_string(),
                ],
            }),
            predicate: Predicate::ColumnsEq(0, 3), // x = z, references both sides
        };

        let optimized = optimizer.pushdown_filters(ir);

        // Filter should stay on top
        assert!(matches!(optimized, IRNode::Filter { .. }));
    }

    #[test]
    fn test_eliminate_empty_unions() {
        let optimizer = Optimizer::new();

        // Map over empty union should become empty union
        let ir = IRNode::Map {
            input: Box::new(IRNode::Union { inputs: vec![] }),
            projection: vec![0],
            output_schema: vec!["x".to_string()],
        };

        let optimized = optimizer.eliminate_empty_unions(ir);

        match optimized {
            IRNode::Union { inputs } => assert!(inputs.is_empty()),
            _ => panic!("Expected empty union"),
        }
    }

    #[test]
    fn test_singleton_union_elimination() {
        let optimizer = Optimizer::new();

        // Union with single input should be unwrapped
        let ir = IRNode::Union {
            inputs: vec![IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }],
        };

        let optimized = optimizer.eliminate_empty_unions(ir);

        assert!(optimized.is_scan());
    }

    #[test]
    fn test_full_optimization_pipeline() {
        let optimizer = Optimizer::new();

        // Complex IR that exercises multiple optimizations
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Filter {
                input: Box::new(IRNode::Map {
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
                }),
                predicate: Predicate::True,
            }),
            predicate: Predicate::ColumnGtConst(0, 5),
        };

        let optimized = optimizer.optimize(ir);

        // Should simplify to Filter(Scan, x > 5)
        match optimized {
            IRNode::Filter { input, predicate } => {
                assert!(input.is_scan());
                assert!(matches!(predicate, Predicate::ColumnGtConst(0, 5)));
            }
            _ => panic!("Expected simplified Filter(Scan)"),
        }
    }
}
