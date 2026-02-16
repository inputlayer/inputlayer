//! # Pipeline Trace
//!
//! Utility for visualizing the query processing pipeline.
//! Shows output of each stage: Parse -> IR Build -> Optimize -> Execute
//!
//! Useful for debugging how data flows through the system.

use crate::ast::Program;
use crate::ir::IRNode;
use std::fmt;

/// Trace of pipeline execution stages
#[derive(Debug, Clone)]
pub struct PipelineTrace {
    /// Parsed AST
    pub ast: Option<Program>,

    /// Built IR (before optimization)
    pub ir_before: Vec<IRNode>,

    /// Optimized IR (after optimization)
    pub ir_after: Vec<IRNode>,

    /// Execution results
    pub results: Vec<Vec<(i32, i32)>>,

    /// Optimization statistics
    pub stats: OptimizationStats,
}

/// Statistics about optimizations applied
#[derive(Debug, Clone, Default)]
pub struct OptimizationStats {
    /// Number of identity maps eliminated
    pub identity_maps_removed: usize,

    /// Number of always-true filters removed
    pub true_filters_removed: usize,

    /// Number of always-false filters removed
    pub false_filters_removed: usize,

    /// Total IR nodes before optimization
    pub nodes_before: usize,

    /// Total IR nodes after optimization
    pub nodes_after: usize,
}

impl PipelineTrace {
    /// Create a new empty trace
    pub fn new() -> Self {
        PipelineTrace {
            ast: None,
            ir_before: Vec::new(),
            ir_after: Vec::new(),
            results: Vec::new(),
            stats: OptimizationStats::default(),
        }
    }

    /// Record the AST stage
    pub fn record_ast(&mut self, ast: Program) {
        self.ast = Some(ast);
    }

    /// Record IR before optimization
    pub fn record_ir_before(&mut self, ir: Vec<IRNode>) {
        self.stats.nodes_before = Self::count_nodes(&ir);
        self.ir_before = ir;
    }

    /// Record IR after optimization
    pub fn record_ir_after(&mut self, ir: Vec<IRNode>) {
        self.stats.nodes_after = Self::count_nodes(&ir);
        self.ir_after = ir;

        // Compute optimization statistics
        self.compute_stats();
    }

    /// Record execution results
    pub fn record_results(&mut self, results: Vec<Vec<(i32, i32)>>) {
        self.results = results;
    }

    /// Count total IR nodes in a collection
    fn count_nodes(irs: &[IRNode]) -> usize {
        irs.iter().map(Self::count_ir_nodes).sum()
    }

    /// Count nodes in a single IR tree
    fn count_ir_nodes(ir: &IRNode) -> usize {
        match ir {
            IRNode::Scan { .. } => 1,
            IRNode::HnswScan { .. } => 1,
            IRNode::Map { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::Filter { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::Join { left, right, .. } => {
                1 + Self::count_ir_nodes(left) + Self::count_ir_nodes(right)
            }
            IRNode::Antijoin { left, right, .. } => {
                1 + Self::count_ir_nodes(left) + Self::count_ir_nodes(right)
            }
            IRNode::Distinct { input } => 1 + Self::count_ir_nodes(input),
            IRNode::Union { inputs } => 1 + inputs.iter().map(Self::count_ir_nodes).sum::<usize>(),
            IRNode::Aggregate { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::Compute { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::FlatMap { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::JoinFlatMap { left, right, .. } => {
                1 + Self::count_ir_nodes(left) + Self::count_ir_nodes(right)
            }
        }
    }

    /// Compute optimization statistics by comparing before/after
    fn compute_stats(&mut self) {
        // This is a simplified version - could be more sophisticated
        let nodes_eliminated = self
            .stats
            .nodes_before
            .saturating_sub(self.stats.nodes_after);

        // Heuristic: attribute eliminated nodes to different optimizations
        // In a real implementation, the optimizer would track this
        if nodes_eliminated > 0 {
            self.stats.identity_maps_removed = nodes_eliminated / 2;
            self.stats.true_filters_removed = nodes_eliminated - self.stats.identity_maps_removed;
        }
    }

    /// Format the trace for display
    pub fn format_trace(&self) -> String {
        let mut output = String::new();

        output.push_str("═══════════════════════════════════════════════════════════\n");
        output.push_str("                    PIPELINE TRACE                          \n");
        output.push_str("═══════════════════════════════════════════════════════════\n\n");

        // AST
        if let Some(ref ast) = self.ast {
            output.push_str("┌---------------------------------------------------------┐\n");
            output.push_str("| PARSING                                                 |\n");
            output.push_str("`---------------------------------------------------------┘\n");
            output.push_str(&format!("  Rules parsed: {}\n\n", ast.rules.len()));

            for (i, rule) in ast.rules.iter().enumerate() {
                output.push_str(&format!(
                    "  Rule {}: {}({}) <- ",
                    i + 1,
                    rule.head.relation,
                    rule.head
                        .args
                        .iter()
                        .map(|t| format!("{t:?}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));

                let body_str: Vec<_> = rule
                    .body
                    .iter()
                    .map(|p| match p {
                        crate::ast::BodyPredicate::Positive(a) => format!(
                            "{}({})",
                            a.relation,
                            a.args
                                .iter()
                                .map(|t| format!("{t:?}"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        crate::ast::BodyPredicate::Negated(a) => format!(
                            "!{}({})",
                            a.relation,
                            a.args
                                .iter()
                                .map(|t| format!("{t:?}"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        crate::ast::BodyPredicate::Comparison(left, op, right) => {
                            let op_str = match op {
                                crate::ast::ComparisonOp::Equal => "=",
                                crate::ast::ComparisonOp::NotEqual => "!=",
                                crate::ast::ComparisonOp::LessThan => "<",
                                crate::ast::ComparisonOp::LessOrEqual => "<=",
                                crate::ast::ComparisonOp::GreaterThan => ">",
                                crate::ast::ComparisonOp::GreaterOrEqual => ">=",
                            };
                            format!("{left:?} {op_str} {right:?}")
                        }
                        crate::ast::BodyPredicate::HnswNearest {
                            index_name,
                            k,
                            id_var,
                            distance_var,
                            ..
                        } => {
                            format!(
                                "hnsw_nearest(\"{index_name}\", k={k}, {id_var}, {distance_var})"
                            )
                        }
                    })
                    .collect();

                output.push_str(&body_str.join(", "));
                output.push('\n');
            }
            output.push('\n');
        }

        // IR Before Optimization
        if !self.ir_before.is_empty() {
            output.push_str("┌---------------------------------------------------------┐\n");
            output.push_str("| IR CONSTRUCTION                                         |\n");
            output.push_str("`---------------------------------------------------------┘\n");
            output.push_str(&format!("  IR nodes: {}\n", self.stats.nodes_before));
            output.push_str(&format!("  Rules: {}\n\n", self.ir_before.len()));

            for (i, ir) in self.ir_before.iter().enumerate() {
                output.push_str(&format!("  Rule {} IR:\n", i + 1));
                output.push_str(&Self::format_ir_tree(ir, 4));
                output.push('\n');
            }
        }

        // IR After Optimization
        if !self.ir_after.is_empty() {
            output.push_str("┌---------------------------------------------------------┐\n");
            output.push_str("| OPTIMIZATION                                            |\n");
            output.push_str("`---------------------------------------------------------┘\n");
            output.push_str(&format!(
                "  IR nodes: {} -> {} ({} eliminated)\n",
                self.stats.nodes_before,
                self.stats.nodes_after,
                self.stats
                    .nodes_before
                    .saturating_sub(self.stats.nodes_after)
            ));

            if self.stats.identity_maps_removed > 0 {
                output.push_str(&format!(
                    "  - Identity maps removed: {}\n",
                    self.stats.identity_maps_removed
                ));
            }
            if self.stats.true_filters_removed > 0 {
                output.push_str(&format!(
                    "  - Always-true filters removed: {}\n",
                    self.stats.true_filters_removed
                ));
            }
            if self.stats.false_filters_removed > 0 {
                output.push_str(&format!(
                    "  - Always-false filters removed: {}\n",
                    self.stats.false_filters_removed
                ));
            }
            output.push('\n');

            for (i, ir) in self.ir_after.iter().enumerate() {
                output.push_str(&format!("  Rule {} Optimized IR:\n", i + 1));
                output.push_str(&Self::format_ir_tree(ir, 4));
                output.push('\n');
            }
        }

        // Results
        if !self.results.is_empty() {
            output.push_str("┌---------------------------------------------------------┐\n");
            output.push_str("| EXECUTION                                               |\n");
            output.push_str("`---------------------------------------------------------┘\n");

            for (i, result) in self.results.iter().enumerate() {
                output.push_str(&format!(
                    "  Rule {} results: {} tuples\n",
                    i + 1,
                    result.len()
                ));

                if result.len() <= 10 {
                    for tuple in result {
                        output.push_str(&format!("    {tuple:?}\n"));
                    }
                } else {
                    for tuple in result.iter().take(5) {
                        output.push_str(&format!("    {tuple:?}\n"));
                    }
                    output.push_str(&format!("    ... ({} more)\n", result.len() - 5));
                }
                output.push('\n');
            }
        }

        output.push_str("═══════════════════════════════════════════════════════════\n");

        output
    }

    /// Format an IR tree with indentation
    fn format_ir_tree(ir: &IRNode, indent: usize) -> String {
        let prefix = " ".repeat(indent);
        let mut output = String::new();

        match ir {
            IRNode::Scan { relation, schema } => {
                output.push_str(&format!(
                    "{}Scan({})[{}]\n",
                    prefix,
                    relation,
                    schema.join(", ")
                ));
            }
            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                output.push_str(&format!(
                    "{}Map[{:?}] -> [{}]\n",
                    prefix,
                    projection,
                    output_schema.join(", ")
                ));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::Filter { input, predicate } => {
                output.push_str(&format!("{prefix}Filter({predicate:?})\n"));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::Join {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                output.push_str(&format!(
                    "{}Join[L:{:?}, R:{:?}] -> [{}]\n",
                    prefix,
                    left_keys,
                    right_keys,
                    output_schema.join(", ")
                ));
                output.push_str(&format!("{prefix}|- Left:\n"));
                output.push_str(&Self::format_ir_tree(left, indent + 4));
                output.push_str(&format!("{prefix}`- Right:\n"));
                output.push_str(&Self::format_ir_tree(right, indent + 4));
            }
            IRNode::Distinct { input } => {
                output.push_str(&format!("{prefix}Distinct\n"));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::Union { inputs } => {
                output.push_str(&format!("{}Union ({} inputs)\n", prefix, inputs.len()));
                for (i, input) in inputs.iter().enumerate() {
                    output.push_str(&format!("{}|- Input {}:\n", prefix, i + 1));
                    output.push_str(&Self::format_ir_tree(input, indent + 4));
                }
            }
            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => {
                let agg_strs: Vec<String> = aggregations
                    .iter()
                    .map(|(f, c)| format!("{f:?}({c})"))
                    .collect();
                output.push_str(&format!(
                    "{}Aggregate[group_by={:?}, aggs=[{}]] -> [{}]\n",
                    prefix,
                    group_by,
                    agg_strs.join(", "),
                    output_schema.join(", ")
                ));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::Antijoin {
                left,
                right,
                left_keys,
                right_keys,
                output_schema,
            } => {
                output.push_str(&format!(
                    "{}Antijoin[L:{:?}, R:{:?}] -> [{}]\n",
                    prefix,
                    left_keys,
                    right_keys,
                    output_schema.join(", ")
                ));
                output.push_str(&format!("{prefix}|- Left:\n"));
                output.push_str(&Self::format_ir_tree(left, indent + 4));
                output.push_str(&format!("{prefix}`- Right:\n"));
                output.push_str(&Self::format_ir_tree(right, indent + 4));
            }
            IRNode::Compute { input, expressions } => {
                let expr_strs: Vec<String> =
                    expressions.iter().map(|(name, _)| name.clone()).collect();
                output.push_str(&format!("{}Compute[{}]\n", prefix, expr_strs.join(", ")));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::HnswScan {
                index_name,
                k,
                ef_search,
                output_schema,
                ..
            } => {
                output.push_str(&format!(
                    "{}HnswScan({})[k={}, ef_search={:?}] -> [{}]\n",
                    prefix,
                    index_name,
                    k,
                    ef_search,
                    output_schema.join(", ")
                ));
            }
            IRNode::FlatMap {
                input,
                projection,
                filter_predicate,
                output_schema,
            } => {
                output.push_str(&format!(
                    "{}FlatMap[{:?}, filter={:?}] -> [{}]\n",
                    prefix,
                    projection,
                    filter_predicate,
                    output_schema.join(", ")
                ));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::JoinFlatMap {
                left,
                right,
                left_keys,
                right_keys,
                projection,
                filter_predicate,
                output_schema,
            } => {
                output.push_str(&format!(
                    "{}JoinFlatMap[L:{:?}, R:{:?}, proj={:?}, filter={:?}] -> [{}]\n",
                    prefix,
                    left_keys,
                    right_keys,
                    projection,
                    filter_predicate,
                    output_schema.join(", ")
                ));
                output.push_str(&format!("{prefix}|- Left:\n"));
                output.push_str(&Self::format_ir_tree(left, indent + 4));
                output.push_str(&format!("{prefix}`- Right:\n"));
                output.push_str(&Self::format_ir_tree(right, indent + 4));
            }
        }

        output
    }
}

impl Default for PipelineTrace {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for PipelineTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_trace())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_trace_creation() {
        let trace = PipelineTrace::new();
        assert!(trace.ast.is_none());
        assert_eq!(trace.ir_before.len(), 0);
        assert_eq!(trace.ir_after.len(), 0);
    }

    #[test]
    fn test_count_ir_nodes() {
        let ir = IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: crate::ir::Predicate::True,
        };

        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 2);
    }

    #[test]
    fn test_default_trace() {
        let trace = PipelineTrace::default();
        assert!(trace.ast.is_none());
        assert!(trace.ir_before.is_empty());
        assert!(trace.ir_after.is_empty());
        assert!(trace.results.is_empty());
    }

    #[test]
    fn test_optimization_stats_default() {
        let stats = OptimizationStats::default();
        assert_eq!(stats.identity_maps_removed, 0);
        assert_eq!(stats.true_filters_removed, 0);
        assert_eq!(stats.false_filters_removed, 0);
        assert_eq!(stats.nodes_before, 0);
        assert_eq!(stats.nodes_after, 0);
    }

    #[test]
    fn test_record_ir_before() {
        let mut trace = PipelineTrace::new();
        let ir = vec![IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }];
        trace.record_ir_before(ir);
        assert_eq!(trace.stats.nodes_before, 1);
        assert_eq!(trace.ir_before.len(), 1);
    }

    #[test]
    fn test_record_ir_after() {
        let mut trace = PipelineTrace::new();
        // Record before first (2 nodes)
        let before = vec![IRNode::Filter {
            input: Box::new(IRNode::Scan {
                relation: "edge".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            predicate: crate::ir::Predicate::True,
        }];
        trace.record_ir_before(before);
        assert_eq!(trace.stats.nodes_before, 2);

        // After optimization: 1 node (filter removed)
        let after = vec![IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }];
        trace.record_ir_after(after);
        assert_eq!(trace.stats.nodes_after, 1);
        // 1 node eliminated, stats should reflect this
        assert!(trace.stats.identity_maps_removed + trace.stats.true_filters_removed > 0);
    }

    #[test]
    fn test_record_results() {
        let mut trace = PipelineTrace::new();
        let results = vec![vec![(1, 2), (3, 4)]];
        trace.record_results(results);
        assert_eq!(trace.results.len(), 1);
        assert_eq!(trace.results[0].len(), 2);
    }

    #[test]
    fn test_record_ast() {
        let mut trace = PipelineTrace::new();
        let program = crate::ast::Program { rules: vec![] };
        trace.record_ast(program);
        assert!(trace.ast.is_some());
    }

    #[test]
    fn test_count_ir_nodes_join() {
        let ir = IRNode::Join {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["y".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string(), "y".to_string()],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 3);
    }

    #[test]
    fn test_count_ir_nodes_union() {
        let ir = IRNode::Union {
            inputs: vec![
                IRNode::Scan {
                    relation: "a".to_string(),
                    schema: vec!["x".to_string()],
                },
                IRNode::Scan {
                    relation: "b".to_string(),
                    schema: vec!["x".to_string()],
                },
            ],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 3);
    }

    #[test]
    fn test_count_ir_nodes_distinct() {
        let ir = IRNode::Distinct {
            input: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 2);
    }

    #[test]
    fn test_count_ir_nodes_map() {
        let ir = IRNode::Map {
            input: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![0],
            output_schema: vec!["x".to_string()],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 2);
    }

    #[test]
    fn test_count_ir_nodes_antijoin() {
        let ir = IRNode::Antijoin {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            output_schema: vec!["x".to_string()],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 3);
    }

    #[test]
    fn test_count_nodes_multiple() {
        let irs = vec![
            IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            },
            IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["y".to_string()],
            },
        ];
        assert_eq!(PipelineTrace::count_nodes(&irs), 2);
    }

    #[test]
    fn test_format_trace_empty() {
        let trace = PipelineTrace::new();
        let output = trace.format_trace();
        assert!(output.contains("PIPELINE TRACE"));
    }

    #[test]
    fn test_format_trace_with_ir() {
        let mut trace = PipelineTrace::new();
        let ir = vec![IRNode::Scan {
            relation: "edge".to_string(),
            schema: vec!["x".to_string(), "y".to_string()],
        }];
        trace.record_ir_before(ir.clone());
        trace.record_ir_after(ir);

        let output = trace.format_trace();
        assert!(output.contains("IR CONSTRUCTION"));
        assert!(output.contains("Scan(edge)"));
    }

    #[test]
    fn test_format_trace_with_results() {
        let mut trace = PipelineTrace::new();
        trace.record_results(vec![vec![(1, 2), (3, 4)]]);

        let output = trace.format_trace();
        assert!(output.contains("EXECUTION"));
        assert!(output.contains("2 tuples"));
    }

    #[test]
    fn test_format_trace_with_many_results() {
        let mut trace = PipelineTrace::new();
        let results: Vec<(i32, i32)> = (0..20).map(|i| (i, i * 2)).collect();
        trace.record_results(vec![results]);

        let output = trace.format_trace();
        assert!(output.contains("... (15 more)"));
    }

    #[test]
    fn test_display_trait() {
        let trace = PipelineTrace::new();
        let display = format!("{trace}");
        assert!(display.contains("PIPELINE TRACE"));
    }

    #[test]
    fn test_compute_stats_no_elimination() {
        let mut trace = PipelineTrace::new();
        let ir = vec![IRNode::Scan {
            relation: "a".to_string(),
            schema: vec!["x".to_string()],
        }];
        trace.record_ir_before(ir.clone());
        trace.record_ir_after(ir);
        assert_eq!(trace.stats.identity_maps_removed, 0);
        assert_eq!(trace.stats.true_filters_removed, 0);
    }

    #[test]
    fn test_format_ir_tree_aggregate() {
        let ir = IRNode::Aggregate {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            group_by: vec![0],
            aggregations: vec![(crate::ir::AggregateFunction::Sum, 1)],
            output_schema: vec!["x".to_string(), "sum_y".to_string()],
        };
        let output = PipelineTrace::format_ir_tree(&ir, 0);
        assert!(output.contains("Aggregate"));
        assert!(output.contains("Sum"));
    }

    #[test]
    fn test_format_ir_tree_flatmap() {
        let ir = IRNode::FlatMap {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["x".to_string(), "y".to_string()],
            }),
            projection: vec![0],
            filter_predicate: Some(crate::ir::Predicate::ColumnGtConst(0, 5)),
            output_schema: vec!["x".to_string()],
        };
        let output = PipelineTrace::format_ir_tree(&ir, 0);
        assert!(output.contains("FlatMap"));
    }

    #[test]
    fn test_format_ir_tree_join_flatmap() {
        let ir = IRNode::JoinFlatMap {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            projection: vec![0, 1],
            filter_predicate: None,
            output_schema: vec!["x".to_string(), "y".to_string()],
        };
        let output = PipelineTrace::format_ir_tree(&ir, 0);
        assert!(output.contains("JoinFlatMap"));
    }

    #[test]
    fn test_format_ir_tree_hnsw_scan() {
        let ir = IRNode::HnswScan {
            index_name: "my_index".to_string(),
            query: crate::ir::IRExpression::VectorLiteral(vec![1.0, 2.0, 3.0]),
            k: 5,
            ef_search: Some(50),
            output_schema: vec!["id".to_string(), "distance".to_string()],
        };
        let output = PipelineTrace::format_ir_tree(&ir, 0);
        assert!(output.contains("HnswScan"));
        assert!(output.contains("my_index"));
        assert!(output.contains("k=5"));
    }

    #[test]
    fn test_count_ir_nodes_flatmap() {
        let ir = IRNode::FlatMap {
            input: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            projection: vec![0],
            filter_predicate: None,
            output_schema: vec!["x".to_string()],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 2);
    }

    #[test]
    fn test_count_ir_nodes_join_flatmap() {
        let ir = IRNode::JoinFlatMap {
            left: Box::new(IRNode::Scan {
                relation: "a".to_string(),
                schema: vec!["x".to_string()],
            }),
            right: Box::new(IRNode::Scan {
                relation: "b".to_string(),
                schema: vec!["x".to_string()],
            }),
            left_keys: vec![0],
            right_keys: vec![0],
            projection: vec![0],
            filter_predicate: None,
            output_schema: vec!["x".to_string()],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 3);
    }

    #[test]
    fn test_count_ir_nodes_hnsw_scan() {
        let ir = IRNode::HnswScan {
            index_name: "idx".to_string(),
            query: crate::ir::IRExpression::VectorLiteral(vec![1.0]),
            k: 1,
            ef_search: None,
            output_schema: vec!["id".to_string()],
        };
        assert_eq!(PipelineTrace::count_ir_nodes(&ir), 1);
    }

    #[test]
    fn test_format_trace_with_ast() {
        use crate::ast::{Atom, BodyPredicate, Program, Rule, Term};
        let mut trace = PipelineTrace::new();
        let mut program = Program::new();
        program.add_rule(Rule::new(
            Atom::new("result".to_string(), vec![Term::Variable("X".to_string())]),
            vec![
                BodyPredicate::Positive(Atom::new(
                    "source".to_string(),
                    vec![Term::Variable("X".to_string())],
                )),
                BodyPredicate::Negated(Atom::new(
                    "excluded".to_string(),
                    vec![Term::Variable("X".to_string())],
                )),
            ],
        ));
        trace.record_ast(program);
        let output = trace.format_trace();
        assert!(output.contains("PARSING"));
        assert!(output.contains("Rules parsed: 1"));
        assert!(output.contains("result"));
        assert!(output.contains("!excluded"));
    }

    #[test]
    fn test_format_ir_tree_compute() {
        let ir = IRNode::Compute {
            input: Box::new(IRNode::Scan {
                relation: "data".to_string(),
                schema: vec!["x".to_string()],
            }),
            expressions: vec![("y".to_string(), crate::ir::IRExpression::Column(0))],
        };
        let output = PipelineTrace::format_ir_tree(&ir, 0);
        assert!(output.contains("Compute"));
    }
}
