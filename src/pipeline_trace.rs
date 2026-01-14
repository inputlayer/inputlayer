//! # Pipeline Trace
//!
//! Utility for visualizing the query processing pipeline.
//! Shows output of each stage: Parse → IR Build → Optimize → Execute
//!
//! This helps students understand how data flows through the system.

use crate::ast::Program;
use crate::ir::IRNode;
use std::fmt;

/// Trace of pipeline execution stages
#[derive(Debug, Clone)]
pub struct PipelineTrace {
    /// Stage 1: Parsed AST
    pub ast: Option<Program>,

    /// Stage 2: Built IR (before optimization)
    pub ir_before: Vec<IRNode>,

    /// Stage 3: Optimized IR (after optimization)
    pub ir_after: Vec<IRNode>,

    /// Stage 4: Execution results
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
        irs.iter().map(|ir| Self::count_ir_nodes(ir)).sum()
    }

    /// Count nodes in a single IR tree
    fn count_ir_nodes(ir: &IRNode) -> usize {
        match ir {
            IRNode::Scan { .. } => 1,
            IRNode::Map { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::Filter { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::Join { left, right, .. } => {
                1 + Self::count_ir_nodes(left) + Self::count_ir_nodes(right)
            }
            IRNode::Antijoin { left, right, .. } => {
                1 + Self::count_ir_nodes(left) + Self::count_ir_nodes(right)
            }
            IRNode::Distinct { input } => 1 + Self::count_ir_nodes(input),
            IRNode::Union { inputs } => {
                1 + inputs
                    .iter()
                    .map(|i| Self::count_ir_nodes(i))
                    .sum::<usize>()
            }
            IRNode::Aggregate { input, .. } => 1 + Self::count_ir_nodes(input),
            IRNode::Compute { input, .. } => 1 + Self::count_ir_nodes(input),
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

        // Stage 1: AST
        if let Some(ref ast) = self.ast {
            output.push_str("┌─────────────────────────────────────────────────────────┐\n");
            output.push_str("│ STAGE 1: PARSING                                        │\n");
            output.push_str("└─────────────────────────────────────────────────────────┘\n");
            output.push_str(&format!("  Rules parsed: {}\n\n", ast.rules.len()));

            for (i, rule) in ast.rules.iter().enumerate() {
                output.push_str(&format!(
                    "  Rule {}: {}({}) :- ",
                    i + 1,
                    rule.head.relation,
                    rule.head
                        .args
                        .iter()
                        .map(|t| format!("{:?}", t))
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
                                .map(|t| format!("{:?}", t))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        crate::ast::BodyPredicate::Negated(a) => format!(
                            "!{}({})",
                            a.relation,
                            a.args
                                .iter()
                                .map(|t| format!("{:?}", t))
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
                            format!("{:?} {} {:?}", left, op_str, right)
                        }
                    })
                    .collect();

                output.push_str(&body_str.join(", "));
                output.push_str(".\n");
            }
            output.push_str("\n");
        }

        // Stage 2: IR Before Optimization
        if !self.ir_before.is_empty() {
            output.push_str("┌─────────────────────────────────────────────────────────┐\n");
            output.push_str("│ STAGE 2: IR CONSTRUCTION                                │\n");
            output.push_str("└─────────────────────────────────────────────────────────┘\n");
            output.push_str(&format!("  IR nodes: {}\n", self.stats.nodes_before));
            output.push_str(&format!("  Rules: {}\n\n", self.ir_before.len()));

            for (i, ir) in self.ir_before.iter().enumerate() {
                output.push_str(&format!("  Rule {} IR:\n", i + 1));
                output.push_str(&Self::format_ir_tree(ir, 4));
                output.push_str("\n");
            }
        }

        // Stage 3: IR After Optimization
        if !self.ir_after.is_empty() {
            output.push_str("┌─────────────────────────────────────────────────────────┐\n");
            output.push_str("│ STAGE 3: OPTIMIZATION                                   │\n");
            output.push_str("└─────────────────────────────────────────────────────────┘\n");
            output.push_str(&format!(
                "  IR nodes: {} → {} ({} eliminated)\n",
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
            output.push_str("\n");

            for (i, ir) in self.ir_after.iter().enumerate() {
                output.push_str(&format!("  Rule {} Optimized IR:\n", i + 1));
                output.push_str(&Self::format_ir_tree(ir, 4));
                output.push_str("\n");
            }
        }

        // Stage 4: Results
        if !self.results.is_empty() {
            output.push_str("┌─────────────────────────────────────────────────────────┐\n");
            output.push_str("│ STAGE 4: EXECUTION                                      │\n");
            output.push_str("└─────────────────────────────────────────────────────────┘\n");

            for (i, result) in self.results.iter().enumerate() {
                output.push_str(&format!(
                    "  Rule {} results: {} tuples\n",
                    i + 1,
                    result.len()
                ));

                if result.len() <= 10 {
                    for tuple in result {
                        output.push_str(&format!("    {:?}\n", tuple));
                    }
                } else {
                    for tuple in result.iter().take(5) {
                        output.push_str(&format!("    {:?}\n", tuple));
                    }
                    output.push_str(&format!("    ... ({} more)\n", result.len() - 5));
                }
                output.push_str("\n");
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
                    "{}Map[{:?}] → [{}]\n",
                    prefix,
                    projection,
                    output_schema.join(", ")
                ));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::Filter { input, predicate } => {
                output.push_str(&format!("{}Filter({:?})\n", prefix, predicate));
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
                    "{}Join[L:{:?}, R:{:?}] → [{}]\n",
                    prefix,
                    left_keys,
                    right_keys,
                    output_schema.join(", ")
                ));
                output.push_str(&format!("{}├─ Left:\n", prefix));
                output.push_str(&Self::format_ir_tree(left, indent + 4));
                output.push_str(&format!("{}└─ Right:\n", prefix));
                output.push_str(&Self::format_ir_tree(right, indent + 4));
            }
            IRNode::Distinct { input } => {
                output.push_str(&format!("{}Distinct\n", prefix));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
            }
            IRNode::Union { inputs } => {
                output.push_str(&format!("{}Union ({} inputs)\n", prefix, inputs.len()));
                for (i, input) in inputs.iter().enumerate() {
                    output.push_str(&format!("{}├─ Input {}:\n", prefix, i + 1));
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
                    .map(|(f, c)| format!("{:?}({})", f, c))
                    .collect();
                output.push_str(&format!(
                    "{}Aggregate[group_by={:?}, aggs=[{}]] → [{}]\n",
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
                    "{}Antijoin[L:{:?}, R:{:?}] → [{}]\n",
                    prefix,
                    left_keys,
                    right_keys,
                    output_schema.join(", ")
                ));
                output.push_str(&format!("{}├─ Left:\n", prefix));
                output.push_str(&Self::format_ir_tree(left, indent + 4));
                output.push_str(&format!("{}└─ Right:\n", prefix));
                output.push_str(&Self::format_ir_tree(right, indent + 4));
            }
            IRNode::Compute { input, expressions } => {
                let expr_strs: Vec<String> =
                    expressions.iter().map(|(name, _)| name.clone()).collect();
                output.push_str(&format!("{}Compute[{}]\n", prefix, expr_strs.join(", ")));
                output.push_str(&Self::format_ir_tree(input, indent + 2));
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
}
