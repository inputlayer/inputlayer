//! # Join Planning (Module 07)
//!
//! **Course Module**: Module 07: Join Ordering & Cost Models
//!
//! ## What This Module Does
//!
//! Join planning optimizes the order of join operations to minimize intermediate
//! result sizes and overall query execution time. This implementation uses:
//!
//! 1. **Join Graph Construction**: Build graph where nodes are relations and
//!    edges represent shared variables
//! 2. **Maximum Spanning Tree (MST)**: Find tree with maximum shared variables
//! 3. **Rooted Join Spanning Tree**: Convert MST to rooted tree for execution
//! 4. **Structural Cost Model**: Estimate cost based on variable propagation
//!
//! ## Algorithm Overview
//!
//! 1. Extract all joins from the IR tree
//! 2. Build a join graph: nodes = scan/relation names, edges = shared join keys
//! 3. Compute Maximum Spanning Tree (MST) using edge weights = # shared vars
//! 4. For each possible root, compute structural cost (max vars at any point)
//! 5. Select root that minimizes structural cost
//! 6. Reorder joins according to the optimal rooted JST
//!
//! ## Example
//!
//! ```datalog
//! path(x, z) :- edge(x, y), edge(y, z).
//! ```
//!
//! Join graph:
//! ```text
//!   edge(x,y) --[y]-- edge(y,z)
//! ```
//!
//! MST is trivially this single edge. The optimal order depends on:
//! - If edge(x,y) is root: we build up (x,y) then join to get (x,y,z)
//! - If edge(y,z) is root: we build up (y,z) then join to get (x,y,z)
//!
//! ## Key Concepts
//!
//! - **Join Graph**: Vertices = atoms/relations, Edges = shared variables
//! - **Join Spanning Tree (JST)**: Tree connecting all atoms via equi-joins
//! - **Structural Cost**: Maximum number of variables at any intermediate step
//! - **Worst-Case Optimal**: Minimize worst-case intermediate result size
//!
//! ## Pipeline Position
//!
//! ```text
//! IRNode with Joins → [Join Planning] → Reordered IRNode → Later optimizations
//! ```

use crate::ir::IRNode;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Node in the join graph representing a relation/scan
#[derive(Debug, Clone)]
pub struct JoinGraphNode {
    // TODO: Implement selectivity-based cost model that uses relation statistics.
    // Reserved for future join planning optimizations:
    // - `index`: Maps back to original relation position for cost model lookups
    // - `relation`: Stores relation name for cardinality estimation and debugging
    // Currently unused because join planner uses simplified cost model based only
    // on shared variable counts. Will be needed for selectivity-based optimization.
    #[allow(dead_code)]
    pub index: usize,
    #[allow(dead_code)]
    pub relation: String,
    /// Variables (column names) from this relation
    pub variables: HashSet<String>,
    /// The original IR node (Scan)
    pub ir_node: IRNode,
}

/// Edge in the join graph representing shared variables between relations
#[derive(Debug, Clone)]
pub struct JoinGraphEdge {
    /// Source node index
    pub from: usize,
    /// Target node index
    pub to: usize,
    /// Weight = number of shared variables
    pub weight: usize,
    // TODO: Implement join key selectivity estimation using shared variable types.
    // Reserved for advanced cost model that considers which variables are shared,
    // not just how many. Currently only `weight` (count of shared vars) is used
    // by the MST algorithm. Will enable selectivity hints and key type analysis.
    #[allow(dead_code)]
    pub shared_vars: HashSet<String>,
}

impl Eq for JoinGraphEdge {}

impl PartialEq for JoinGraphEdge {
    fn eq(&self, other: &Self) -> bool {
        self.weight == other.weight
    }
}

impl Ord for JoinGraphEdge {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher weight = higher priority (for max spanning tree)
        self.weight.cmp(&other.weight)
    }
}

impl PartialOrd for JoinGraphEdge {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Join graph for a query
#[derive(Debug, Clone)]
pub struct JoinGraph {
    /// Nodes (relations/atoms)
    pub nodes: Vec<JoinGraphNode>,
    /// Edges (shared variables)
    pub edges: Vec<JoinGraphEdge>,
    /// Adjacency list for efficient traversal
    adjacency: HashMap<usize, Vec<(usize, JoinGraphEdge)>>,
}

impl JoinGraph {
    /// Create a new empty join graph
    pub fn new() -> Self {
        JoinGraph {
            nodes: Vec::new(),
            edges: Vec::new(),
            adjacency: HashMap::new(),
        }
    }

    /// Build join graph from IR nodes (extracts scans and analyzes joins)
    pub fn from_ir(ir: &IRNode) -> Self {
        let mut graph = JoinGraph::new();
        let scans = Self::extract_scans(ir);

        // Add nodes
        for (i, (relation, schema, ir_node)) in scans.iter().enumerate() {
            let variables: HashSet<String> = schema.iter().cloned().collect();
            graph.nodes.push(JoinGraphNode {
                index: i,
                relation: relation.clone(),
                variables,
                ir_node: ir_node.clone(),
            });
        }

        // Add edges based on shared variables
        for i in 0..graph.nodes.len() {
            for j in (i + 1)..graph.nodes.len() {
                let shared: HashSet<String> = graph.nodes[i]
                    .variables
                    .intersection(&graph.nodes[j].variables)
                    .cloned()
                    .collect();

                if !shared.is_empty() {
                    let edge = JoinGraphEdge {
                        from: i,
                        to: j,
                        weight: shared.len(),
                        shared_vars: shared,
                    };
                    graph.add_edge(edge);
                }
            }
        }

        graph
    }

    /// Add an edge to the graph
    fn add_edge(&mut self, edge: JoinGraphEdge) {
        self.adjacency
            .entry(edge.from)
            .or_insert_with(Vec::new)
            .push((edge.to, edge.clone()));
        self.adjacency
            .entry(edge.to)
            .or_insert_with(Vec::new)
            .push((edge.from, edge.clone()));
        self.edges.push(edge);
    }

    /// Extract all scans from an IR tree
    fn extract_scans(ir: &IRNode) -> Vec<(String, Vec<String>, IRNode)> {
        let mut scans = Vec::new();
        Self::extract_scans_recursive(ir, &mut scans);
        scans
    }

    fn extract_scans_recursive(ir: &IRNode, scans: &mut Vec<(String, Vec<String>, IRNode)>) {
        match ir {
            IRNode::Scan { relation, schema } => {
                scans.push((relation.clone(), schema.clone(), ir.clone()));
            }
            IRNode::Map { input, .. } => Self::extract_scans_recursive(input, scans),
            IRNode::Filter { input, .. } => Self::extract_scans_recursive(input, scans),
            IRNode::Join { left, right, .. } => {
                Self::extract_scans_recursive(left, scans);
                Self::extract_scans_recursive(right, scans);
            }
            IRNode::Antijoin { left, right, .. } => {
                Self::extract_scans_recursive(left, scans);
                Self::extract_scans_recursive(right, scans);
            }
            IRNode::Distinct { input } => Self::extract_scans_recursive(input, scans),
            IRNode::Union { inputs } => {
                for input in inputs {
                    Self::extract_scans_recursive(input, scans);
                }
            }
            IRNode::Aggregate { input, .. } => Self::extract_scans_recursive(input, scans),
            IRNode::Compute { input, .. } => Self::extract_scans_recursive(input, scans),
        }
    }

    /// Compute Maximum Spanning Tree using Prim's algorithm
    /// Returns edges in the MST
    pub fn compute_mst(&self) -> Vec<(usize, usize)> {
        if self.nodes.is_empty() {
            return Vec::new();
        }

        let mut mst_edges = Vec::new();
        let mut in_mst = HashSet::new();
        let mut heap = BinaryHeap::new();

        // Start from node 0
        in_mst.insert(0);

        // Add all edges from node 0 to the heap
        if let Some(neighbors) = self.adjacency.get(&0) {
            for (_, edge) in neighbors {
                heap.push(edge.clone());
            }
        }

        while mst_edges.len() < self.nodes.len() - 1 && !heap.is_empty() {
            if let Some(edge) = heap.pop() {
                let new_node = if in_mst.contains(&edge.from) && !in_mst.contains(&edge.to) {
                    Some(edge.to)
                } else if !in_mst.contains(&edge.from) && in_mst.contains(&edge.to) {
                    Some(edge.from)
                } else {
                    None
                };

                if let Some(node) = new_node {
                    in_mst.insert(node);
                    mst_edges.push((edge.from, edge.to));

                    // Add edges from new node
                    if let Some(neighbors) = self.adjacency.get(&node) {
                        for (neighbor, edge) in neighbors {
                            if !in_mst.contains(neighbor) {
                                heap.push(edge.clone());
                            }
                        }
                    }
                }
            }
        }

        mst_edges
    }

    /// Check if graph is connected
    pub fn is_connected(&self) -> bool {
        if self.nodes.is_empty() {
            return true;
        }

        let mut visited = HashSet::new();
        let mut stack = vec![0];

        while let Some(node) = stack.pop() {
            if visited.insert(node) {
                if let Some(neighbors) = self.adjacency.get(&node) {
                    for (neighbor, _) in neighbors {
                        if !visited.contains(neighbor) {
                            stack.push(*neighbor);
                        }
                    }
                }
            }
        }

        visited.len() == self.nodes.len()
    }
}

impl Default for JoinGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// A rooted Join Spanning Tree with computed join order
#[derive(Debug, Clone)]
pub struct RootedJST {
    // TODO: Implement join tree visualization and incremental tree modification.
    // Reserved for debugging tools and advanced tree operations:
    // - `root`: For tree traversal starting point in visualization
    // - `parent`/`children`: For incremental tree restructuring without rebuilding
    // Currently only `join_order` and `cost` are used; tree structure is discarded
    // after post-order traversal computes the execution order.
    #[allow(dead_code)]
    pub root: usize,
    /// Join order (post-order traversal of the tree)
    pub join_order: Vec<usize>,
    /// Structural cost: max variables accumulated at any point
    pub cost: usize,
    #[allow(dead_code)]
    parent: HashMap<usize, usize>,
    #[allow(dead_code)]
    children: HashMap<usize, Vec<usize>>,
}

impl RootedJST {
    /// Build a rooted JST from MST edges with specified root
    pub fn from_mst(graph: &JoinGraph, mst_edges: &[(usize, usize)], root: usize) -> Self {
        let mut parent = HashMap::new();
        let mut children: HashMap<usize, Vec<usize>> = HashMap::new();

        // Build undirected adjacency from MST edges
        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
        for &(u, v) in mst_edges {
            adj.entry(u).or_insert_with(Vec::new).push(v);
            adj.entry(v).or_insert_with(Vec::new).push(u);
        }

        // BFS from root to determine parent/child relationships
        let mut visited = HashSet::new();
        let mut queue = vec![root];
        parent.insert(root, root);
        visited.insert(root);

        while let Some(node) = queue.pop() {
            if let Some(neighbors) = adj.get(&node) {
                for &neighbor in neighbors {
                    if visited.insert(neighbor) {
                        parent.insert(neighbor, node);
                        children.entry(node).or_insert_with(Vec::new).push(neighbor);
                        queue.push(neighbor);
                    }
                }
            }
        }

        // Compute post-order traversal (children before parents)
        let mut join_order = Vec::new();
        let mut stack = vec![(root, false)];
        while let Some((node, processed)) = stack.pop() {
            if processed {
                join_order.push(node);
            } else {
                stack.push((node, true));
                if let Some(node_children) = children.get(&node) {
                    for &child in node_children {
                        stack.push((child, false));
                    }
                }
            }
        }

        // Compute structural cost
        let cost = Self::compute_cost(&join_order, graph);

        RootedJST {
            root,
            join_order,
            cost,
            parent,
            children,
        }
    }

    /// Compute structural cost: max variables at any intermediate step
    fn compute_cost(join_order: &[usize], graph: &JoinGraph) -> usize {
        let mut accumulated_vars: HashSet<String> = HashSet::new();
        let mut max_vars = 0;

        for &node_idx in join_order {
            if node_idx < graph.nodes.len() {
                // Add variables from this node
                accumulated_vars.extend(graph.nodes[node_idx].variables.iter().cloned());
                max_vars = max_vars.max(accumulated_vars.len());
            }
        }

        max_vars
    }
}

/// Statistics about join planning
#[derive(Debug, Clone, Default)]
pub struct JoinPlanningStats {
    /// Number of joins in the query
    pub num_joins: usize,
    /// Number of atoms/relations
    pub num_atoms: usize,
    /// Whether the join graph is connected
    pub is_connected: bool,
    /// Cost of the chosen join order
    pub chosen_cost: usize,
    /// Best possible cost found
    pub best_cost: usize,
}

/// Join planner for optimizing join order in queries
///
/// This implementation analyzes the join structure and reorders joins
/// based on structural cost estimation using the Join Spanning Tree (JST)
/// algorithm.
pub struct JoinPlanner {
    /// Whether to enable join reordering
    enable_reordering: bool,
}

impl JoinPlanner {
    /// Create a new join planner
    pub fn new() -> Self {
        JoinPlanner {
            enable_reordering: true,
        }
    }

    /// Enable or disable join reordering
    pub fn set_reordering(&mut self, enable: bool) {
        self.enable_reordering = enable;
    }

    /// Plan join execution order for the given IR tree
    ///
    /// # Algorithm
    ///
    /// 1. Extract joins from IR tree
    /// 2. Build join graph
    /// 3. Compute Maximum Spanning Tree (MST)
    /// 4. Try each node as root, compute structural cost
    /// 5. Select order with minimum cost
    /// 6. Rebuild IR tree with optimal join order
    ///
    /// # Returns
    ///
    /// Optimized IR with joins reordered for efficiency
    pub fn plan_joins(&self, ir: IRNode) -> IRNode {
        if !self.enable_reordering {
            return ir;
        }

        // Only optimize if there are joins
        if !Self::has_joins(&ir) {
            return ir;
        }

        // Skip join planning when Antijoins are present
        // Antijoin has specific semantics (negation) that must be preserved
        if Self::has_antijoin(&ir) {
            return ir;
        }

        // Build join graph
        let graph = JoinGraph::from_ir(&ir);

        // If graph has only one node or is not connected, return unchanged
        if graph.nodes.len() <= 1 || !graph.is_connected() {
            return ir;
        }

        // Compute MST
        let mst_edges = graph.compute_mst();

        // Find optimal root
        let optimal_jst = self.find_optimal_root(&graph, &mst_edges);

        // Rebuild IR with optimal join order
        self.rebuild_ir_with_order(&ir, &graph, &optimal_jst)
    }

    /// Check if IR contains joins
    fn has_joins(ir: &IRNode) -> bool {
        match ir {
            IRNode::Join { .. } => true,
            IRNode::Antijoin { left, right, .. } => Self::has_joins(left) || Self::has_joins(right),
            IRNode::Scan { .. } => false,
            IRNode::Map { input, .. } => Self::has_joins(input),
            IRNode::Filter { input, .. } => Self::has_joins(input),
            IRNode::Distinct { input } => Self::has_joins(input),
            IRNode::Union { inputs } => inputs.iter().any(Self::has_joins),
            IRNode::Aggregate { input, .. } => Self::has_joins(input),
            IRNode::Compute { input, .. } => Self::has_joins(input),
        }
    }

    /// Check if IR contains any Antijoin nodes
    /// Antijoin represents negation and must be preserved exactly
    fn has_antijoin(ir: &IRNode) -> bool {
        match ir {
            IRNode::Antijoin { .. } => true,
            IRNode::Join { left, right, .. } => {
                Self::has_antijoin(left) || Self::has_antijoin(right)
            }
            IRNode::Scan { .. } => false,
            IRNode::Map { input, .. } => Self::has_antijoin(input),
            IRNode::Filter { input, .. } => Self::has_antijoin(input),
            IRNode::Distinct { input } => Self::has_antijoin(input),
            IRNode::Union { inputs } => inputs.iter().any(Self::has_antijoin),
            IRNode::Aggregate { input, .. } => Self::has_antijoin(input),
            IRNode::Compute { input, .. } => Self::has_antijoin(input),
        }
    }

    /// Find the optimal root for the rooted JST
    fn find_optimal_root(&self, graph: &JoinGraph, mst_edges: &[(usize, usize)]) -> RootedJST {
        let mut best_jst: Option<RootedJST> = None;

        for root in 0..graph.nodes.len() {
            let jst = RootedJST::from_mst(graph, mst_edges, root);

            match &best_jst {
                None => best_jst = Some(jst),
                Some(current_best) => {
                    if jst.cost < current_best.cost {
                        best_jst = Some(jst);
                    }
                }
            }
        }

        best_jst.unwrap_or_else(|| RootedJST::from_mst(graph, mst_edges, 0))
    }

    /// Rebuild IR with the optimal join order
    fn rebuild_ir_with_order(
        &self,
        original_ir: &IRNode,
        graph: &JoinGraph,
        jst: &RootedJST,
    ) -> IRNode {
        if jst.join_order.is_empty() {
            return original_ir.clone();
        }

        // Build joins in the order specified by JST
        let mut current = graph.nodes[jst.join_order[0]].ir_node.clone();

        for &node_idx in jst.join_order.iter().skip(1) {
            let next_node = &graph.nodes[node_idx];

            // Find shared variables for join keys
            let current_schema = current.output_schema();
            let next_schema = next_node.ir_node.output_schema();

            let mut left_keys = Vec::new();
            let mut right_keys = Vec::new();

            for (i, var) in current_schema.iter().enumerate() {
                if let Some(j) = next_schema.iter().position(|v| v == var) {
                    left_keys.push(i);
                    right_keys.push(j);
                }
            }

            // Build output schema (union of variables, shared vars once)
            let mut output_schema = current_schema.clone();
            for var in &next_schema {
                if !output_schema.contains(var) {
                    output_schema.push(var.clone());
                }
            }

            current = IRNode::Join {
                left: Box::new(current),
                right: Box::new(next_node.ir_node.clone()),
                left_keys,
                right_keys,
                output_schema,
            };
        }

        // Preserve operations above the joins (Map, Filter, Distinct, etc.)
        self.preserve_top_operations(original_ir, current)
    }

    /// Preserve operations that were on top of the original joins
    ///
    /// IMPORTANT: When the join order is reordered, the output schema changes.
    /// We need to remap projection indices based on the new schema.
    fn preserve_top_operations(&self, original: &IRNode, new_joins: IRNode) -> IRNode {
        match original {
            IRNode::Map {
                input,
                projection,
                output_schema,
            } => {
                // Recursively preserve, then wrap
                let inner = self.preserve_top_operations(input, new_joins);

                // Get the old and new schemas
                let old_input_schema = input.output_schema();
                let new_input_schema = inner.output_schema();

                // Remap projection indices: find where each old column is in new schema
                let new_projection: Vec<usize> = projection
                    .iter()
                    .map(|&old_idx| {
                        let column_name = &old_input_schema[old_idx];
                        new_input_schema
                            .iter()
                            .position(|c| c == column_name)
                            .unwrap_or(old_idx) // fallback to old index if not found
                    })
                    .collect();

                IRNode::Map {
                    input: Box::new(inner),
                    projection: new_projection,
                    output_schema: output_schema.clone(),
                }
            }
            IRNode::Filter { input, predicate } => {
                let inner = self.preserve_top_operations(input, new_joins);

                // Remap filter predicate column indices
                let old_input_schema = input.output_schema();
                let new_input_schema = inner.output_schema();
                let remapped_predicate =
                    Self::remap_predicate(predicate, &old_input_schema, &new_input_schema);

                IRNode::Filter {
                    input: Box::new(inner),
                    predicate: remapped_predicate,
                }
            }
            IRNode::Distinct { input } => {
                let inner = self.preserve_top_operations(input, new_joins);
                IRNode::Distinct {
                    input: Box::new(inner),
                }
            }
            IRNode::Aggregate {
                input,
                group_by,
                aggregations,
                output_schema,
            } => {
                let inner = self.preserve_top_operations(input, new_joins);
                IRNode::Aggregate {
                    input: Box::new(inner),
                    group_by: group_by.clone(),
                    aggregations: aggregations.clone(),
                    output_schema: output_schema.clone(),
                }
            }
            IRNode::Compute { input, expressions } => {
                let inner = self.preserve_top_operations(input, new_joins);

                // Remap expression column indices based on schema change
                let old_input_schema = input.output_schema();
                let new_input_schema = inner.output_schema();
                let remapped_expressions: Vec<(String, crate::ir::IRExpression)> = expressions
                    .iter()
                    .map(|(name, expr)| {
                        (
                            name.clone(),
                            Self::remap_expression(expr, &old_input_schema, &new_input_schema),
                        )
                    })
                    .collect();

                IRNode::Compute {
                    input: Box::new(inner),
                    expressions: remapped_expressions,
                }
            }
            // If we hit a join or scan, return the new joins
            IRNode::Join { .. } | IRNode::Scan { .. } => new_joins,
            IRNode::Antijoin { .. } => new_joins,
            IRNode::Union { .. } => new_joins,
        }
    }

    /// Remap column indices in a predicate when schema has been reordered
    fn remap_predicate(
        predicate: &crate::ir::Predicate,
        old_schema: &[String],
        new_schema: &[String],
    ) -> crate::ir::Predicate {
        use crate::ir::Predicate;

        let remap_idx = |old_idx: usize| -> usize {
            if old_idx < old_schema.len() {
                let col_name = &old_schema[old_idx];
                new_schema
                    .iter()
                    .position(|c| c == col_name)
                    .unwrap_or(old_idx)
            } else {
                old_idx
            }
        };

        match predicate {
            // Column to constant comparisons
            Predicate::ColumnEqConst(col, val) => Predicate::ColumnEqConst(remap_idx(*col), *val),
            Predicate::ColumnNeConst(col, val) => Predicate::ColumnNeConst(remap_idx(*col), *val),
            Predicate::ColumnGtConst(col, val) => Predicate::ColumnGtConst(remap_idx(*col), *val),
            Predicate::ColumnLtConst(col, val) => Predicate::ColumnLtConst(remap_idx(*col), *val),
            Predicate::ColumnGeConst(col, val) => Predicate::ColumnGeConst(remap_idx(*col), *val),
            Predicate::ColumnLeConst(col, val) => Predicate::ColumnLeConst(remap_idx(*col), *val),
            // String comparisons
            Predicate::ColumnEqStr(col, val) => {
                Predicate::ColumnEqStr(remap_idx(*col), val.clone())
            }
            Predicate::ColumnNeStr(col, val) => {
                Predicate::ColumnNeStr(remap_idx(*col), val.clone())
            }
            // Float comparisons
            Predicate::ColumnEqFloat(col, val) => Predicate::ColumnEqFloat(remap_idx(*col), *val),
            Predicate::ColumnNeFloat(col, val) => Predicate::ColumnNeFloat(remap_idx(*col), *val),
            Predicate::ColumnGtFloat(col, val) => Predicate::ColumnGtFloat(remap_idx(*col), *val),
            Predicate::ColumnLtFloat(col, val) => Predicate::ColumnLtFloat(remap_idx(*col), *val),
            Predicate::ColumnGeFloat(col, val) => Predicate::ColumnGeFloat(remap_idx(*col), *val),
            Predicate::ColumnLeFloat(col, val) => Predicate::ColumnLeFloat(remap_idx(*col), *val),
            // Column to column comparisons
            Predicate::ColumnsEq(l, r) => Predicate::ColumnsEq(remap_idx(*l), remap_idx(*r)),
            Predicate::ColumnsNe(l, r) => Predicate::ColumnsNe(remap_idx(*l), remap_idx(*r)),
            // Logical combinators
            Predicate::And(p1, p2) => Predicate::And(
                Box::new(Self::remap_predicate(p1, old_schema, new_schema)),
                Box::new(Self::remap_predicate(p2, old_schema, new_schema)),
            ),
            Predicate::Or(p1, p2) => Predicate::Or(
                Box::new(Self::remap_predicate(p1, old_schema, new_schema)),
                Box::new(Self::remap_predicate(p2, old_schema, new_schema)),
            ),
            Predicate::True => Predicate::True,
            Predicate::False => Predicate::False,
        }
    }

    /// Remap column indices in an IRExpression when schema has been reordered
    fn remap_expression(
        expr: &crate::ir::IRExpression,
        old_schema: &[String],
        new_schema: &[String],
    ) -> crate::ir::IRExpression {
        use crate::ir::IRExpression;

        let remap_idx = |old_idx: usize| -> usize {
            if old_idx < old_schema.len() {
                let col_name = &old_schema[old_idx];
                new_schema
                    .iter()
                    .position(|c| c == col_name)
                    .unwrap_or(old_idx)
            } else {
                old_idx
            }
        };

        match expr {
            IRExpression::Column(idx) => IRExpression::Column(remap_idx(*idx)),
            IRExpression::IntConstant(val) => IRExpression::IntConstant(*val),
            IRExpression::FloatConstant(val) => IRExpression::FloatConstant(*val),
            IRExpression::VectorLiteral(vals) => IRExpression::VectorLiteral(vals.clone()),
            IRExpression::FunctionCall(func, args) => {
                let remapped_args: Vec<IRExpression> = args
                    .iter()
                    .map(|arg| Self::remap_expression(arg, old_schema, new_schema))
                    .collect();
                IRExpression::FunctionCall(func.clone(), remapped_args)
            }
            IRExpression::Arithmetic { op, left, right } => IRExpression::Arithmetic {
                op: *op,
                left: Box::new(Self::remap_expression(left, old_schema, new_schema)),
                right: Box::new(Self::remap_expression(right, old_schema, new_schema)),
            },
        }
    }

    /// Analyze join structure and return statistics
    pub fn analyze(&self, ir: &IRNode) -> JoinPlanningStats {
        let graph = JoinGraph::from_ir(ir);

        let num_joins = Self::count_joins(ir);
        let is_connected = graph.is_connected();

        let (chosen_cost, best_cost) = if graph.nodes.len() > 1 && is_connected {
            let mst_edges = graph.compute_mst();
            let optimal = self.find_optimal_root(&graph, &mst_edges);
            (optimal.cost, optimal.cost)
        } else {
            (0, 0)
        };

        JoinPlanningStats {
            num_joins,
            num_atoms: graph.nodes.len(),
            is_connected,
            chosen_cost,
            best_cost,
        }
    }

    /// Count joins in IR
    fn count_joins(ir: &IRNode) -> usize {
        match ir {
            IRNode::Join { left, right, .. } => {
                1 + Self::count_joins(left) + Self::count_joins(right)
            }
            IRNode::Antijoin { left, right, .. } => {
                1 + Self::count_joins(left) + Self::count_joins(right)
            }
            IRNode::Map { input, .. } => Self::count_joins(input),
            IRNode::Filter { input, .. } => Self::count_joins(input),
            IRNode::Distinct { input } => Self::count_joins(input),
            IRNode::Union { inputs } => inputs.iter().map(Self::count_joins).sum(),
            IRNode::Aggregate { input, .. } => Self::count_joins(input),
            IRNode::Scan { .. } => 0,
            IRNode::Compute { input, .. } => Self::count_joins(input),
        }
    }
}

impl Default for JoinPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_scan(relation: &str, vars: &[&str]) -> IRNode {
        IRNode::Scan {
            relation: relation.to_string(),
            schema: vars.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn make_join(left: IRNode, right: IRNode, shared_var: &str) -> IRNode {
        let left_schema = left.output_schema();
        let right_schema = right.output_schema();

        let left_key = left_schema
            .iter()
            .position(|v| v == shared_var)
            .unwrap_or(0);
        let right_key = right_schema
            .iter()
            .position(|v| v == shared_var)
            .unwrap_or(0);

        let mut output_schema = left_schema.clone();
        for var in &right_schema {
            if !output_schema.contains(var) {
                output_schema.push(var.clone());
            }
        }

        IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys: vec![left_key],
            right_keys: vec![right_key],
            output_schema,
        }
    }

    #[test]
    fn test_join_planner_simple() {
        let planner = JoinPlanner::new();
        let ir = make_scan("edge", &["x", "y"]);

        let result = planner.plan_joins(ir.clone());
        // Single scan should be unchanged
        assert!(matches!(result, IRNode::Scan { .. }));
    }

    #[test]
    fn test_join_graph_construction() {
        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1, scan2, "y");

        let graph = JoinGraph::from_ir(&ir);

        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].weight, 1); // One shared variable: y
    }

    #[test]
    fn test_mst_computation() {
        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let scan3 = make_scan("T", &["z", "w"]);

        let join1 = make_join(scan1, scan2, "y");
        let ir = make_join(join1, scan3, "z");

        let graph = JoinGraph::from_ir(&ir);
        let mst = graph.compute_mst();

        // Should have n-1 = 2 edges
        assert_eq!(mst.len(), 2);
    }

    #[test]
    fn test_rooted_jst_cost() {
        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1, scan2, "y");

        let graph = JoinGraph::from_ir(&ir);
        let mst = graph.compute_mst();
        let jst = RootedJST::from_mst(&graph, &mst, 0);

        // Cost should be max variables at any point
        // R has {x, y}, after joining S we have {x, y, z}
        assert!(jst.cost >= 2);
    }

    #[test]
    fn test_join_planning_preserves_semantics() {
        let planner = JoinPlanner::new();

        let scan1 = make_scan("edge", &["x", "y"]);
        let scan2 = make_scan("edge", &["y", "z"]);
        let ir = make_join(scan1, scan2, "y");

        let result = planner.plan_joins(ir);

        // Result should still be a join
        assert!(matches!(result, IRNode::Join { .. }));
    }

    #[test]
    fn test_graph_connectivity() {
        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1, scan2, "y");

        let graph = JoinGraph::from_ir(&ir);
        assert!(graph.is_connected());
    }

    #[test]
    fn test_three_way_join() {
        let planner = JoinPlanner::new();

        let scan1 = make_scan("R", &["a", "b"]);
        let scan2 = make_scan("S", &["b", "c"]);
        let scan3 = make_scan("T", &["c", "d"]);

        let join1 = make_join(scan1, scan2, "b");
        let ir = make_join(join1, scan3, "c");

        let stats = planner.analyze(&ir);
        assert_eq!(stats.num_atoms, 3);
        assert_eq!(stats.num_joins, 2);
        assert!(stats.is_connected);
    }

    #[test]
    fn test_disabled_reordering() {
        let mut planner = JoinPlanner::new();
        planner.set_reordering(false);

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1.clone(), scan2.clone(), "y");

        let result = planner.plan_joins(ir.clone());

        // Should return original IR unchanged when disabled
        // Compare structure - both should be joins
        match (&ir, &result) {
            (IRNode::Join { .. }, IRNode::Join { .. }) => (),
            _ => panic!("Expected both to be joins"),
        }
    }

    #[test]
    fn test_analyze_stats() {
        let planner = JoinPlanner::new();

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let ir = make_join(scan1, scan2, "y");

        let stats = planner.analyze(&ir);

        assert_eq!(stats.num_atoms, 2);
        assert_eq!(stats.num_joins, 1);
        assert!(stats.is_connected);
    }

    #[test]
    fn test_star_query_planning() {
        // Star query: R(x,y), S(x,z), T(x,w) - all share x
        let planner = JoinPlanner::new();

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["x", "z"]);
        let scan3 = make_scan("T", &["x", "w"]);

        let join1 = make_join(scan1, scan2, "x");
        let ir = make_join(join1, scan3, "x");

        let result = planner.plan_joins(ir);

        // Should produce a valid join tree
        assert!(matches!(result, IRNode::Join { .. }));
    }

    #[test]
    fn test_chain_query_planning() {
        // Chain query: R(x,y), S(y,z), T(z,w)
        let planner = JoinPlanner::new();

        let scan1 = make_scan("R", &["x", "y"]);
        let scan2 = make_scan("S", &["y", "z"]);
        let scan3 = make_scan("T", &["z", "w"]);

        let join1 = make_join(scan1, scan2, "y");
        let ir = make_join(join1, scan3, "z");

        let stats = planner.analyze(&ir);

        // Chain should be connected
        assert!(stats.is_connected);
        assert_eq!(stats.num_atoms, 3);
    }
}
