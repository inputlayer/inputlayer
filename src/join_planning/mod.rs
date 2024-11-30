//! # Join Planning
//!
//! Reorders multi-way joins via MST to minimize intermediate result sizes.
//!
//! 1. Build join graph: nodes = relations, edges = shared variables
//! 2. Compute Maximum Spanning Tree (weight = # shared vars)
//! 3. Try each node as root, pick the one minimizing structural cost
//!    (max live variables at any intermediate step)
//! 4. Rebuild the IR tree in optimal join order
//!
//! ```text
//! IRNode with Joins -> [Join Planning] -> Reordered IRNode -> Later optimizations
//! ```

use crate::ir::IRNode;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

/// Node in the join graph representing a relation/scan
#[derive(Debug, Clone)]
pub struct JoinGraphNode {
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
        for (_relation, schema, ir_node) in &scans {
            let variables: HashSet<String> = schema.iter().cloned().collect();
            graph.nodes.push(JoinGraphNode {
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
            .or_default()
            .push((edge.to, edge.clone()));
        self.adjacency
            .entry(edge.to)
            .or_default()
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
            // Preserve Filter chains wrapping Scan nodes as single leaf nodes
            // so constant filters (e.g., ColumnEqStr for string constants in atoms)
            // are not lost during join reordering.
            IRNode::Filter { .. }
                if Self::find_scan_relation(ir).is_some() && Self::is_filter_scan_chain(ir) =>
            {
                let schema = ir.output_schema();
                let relation = Self::find_scan_relation(ir).unwrap();
                scans.push((relation, schema, ir.clone()));
            }
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
            IRNode::HnswScan { .. } => {} // HNSW scans are not part of join graph
            IRNode::FlatMap { input, .. } => Self::extract_scans_recursive(input, scans),
            IRNode::JoinFlatMap { left, right, .. } => {
                Self::extract_scans_recursive(left, scans);
                Self::extract_scans_recursive(right, scans);
            }
        }
    }

    /// Find the relation name from a (possibly Filter-wrapped) Scan node
    fn find_scan_relation(ir: &IRNode) -> Option<String> {
        match ir {
            IRNode::Scan { relation, .. } => Some(relation.clone()),
            IRNode::Filter { input, .. } => Self::find_scan_relation(input),
            _ => None,
        }
    }

    /// Check if an IR node is a chain of Filter nodes wrapping a Scan
    fn is_filter_scan_chain(ir: &IRNode) -> bool {
        match ir {
            IRNode::Scan { .. } => true,
            IRNode::Filter { input, .. } => Self::is_filter_scan_chain(input),
            _ => false,
        }
    }

    /// Compute Maximum Spanning Tree using Prim's algorithm
    /// Returns edges in the MST
    pub fn compute_mst(&self) -> Vec<(usize, usize)> {
        // TODO: verify this condition
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
    #[allow(dead_code)]
    pub root: usize,
    /// Join order (post-order traversal of the tree)
    pub join_order: Vec<usize>,
    /// Tree-width cost: max "planning variables" at any join step
    pub cost: usize,
    /// Tree depth (for bushy tiebreaking: prefer lower depth)
    pub depth: usize,
    #[allow(dead_code)]
    parent: HashMap<usize, usize>,
    #[allow(dead_code)]
    children: HashMap<usize, Vec<usize>>,
}

impl RootedJST {
    /// Build a rooted JST from MST edges with specified root
    #[allow(dead_code)]
    pub fn from_mst(graph: &JoinGraph, mst_edges: &[(usize, usize)], root: usize) -> Self {
        Self::from_mst_with_head_vars(graph, mst_edges, root, None)
    }

    /// Build a rooted JST from MST edges with specified root and head variables
    fn from_mst_with_head_vars(
        graph: &JoinGraph,
        mst_edges: &[(usize, usize)],
        root: usize,
        head_vars: Option<&HashSet<String>>,
    ) -> Self {
        let mut parent = HashMap::new();
        let mut children: HashMap<usize, Vec<usize>> = HashMap::new();

        // Build undirected adjacency from MST edges
        let mut adj: HashMap<usize, Vec<usize>> = HashMap::new();
        for &(u, v) in mst_edges {
            adj.entry(u).or_default().push(v);
            adj.entry(v).or_default().push(u);
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
                        children.entry(node).or_default().push(neighbor);
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
                // TODO: verify this condition
                if let Some(node_children) = children.get(&node) {
                    for &child in node_children {
                        stack.push((child, false));
                    }
                }
            }
        }

        // Compute tree depth
        let depth = Self::compute_depth(&children, root);

        // Compute tree-width cost (planning variables model)
        let cost = Self::compute_tree_width(&join_order, graph, head_vars);

        RootedJST {
            root,
            join_order,
            cost,
            depth,
            parent,
            children,
        }
    }

    /// Compute tree-width: at each join step, count the "planning variables"
    /// that are needed for future joins or are head variables.
    ///
    /// The tree-width formula:
    ///   tw = max_i { |accumulated_vars_i & (future_vars_i | head_vars)| }
    ///
    /// Where:
    /// - accumulated_vars_i = all variables seen in join steps 0..=i
    /// - future_vars_i = variables needed by join steps i+1..n
    /// - head_vars = variables needed in the final output (from the rule head / Map projection)
    ///
    /// When head_vars is provided, variables that are neither needed by future joins
    /// nor in the output can be projected away, leading to tighter width estimates.
    /// When head_vars is None, falls back to using all variables (conservative).
    fn compute_tree_width(
        join_order: &[usize],
        graph: &JoinGraph,
        head_vars: Option<&HashSet<String>>,
    ) -> usize {
        if join_order.is_empty() {
            return 0;
        }

        // If no head_vars provided, fall back to all variables (conservative upper bound)
        let all_vars: HashSet<String> = graph
            .nodes
            .iter()
            .flat_map(|n| n.variables.iter().cloned())
            .collect();
        let effective_head_vars = head_vars.unwrap_or(&all_vars);

        let mut accumulated_vars: HashSet<String> = HashSet::new();
        let mut max_width = 0;

        for (step, &node_idx) in join_order.iter().enumerate() {
            if node_idx >= graph.nodes.len() {
                continue;
            }

            // Add variables from this node
            accumulated_vars.extend(graph.nodes[node_idx].variables.iter().cloned());

            // Compute "future" variables: vars that appear in subsequent steps
            let mut future_vars: HashSet<String> = HashSet::new();
            for &future_idx in join_order.iter().skip(step + 1) {
                if future_idx < graph.nodes.len() {
                    future_vars.extend(graph.nodes[future_idx].variables.iter().cloned());
                }
            }

            // Planning variables = accumulated & (future_vars | head_vars)
            // Variables not in future joins AND not in output can be projected away.
            // TODO: verify this condition
            let width = if future_vars.is_empty() {
                // Last step: only head variables matter for the output width
                accumulated_vars
                    .iter()
                    .filter(|v| effective_head_vars.contains(*v))
                    .count()
            } else {
                // Intermediate step: keep vars needed for future joins or final output
                accumulated_vars
                    .iter()
                    .filter(|v| future_vars.contains(*v) || effective_head_vars.contains(*v))
                    .count()
            };

            max_width = max_width.max(width);
        }

        max_width
    }

    /// Compute tree depth (max distance from root to any leaf)
    fn compute_depth(children: &HashMap<usize, Vec<usize>>, node: usize) -> usize {
        match children.get(&node) {
            Some(child_list) if !child_list.is_empty() => {
                1 + child_list
                    .iter()
                    .map(|&c| Self::compute_depth(children, c))
                    .max()
                    .unwrap_or(0)
            }
            _ => 0,
        }
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
        // TODO: verify this condition
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

        // Extract head variables from the top-level operation above the joins.
        // These are the variables that survive to the final result, allowing
        // compute_tree_width to account for early projection.
        let head_vars = Self::extract_head_vars(&ir);
        let head_vars_ref = head_vars.as_ref();

        // Compute MST
        let mst_edges = graph.compute_mst();

        // Find optimal root
        let optimal_jst = self.find_optimal_root(&graph, &mst_edges, head_vars_ref);

        // Rebuild IR with optimal join order
        self.rebuild_ir_with_order(&ir, &graph, &optimal_jst)
    }

    /// Extract head variables from the top-level IR operation above the joins.
    ///
    /// Walks the IR tree looking for Map/FlatMap nodes whose output_schema
    /// tells us which variables survive to the final result. Returns None
    /// if the IR is just a bare join tree (all variables are needed).
    fn extract_head_vars(ir: &IRNode) -> Option<HashSet<String>> {
        match ir {
            // Map projects to a subset - its output_schema is the head variables
            IRNode::Map { output_schema, .. } | IRNode::FlatMap { output_schema, .. } => {
                Some(output_schema.iter().cloned().collect())
            }
            // Distinct/Filter don't change the schema, recurse into child
            IRNode::Distinct { input } | IRNode::Filter { input, .. } => {
                Self::extract_head_vars(input)
            }
            // Aggregate output schema defines what survives
            IRNode::Aggregate { output_schema, .. } => {
                Some(output_schema.iter().cloned().collect())
            }
            // Compute adds columns but doesn't remove - keep looking
            IRNode::Compute { input, .. } => Self::extract_head_vars(input),
            // Join/Scan/etc: no projection above, all vars needed
            _ => None,
        }
    }

    /// Check if IR contains joins
    fn has_joins(ir: &IRNode) -> bool {
        match ir {
            IRNode::Join { .. } => true,
            IRNode::Antijoin { left, right, .. } => Self::has_joins(left) || Self::has_joins(right),
            IRNode::Scan { .. } => false,
            IRNode::HnswScan { .. } => false,
            IRNode::Map { input, .. } => Self::has_joins(input),
            IRNode::Filter { input, .. } => Self::has_joins(input),
            IRNode::Distinct { input } => Self::has_joins(input),
            IRNode::Union { inputs } => inputs.iter().any(Self::has_joins),
            IRNode::Aggregate { input, .. } => Self::has_joins(input),
            IRNode::Compute { input, .. } => Self::has_joins(input),
            IRNode::FlatMap { input, .. } => Self::has_joins(input),
            IRNode::JoinFlatMap { left, right, .. } => {
                Self::has_joins(left) || Self::has_joins(right)
            }
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
            IRNode::HnswScan { .. } => false,
            IRNode::Map { input, .. } => Self::has_antijoin(input),
            IRNode::Filter { input, .. } => Self::has_antijoin(input),
            IRNode::Distinct { input } => Self::has_antijoin(input),
            IRNode::Union { inputs } => inputs.iter().any(Self::has_antijoin),
            IRNode::Aggregate { input, .. } => Self::has_antijoin(input),
            IRNode::Compute { input, .. } => Self::has_antijoin(input),
            IRNode::FlatMap { input, .. } => Self::has_antijoin(input),
            IRNode::JoinFlatMap { left, right, .. } => {
                Self::has_antijoin(left) || Self::has_antijoin(right)
            }
        }
    }

    /// Find the optimal root for the rooted JST
    ///
    /// Tries every node as root and selects the one with minimum tree-width cost.
    /// On ties, prefers lower depth (bushier trees minimize intermediate result sizes).
    fn find_optimal_root(
        &self,
        graph: &JoinGraph,
        mst_edges: &[(usize, usize)],
        head_vars: Option<&HashSet<String>>,
    ) -> RootedJST {
        let mut best_jst: Option<RootedJST> = None;

        for root in 0..graph.nodes.len() {
            let jst = RootedJST::from_mst_with_head_vars(graph, mst_edges, root, head_vars);

            match &best_jst {
                None => best_jst = Some(jst),
                Some(current_best) => {
                    // Prefer lower cost; on tie, prefer lower depth (bushier tree)
                    if jst.cost < current_best.cost
                        || (jst.cost == current_best.cost && jst.depth < current_best.depth)
                    {
                        best_jst = Some(jst);
                    }
                }
            }
        }

        best_jst
            .unwrap_or_else(|| RootedJST::from_mst_with_head_vars(graph, mst_edges, 0, head_vars))
    }

    /// Rebuild IR with the optimal join order
    fn rebuild_ir_with_order(
        self,
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
                // TODO: verify this condition
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
    #[allow(
        unknown_lints,
        clippy::only_used_in_recursion,
        clippy::self_only_used_in_recursion
    )]
