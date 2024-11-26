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

