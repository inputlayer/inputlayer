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
