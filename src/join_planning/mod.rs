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

