//! Fixed-seed graph datasets and their independently computed answers.
//!
//! Fixtures check every measured result against these answers, so a faster
//! but wrong candidate cannot pass.

use std::collections::{BTreeSet, VecDeque};

/// Largest tuple count per insert statement (the server default cap is 10K).
const INSERT_CHUNK: usize = 5_000;

/// A directed graph of distinct `edge(src, dst)` facts over nodes `1..=nodes`.
#[derive(Debug, Clone)]
pub struct Graph {
    edges: BTreeSet<(u64, u64)>,
}

impl Graph {
    /// `edges` distinct random edges, no self-loops, seeded by `seed`.
    pub fn random(nodes: u64, edges: usize, seed: u64) -> Self {
        assert!(nodes > 1, "need at least two nodes");
        let mut rng = SplitMix64::new(seed);
        let mut set = BTreeSet::new();
        while set.len() < edges {
            let src = rng.below(nodes) + 1;
            let dst = rng.below(nodes) + 1;
            if src != dst {
                set.insert((src, dst));
            }
        }
        Self { edges: set }
    }

    /// Add `edge(src, dst)`.
    pub fn add(&mut self, src: u64, dst: u64) {
        self.edges.insert((src, dst));
    }

    /// Insert statements loading every edge into `relation`.
    pub fn insert_programs(&self, relation: &str) -> Vec<String> {
        let edges: Vec<_> = self.edges.iter().collect();
        edges
            .chunks(INSERT_CHUNK)
            .map(|chunk| {
                let tuples: Vec<String> =
                    chunk.iter().map(|(s, d)| format!("({s}, {d})")).collect();
                format!("+{relation}[{}]", tuples.join(", "))
            })
            .collect()
    }

    /// Number of `dst` with `edge(src, dst)`.
    pub fn out_degree(&self, src: u64) -> usize {
        self.successors(src).count()
    }

    /// Nodes reachable from `src` by one or more edges (`reach(src, Y)`).
    pub fn reachable(&self, src: u64) -> usize {
        let mut seen = BTreeSet::new();
        let mut queue = VecDeque::from([src]);
        while let Some(node) = queue.pop_front() {
            for &(_, next) in self.successors(node) {
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
        seen.len()
    }

    /// Distinct `z` with `edge(src, y), edge(y, z)` (`two_hop(src, Z)`).
    pub fn two_hop(&self, src: u64) -> usize {
        self.successors(src)
            .flat_map(|&(_, mid)| self.successors(mid))
            .map(|&(_, dst)| dst)
            .collect::<BTreeSet<_>>()
            .len()
    }

    /// Distinct `(x, z)` with `two_hop(x, z), edge(z, x)`.
    pub fn closed_two_hops(&self) -> usize {
        self.edges
            .iter()
            .flat_map(|&(x, y)| self.successors(y).map(move |&(_, z)| (x, z)))
            .filter(|&(x, z)| self.edges.contains(&(z, x)))
            .collect::<BTreeSet<_>>()
            .len()
    }

    fn successors(&self, node: u64) -> std::collections::btree_set::Range<'_, (u64, u64)> {
        self.edges.range((node, 0)..=(node, u64::MAX))
    }
}

/// Deterministic SplitMix64; datasets must not change when a dependency
/// changes its generator.
#[derive(Debug, Clone)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound` (`bound > 0`).
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }
}

/// Transitive-closure rules over `edge`.
pub const REACH_RULES: [&str; 2] = [
    "+reach(X, Y) <- edge(X, Y)",
    "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
];

/// Two-hop rule over `edge`.
pub const TWO_HOP_RULE: &str = "+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)";

#[cfg(test)]
mod tests {
    use super::*;

    fn line() -> Graph {
        let mut g = Graph {
            edges: BTreeSet::new(),
        };
        for (s, d) in [(1, 2), (2, 3), (3, 4), (2, 5)] {
            g.add(s, d);
        }
        g
    }

    #[test]
    fn answers_match_hand_computed_values() {
        let g = line();
        assert_eq!(g.out_degree(2), 2);
        assert_eq!(g.reachable(1), 4);
        assert_eq!(g.reachable(4), 0);
        assert_eq!(g.two_hop(1), 2);
        assert_eq!(g.closed_two_hops(), 0);
    }

    #[test]
    fn cycles_reach_their_source() {
        let mut g = line();
        g.add(4, 1);
        assert_eq!(g.reachable(1), 5);
        g.add(3, 1);
        // 1->2->3->1 closes (1, 3), (2, 1) and (3, 2); 2->3->4 with 4->1 does not.
        assert_eq!(g.closed_two_hops(), 3);
    }

    #[test]
    fn random_graph_is_reproducible_and_exact() {
        let a = Graph::random(100, 300, 42);
        let b = Graph::random(100, 300, 42);
        assert_eq!(a.edges, b.edges);
        assert_eq!(a.edges.len(), 300);
        assert!(a.edges.iter().all(|&(s, d)| s != d && s >= 1 && d <= 100));
    }

    #[test]
    fn insert_programs_cover_every_edge_in_chunks() {
        let g = Graph::random(5_000, 12_000, 1);
        let programs = g.insert_programs("edge");
        assert_eq!(programs.len(), 3);
        let tuples: usize = programs.iter().map(|p| p.matches('(').count()).sum();
        assert_eq!(tuples, 12_000);
    }
}
