//! Seeded random histories over a small graph universe.
//!
//! The universe is chosen to hit the hard cases of incremental maintenance:
//! recursion over cycles (edge removal must retract only unsupported
//! consequences), several derivations of one row (two-hop paths, symmetric
//! clauses), negation over recursion, aggregates over base and recursive
//! relations, rule replacement with a different shape, and restarts.

use std::collections::BTreeSet;

use crate::model::{History, Step};

/// SplitMix64: tiny, fast and stable across platforms and crate versions,
/// so a seed reproduces the same history forever.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const NODES: i64 = 5;

/// Rule definitions by name; each name has interchangeable variants.
pub const RULES: &[(&str, &[&[&str]])] = &[
    (
        "reach",
        &[
            &[
                "+reach(X, Y) <- edge(X, Y)",
                "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
            ],
            &[
                "+reach(X, Y) <- edge(X, Y)",
                "+reach(X, Z) <- edge(X, Y), reach(Y, Z)",
            ],
            &["+reach(X, Y) <- edge(X, Y), X < Y"],
        ],
    ),
    ("two_hop", &[&["+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)"]]),
    (
        "linked",
        &[&["+linked(X, Y) <- edge(X, Y)", "+linked(X, Y) <- edge(Y, X)"]],
    ),
    (
        "open",
        &[
            &["+open(X, Y) <- reach(X, Y), !blocked(Y)"],
            &["+open(X, Y) <- edge(X, Y), !blocked(X)"],
        ],
    ),
    (
        "degree",
        &[
            &["+degree(X, count<Y>) <- edge(X, Y)"],
            &["+degree(X, max<Y>) <- edge(X, Y)"],
        ],
    ),
    (
        "reach_count",
        &[&["+reach_count(X, count<Y>) <- reach(X, Y)"]],
    ),
    ("weight_sum", &[&["+weight_sum(sum<Y>) <- edge(_, Y)"]]),
];

pub fn queries() -> Vec<String> {
    [
        "?edge(X, Y)",
        "?blocked(X)",
        "?reach(X, Y)",
        "?reach(0, Y)",
        "?two_hop(X, Y)",
        "?linked(X, Y)",
        "?open(X, Y)",
        "?degree(X, N)",
        "?reach_count(X, N)",
        "?weight_sum(S)",
    ]
    .iter()
    .map(ToString::to_string)
    .collect()
}

/// Generator-side view of the state, used to pick meaningful deletes and drops.
#[derive(Default)]
struct Model {
    edges: BTreeSet<(i64, i64)>,
    blocked: BTreeSet<i64>,
    rules: BTreeSet<&'static str>,
}

/// A history of `length` steps for `seed`.
pub fn history(seed: u64, length: usize) -> History {
    let mut rng = Rng::new(seed);
    let mut model = Model::default();
    let mut steps = Vec::with_capacity(length + length / 3);
    for i in 0..length {
        steps.extend(step(&mut rng, &mut model));
        if i % 3 == 2 {
            steps.push(Step::Checkpoint(format!("c{}", i / 3)));
        }
    }
    History {
        queries: queries(),
        steps,
    }
}

fn node(rng: &mut Rng) -> i64 {
    rng.below(NODES as usize) as i64
}

fn step(rng: &mut Rng, model: &mut Model) -> Vec<Step> {
    let exec = |s: String| Step::Execute(s);
    match rng.below(20) {
        0..=6 => {
            // One to three edges; some may already exist (duplicate inserts).
            let edges: Vec<(i64, i64)> =
                (0..=rng.below(3)).map(|_| (node(rng), node(rng))).collect();
            model.edges.extend(edges.iter().copied());
            let tuples: Vec<String> = edges.iter().map(|(a, b)| format!("({a}, {b})")).collect();
            vec![exec(format!("+edge[{}]", tuples.join(", ")))]
        }
        7..=11 => {
            // Mostly existing edges; occasionally an absent one.
            let existing: Vec<(i64, i64)> = model.edges.iter().copied().collect();
            let (a, b) = if existing.is_empty() || rng.below(5) == 0 {
                (node(rng), node(rng))
            } else {
                *rng.pick(&existing)
            };
            model.edges.remove(&(a, b));
            vec![exec(format!("-edge({a}, {b})"))]
        }
        12 => {
            let n = node(rng);
            model.blocked.insert(n);
            vec![exec(format!("+blocked({n})"))]
        }
        13 => {
            let n = model
                .blocked
                .iter()
                .copied()
                .next()
                .unwrap_or_else(|| node(rng));
            model.blocked.remove(&n);
            vec![exec(format!("-blocked({n})"))]
        }
        14..=17 => {
            // Define or replace a rule (drop first when the model has it).
            let (name, variants) = *rng.pick(RULES);
            let mut steps = Vec::new();
            if !model.rules.insert(name) {
                steps.push(exec(format!(".rule drop {name}")));
            }
            steps.extend(rng.pick(variants).iter().map(|c| exec((*c).to_string())));
            steps
        }
        18 => {
            let defined: Vec<&str> = model.rules.iter().copied().collect();
            if defined.is_empty() {
                return Vec::new();
            }
            let name = *rng.pick(&defined);
            model.rules.remove(name);
            vec![exec(format!(".rule drop {name}"))]
        }
        _ => vec![Step::Restart],
    }
}
