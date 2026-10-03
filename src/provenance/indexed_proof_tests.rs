//! Indexed proof search must build exactly the proofs a linear scan builds.
//!
//! Each proof is built twice from the same snapshot and evaluation: once with
//! every lookup answered by a linear scan (the behaviour before bound-column
//! indexes), and once with lookups indexed from the first, or after a few,
//! scans. The serialized DAGs, node ids included, must be equal.

use crate::provenance::backward_chaining::{build_proof_tree, ProofContext};
use crate::provenance::why_not::explain_why_not;
use crate::provenance::ProofConfig;
use crate::storage_engine::KnowledgeGraphSnapshot;
use crate::value::{Tuple, Value};
use std::collections::HashMap;

/// Index policies compared against scanning only.
const INDEXED: [usize; 3] = [0, 1, 4];
const SCAN_ONLY: usize = usize::MAX;

const RULES: &str = "
path(X, Y) <- edge(X, Y)
path(X, Z) <- path(X, Y), edge(Y, Z)
flagged(X, Y) <- path(X, Y), risk(Y)
safe(X) <- node(X), !risk(X)
labelled(X, L) <- node(X), label(X, L), score(X, S), S >= 0.0
total(X, sum<S>) <- score(X, S)
reach_count(X, count<Y>) <- path(X, Y)
hop(X, Z) <- node(X), edge(X, Z)
";

/// Nodes of the test graph. Proof search on left-recursive rules is
/// exponential in path length, so the graph stays small.
const NODES: i32 = 10;

fn int(n: i32) -> Value {
    Value::Int32(n)
}

/// A graph with cycles and several paths between most node pairs; mixed
/// integer widths, signed zeros and strings in bound columns.
fn snapshot() -> KnowledgeGraphSnapshot {
    let mut edges = Vec::new();
    for i in 0..NODES {
        edges.push(vec![int(i), int((i + 1) % NODES)]);
        edges.push(vec![int(i), int((i * 7 + 3) % NODES)]);
        if i % 3 == 0 {
            edges.push(vec![int(i), int((i + 2) % NODES)]);
        }
    }
    edges.push(vec![Value::Int64(4), Value::Int64(9)]);
    let mut data: HashMap<String, Vec<Tuple>> = HashMap::new();
    let mut put = |name: &str, rows: Vec<Vec<Value>>| {
        data.insert(name.to_string(), rows.into_iter().map(Tuple::new).collect());
    };
    put("edge", edges);
    put("node", (0..NODES).map(|i| vec![int(i)]).collect());
    put(
        "risk",
        (0..NODES).step_by(4).map(|i| vec![int(i)]).collect(),
    );
    put(
        "label",
        (0..NODES)
            .map(|i| vec![int(i), Value::string(&format!("n{}", i % 6))])
            .collect(),
    );
    put(
        "score",
        (0..NODES)
            .flat_map(|i| {
                let zero = if i % 2 == 0 { -0.0 } else { 0.0 };
                [
                    vec![int(i), Value::Float64(zero)],
                    vec![int(i), Value::Float64(f64::from(i) / 4.0)],
                ]
            })
            .collect(),
    );
    let rules = crate::parser::parse_program(RULES)
        .expect("rules parse")
        .rules;
    KnowledgeGraphSnapshot::new(data, rules)
}

/// Proof DAGs of every result of `query`, built with one context.
fn proofs(
    snapshot: &KnowledgeGraphSnapshot,
    relation: &str,
    query: &str,
    full_mode: bool,
    scans: usize,
) -> Vec<serde_json::Value> {
    let (mut results, derived) = snapshot
        .execute_with_rules_tuples_and_derived(query)
        .expect("query evaluates");
    results.sort();
    assert!(!results.is_empty(), "{query} has results");
    let config = ProofConfig {
        full_mode,
        ..ProofConfig::default()
    };
    let ctx = ProofContext::new(&snapshot.rules, &snapshot.input_tuples, config)
        .with_derived_data(&derived)
        .with_scans_before_index(scans);
    results
        .iter()
        .map(|tuple| {
            let proof = build_proof_tree(relation, tuple, &ctx);
            serde_json::to_value(proof.map(|tree| serde_json::to_value(tree).expect("serializes")))
                .expect("serializes")
        })
        .collect()
}

#[test]
fn indexed_proofs_equal_scanned_proofs() {
    let snapshot = snapshot();
    for (relation, query) in [
        ("path", "path(X, Y) <- path(X, Y)"),
        ("path", "path(X, Y) <- path(3, Y), X = 3"),
        ("flagged", "flagged(X, Y) <- flagged(X, Y)"),
        ("safe", "safe(X) <- safe(X)"),
        ("labelled", "labelled(X, L) <- labelled(X, L)"),
        ("total", "total(X, S) <- total(X, S)"),
        ("reach_count", "reach_count(X, N) <- reach_count(X, N)"),
    ] {
        for full_mode in [false, true] {
            let scanned = proofs(&snapshot, relation, query, full_mode, SCAN_ONLY);
            for scans in INDEXED {
                assert_eq!(
                    proofs(&snapshot, relation, query, full_mode, scans),
                    scanned,
                    "{query} (full: {full_mode}, index after {scans} scans)"
                );
            }
        }
    }
}

#[test]
fn indexed_why_not_equals_scanned_why_not() {
    let snapshot = snapshot();
    let targets = [
        ("path", vec![int(0), int(99)]),
        ("flagged", vec![int(1), int(1)]),
        ("safe", vec![int(4)]),
        ("safe", vec![int(99)]),
        ("labelled", vec![int(2), Value::string("n5")]),
        ("node", vec![int(1)]),
        // Bound Int32 values against the Int64 `edge(4, 9)`.
        ("hop", vec![int(4), int(9)]),
    ];
    let explain = |scans: usize| -> Vec<serde_json::Value> {
        let ctx = ProofContext::new(
            &snapshot.rules,
            &snapshot.input_tuples,
            ProofConfig::default(),
        )
        .with_scans_before_index(scans);
        targets
            .iter()
            .map(|(relation, values)| {
                let tree = explain_why_not(relation, &Tuple::new(values.clone()), &ctx);
                serde_json::to_value(tree).expect("serializes")
            })
            .collect()
    };
    let scanned = explain(SCAN_ONLY);
    for scans in INDEXED {
        assert_eq!(explain(scans), scanned, "index after {scans} scans");
    }
}
