//! Proofs of bound queries on recursive relations over cyclic data.
//!
//! A bound recursive query is evaluated by Magic Sets under an adorned
//! relation name. Proof search must find the evaluated tuples under the
//! relation's own name: re-deriving them from the rules explores every path
//! around a cycle, and took 32 s (release) for `.why ?path(1, Y)` on the
//! six edges below.

use super::*;
use crate::provenance::proof_tree::{FactSource, NodeKind, ProofTree};

const KG: &str = "recursive_proofs";

/// A cycle 1 -> 2 -> 3 -> 1 with exits 3 -> 4 -> 5 and a chord 2 -> 5.
const EDGES: &str = "+edge[(1, 2), (2, 3), (3, 1), (3, 4), (4, 5), (2, 5)]";
const LEFT: &str = "+path(X, Y) <- edge(X, Y)\n+path(X, Z) <- path(X, Y), edge(Y, Z)";
const RIGHT: &str = "+rpath(X, Y) <- edge(X, Y)\n+rpath(X, Z) <- edge(X, Y), rpath(Y, Z)";

fn handler_with_fixture() -> (Handler, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let mut config = Config::default();
    config.storage.data_dir = tmp.path().to_path_buf();
    let handler = Handler::from_config(config).expect("handler creation failed");
    handler
        .storage
        .read()
        .create_knowledge_graph(KG)
        .expect("knowledge graph creation failed");
    for program in [EDGES, LEFT, RIGHT] {
        execute(&handler, program);
    }
    (handler, tmp)
}

fn execute(handler: &Handler, program: &str) -> QueryResult {
    let result = handler
        .make_query_job()
        .execute(Some(KG.to_string()), program.to_string(), None)
        .expect("program failed");
    assert!(result.errors.is_empty(), "{program}: {:?}", result.errors);
    result
}

/// Asserts `tree` proves `row` of `relation` down to `edge` facts: every
/// rule application has children, and no leaf is truncated or an untraced
/// derived tuple.
fn assert_grounded(tree: &ProofTree, relation: &str, row: &[Value]) {
    let root = &tree.nodes[&tree.roots[0]];
    assert_eq!(root.conclusion.pred, relation);
    assert_eq!(root.conclusion.args, row);
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        match node.kind {
            NodeKind::Rule => assert!(!node.children.is_empty(), "{node:?}"),
            NodeKind::Fact => {
                assert_eq!(node.conclusion.pred, "edge", "{node:?}");
                assert_eq!(node.source, Some(FactSource::Edb), "{node:?}");
            }
            _ => panic!("unexpected proof node {node:?}"),
        }
        stack.extend(node.children.iter().map(|id| &tree.nodes[id]));
    }
}

fn int_rows(result: &QueryResult) -> Vec<Vec<i64>> {
    result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|value| value.as_i64().expect("integer column"))
                .collect()
        })
        .collect()
}

#[test]
fn why_proves_bound_recursive_queries_around_a_cycle() {
    let (handler, _tmp) = handler_with_fixture();
    let reachable = vec![vec![1, 1], vec![1, 2], vec![1, 3], vec![1, 4], vec![1, 5]];
    for (program, relation, rows) in [
        (".why ?path(1, Y)", "path", reachable.clone()),
        (".why full ?path(1, Y)", "path", reachable.clone()),
        (".why ?path(2, 2)", "path", vec![vec![2, 2]]),
        (".why ?rpath(1, Y)", "rpath", reachable),
    ] {
        let result = execute(&handler, program);
        assert_eq!(int_rows(&result), rows, "{program}");
        let trees = result.proof_trees.as_ref().expect("proof trees");
        assert_eq!(trees.len(), rows.len(), "{program}");
        for (tree, row) in trees.iter().zip(&rows) {
            let row: Vec<Value> = row.iter().map(|&v| Value::Int64(v)).collect();
            assert_grounded(tree, relation, &row);
        }
    }
}
