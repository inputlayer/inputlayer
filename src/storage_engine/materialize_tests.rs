//! Auto-materialization must serve exactly what recomputation would.

#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::Config;
use crate::value::Value;
use tempfile::TempDir;

const RULES: &[&str] = &[
    "path(X, Y) <- edge(X, Y)",
    "path(X, Z) <- path(X, Y), edge(Y, Z)",
    "sym(X, Y) <- edge(Y, X)",
    "hop(X, Z) <- path(X, Y), sym(Y, Z)",
    "src(X) <- edge(X, Y)",
    "leaf(X) <- edge(Y, X), !src(X)",
    "fanout(X, count<Y>) <- edge(X, Y)",
];

const QUERIES: &[&str] = &[
    "q(X, Y) <- path(X, Y)",
    "q(X, Y) <- sym(X, Y)",
    "q(X, Y) <- hop(X, Y)",
    "q(X) <- src(X)",
    "q(X) <- leaf(X)",
    "q(X, N) <- fanout(X, N)",
];

fn storage(temp: &TempDir, materialize: bool) -> StorageEngine {
    let mut config = Config::default();
    config.storage.data_dir = temp.path().to_path_buf();
    let mut storage = StorageEngine::new(config).unwrap();
    storage.use_knowledge_graph("default").unwrap();
    if materialize {
        let kg = storage.knowledge_graphs.get("default").unwrap();
        let mut kg = kg.write();
        kg.enable_incremental().unwrap();
        kg.set_auto_materialize(true);
    }
    storage
}

fn edge(a: i64, b: i64) -> Tuple {
    Tuple::new(vec![Value::Int64(a), Value::Int64(b)])
}

fn results(storage: &StorageEngine, query: &str) -> Vec<String> {
    let mut out: Vec<String> = match storage.execute_query_with_rules_tuples(query) {
        Ok(tuples) => tuples.iter().map(|t| format!("{t:?}")).collect(),
        Err(e) => vec![format!("error: {e}")],
    };
    out.sort();
    out
}

fn register(storage: &StorageEngine, text: &str) {
    let def = crate::statement::parse_rule_definition(text).unwrap();
    storage.register_rule(&def).unwrap();
}

fn materialized(storage: &StorageEngine) -> HashSet<String> {
    let kg = storage.knowledge_graphs.get("default").unwrap();
    let kg = kg.read();
    let manager = kg.incremental().unwrap().derived_relations();
    let names = manager.lock().get_materialized_relation_names();
    names
}

/// xorshift64, so failures replay from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

#[test]
fn test_auto_materialize_matches_recomputation() {
    for seed in 1..=4u64 {
        let (plain_dir, mat_dir) = (TempDir::new().unwrap(), TempDir::new().unwrap());
        let plain = storage(&plain_dir, false);
        let mat = storage(&mat_dir, true);
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let mut edges: Vec<(i64, i64)> = Vec::new();
        let mut registered: Vec<&str> = Vec::new();

        for step in 0..25 {
            match rng.next(10) {
                0..=4 => {
                    let e = (rng.next(6) as i64, rng.next(6) as i64);
                    for s in [&plain, &mat] {
                        s.insert_tuples("edge", vec![edge(e.0, e.1)]).unwrap();
                    }
                    if !edges.contains(&e) {
                        edges.push(e);
                    }
                }
                5 | 6 if !edges.is_empty() => {
                    let e = edges.swap_remove(rng.next(edges.len() as u64) as usize);
                    for s in [&plain, &mat] {
                        s.delete_tuples_from("default", "edge", vec![edge(e.0, e.1)])
                            .unwrap();
                    }
                }
                7 | 8 => {
                    let rule = RULES[rng.next(RULES.len() as u64) as usize];
                    if !registered.contains(&rule) {
                        registered.push(rule);
                        for s in [&plain, &mat] {
                            register(s, rule);
                        }
                    }
                }
                _ => {
                    let name = ["sym", "src", "hop"][rng.next(3) as usize];
                    let dropped = plain.drop_rule(name).is_ok();
                    assert_eq!(mat.drop_rule(name).is_ok(), dropped);
                    registered.retain(|r| !r.starts_with(&format!("{name}(")));
                }
            }

            let rules: HashSet<String> = plain.list_rules().unwrap().into_iter().collect();
            assert_eq!(materialized(&mat), rules, "seed {seed} step {step}");
            for query in QUERIES {
                assert_eq!(
                    results(&mat, query),
                    results(&plain, query),
                    "seed {seed} step {step}: {query}"
                );
            }
        }
    }
}

#[test]
fn test_auto_materialize_is_off_by_default() {
    let temp = TempDir::new().unwrap();
    let storage = storage(&temp, false);
    {
        let kg = storage.knowledge_graphs.get("default").unwrap();
        kg.write().enable_incremental().unwrap();
    }
    storage.insert_tuples("edge", vec![edge(1, 2)]).unwrap();
    register(&storage, "path(X, Y) <- edge(X, Y)");
    assert!(materialized(&storage).is_empty());
    assert_eq!(results(&storage, "q(X, Y) <- path(X, Y)").len(), 1);
}
