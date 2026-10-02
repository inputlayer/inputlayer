//! Magic Sets must not change query results: each case is checked against
//! the same program evaluated with Magic Sets disabled.

use inputlayer::{IQLEngine, OptimizationConfig, Tuple, Value};

const TC: &str = "reach(X, Y) <- edge(X, Y)\n\
                  reach(X, Z) <- reach(X, Y), edge(Y, Z)\n";

fn run(program: &str, magic: bool) -> Vec<Tuple> {
    let config = OptimizationConfig {
        enable_magic_sets: magic,
        ..OptimizationConfig::default()
    };
    let mut engine = IQLEngine::with_config(config);
    engine.add_tuples(
        "edge",
        [(1, 2), (2, 3), (5, 6)]
            .into_iter()
            .map(|(a, b)| Tuple::new(vec![Value::Int64(a), Value::Int64(b)]))
            .collect(),
    );
    let mut rows = engine.execute_tuples(program).expect("query failed");
    rows.sort();
    rows.dedup();
    rows
}

fn rows(expected: &[&[i64]]) -> Vec<Tuple> {
    let mut rows: Vec<Tuple> = expected
        .iter()
        .map(|r| Tuple::new(r.iter().copied().map(Value::Int64).collect()))
        .collect();
    rows.sort();
    rows
}

fn assert_query(program: &str, expected: &[&[i64]]) {
    let with_magic = run(program, true);
    assert_eq!(
        with_magic,
        run(program, false),
        "magic sets changed results"
    );
    assert_eq!(with_magic, rows(expected));
}

#[test]
fn bound_and_unbound_use_of_same_relation() {
    let program = format!("{TC}__query__(_c0, Y, Z) <- reach(_c0, Y), reach(Y, Z), _c0 = 1");
    assert_query(&program, &[&[1, 2, 3]]);
}

#[test]
fn two_constants_on_same_relation() {
    let program =
        format!("{TC}__query__(_c0, Y, _c2, Z) <- reach(_c0, Y), reach(_c2, Z), _c0 = 1, _c2 = 5");
    assert_query(&program, &[&[1, 2, 5, 6], &[1, 3, 5, 6]]);
}

#[test]
fn unbound_use_in_dependent_rule() {
    let program = format!(
        "{TC}fromany(Y) <- reach(X, Y), X != 1\n\
         __query__(_c0, Y) <- reach(_c0, Y), fromany(Y), _c0 = 1"
    );
    assert_query(&program, &[&[1, 3]]);
}

#[test]
fn negated_use_in_dependent_rule() {
    let program = format!(
        "{TC}node(X) <- edge(X, _)\n\
         node(Y) <- edge(_, Y)\n\
         unreached(X, Y) <- node(X), node(Y), !reach(X, Y)\n\
         __query__(_c0, Y) <- reach(_c0, Y), unreached(2, Y), _c0 = 1"
    );
    // reach from 1 = {2, 3}; from 2 only 3 is reached, so 2 is unreached.
    assert_query(&program, &[&[1, 2]]);
}

#[test]
fn single_bound_use_still_correct() {
    let program = format!("{TC}__query__(_c0, Y) <- reach(_c0, Y), _c0 = 1");
    assert_query(&program, &[&[1, 2], &[1, 3]]);
}

#[test]
fn different_adornments_on_same_relation() {
    let program = "friends(X, Y) <- edge(X, Y)\n\
                   friends(X, Y) <- friends(X, Y), edge(X, Y)\n\
                   __query__(_c0, Y, Z, _c1) <- friends(_c0, Y), friends(Z, _c1), _c0 = 1, _c1 = 6";
    assert_query(program, &[&[1, 2, 5, 6]]);
}
