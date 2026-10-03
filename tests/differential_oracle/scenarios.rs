//! Hand-written histories for the cases incremental maintenance gets wrong.

use crate::generate::queries;
use crate::model::{History, Step};

/// Build a history: `"#name"` is a checkpoint, `"restart"` a restart, and
/// anything else a statement. Observes the generator's query set.
fn script(lines: &[&str]) -> History {
    let steps = lines
        .iter()
        .map(|line| match *line {
            "restart" => Step::Restart,
            l if l.starts_with('#') => Step::Checkpoint(l[1..].to_string()),
            l => Step::Execute(l.to_string()),
        })
        .collect();
    History {
        queries: queries(),
        steps,
    }
}

pub fn all() -> Vec<(&'static str, History)> {
    vec![
        ("duplicate supports", duplicate_supports()),
        ("recursive edge removal", recursive_edge_removal()),
        ("negation", negation()),
        ("aggregates", aggregates()),
        ("rule replacement", rule_replacement()),
        ("restart", restart()),
    ]
}

/// A row derived several ways survives until its last support goes.
fn duplicate_supports() -> History {
    script(&[
        "+two_hop(X, Z) <- edge(X, Y), edge(Y, Z)",
        "+linked(X, Y) <- edge(X, Y)",
        "+linked(X, Y) <- edge(Y, X)",
        // 0 -> 3 through both 1 and 2; 1 <-> 2 linked both ways.
        "+edge[(0, 1), (1, 3), (0, 2), (2, 3), (1, 2), (2, 1)]",
        "+edge(0, 1)",
        "#both-supports",
        "-edge(1, 3)",
        "-edge(1, 2)",
        "#one-support-left",
        "-edge(2, 3)",
        "-edge(2, 1)",
        "#no-support",
        "+edge[(1, 3), (2, 1)]",
        "#reinserted",
    ])
}

/// Removing an edge on a cycle retracts exactly the paths that needed it.
fn recursive_edge_removal() -> History {
    script(&[
        "+reach(X, Y) <- edge(X, Y)",
        "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
        "+edge[(0, 1), (1, 2), (2, 0), (2, 3), (0, 3)]",
        "#cycle",
        "-edge(1, 2)",
        "#cycle-broken",
        "-edge(0, 3)",
        "#tail-only-via-cycle",
        "+edge(1, 2)",
        "#cycle-restored",
        "-edge[(0, 1), (1, 2), (2, 0), (2, 3)]",
        "#empty",
    ])
}

/// Negation over recursion flips rows both ways.
fn negation() -> History {
    script(&[
        "+reach(X, Y) <- edge(X, Y)",
        "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
        "+open(X, Y) <- reach(X, Y), !blocked(Y)",
        "+edge[(0, 1), (1, 2), (2, 3)]",
        "#none-blocked",
        "+blocked(2)",
        "#blocked-2",
        "+blocked(3)",
        "-blocked(2)",
        "#blocked-3",
        "-edge(1, 2)",
        "-blocked(3)",
        "#unblocked-shorter",
    ])
}

/// Aggregates follow inserts and deletes, including groups that empty out.
fn aggregates() -> History {
    script(&[
        "+degree(X, count<Y>) <- edge(X, Y)",
        "+weight_sum(sum<Y>) <- edge(_, Y)",
        "+reach(X, Y) <- edge(X, Y)",
        "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
        "+reach_count(X, count<Y>) <- reach(X, Y)",
        // (0, 2) and (1, 2) share Y = 2: `_` keeps both in the sum.
        "+edge[(0, 1), (0, 2), (1, 2), (2, 3)]",
        "#initial",
        "-edge(0, 1)",
        "#one-removed",
        "-edge[(0, 2), (1, 2), (2, 3)]",
        "#all-groups-empty",
        "+edge(4, 4)",
        "#self-loop",
    ])
}

/// Replacing, clearing and dropping rules changes derived and dependent rows.
fn rule_replacement() -> History {
    script(&[
        "+edge[(0, 1), (1, 2), (2, 0), (3, 1)]",
        "+reach(X, Y) <- edge(X, Y)",
        "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
        "+reach_count(X, count<Y>) <- reach(X, Y)",
        "#recursive",
        ".rule drop reach",
        "+reach(X, Y) <- edge(X, Y), X < Y",
        "#non-recursive",
        ".rule clear reach",
        "#cleared",
        "+reach(X, Y) <- edge(X, Y)",
        "+reach(X, Z) <- edge(X, Y), reach(Y, Z)",
        "#right-recursive",
        ".rule drop degree",
        "+degree(X, count<Y>) <- edge(X, Y)",
        ".rule drop degree",
        "+degree(X, max<Y>) <- edge(X, Y)",
        "#degree-replaced",
        ".rule drop reach_count",
        "#dependent-dropped",
    ])
}

/// Facts and rules survive a restart, and maintenance continues after it.
fn restart() -> History {
    script(&[
        "+reach(X, Y) <- edge(X, Y)",
        "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
        "+open(X, Y) <- reach(X, Y), !blocked(Y)",
        "+edge[(0, 1), (1, 2), (2, 3)]",
        "+blocked(3)",
        "#before",
        "restart",
        "#after",
        "-edge(1, 2)",
        "-blocked(3)",
        "#changed-after",
        "restart",
        "+edge(1, 2)",
        "#after-second",
    ])
}
