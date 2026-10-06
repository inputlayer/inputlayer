//! The scenario suite's shop pack as oracle histories.
//!
//! `Fixture::shop_pack(Size::Small)` is the fixture every catalogue scenario
//! runs on (TESTING.md, Scenario Suite): its rules cover join, comparison,
//! negation, recursion, negation over recursion and an aggregate, the
//! constructs maintained views must get right. Here it is a corpus pack: the
//! pack with writes through each of those constructs and a restart, and the
//! histories of scenario S9 (retraction through recursion and negation),
//! which `tests/scenarios/retraction.rs` runs live from the same steps.

use inputlayer_testkit::{Fixture, Size};

use crate::model::{History, Step};

/// S9's cut of the chain `i1 -> i2 -> i3 -> i4`.
pub const CUT: &str = r#"-link("i2", "i3")"#;
pub const BLOCK_I4: &str = r#"+blocked("i4")"#;
pub const UNBLOCK_I4: &str = r#"-blocked("i4")"#;

/// A retraction scenario on the shop pack: writes before the agent
/// subscribes, then the steps it observes.
pub struct Retraction {
    pub name: &'static str,
    pub setup: &'static [&'static str],
    pub steps: &'static [&'static str],
}

/// S9: the cut, then blocking and unblocking i4.
pub const S9: Retraction = Retraction {
    name: "S9",
    setup: &[],
    steps: &[CUT, BLOCK_I4, UNBLOCK_I4],
};

/// S9b: S9 with a second support for (o-42, i4): o-42's item i5 links to i4.
pub const S9B: Retraction = Retraction {
    name: "S9b",
    setup: &[r#"+link("i5", "i4")"#],
    steps: &[CUT, BLOCK_I4, UNBLOCK_I4],
};

/// What the S9 histories observe: the agent's two subscriptions and the
/// rules behind them.
pub const S9_QUERIES: [&str; 4] = [
    r#"?related("i1", X)"#,
    r#"?offer("o-42", X)"#,
    "?eligible(O, I, W)",
    "?n_eligible(O, N)",
];

/// The pack's rules, whole and for the anchor order `o-42`.
const PACK_QUERIES: [&str; 6] = [
    "?eligible(O, I, W)",
    "?related(I, X)",
    "?offer(O, X)",
    "?n_eligible(O, N)",
    r#"?eligible("o-42", I, W)"#,
    r#"?offer("o-42", X)"#,
];

/// Writes through each construct, a checkpoint after each (`#name`), and a
/// restart: stock an out-of-stock item (comparison, aggregate), claim an
/// offer (negation over recursion), block an eligible item (negation, every
/// rule downstream), restart, unblock it.
const PACK_STEPS: [&str; 11] = [
    r#"-stock("i13", 0)"#,
    r#"+stock("i13", 5)"#,
    "#restocked",
    r#"+claim("o-42", "i7", "a1")"#,
    "#claimed",
    r#"+blocked("i1")"#,
    "#blocked",
    "restart",
    "#restarted",
    r#"-blocked("i1")"#,
    "#unblocked",
];

/// The shop pack corpus: the pack's own history, then S9 and S9b.
pub fn histories() -> Vec<(&'static str, History)> {
    let mut pack = Fixture::shop_pack(Size::Small).statements;
    pack.push("#installed".to_string());
    pack.extend(PACK_STEPS.iter().map(ToString::to_string));
    let mut histories = vec![("shop pack", history(&PACK_QUERIES, &pack))];
    for retraction in [&S9, &S9B] {
        histories.push((retraction.name, s9_history(retraction)));
    }
    histories
}

/// `retraction` on the installed pack, observed after the pack and its setup
/// and after every step.
pub fn s9_history(retraction: &Retraction) -> History {
    let mut statements = Fixture::shop_pack(Size::Small).statements;
    statements.extend(retraction.setup.iter().map(ToString::to_string));
    statements.push("#installed".to_string());
    for (n, step) in retraction.steps.iter().enumerate() {
        statements.push((*step).to_string());
        statements.push(format!("#step-{}", n + 1));
    }
    history(&S9_QUERIES, &statements)
}

/// A history of `lines`: a statement, `#name` (a checkpoint) or `restart`.
fn history(queries: &[&str], lines: &[String]) -> History {
    History {
        queries: queries.iter().map(|q| (*q).to_string()).collect(),
        steps: lines
            .iter()
            .map(|line| match line.as_str() {
                "restart" => Step::Restart,
                l if l.starts_with('#') => Step::Checkpoint(l[1..].to_string()),
                l => Step::Execute(l.to_string()),
            })
            .collect(),
    }
}
