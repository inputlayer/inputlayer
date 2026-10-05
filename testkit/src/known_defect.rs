//! Expected failures for defects the reactive plan already tracks.
//!
//! A known-defect scenario asserts the *correct* contract. While the defect
//! exists the check fails with the defect's own [`Violation`] and the test
//! passes as an expected failure (XFAIL). Any other violation is a real
//! failure, and a passing check fails loudly (XPASS) so the marker is removed
//! and the scenario becomes a required pass when the plan item lands.
//!
//! A [`Reproduction::Racy`] defect depends on thread timing: its probe retries
//! within a bounded budget and a run that does not reproduce it passes with a
//! `NOT REPRODUCED` note instead of XPASS, so the suite never flakes.
//!
//! When [`XFAIL_LOG_ENV`] names a file, every XFAIL and NOT REPRODUCED line
//! is also appended to it, so a run can print the list of open expected
//! failures at its end (`make unit-test`, `make e2e-reactive`).

use std::io::Write;

use crate::contract::{Checked, Violation};

/// Environment variable naming the file expected-failure lines append to.
pub const XFAIL_LOG_ENV: &str = "INPUTLAYER_XFAIL_LOG";

/// Whether a defect shows on every run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reproduction {
    /// Every run observes the violation; a run without it is XPASS.
    Deterministic,
    /// Depends on a race; a run without the violation proves nothing.
    Racy,
}

/// A tracked defect and the violation that identifies it.
#[derive(Debug, Clone, Copy)]
pub struct KnownDefect {
    /// Reactive plan item that fixes it, e.g. `S05`.
    pub plan_item: &'static str,
    pub summary: &'static str,
    /// True for the violation this defect produces.
    pub signature: fn(&Violation) -> bool,
    pub reproduction: Reproduction,
}

impl KnownDefect {
    /// Judge `outcome` of the contract check this defect breaks.
    ///
    /// # Panics
    /// When a deterministic defect's contract holds (XPASS) or the check fails
    /// with a different violation.
    pub fn judge(&self, outcome: Checked<()>) {
        let Self {
            plan_item, summary, ..
        } = self;
        match outcome {
            Err(violation) if (self.signature)(&violation) => {
                record(&format!("XFAIL [{plan_item}] {summary}: {violation}"));
            }
            Err(violation) => panic!(
                "[{plan_item}] expected failure '{summary}' hit an unrelated violation: {violation}"
            ),
            Ok(()) if self.reproduction == Reproduction::Racy => record(&format!(
                "NOT REPRODUCED [{plan_item}] {summary}: racy defect not observed this run"
            )),
            Ok(()) => panic!(
                "XPASS [{plan_item}] {summary}: the contract now holds. Remove this \
                 known-defect marker so the scenario becomes a required pass."
            ),
        }
    }

    /// Judge `outcome` against defects that stand in front of one another:
    /// `defects[0]` is the first one the check runs into, and once it is
    /// fixed the next one's violation shows. The first defect whose signature
    /// matches is the expected failure; when the contract holds, the last
    /// defect judges it (XPASS unless it is racy).
    ///
    /// # Panics
    /// When `defects` is empty, on XPASS, or on a violation none of them
    /// produces.
    pub fn judge_first(defects: &[Self], outcome: Checked<()>) {
        let last = defects.last().expect("at least one known defect");
        let defect = match &outcome {
            Err(violation) => defects
                .iter()
                .find(|defect| (defect.signature)(violation))
                .unwrap_or(last),
            Ok(()) => last,
        };
        defect.judge(outcome);
    }
}

/// Print an expected-failure line and append it to the [`XFAIL_LOG_ENV`] file.
/// This crate's own unit tests only print, so the list holds real XFAILs.
fn record(line: &str) {
    println!("{line}");
    if cfg!(test) {
        return;
    }
    let Some(path) = std::env::var_os(XFAIL_LOG_ENV) else {
        return;
    };
    let appended = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut file| file.write_all(format!("{line}\n").as_bytes()));
    if let Err(e) = appended {
        let path = std::path::Path::new(&path).display();
        eprintln!("cannot append to {XFAIL_LOG_ENV} file {path}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFECT: KnownDefect = KnownDefect {
        plan_item: "X00",
        summary: "test defect",
        signature: |v| matches!(v, Violation::Timeout(_)),
        reproduction: Reproduction::Deterministic,
    };

    #[test]
    fn test_matching_violation_is_expected_failure() {
        DEFECT.judge(Err(Violation::Timeout("delta".into())));
    }

    #[test]
    #[should_panic(expected = "XPASS [X00]")]
    fn test_holding_contract_is_unexpected_pass() {
        DEFECT.judge(Ok(()));
    }

    #[test]
    fn test_racy_defect_not_reproduced_passes() {
        let racy = KnownDefect {
            reproduction: Reproduction::Racy,
            ..DEFECT
        };
        racy.judge(Ok(()));
    }

    #[test]
    #[should_panic(expected = "unrelated violation")]
    fn test_other_violation_is_real_failure() {
        DEFECT.judge(Err(Violation::Transport("closed".into())));
    }

    const LATER: KnownDefect = KnownDefect {
        plan_item: "X01",
        summary: "the defect behind the first one",
        signature: |v| matches!(v, Violation::UnexpectedWork(_)),
        reproduction: Reproduction::Deterministic,
    };

    #[test]
    fn test_first_defect_in_front_is_expected_failure() {
        KnownDefect::judge_first(&[DEFECT, LATER], Err(Violation::Timeout("delta".into())));
    }

    #[test]
    fn test_defect_behind_shows_once_the_first_is_fixed() {
        KnownDefect::judge_first(&[DEFECT, LATER], Err(Violation::UnexpectedWork("x".into())));
    }

    #[test]
    #[should_panic(expected = "XPASS [X01]")]
    fn test_holding_contract_is_unexpected_pass_of_the_last_defect() {
        KnownDefect::judge_first(&[DEFECT, LATER], Ok(()));
    }

    #[test]
    #[should_panic(expected = "[X01] expected failure")]
    fn test_violation_of_no_defect_is_real_failure() {
        KnownDefect::judge_first(&[DEFECT, LATER], Err(Violation::Transport("closed".into())));
    }
}
