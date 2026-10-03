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

use crate::contract::{Checked, Violation};

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
                println!("XFAIL [{plan_item}] {summary}: {violation}");
            }
            Err(violation) => panic!(
                "[{plan_item}] expected failure '{summary}' hit an unrelated violation: {violation}"
            ),
            Ok(()) if self.reproduction == Reproduction::Racy => println!(
                "NOT REPRODUCED [{plan_item}] {summary}: racy defect not observed this run"
            ),
            Ok(()) => panic!(
                "XPASS [{plan_item}] {summary}: the contract now holds. Remove this \
                 known-defect marker so the scenario becomes a required pass."
            ),
        }
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
}
