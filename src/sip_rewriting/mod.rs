//! # SIP Rewriting
//!
//! Sideways Information Passing - semijoin reduction at the AST level.
//!
//! 1. Detect core atoms (variables not a strict subset of any other atom)
//! 2. Forward pass: semijoin-filter each core atom against prior ones
//! 3. Backward pass: same in reverse
//! 4. Final rule: join the fully reduced atoms
//!
//! ## Example
//!
//! Original:
//! ```datalog
//! result(X, Z) :- R(X, Y), S(Y, Z), T(Z, W).
//! ```
//!
//! After SIP (forward pass):
//! ```datalog
//! R_sip0f0(X, Y) :- R(X, Y).
//! S_sip0f1(Y, Z) :- S(Y, Z), R_sip0f0(_, Y).
//! T_sip0f2(Z, W) :- T(Z, W), S_sip0f1(_, Z).
//! ```
//!
//! Then backward pass, then final rule using SIP-renamed atoms.
//!
//! ## Pipeline Position
//!
//! ```text
//! parse(source) -> [SIP Rewriting] -> build_ir() -> optimize_ir() -> execute()
//! ```

use crate::ast::{Atom, BodyPredicate, Program, Rule, Term};
use std::collections::HashSet;

/// Statistics about SIP rewriting
#[derive(Debug, Clone, Default)]
pub struct SipStats {
    /// Number of SIP rules generated
    pub rules_generated: usize,
    /// Number of original rules rewritten
    pub rules_rewritten: usize,
}

/// SIP (Sideways Information Passing) rewriter for Datalog rules
///
/// Operates at the AST level, rewriting rules into semijoin reduction chains
/// before IR building.
pub struct SipRewriter {
    /// Statistics
    stats: SipStats,
    /// Relations involved in recursive computation (SCCs with cycles).
    /// Rules that reference these relations are skipped by SIP because
    /// semijoin reduction can produce empty intermediate results when
    /// the filtered relation is being incrementally computed in a fixpoint loop.
    /// This is a known limitation, not a bug  -  in principle SIP can apply within
    /// recursive strata, but our engine creates fresh CodeGenerators per rule
    /// execution and doesn't support incremental SIP intermediates during fixpoint.
    recursive_relations: HashSet<String>,
}

