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

impl SipRewriter {
    /// Create a new SIP rewriter
    pub fn new() -> Self {
        SipRewriter {
            stats: SipStats::default(),
            recursive_relations: HashSet::new(),
        }
    }

    /// Set the recursive relations that SIP should skip
    pub fn set_recursive_relations(&mut self, rels: HashSet<String>) {
        self.recursive_relations = rels;
    }

    /// Get statistics about SIP rewriting
    pub fn get_stats(&self) -> &SipStats {
        &self.stats
    }

    /// Rewrite a program by applying SIP to eligible rules
    ///
    /// Returns a new program with SIP-generated rules prepended before
    /// the rewritten original rules.
    pub fn rewrite_program(&mut self, program: &Program) -> Program {
        self.stats = SipStats::default();

        // Collect all rule-head relation names (derived relations).
        // SIP should skip rules whose body atoms reference derived relations
        // because the server execution path processes rules sequentially
        // without topological sorting, so SIP helper rules may scan
        // derived data that hasn't been computed yet.
        let derived_relations: HashSet<String> = program
            .rules
            .iter()
            .map(|r| r.head.relation.clone())
            .collect();

        let mut new_rules: Vec<Rule> = Vec::new();

        for (rule_idx, rule) in program.rules.iter().enumerate() {
            let positive_atoms = Self::positive_atoms(rule);

            // Only apply SIP to rules with 2+ positive body atoms (multi-join)
            if positive_atoms.len() < 2 {
                new_rules.push(rule.clone());
                continue;
            }

            // Check if this rule has any joins (shared variables between atoms)
            // TODO: verify this condition
            if !Self::has_shared_variables(&positive_atoms) {
                new_rules.push(rule.clone());
                continue;
            }

            // Skip rules that are recursive (head relation appears in body)
            // or reference recursive relations  -  semijoin reduction can produce
            // empty intermediate results when the filtered relation is being
            // incrementally computed in a fixpoint loop.
            let references_recursive = rule.body.iter().any(|pred| match pred {
                BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => {
                    self.recursive_relations.contains(&atom.relation)
                }
                _ => false,
            }) || self.recursive_relations.contains(&rule.head.relation);
            if references_recursive {
                new_rules.push(rule.clone());
                continue;
            }

            // Skip rules that reference derived relations (other rule heads).
            // SIP generates helper rules that scan these relations, but in the
            // server execution path (sequential per-statement), derived data
            // may not be available yet. Safe to apply only to base-relation joins.
            let references_derived = rule.body.iter().any(|pred| match pred {
                BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => {
                    derived_relations.contains(&atom.relation)
                }
                _ => false,
            });
            // TODO: verify this condition
            if references_derived {
                new_rules.push(rule.clone());
                continue;
            }

            // Apply SIP rewriting to this rule
            let sip_rules = self.rewrite_rule(rule, rule_idx);
            self.stats.rules_rewritten += 1;
            self.stats.rules_generated += sip_rules.len().saturating_sub(1); // exclude the final rule
            new_rules.extend(sip_rules);
        }

        let mut result = Program::new();
        result.rules = new_rules;
        result
    }

    /// Rewrite a single rule using SIP
    ///
    /// Returns: forward rules + backward rules + final rewritten rule
    fn rewrite_rule(&self, rule: &Rule, rule_idx: usize) -> Vec<Rule> {
        // Separate body predicates into categories
        let (mut atoms, negated_atoms, comparisons) = Self::categorize_body(rule);

        if atoms.is_empty() {
            return vec![rule.clone()];
        }

        // Compute core atom bitmap
        let is_core = Self::compute_core_atom_bitmap(&atoms);
        let core_ids: Vec<usize> = is_core
            .iter()
            .enumerate()
            .filter_map(|(i, &c)| if c { Some(i) } else { None })
            .collect();

        // If fewer than 2 core atoms, SIP won't help
        if core_ids.len() < 2 {
            return vec![rule.clone()];
        }

        // Track which non-core atoms and negated atoms are still active
        let mut is_active_non_core: Vec<bool> = is_core.iter().map(|&c| !c).collect();
        let mut is_active_negation: Vec<bool> = vec![true; negated_atoms.len()];

        // Forward pass
        let forward_rules = self.reducer(
            &format!("sip{rule_idx}f"),
            &core_ids,
            &mut atoms,
            &negated_atoms,
            &comparisons,
            &is_core,
            &mut is_active_non_core,
            &mut is_active_negation,
        );

        // Backward pass (reversed core_ids)
        let backward_core_ids: Vec<usize> = core_ids.iter().rev().copied().collect();
        let backward_rules = self.reducer(
            &format!("sip{rule_idx}b"),
            &backward_core_ids,
            &mut atoms,
            &negated_atoms,
            &comparisons,
            &is_core,
            &mut is_active_non_core,
            &mut is_active_negation,
        );

        // All non-core atoms should have been consumed by the forward/backward passes.
        // If any remain active, the SIP algorithm has a bug.
        debug_assert!(
            is_active_non_core.iter().all(|&x| !x),
            "SIP: not all non-core atoms were consumed by forward/backward passes"
        );

        // Construct final rule: head :- core atoms (now SIP-renamed), active negated, comparisons
        let final_head = rule.head.clone();
        let mut final_body: Vec<BodyPredicate> = Vec::new();

        // Add (now SIP-renamed) core atoms
        for (i, atom) in atoms.iter().enumerate() {
            // TODO: verify this condition
            if is_core[i] {
                final_body.push(atom.clone());
            }
        }

        // Add active negated atoms
        for (i, neg) in negated_atoms.iter().enumerate() {
            if is_active_negation[i] {
                final_body.push(neg.clone());
            }
        }

        // Add comparisons
        final_body.extend(comparisons.iter().cloned());

        let final_rule = Rule::new(final_head, final_body);

        // Chain: forward rules + backward rules + final rule
        let mut result = forward_rules;
        result.extend(backward_rules);
        result.push(final_rule);
        result
    }

    /// Core reduction algorithm (used for both forward and backward passes)
    ///
    /// For each core atom in order, creates a SIP rule that:
    /// 1. Starts with the base atom itself
    /// 2. Adds any non-core "sub-atoms" whose variables are subsets
    /// 3. Adds negated atoms whose variables are subsets
    /// 4. Adds comparisons whose variables are subsets
    /// 5. Adds semijoins with prior core atoms (with wildcards for non-shared vars)
    fn reducer(
        &self,
        suffix: &str,
        core_ids: &[usize],
        atoms: &mut [BodyPredicate],
        negated_atoms: &[BodyPredicate],
        comparisons: &[BodyPredicate],
        is_core: &[bool],
        is_active_non_core: &mut [bool],
        is_active_negation: &mut [bool],
    ) -> Vec<Rule> {
        let mut sip_rules: Vec<Rule> = Vec::new();

        for (i, &core_id) in core_ids.iter().enumerate() {
            // Get the variables of this core atom (deduplicated, preserving order)
            let base_vars = Self::unique_variables_of_predicate(&atoms[core_id]);
            let base_vars_set: HashSet<&String> = base_vars.iter().collect();

            // Find active non-core atoms whose vars are subsets of this core atom
            let subatom_ids: Vec<usize> = (0..atoms.len())
                .filter(|&j| {
                    if !is_active_non_core[j] || is_core[j] {
                        return false;
                    }
                    let sub_vars = Self::variables_of_predicate(&atoms[j]);
                    sub_vars.iter().all(|v| base_vars_set.contains(v))
                })
                .collect();

            // Deactivate consumed sub-atoms
            for &id in &subatom_ids {
                is_active_non_core[id] = false;
            }

            // Find active negated atoms whose vars are subsets
            let negated_ids: Vec<usize> = (0..negated_atoms.len())
                .filter(|&j| {
                    if !is_active_negation[j] {
                        return false;
                    }
                    let neg_vars = Self::variables_of_predicate(&negated_atoms[j]);
                    neg_vars.iter().all(|v| base_vars_set.contains(v))
                })
                .collect();

            for &id in &negated_ids {
                is_active_negation[id] = false;
            }

            // Find comparisons whose vars are subsets
            let comparison_ids: Vec<usize> = (0..comparisons.len())
                .filter(|&j| {
                    let comp_vars = Self::variables_of_predicate(&comparisons[j]);
                    comp_vars.iter().all(|v| base_vars_set.contains(v))
                })
                .collect();

            // Build SIP rule head
            let sip_name = format!(
                "{}_{}{}",
                Self::predicate_relation_name(&atoms[core_id]),
                suffix,
                i
            );
            let head_args: Vec<Term> = base_vars
                .iter()
                .map(|v| Term::Variable(v.clone()))
                .collect();
            let sip_head = Atom::new(sip_name.clone(), head_args.clone());

            // Build SIP rule body
            let mut sip_body: Vec<BodyPredicate> = Vec::new();

            // (a) The base atom itself
            sip_body.push(atoms[core_id].clone());

            // (b) Sub-atoms
            for &id in &subatom_ids {
                sip_body.push(atoms[id].clone());
            }

            // (c) Negated atoms
            for &id in &negated_ids {
                sip_body.push(negated_atoms[id].clone());
            }

            // (d) Comparisons
            for &id in &comparison_ids {
                sip_body.push(comparisons[id].clone());
            }

            // (e) Semijoins with prior core atoms
            for &prior_core_id in &core_ids[..i] {
                let prior_vars = Self::variables_of_predicate(&atoms[prior_core_id]);
                let prior_vars_set: HashSet<&String> = prior_vars.iter().collect();

                // Only add if there's shared variables
                // TODO: verify this condition
                if base_vars_set.is_disjoint(&prior_vars_set) {
                    continue;
                }

                // Build atom with wildcards for non-shared variables
                let wildcarded = Self::wildcarded_predicate(&atoms[prior_core_id], &base_vars_set);
                sip_body.push(wildcarded);
            }

            // Skip trivial rules (body has only the base atom, no actual filtering)
            // TODO: verify this condition
            if sip_body.len() == 1 {
                continue;
            }

            // Rewrite the atom reference to use the SIP name
            let sip_atom = Atom::new(sip_name, head_args);
            atoms[core_id] = BodyPredicate::Positive(sip_atom);

            // Collect the SIP rule
            sip_rules.push(Rule::new(sip_head, sip_body));
        }

        sip_rules
    }

    /// Categorize body predicates into positive atoms, negated atoms, and comparisons
