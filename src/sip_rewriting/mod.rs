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
                if base_vars_set.is_disjoint(&prior_vars_set) {
                    continue;
                }

                // Build atom with wildcards for non-shared variables
                let wildcarded = Self::wildcarded_predicate(&atoms[prior_core_id], &base_vars_set);
                sip_body.push(wildcarded);
            }

            // Skip trivial rules (body has only the base atom, no actual filtering)
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
    fn categorize_body(
        rule: &Rule,
    ) -> (Vec<BodyPredicate>, Vec<BodyPredicate>, Vec<BodyPredicate>) {
        let mut atoms = Vec::new();
        let mut negated = Vec::new();
        let mut comparisons = Vec::new();

        for pred in &rule.body {
            match pred {
                BodyPredicate::Positive(_) => atoms.push(pred.clone()),
                BodyPredicate::Negated(_) => negated.push(pred.clone()),
                BodyPredicate::Comparison(_, _, _) => comparisons.push(pred.clone()),
                BodyPredicate::HnswNearest { .. } => comparisons.push(pred.clone()),
            }
        }

        (atoms, negated, comparisons)
    }

    /// Extract positive atoms from a rule
    fn positive_atoms(rule: &Rule) -> Vec<&Atom> {
        rule.body
            .iter()
            .filter_map(|pred| match pred {
                BodyPredicate::Positive(atom) => Some(atom),
                _ => None,
            })
            .collect()
    }

    /// Check if any two positive atoms share variables (indicating a join)
    fn has_shared_variables(atoms: &[&Atom]) -> bool {
        for (i, atom_i) in atoms.iter().enumerate() {
            let vars_i = atom_i.variables();
            for atom_j in &atoms[i + 1..] {
                let vars_j = atom_j.variables();
                if !vars_i.is_disjoint(&vars_j) {
                    return true;
                }
            }
        }
        false
    }

    /// Compute core atom bitmap
    ///
    /// An atom is "core" if its variable set is NOT a strict subset of any other atom.
    /// When two atoms have identical variable sets, the one with the lower index is kept.
    fn compute_core_atom_bitmap(atoms: &[BodyPredicate]) -> Vec<bool> {
        let var_sets: Vec<HashSet<String>> =
            atoms.iter().map(Self::variables_of_predicate).collect();

        let mut is_core = vec![true; atoms.len()];

        for i in 0..atoms.len() {
            for j in 0..atoms.len() {
                if i == j {
                    continue;
                }
                if var_sets[i].is_subset(&var_sets[j]) {
                    if var_sets[i].len() < var_sets[j].len() {
                        // Strict subset: i is non-core
                        is_core[i] = false;
                    // TODO: verify this condition
                    } else if i > j {
                        // Same variables, higher index is non-core
                        is_core[i] = false;
                    }
                }
            }
        }

        is_core
    }

    /// Get variables of a body predicate
    fn variables_of_predicate(pred: &BodyPredicate) -> HashSet<String> {
        pred.variables()
    }

    /// Get unique variables of a predicate (deduplicated, preserving first-occurrence order)
    fn unique_variables_of_predicate(pred: &BodyPredicate) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        match pred {
            BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => {
                for term in &atom.args {
                    if let Term::Variable(v) = term {
                        if seen.insert(v.clone()) {
                            result.push(v.clone());
                        }
                    }
                }
            }
            _ => {
                for v in pred.variables() {
                    if seen.insert(v.clone()) {
                        result.push(v);
                    }
                }
            }
        }

        result
    }

    /// Get the relation name from a body predicate
    fn predicate_relation_name(pred: &BodyPredicate) -> String {
        match pred {
            BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => atom.relation.clone(),
            _ => "_cmp".to_string(),
        }
    }

    /// Create a copy of a predicate with non-shared variables replaced by wildcards
    fn wildcarded_predicate(pred: &BodyPredicate, shared_vars: &HashSet<&String>) -> BodyPredicate {
        match pred {
            BodyPredicate::Positive(atom) => {
                let new_args: Vec<Term> = atom
                    .args
                    .iter()
                    .map(|term| match term {
                        Term::Variable(v) if !shared_vars.contains(v) => Term::Placeholder,
                        other => other.clone(),
                    })
                    .collect();
                BodyPredicate::Positive(Atom::new(atom.relation.clone(), new_args))
            }
            // For non-atom predicates, return as-is
            other => other.clone(),
        }
    }
}

impl Default for SipRewriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Atom, BodyPredicate, ComparisonOp, Program, Rule, Term};

    fn var(name: &str) -> Term {
        Term::Variable(name.to_string())
    }

    fn atom(rel: &str, args: Vec<Term>) -> Atom {
        Atom::new(rel.to_string(), args)
    }

    fn pos(a: Atom) -> BodyPredicate {
        BodyPredicate::Positive(a)
    }

    fn neg(a: Atom) -> BodyPredicate {
        BodyPredicate::Negated(a)
    }

    fn cmp(left: Term, op: ComparisonOp, right: Term) -> BodyPredicate {
        BodyPredicate::Comparison(left, op, right)
    }

    #[test]
    fn test_single_atom_rule_unchanged() {
        let mut rewriter = SipRewriter::new();
        let rule = Rule::new(
            atom("result", vec![var("X")]),
            vec![pos(atom("r", vec![var("X")]))],
        );
        let program = Program {
            rules: vec![rule.clone()],
        };
        let result = rewriter.rewrite_program(&program);

        assert_eq!(result.rules.len(), 1);
        assert_eq!(result.rules[0].head.relation, "result");
    }

    #[test]
    fn test_two_way_join_produces_sip_rules() {
        let mut rewriter = SipRewriter::new();
        // result(X, Z) :- R(X, Y), S(Y, Z).
        let rule = Rule::new(
            atom("result", vec![var("X"), var("Z")]),
            vec![
                pos(atom("R", vec![var("X"), var("Y")])),
                pos(atom("S", vec![var("Y"), var("Z")])),
            ],
        );
        let program = Program { rules: vec![rule] };
        let result = rewriter.rewrite_program(&program);

        // Should produce forward rules + backward rules + final rule
        // With 2 core atoms, forward may generate 1 non-trivial rule,
        // backward may generate 1 non-trivial rule, plus the final rule
        assert!(
            result.rules.len() >= 2,
            "Expected SIP rules, got {} rules",
            result.rules.len()
        );

        // The last rule should have head "result"
        let last = result.rules.last().unwrap();
        assert_eq!(last.head.relation, "result");
    }

    #[test]
    fn test_three_way_join_produces_sip_rules() {
        let mut rewriter = SipRewriter::new();
        // result(X, W) :- R(X, Y), S(Y, Z), T(Z, W).
        let rule = Rule::new(
            atom("result", vec![var("X"), var("W")]),
            vec![
                pos(atom("R", vec![var("X"), var("Y")])),
                pos(atom("S", vec![var("Y"), var("Z")])),
                pos(atom("T", vec![var("Z"), var("W")])),
            ],
        );
        let program = Program { rules: vec![rule] };
        let result = rewriter.rewrite_program(&program);

        // With 3 core atoms, expect more SIP rules
        assert!(
            result.rules.len() >= 3,
            "Expected at least 3 rules, got {}",
            result.rules.len()
        );

        // Last rule should still be "result"
        let last = result.rules.last().unwrap();
        assert_eq!(last.head.relation, "result");
    }

    #[test]
    fn test_no_shared_variables_unchanged() {
        let mut rewriter = SipRewriter::new();
        // result(X, Z) :- R(X, Y), S(A, B). (no shared variables)
        let rule = Rule::new(
            atom("result", vec![var("X"), var("Z")]),
            vec![
                pos(atom("R", vec![var("X"), var("Y")])),
                pos(atom("S", vec![var("A"), var("B")])),
            ],
        );
        let program = Program { rules: vec![rule] };
        let result = rewriter.rewrite_program(&program);

        assert_eq!(result.rules.len(), 1);
    }

    #[test]
    fn test_core_atom_bitmap() {
        // R(X, Y), S(Y, Z), T(Y) - T has vars {Y} subset of {Y, Z} of S, so T is non-core
        let atoms = vec![
            pos(atom("R", vec![var("X"), var("Y")])),
            pos(atom("S", vec![var("Y"), var("Z")])),
            pos(atom("T", vec![var("Y")])),
        ];
        let bitmap = SipRewriter::compute_core_atom_bitmap(&atoms);

        assert!(bitmap[0], "R should be core");
        assert!(bitmap[1], "S should be core");
        assert!(!bitmap[2], "T should be non-core (subset of S)");
    }

    #[test]
    fn test_wildcarded_predicate() {
        let pred = pos(atom("R", vec![var("X"), var("Y"), var("Z")]));
        let x = "X".to_string();
        let z = "Z".to_string();
        let shared: HashSet<&String> = [&x, &z].into_iter().collect();

        let result = SipRewriter::wildcarded_predicate(&pred, &shared);

        if let BodyPredicate::Positive(a) = result {
            assert_eq!(a.args[0], var("X"));
            assert_eq!(a.args[1], Term::Placeholder);
            assert_eq!(a.args[2], var("Z"));
        } else {
            panic!("Expected positive atom");
        }
    }

