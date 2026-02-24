//! # Magic Sets Transformation
//!
//! Demand-driven rewriting for recursive Datalog queries with bound arguments.
//!
//! When a query like `?reach(1, Y)` is issued against recursive rules, the engine
//! normally computes the full transitive closure and then filters. Magic Sets rewrites
//! the program so that the fixpoint computation is restricted to only tuples demanded
//! by the query's constant bindings.
//!
//! ## Example
//!
//! Original program (after `transform_query_shorthand`):
//! ```datalog
//! reach(X, Y) <- edge(X, Y)
//! reach(X, Z) <- reach(X, Y), edge(Y, Z)
//! __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1
//! ```
//!
//! After Magic Sets rewrite:
//! ```datalog
//! reach_bf(X, Y) <- magic_reach_bf(X), edge(X, Y)
//! reach_bf(X, Z) <- magic_reach_bf(X), reach_bf(X, Y), edge(Y, Z)
//! __query__(_c0, Y) <- reach_bf(_c0, Y), _c0 = 1
//! ```
//!
//! The magic seed `magic_reach_bf = {(1,)}` is injected as input data (not a rule),
//! restricting the fixpoint to only compute reach(1, *).
//!
//! ## Pipeline Position
//!
//! ```text
//! parse(source) -> SIP Rewriting -> [Magic Sets] -> build_ir() -> optimize -> execute
//! ```

use crate::ast::{Atom, BodyPredicate, ComparisonOp, Program, Rule, Term};
use crate::recursion;
use crate::value::{Tuple, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Binding adornment for a relation: `true` = bound, `false` = free
///
/// For `reach^bf`, positions = [true, false] meaning first arg bound, second free.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Adornment {
    pub positions: Vec<bool>,
}

impl Adornment {
    /// Create adornment from bound/free pattern
    pub fn new(positions: Vec<bool>) -> Self {
        Adornment { positions }
    }

    /// Produce the adornment string suffix like "bf", "bb", "fb"
    pub fn suffix(&self) -> String {
        self.positions
            .iter()
            .map(|b| if *b { 'b' } else { 'f' })
            .collect()
    }

    /// True if at least one position is bound
    pub fn has_bound(&self) -> bool {
        self.positions.iter().any(|b| *b)
    }

    /// Get indices of bound positions
    pub fn bound_indices(&self) -> Vec<usize> {
        self.positions
            .iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Info about a detected binding on a recursive relation in the query
#[derive(Debug, Clone)]
pub(crate) struct QueryBinding {
    /// The adornment pattern
    adornment: Adornment,
    /// Map from bound position index to the constant Term from the equality constraint
    bound_constants: Vec<(usize, Term)>,
}

/// Magic Sets rewriter
pub struct MagicSetRewriter;

impl MagicSetRewriter {
    /// Detect query bindings on recursive relations.
    ///
    /// Scans `__query__` rules for equality constraints (`_c0 = 1`) that bind
    /// arguments of recursive body atoms. Returns a map from relation name to
    /// its adornment and bound constants.
    ///
    /// A position is only marked "bound" if the variable at that position is
    /// **invariant** across recursion — i.e., the same variable appears at the
    /// same position in both the head and any recursive body atom. If the variable
    /// changes (like Z→Y in TC's second arg), the position is marked free, since
    /// the magic guard cannot restrict a changing variable.
    pub fn detect_query_bindings(
        program: &Program,
        recursive_relations: &HashSet<String>,
    ) -> HashMap<String, QueryBinding> {
        // Pre-compute which argument positions are invariant across recursion
        // for each recursive relation.
        let invariant_positions = compute_invariant_positions(program, recursive_relations);

        let mut result = HashMap::new();

        for rule in &program.rules {
            if rule.head.relation != "__query__" {
                continue;
            }

            // Collect equality constraints: variable = constant
            let mut var_to_constant: HashMap<String, Term> = HashMap::new();
            for pred in &rule.body {
                if let BodyPredicate::Comparison(left, ComparisonOp::Equal, right) = pred {
                    match (left, right) {
                        (Term::Variable(v), c) if is_ground(c) => {
                            var_to_constant.insert(v.clone(), c.clone());
                        }
                        (c, Term::Variable(v)) if is_ground(c) => {
                            var_to_constant.insert(v.clone(), c.clone());
                        }
                        _ => {}
                    }
                }
            }

            if var_to_constant.is_empty() {
                continue;
            }

            // For each recursive body atom, compute adornment
            for pred in &rule.body {
                if let BodyPredicate::Positive(atom) = pred {
                    if !recursive_relations.contains(&atom.relation) {
                        continue;
                    }

                    let invariants = invariant_positions
                        .get(&atom.relation)
                        .cloned()
                        .unwrap_or_default();

                    let mut adornment_positions = Vec::new();
                    let mut bound_constants = Vec::new();

                    for (i, arg) in atom.args.iter().enumerate() {
                        if let Term::Variable(v) = arg {
                            // Only mark bound if (a) has constant binding AND
                            // (b) position is invariant across recursion
                            if let Some(constant) = var_to_constant.get(v) {
                                if invariants.contains(&i) {
                                    adornment_positions.push(true);
                                    bound_constants.push((i, constant.clone()));
                                } else {
                                    adornment_positions.push(false);
                                }
                            } else {
                                adornment_positions.push(false);
                            }
                        } else {
                            adornment_positions.push(false);
                        }
                    }

                    let adornment = Adornment::new(adornment_positions);
                    if adornment.has_bound() {
                        result.insert(
                            atom.relation.clone(),
                            QueryBinding {
                                adornment,
                                bound_constants,
                            },
                        );
                    }
                }
            }
        }

        result
    }

    /// Rewrite the program with Magic Sets transformation.
    ///
    /// Returns the rewritten program and a map of magic seed relation names to their
    /// seed tuples (to be injected into `input_tuples`).
    pub fn rewrite_program(
        program: &Program,
        bindings: &HashMap<String, QueryBinding>,
    ) -> (Program, HashMap<String, Vec<Tuple>>) {
        let mut new_rules: Vec<Rule> = Vec::new();
        let mut magic_seeds: HashMap<String, Vec<Tuple>> = HashMap::new();

        // Build set of relations being adorned
        let adorned_relations: HashSet<&String> = bindings.keys().collect();

        for rule in &program.rules {
            if adorned_relations.contains(&rule.head.relation) {
                // This rule defines a relation that needs adorning
                let binding = &bindings[&rule.head.relation];
                let adorned_name = adorned_relation_name(&rule.head.relation, &binding.adornment);
                let magic_name = magic_relation_name(&rule.head.relation, &binding.adornment);

                // Build the adorned rule with magic guard
                let adorned_rule = adorn_rule(
                    rule,
                    &adorned_name,
                    &magic_name,
                    binding,
                    &adorned_relations,
                );
                new_rules.push(adorned_rule);

                // Check if we need a magic propagation rule
                if let Some(prop_rule) =
                    generate_magic_propagation_rule(rule, &magic_name, binding, &adorned_relations)
                {
                    new_rules.push(prop_rule);
                }

                // Generate magic seed tuples (only add once per magic relation)
                magic_seeds
                    .entry(magic_name)
                    .or_insert_with(|| vec![build_seed_tuple(&binding.bound_constants)]);
            } else if rule.head.relation == "__query__" {
                // Rewrite __query__ to reference adorned relations
                let rewritten = rewrite_query_rule(rule, bindings);
                new_rules.push(rewritten);
            } else {
                // Non-adorned, non-query rule — check if its body references adorned relations
                // and rename those references too
                let rewritten = rewrite_body_references(rule, bindings);
                new_rules.push(rewritten);
            }
        }

        (Program { rules: new_rules }, magic_seeds)
    }
}

/// Compute which argument positions are invariant across recursion for each
/// recursive relation.
///
/// A position `i` is invariant if, in EVERY recursive rule for that relation,
/// the variable at position `i` of the head is the SAME variable at position `i`
/// of every recursive body atom referencing the same relation.
///
/// Example: `reach(X, Z) <- reach(X, Y), edge(Y, Z)`
///   - Position 0: head=X, body=X → invariant
///   - Position 1: head=Z, body=Y → NOT invariant
fn compute_invariant_positions(
    program: &Program,
    recursive_relations: &HashSet<String>,
) -> HashMap<String, HashSet<usize>> {
    let mut result: HashMap<String, HashSet<usize>> = HashMap::new();

    for rel in recursive_relations {
        // Find all rules for this relation
        let rules: Vec<_> = program
            .rules
            .iter()
            .filter(|r| r.head.relation == *rel)
            .collect();

        if rules.is_empty() {
            continue;
        }

        // Start with all positions as invariant, then remove any that change
        let arity = rules[0].head.args.len();
        let mut invariant: HashSet<usize> = (0..arity).collect();

        for rule in &rules {
            // Find recursive body atoms (same relation as head)
            for pred in &rule.body {
                if let BodyPredicate::Positive(atom) = pred {
                    if atom.relation == *rel {
                        // Check each position
                        for pos in 0..arity {
                            if pos >= atom.args.len() {
                                invariant.remove(&pos);
                                continue;
                            }
                            match (&rule.head.args[pos], &atom.args[pos]) {
                                (Term::Variable(hv), Term::Variable(bv)) if hv == bv => {
                                    // Same variable at same position — still invariant
                                }
                                _ => {
                                    // Different variable or non-variable — not invariant
                                    invariant.remove(&pos);
                                }
                            }
                        }
                    }
                }
            }
        }

        result.insert(rel.clone(), invariant);
    }

    result
}

/// Check if a term is a ground (constant) term
fn is_ground(term: &Term) -> bool {
    matches!(
        term,
        Term::Constant(_)
            | Term::FloatConstant(_)
            | Term::StringConstant(_)
            | Term::BoolConstant(_)
    )
}

/// Generate the adorned relation name: "reach" + "_bf" = "reach_bf"
fn adorned_relation_name(relation: &str, adornment: &Adornment) -> String {
    format!("{}_{}", relation, adornment.suffix())
}

/// Generate the magic relation name: "magic_reach_bf"
fn magic_relation_name(relation: &str, adornment: &Adornment) -> String {
    format!("magic_{}_{}", relation, adornment.suffix())
}

/// Create an adorned version of a rule with the magic guard.
///
/// Original: `reach(X, Y) <- edge(X, Y)`
/// Adorned:  `reach_bf(X, Y) <- magic_reach_bf(X), edge(X, Y)`
///
/// For recursive body atoms, rename to adorned version:
/// Original: `reach(X, Z) <- reach(X, Y), edge(Y, Z)`
/// Adorned:  `reach_bf(X, Z) <- magic_reach_bf(X), reach_bf(X, Y), edge(Y, Z)`
fn adorn_rule(
    rule: &Rule,
    adorned_name: &str,
    magic_name: &str,
    binding: &QueryBinding,
    adorned_relations: &HashSet<&String>,
) -> Rule {
    // Build magic guard atom: magic_reach_bf(X) using bound argument variables
    let magic_args: Vec<Term> = binding
        .adornment
        .bound_indices()
        .iter()
        .map(|&i| rule.head.args[i].clone())
        .collect();
    let magic_atom = Atom::new(magic_name.to_string(), magic_args);

    // Build adorned head
    let adorned_head = Atom::new(adorned_name.to_string(), rule.head.args.clone());

    // Build adorned body: magic guard first, then original body with renamed recursive refs
    let mut adorned_body = vec![BodyPredicate::Positive(magic_atom)];

    for pred in &rule.body {
        match pred {
            BodyPredicate::Positive(atom) if adorned_relations.contains(&atom.relation) => {
                let rel_binding = &binding;
                let new_name = adorned_relation_name(&atom.relation, &rel_binding.adornment);
                adorned_body.push(BodyPredicate::Positive(Atom::new(
                    new_name,
                    atom.args.clone(),
                )));
            }
            _ => {
                adorned_body.push(pred.clone());
            }
        }
    }

    Rule::new(adorned_head, adorned_body)
}

/// Generate a magic propagation rule if the bound variable changes across recursion.
///
/// For standard TC: `reach(X, Z) <- reach(X, Y), edge(Y, Z)`
/// X stays the same → no propagation needed (magic_reach_bf is constant).
///
/// For same-generation: `sg(X, Y) <- parent(X, Xp), sg(Xp, Yp), parent(Y, Yp)`
/// X changes to Xp → propagation rule: `magic_sg_bf(Xp) <- magic_sg_bf(X), parent(X, Xp)`
fn generate_magic_propagation_rule(
    rule: &Rule,
    magic_name: &str,
    binding: &QueryBinding,
    adorned_relations: &HashSet<&String>,
) -> Option<Rule> {
    // Only check recursive rules (body references head relation)
    let recursive_atoms: Vec<&Atom> = rule
        .body
        .iter()
        .filter_map(|pred| {
            if let BodyPredicate::Positive(atom) = pred {
                if adorned_relations.contains(&atom.relation) && atom.relation == rule.head.relation
                {
                    return Some(atom);
                }
            }
            None
        })
        .collect();

    if recursive_atoms.is_empty() {
        return None;
    }

    // For each recursive body atom, check if bound positions use different variables
    for rec_atom in &recursive_atoms {
        let bound_indices = binding.adornment.bound_indices();
        let mut needs_propagation = false;

        for &idx in &bound_indices {
            if idx < rule.head.args.len()
                && idx < rec_atom.args.len()
                && rule.head.args[idx] != rec_atom.args[idx]
            {
                needs_propagation = true;
                break;
            }
        }

        if needs_propagation {
            // Build propagation rule:
            // magic_<rel>_<adornment>(<rec_bound_vars>) <-
            //     magic_<rel>_<adornment>(<head_bound_vars>), <non-recursive body atoms>

            // Magic atom with head's bound vars (input)
            let head_magic_args: Vec<Term> = bound_indices
                .iter()
                .map(|&i| rule.head.args[i].clone())
                .collect();
            let head_magic = Atom::new(magic_name.to_string(), head_magic_args);

            // Propagation target: magic atom with recursive atom's bound vars
            let rec_magic_args: Vec<Term> = bound_indices
                .iter()
                .map(|&i| rec_atom.args[i].clone())
                .collect();
            let prop_head = Atom::new(magic_name.to_string(), rec_magic_args);

            // Body: magic guard + all non-recursive positive body atoms that bind the new vars
            let mut prop_body = vec![BodyPredicate::Positive(head_magic)];

            // Include non-recursive body atoms that help bind the propagated variables
            for pred in &rule.body {
                match pred {
                    BodyPredicate::Positive(atom)
                        if !adorned_relations.contains(&atom.relation) =>
                    {
                        prop_body.push(pred.clone());
                    }
                    BodyPredicate::Comparison(_, _, _) => {
                        prop_body.push(pred.clone());
                    }
                    _ => {}
                }
            }

            return Some(Rule::new(prop_head, prop_body));
        }
    }

    None
}

/// Convert bound constants from AST Term to runtime Value for magic seed tuples.
fn term_to_value(term: &Term) -> Value {
    match term {
        Term::Constant(n) => Value::Int64(*n),
        Term::FloatConstant(f) => Value::Float64(*f),
        Term::StringConstant(s) => Value::String(Arc::from(s.as_str())),
        Term::BoolConstant(b) => Value::Bool(*b),
        _ => Value::Null, // Should not happen — only ground terms reach here
    }
}

/// Build a seed tuple from bound constants
fn build_seed_tuple(bound_constants: &[(usize, Term)]) -> Tuple {
    // Sort by position to ensure deterministic tuple ordering
    let mut sorted = bound_constants.to_vec();
    sorted.sort_by_key(|(i, _)| *i);
    let values: Vec<Value> = sorted.iter().map(|(_, term)| term_to_value(term)).collect();
    Tuple::new(values)
}

/// Rewrite __query__ rule to reference adorned relations
fn rewrite_query_rule(rule: &Rule, bindings: &HashMap<String, QueryBinding>) -> Rule {
    let new_body: Vec<BodyPredicate> = rule
        .body
        .iter()
        .map(|pred| match pred {
            BodyPredicate::Positive(atom) if bindings.contains_key(&atom.relation) => {
                let binding = &bindings[&atom.relation];
                let adorned = adorned_relation_name(&atom.relation, &binding.adornment);
                BodyPredicate::Positive(Atom::new(adorned, atom.args.clone()))
            }
            _ => pred.clone(),
        })
        .collect();

    Rule::new(rule.head.clone(), new_body)
}

/// Rewrite body references in non-adorned rules (if they reference adorned relations)
fn rewrite_body_references(rule: &Rule, bindings: &HashMap<String, QueryBinding>) -> Rule {
    let has_ref = rule.body.iter().any(|pred| {
        pred.atom()
            .is_some_and(|a| bindings.contains_key(&a.relation))
    });

    if !has_ref {
        return rule.clone();
    }

    let new_body: Vec<BodyPredicate> = rule
        .body
        .iter()
        .map(|pred| match pred {
            BodyPredicate::Positive(atom) if bindings.contains_key(&atom.relation) => {
                let binding = &bindings[&atom.relation];
                let adorned = adorned_relation_name(&atom.relation, &binding.adornment);
                BodyPredicate::Positive(Atom::new(adorned, atom.args.clone()))
            }
            BodyPredicate::Negated(atom) if bindings.contains_key(&atom.relation) => {
                let binding = &bindings[&atom.relation];
                let adorned = adorned_relation_name(&atom.relation, &binding.adornment);
                BodyPredicate::Negated(Atom::new(adorned, atom.args.clone()))
            }
            _ => pred.clone(),
        })
        .collect();

    Rule::new(rule.head.clone(), new_body)
}

/// Compute the set of recursive relations from a program
pub fn find_recursive_relations(program: &Program) -> HashSet<String> {
    let dep_graph = recursion::build_dependency_graph(program);
    let sccs = recursion::find_sccs(&dep_graph);
    sccs.iter()
        .filter(|scc| {
            scc.len() > 1
                || (scc.len() == 1
                    && dep_graph
                        .get(&scc[0])
                        .is_some_and(|deps| deps.contains(&scc[0])))
        })
        .flat_map(|scc| scc.iter().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_program;

    /// Helper: parse a program and return it
    fn parse(src: &str) -> Program {
        parse_program(src).expect("parse failed")
    }

    #[test]
    fn test_adornment_suffix() {
        assert_eq!(Adornment::new(vec![true, false]).suffix(), "bf");
        assert_eq!(Adornment::new(vec![true, true]).suffix(), "bb");
        assert_eq!(Adornment::new(vec![false, false]).suffix(), "ff");
        assert_eq!(Adornment::new(vec![false, true]).suffix(), "fb");
    }

    #[test]
    fn test_adornment_detection_basic() {
        // ?reach(1, Y) produces: __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        assert!(recursive.contains("reach"));

        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert!(bindings.contains_key("reach"));
        let binding = &bindings["reach"];
        assert_eq!(binding.adornment.suffix(), "bf");
        assert_eq!(binding.bound_constants.len(), 1);
        assert_eq!(binding.bound_constants[0].0, 0); // position 0
        assert_eq!(binding.bound_constants[0].1, Term::Constant(1));
    }

    #[test]
    fn test_adornment_detection_no_bindings() {
        // ?reach(X, Y) has no constant bindings
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(X, Y) <- reach(X, Y)",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert!(bindings.is_empty());
    }

    #[test]
    fn test_adornment_detection_all_bound_tc() {
        // ?reach(1, 3) on TC: second arg is NOT invariant (Z→Y in recursion),
        // so adornment should be "bf" not "bb"
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, _c1) <- reach(_c0, _c1), _c0 = 1, _c1 = 3",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert!(bindings.contains_key("reach"));
        // Only position 0 (X) is invariant, position 1 (Z→Y) is NOT
        assert_eq!(bindings["reach"].adornment.suffix(), "bf");
    }

    #[test]
    fn test_adornment_detection_string_constant() {
        // ?user("admin", X) produces: __query__(_c0, X) <- user(_c0, X), _c0 = "admin"
        let program = parse(
            "user(X, Y) <- base_user(X, Y)\n\
             user(X, Z) <- user(X, Y), link(Y, Z)\n\
             __query__(_c0, X) <- user(_c0, X), _c0 = \"admin\"",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert!(bindings.contains_key("user"));
        assert_eq!(bindings["user"].adornment.suffix(), "bf");
        assert!(matches!(
            &bindings["user"].bound_constants[0].1,
            Term::StringConstant(s) if s == "admin"
        ));
    }

    #[test]
    fn test_rewrite_transitive_closure() {
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        let (rewritten, seeds) = MagicSetRewriter::rewrite_program(&program, &bindings);

        // Check we have adorned rules + query rule (no propagation for TC)
        // Rule 0: reach_bf base case
        // Rule 1: reach_bf recursive case
        // Rule 2: __query__
        assert_eq!(rewritten.rules.len(), 3);

        // Check adorned base case: reach_bf(X, Y) <- magic_reach_bf(X), edge(X, Y)
        let base = &rewritten.rules[0];
        assert_eq!(base.head.relation, "reach_bf");
        assert_eq!(base.body.len(), 2); // magic guard + edge
        if let BodyPredicate::Positive(magic) = &base.body[0] {
            assert_eq!(magic.relation, "magic_reach_bf");
            assert_eq!(magic.args.len(), 1);
        } else {
            panic!("expected magic guard as first body atom");
        }

        // Check adorned recursive case: reach_bf(X, Z) <- magic_reach_bf(X), reach_bf(X, Y), edge(Y, Z)
        let rec = &rewritten.rules[1];
        assert_eq!(rec.head.relation, "reach_bf");
        assert_eq!(rec.body.len(), 3); // magic guard + reach_bf + edge
        if let BodyPredicate::Positive(rec_atom) = &rec.body[1] {
            assert_eq!(rec_atom.relation, "reach_bf");
        } else {
            panic!("expected reach_bf in recursive body");
        }

        // Check query: __query__(_c0, Y) <- reach_bf(_c0, Y), _c0 = 1
        let query = &rewritten.rules[2];
        assert_eq!(query.head.relation, "__query__");
        if let BodyPredicate::Positive(q_atom) = &query.body[0] {
            assert_eq!(q_atom.relation, "reach_bf");
        } else {
            panic!("expected reach_bf in query body");
        }

        // Check magic seeds
        assert!(seeds.contains_key("magic_reach_bf"));
        assert_eq!(seeds["magic_reach_bf"].len(), 1);
        assert_eq!(seeds["magic_reach_bf"][0].values().len(), 1);
        assert_eq!(seeds["magic_reach_bf"][0].get(0), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_rewrite_preserves_non_recursive() {
        // Non-recursive rule should not be adorned
        let program = parse(
            "path(X, Y) <- edge(X, Y)\n\
             reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        let (rewritten, _) = MagicSetRewriter::rewrite_program(&program, &bindings);

        // path rule should be preserved unchanged
        let path_rules: Vec<_> = rewritten
            .rules
            .iter()
            .filter(|r| r.head.relation == "path")
            .collect();
        assert_eq!(path_rules.len(), 1);
        assert_eq!(path_rules[0].head.relation, "path");
    }

    #[test]
    fn test_no_magic_propagation_same_bound_var() {
        // TC pattern: reach(X, Z) <- reach(X, Y), edge(Y, Z)
        // X stays the same in head and recursive body → no propagation rule
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        let (rewritten, _) = MagicSetRewriter::rewrite_program(&program, &bindings);

        // Should NOT have a propagation rule: magic_reach_bf(...) <- magic_reach_bf(...), ...
        let magic_head_rules: Vec<_> = rewritten
            .rules
            .iter()
            .filter(|r| r.head.relation.starts_with("magic_"))
            .collect();
        assert_eq!(magic_head_rules.len(), 0);
    }

    #[test]
    fn test_no_magic_for_non_invariant_bound() {
        // Same-generation: sg(X, Y) <- parent(X, Xp), sg(Xp, Yp), parent(Y, Yp)
        // Position 0: head=X, rec_body=Xp → NOT invariant
        // Since no positions are invariant, no magic rewrite is applied.
        // (Magic propagation for non-invariant positions is a future enhancement.)
        let program = parse(
            "sg(X, Y) <- flat(X, Y)\n\
             sg(X, Y) <- parent(X, Xp), sg(Xp, Yp), parent(Y, Yp)\n\
             __query__(_c0, Y) <- sg(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        // No invariant positions → no adornment → no rewrite
        assert!(
            bindings.is_empty(),
            "Same-generation has no invariant positions, should skip magic sets"
        );
    }

    #[test]
    fn test_query_rewritten_to_adorned() {
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        let (rewritten, _) = MagicSetRewriter::rewrite_program(&program, &bindings);

        let query_rules: Vec<_> = rewritten
            .rules
            .iter()
            .filter(|r| r.head.relation == "__query__")
            .collect();
        assert_eq!(query_rules.len(), 1);

        // Body should reference reach_bf, not reach
        let has_adorned_ref = query_rules[0].body.iter().any(|pred| {
            if let BodyPredicate::Positive(atom) = pred {
                atom.relation == "reach_bf"
            } else {
                false
            }
        });
        assert!(has_adorned_ref, "query should reference reach_bf");

        let has_original_ref = query_rules[0].body.iter().any(|pred| {
            if let BodyPredicate::Positive(atom) = pred {
                atom.relation == "reach"
            } else {
                false
            }
        });
        assert!(
            !has_original_ref,
            "query should NOT reference original reach"
        );
    }

    #[test]
    fn test_magic_seed_facts() {
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, Y) <- reach(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        let (_, seeds) = MagicSetRewriter::rewrite_program(&program, &bindings);

        assert_eq!(seeds.len(), 1);
        assert!(seeds.contains_key("magic_reach_bf"));
        let seed_tuples = &seeds["magic_reach_bf"];
        assert_eq!(seed_tuples.len(), 1);
        assert_eq!(seed_tuples[0].values().len(), 1);
        assert_eq!(seed_tuples[0].get(0), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_magic_seed_string_constant() {
        let program = parse(
            "user(X, Y) <- base_user(X, Y)\n\
             user(X, Z) <- user(X, Y), link(Y, Z)\n\
             __query__(_c0, X) <- user(_c0, X), _c0 = \"admin\"",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        let (_, seeds) = MagicSetRewriter::rewrite_program(&program, &bindings);

        assert!(seeds.contains_key("magic_user_bf"));
        let seed = &seeds["magic_user_bf"][0];
        assert_eq!(seed.get(0), Some(&Value::String(Arc::from("admin"))));
    }

    #[test]
    fn test_all_bound_query_tc() {
        // ?reach(1, 3) → for TC, only first arg is invariant so adornment is "bf"
        // The second constant (3) remains as a post-hoc filter in __query__
        let program = parse(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(_c0, _c1) <- reach(_c0, _c1), _c0 = 1, _c1 = 3",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert_eq!(bindings["reach"].adornment.suffix(), "bf");

        let (rewritten, seeds) = MagicSetRewriter::rewrite_program(&program, &bindings);

        // Adorned name should be reach_bf (not bb)
        let adorned: Vec<_> = rewritten
            .rules
            .iter()
            .filter(|r| r.head.relation == "reach_bf")
            .collect();
        assert_eq!(adorned.len(), 2); // base + recursive

        // Magic seed should have 1 value (only the invariant bound position)
        let seed = &seeds["magic_reach_bf"][0];
        assert_eq!(seed.values().len(), 1);
        assert_eq!(seed.get(0), Some(&Value::Int64(1)));
    }

    #[test]
    fn test_all_bound_query_non_tc() {
        // For a relation where BOTH positions are invariant (e.g., friends),
        // bb adornment should work
        let program = parse(
            "friends(X, Y) <- direct_friends(X, Y)\n\
             friends(X, Y) <- friends(X, Y), mutual(X, Y)\n\
             __query__(_c0, _c1) <- friends(_c0, _c1), _c0 = 1, _c1 = 3",
        );
        let recursive = find_recursive_relations(&program);
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert_eq!(bindings["friends"].adornment.suffix(), "bb");

        let (_, seeds) = MagicSetRewriter::rewrite_program(&program, &bindings);
        let seed = &seeds["magic_friends_bb"][0];
        assert_eq!(seed.values().len(), 2);
        assert_eq!(seed.get(0), Some(&Value::Int64(1)));
        assert_eq!(seed.get(1), Some(&Value::Int64(3)));
    }

    #[test]
    fn test_no_rewrite_when_non_recursive() {
        // If the queried relation is not recursive, no magic rewrite
        let program = parse(
            "path(X, Y) <- edge(X, Y)\n\
             __query__(_c0, Y) <- path(_c0, Y), _c0 = 1",
        );
        let recursive = find_recursive_relations(&program);
        assert!(!recursive.contains("path")); // path is not recursive
        let bindings = MagicSetRewriter::detect_query_bindings(&program, &recursive);
        assert!(bindings.is_empty());
    }
}
