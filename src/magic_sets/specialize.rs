//! Constant specialization of non-recursive derived relations.
//!
//! Magic Sets restricts *recursive* relations to the demanded tuples. This pass
//! does the same for non-recursive ones: when a rule reads a derived relation
//! with some arguments fixed to constants (`two_hop(_c0, Z), _c0 = 1`), the
//! relation's rules are copied under a new name with the matching head
//! variables constrained (`X = 1`), and the atom reads the copy:
//!
//! ```iql
//! two_hop(X, Z) <- edge(X, Y), edge(Y, Z)
//! __query__(_c0, Z) <- two_hop(_c0, Z), _c0 = 1
//! ```
//! becomes
//! ```iql
//! two_hop__spec0(X, Z) <- edge(X, Y), edge(Y, Z), X = 1
//! __query__(_c0, Z) <- two_hop__spec0(_c0, Z), _c0 = 1
//! ```
//!
//! The copy derives a subset of the original containing every tuple the
//! reading atom can match, so results are unchanged. The constraint is only
//! added for head variables bound by a positive body atom; other head terms
//! stay unconstrained (a superset is still correct). Specialized rules are
//! processed in turn, so constants flow down chains of non-recursive rules.
//! Rules no longer reachable from the query are dropped.

use super::{find_recursive_relations, is_ground};
use crate::ast::dependencies::DependencyClosure;
use crate::ast::{BodyPredicate, ComparisonOp, Program, Rule, Term};
use std::collections::{HashMap, HashSet};

/// Rewrite `program` so derived relations read with constant arguments are
/// evaluated only for those constants. Returns `None` when nothing applies.
/// The query is the last rule's head relation.
pub fn specialize_constants(program: &Program) -> Option<Program> {
    let query_head = program.rules.last()?.head.relation.clone();
    let recursive = find_recursive_relations(program);
    let derived: HashSet<&str> = program
        .rules
        .iter()
        .map(|r| r.head.relation.as_str())
        .collect();

    let mut rules = program.rules.clone();
    let mut worklist: Vec<usize> = (0..rules.len())
        .filter(|&i| rules[i].head.relation == query_head)
        .collect();
    let mut specializations: HashMap<String, Option<String>> = HashMap::new();
    let mut changed = false;

    while let Some(idx) = worklist.pop() {
        let bindings = equality_bindings(&rules[idx]);
        for pos in 0..rules[idx].body.len() {
            let BodyPredicate::Positive(atom) = &rules[idx].body[pos] else {
                continue;
            };
            let relation = atom.relation.clone();
            if !derived.contains(relation.as_str())
                || recursive.contains(&relation)
                || relation == rules[idx].head.relation
            {
                continue;
            }
            let bound: Vec<(usize, Term)> = atom
                .args
                .iter()
                .enumerate()
                .filter_map(|(i, arg)| match arg {
                    t if is_ground(t) => Some((i, t.clone())),
                    Term::Variable(v) => bindings.get(v).map(|c| (i, c.clone())),
                    _ => None,
                })
                .collect();
            if bound.is_empty() {
                continue;
            }
            let key = format!("{relation}|{bound:?}");
            let name = match specializations.get(&key) {
                Some(name) => name.clone(),
                None => {
                    let name = format!("{relation}__spec{}", specializations.len());
                    let copies = specialized_rules(program, &relation, &bound, &name);
                    let name = (!copies.is_empty()).then(|| {
                        for copy in copies {
                            worklist.push(rules.len());
                            rules.push(copy);
                        }
                        name
                    });
                    specializations.insert(key, name.clone());
                    name
                }
            };
            if let (Some(name), BodyPredicate::Positive(atom)) = (name, &mut rules[idx].body[pos]) {
                atom.relation = name;
                changed = true;
            }
        }
    }

    if !changed {
        return None;
    }
    Some(prune_and_order(rules, &query_head))
}

/// `var -> ground term` for every `var = ground` (either side) in the body.
fn equality_bindings(rule: &Rule) -> HashMap<String, Term> {
    let mut bindings = HashMap::new();
    for predicate in &rule.body {
        if let BodyPredicate::Comparison(left, ComparisonOp::Equal, right) = predicate {
            match (left, right) {
                (Term::Variable(v), c) | (c, Term::Variable(v)) if is_ground(c) => {
                    bindings.insert(v.clone(), c.clone());
                }
                _ => {}
            }
        }
    }
    bindings
}

/// Copies of `relation`'s rules renamed to `name`, with each bound head
/// variable constrained. Empty when no rule could be constrained.
fn specialized_rules(
    program: &Program,
    relation: &str,
    bound: &[(usize, Term)],
    name: &str,
) -> Vec<Rule> {
    let mut any_constrained = false;
    let copies: Vec<Rule> = program
        .rules
        .iter()
        .filter(|r| r.head.relation == relation)
        .map(|rule| {
            let mut copy = rule.clone();
            copy.head.relation = name.to_string();
            let atom_vars = positive_atom_variables(rule);
            for (position, constant) in bound {
                if let Some(Term::Variable(v)) = rule.head.args.get(*position) {
                    if atom_vars.contains(v.as_str()) {
                        copy.body.push(BodyPredicate::Comparison(
                            Term::Variable(v.clone()),
                            ComparisonOp::Equal,
                            constant.clone(),
                        ));
                        any_constrained = true;
                    }
                }
            }
            copy
        })
        .collect();
    if any_constrained {
        copies
    } else {
        Vec::new()
    }
}

fn positive_atom_variables(rule: &Rule) -> HashSet<&str> {
    rule.body
        .iter()
        .filter_map(|p| match p {
            BodyPredicate::Positive(atom) => Some(atom),
            _ => None,
        })
        .flat_map(|atom| atom.args.iter())
        .filter_map(|t| match t {
            Term::Variable(v) => Some(v.as_str()),
            _ => None,
        })
        .collect()
}

/// Keep rules the query depends on; the query's rules stay last.
fn prune_and_order(rules: Vec<Rule>, query_head: &str) -> Program {
    let mut closure = DependencyClosure::default();
    closure.add_relation(query_head);
    closure.close_over(&rules);
    let (query, others): (Vec<Rule>, Vec<Rule>) = rules
        .into_iter()
        .filter(|r| closure.contains(&r.head.relation))
        .partition(|r| r.head.relation == query_head);
    let mut program = Program::new();
    program.rules = others.into_iter().chain(query).collect();
    program
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::parser::parse_program;

    fn spec(src: &str) -> Option<Vec<String>> {
        specialize_constants(&parse_program(src).unwrap())
            .map(|p| p.rules.iter().map(ToString::to_string).collect())
    }

    #[test]
    fn test_specialize_bound_non_recursive_relation() {
        let rules = spec(
            "two_hop(X, Z) <- edge(X, Y), edge(Y, Z)\n\
             __query__(_c0, Z) <- two_hop(_c0, Z), _c0 = 1",
        )
        .unwrap();
        assert_eq!(rules.len(), 2);
        assert!(rules[0].starts_with("two_hop__spec0(X, Z) <- "));
        assert!(rules[0].contains("X = 1"));
        assert!(rules[1].contains("two_hop__spec0(_c0, Z)"));
    }

    #[test]
    fn test_specialize_flows_through_rule_chains() {
        let rules = spec(
            "a(X, Y) <- e(X, Y)\n\
             b(X, Z) <- a(X, Y), e(Y, Z)\n\
             __query__(Z) <- b(1, Z)",
        )
        .unwrap();
        assert_eq!(rules.len(), 3);
        assert!(rules
            .iter()
            .any(|r| r.starts_with("a__spec") && r.contains("X = 1")));
        assert!(rules.last().unwrap().starts_with("__query__"));
    }

    #[test]
    fn test_specialize_skips_recursive_unbound_and_unconstrainable() {
        // Recursive: left to Magic Sets.
        assert!(spec(
            "reach(X, Y) <- edge(X, Y)\n\
             reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
             __query__(Y) <- reach(1, Y)"
        )
        .is_none());
        // No constants.
        assert!(spec("p(X) <- e(X, Y)\n__query__(X) <- p(X)").is_none());
        // Head term not bound by a body atom: nothing to constrain.
        assert!(spec("p(X, 5) <- e(X)\n__query__(X) <- p(X, 5)").is_none());
    }
}
