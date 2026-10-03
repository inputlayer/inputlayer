//! Stratification and the naive fixpoint over all rules.

use std::collections::{BTreeMap, BTreeSet};

use inputlayer::ast::{BodyPredicate, Rule, Term};

use super::eval::{clause, Budget, Relations};
use crate::model::AdapterError;

/// Persistent rules by head relation; a cleared rule keeps an empty entry.
pub type Rules = BTreeMap<String, Vec<Rule>>;

/// Relations `rule` reads, each with whether the read must be stratified
/// (negation, or any input of an aggregate).
fn reads(rule: &Rule) -> impl Iterator<Item = (&str, bool)> {
    let aggregate = rule.head.args.iter().any(Term::is_aggregate);
    rule.body
        .iter()
        .filter_map(move |predicate| match predicate {
            BodyPredicate::Positive(atom) => Some((atom.relation.as_str(), aggregate)),
            BodyPredicate::Negated(atom) => Some((atom.relation.as_str(), true)),
            _ => None,
        })
}

/// Derived relations reachable from `start` through rule bodies.
fn reachable(start: &str, rules: &Rules) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut stack = vec![start.to_string()];
    while let Some(relation) = stack.pop() {
        for rule in rules.get(&relation).into_iter().flatten() {
            for (read, _) in reads(rule) {
                if rules.contains_key(read) && seen.insert(read.to_string()) {
                    stack.push(read.to_string());
                }
            }
        }
    }
    seen
}

/// Strongly connected components of derived relations, dependencies first.
///
/// Fails when negation or aggregation passes through a cycle: such a
/// program has no stratified meaning for the reference to compute.
pub fn strata(rules: &Rules) -> Result<Vec<BTreeSet<String>>, AdapterError> {
    let reach: BTreeMap<&str, BTreeSet<String>> = rules
        .keys()
        .map(|name| (name.as_str(), reachable(name, rules)))
        .collect();
    let component = |name: &str| -> BTreeSet<String> {
        let mut scc: BTreeSet<String> = reach[name]
            .iter()
            .filter(|other| reach[other.as_str()].contains(name))
            .cloned()
            .collect();
        scc.insert(name.to_string());
        scc
    };
    for (name, clauses) in rules {
        let scc = component(name);
        for rule in clauses {
            for (read, stratified) in reads(rule) {
                if stratified && scc.contains(read) {
                    return Err(AdapterError::Unsupported(format!(
                        "'{name}' reads '{read}' through negation or aggregation inside a recursive cycle"
                    )));
                }
            }
        }
    }
    let mut ordered: Vec<BTreeSet<String>> = Vec::new();
    let mut done: BTreeSet<String> = BTreeSet::new();
    while done.len() < rules.len() {
        let ready = rules
            .keys()
            .filter(|name| !done.contains(*name))
            .map(|name| component(name))
            .find(|scc| {
                scc.iter()
                    .flat_map(|name| reach[name.as_str()].iter())
                    .all(|dep| scc.contains(dep) || done.contains(dep))
            })
            .expect("the component DAG always has a ready component");
        done.extend(ready.iter().cloned());
        ordered.push(ready);
    }
    Ok(ordered)
}

/// Base facts plus every derived relation, computed stratum by stratum with
/// a naive fixpoint (re-run all clauses until nothing changes).
pub fn model(facts: &Relations, rules: &Rules) -> Result<Relations, AdapterError> {
    let mut db = facts.clone();
    let mut budget = Budget::new();
    for stratum in strata(rules)? {
        for name in &stratum {
            db.insert(name.clone(), BTreeSet::new());
        }
        loop {
            let mut changed = false;
            for name in &stratum {
                let mut derived = BTreeSet::new();
                for rule in &rules[name] {
                    derived.extend(clause(rule, &db, &mut budget)?);
                }
                let current = db.get_mut(name).expect("stratum relations are initialized");
                if derived != *current {
                    changed = true;
                    *current = derived;
                }
            }
            if !changed {
                break;
            }
        }
    }
    Ok(db)
}
