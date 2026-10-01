//! Dependency set of a standing query.
//!
//! The relations in the query body plus, transitively, the bodies of the
//! persistent rules that define them. A change outside this set cannot alter
//! the query's result.

use std::collections::BTreeSet;

use crate::ast::{BodyPredicate, Rule};
use crate::statement::QueryGoal;

use super::ChangeSet;

/// Relations a standing query reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dependencies {
    relations: BTreeSet<String>,
    /// Set when the query reads state not tracked by relation name (HNSW
    /// indexes), so every change must be treated as relevant.
    every_change: bool,
}

impl Dependencies {
    /// Compute the dependency closure of `goal` through `rules`.
    pub fn for_query(goal: &QueryGoal, rules: &[Rule]) -> Self {
        let mut deps = Self::default();
        let mut frontier: Vec<String> = goal.goal.iter().map(|g| g.relation.clone()).collect();
        deps.collect_body(&goal.body, &mut frontier);
        while let Some(relation) = frontier.pop() {
            if deps.relations.contains(&relation) {
                continue;
            }
            for rule in rules.iter().filter(|r| r.head.relation == relation) {
                deps.collect_body(&rule.body, &mut frontier);
            }
            deps.relations.insert(relation);
        }
        deps
    }

    fn collect_body(&mut self, body: &[BodyPredicate], frontier: &mut Vec<String>) {
        for predicate in body {
            match predicate {
                BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => {
                    frontier.push(atom.relation.clone());
                }
                BodyPredicate::HnswNearest { .. } => self.every_change = true,
                BodyPredicate::Comparison(..) => {}
            }
        }
    }

    /// Relations in the closure, sorted.
    pub fn relations(&self) -> impl Iterator<Item = &str> {
        self.relations.iter().map(String::as_str)
    }

    /// Whether `change` can alter a result with these dependencies.
    pub fn is_affected_by(&self, change: &ChangeSet) -> bool {
        match change {
            ChangeSet::Everything => true,
            ChangeSet::Relations(changed) => {
                self.every_change || changed.iter().any(|r| self.relations.contains(r))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::statement::parse_query;

    fn rules(text: &[&str]) -> Vec<Rule> {
        text.iter()
            .map(|r| match crate::statement::parse_statement(r).unwrap() {
                crate::statement::Statement::PersistentRule(rule) => rule,
                other => panic!("expected a persistent rule, got {other:?}"),
            })
            .collect()
    }

    fn deps(query: &str, rule_text: &[&str]) -> Vec<String> {
        let goal = parse_query(query).unwrap();
        Dependencies::for_query(&goal, &rules(rule_text))
            .relations()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn test_dependencies_base_relation_only() {
        assert_eq!(deps("edge(1, X)", &[]), vec!["edge"]);
    }

    #[test]
    fn test_dependencies_follow_rules_transitively_and_recursively() {
        let rules = [
            "+reach(X, Y) <- edge(X, Y)",
            "+reach(X, Z) <- reach(X, Y), edge(Y, Z)",
            "+safe(X) <- node(X), !blocked(X)",
        ];
        assert_eq!(deps("reach(1, X)", &rules), vec!["edge", "reach"]);
        assert_eq!(deps("safe(X)", &rules), vec!["blocked", "node", "safe"]);
    }

    #[test]
    fn test_dependencies_include_query_body() {
        assert_eq!(
            deps("pair(X, Y), label(X, L), X < 3", &[]),
            vec!["label", "pair"]
        );
    }

    #[test]
    fn test_dependencies_affected_only_by_overlapping_changes() {
        let goal = parse_query("reach(X)").unwrap();
        let d = Dependencies::for_query(&goal, &rules(&["+reach(X) <- a(X)"]));
        assert!(d.is_affected_by(&ChangeSet::relation("a")));
        assert!(d.is_affected_by(&ChangeSet::relation("reach")));
        assert!(!d.is_affected_by(&ChangeSet::relation("other")));
        assert!(d.is_affected_by(&ChangeSet::Everything));
    }
}
