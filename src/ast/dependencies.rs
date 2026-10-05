//! Dependency closure of relations through rules.
//!
//! Starting from the relations a query reads, follow every rule whose head is
//! in the set and add its body relations, until nothing changes. A relation
//! or rule outside the closure cannot affect the query's result.

use std::collections::BTreeSet;

use super::{BodyPredicate, Rule};

/// Relations reachable from a set of roots through rule bodies.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DependencyClosure {
    relations: BTreeSet<String>,
    frontier: Vec<String>,
    /// Set when a body reads state not named by a relation (HNSW indexes).
    reads_untracked_state: bool,
}

impl DependencyClosure {
    /// Add a root relation.
    pub fn add_relation(&mut self, relation: &str) {
        if !self.relations.contains(relation) {
            self.frontier.push(relation.to_string());
        }
    }

    /// Add every relation a rule body reads.
    pub fn add_body(&mut self, body: &[BodyPredicate]) {
        for predicate in body {
            match predicate {
                BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => {
                    self.add_relation(&atom.relation);
                }
                BodyPredicate::HnswNearest { .. } => self.reads_untracked_state = true,
                BodyPredicate::Comparison(..) => {}
            }
        }
    }

    /// Add a rule's head and body relations as roots.
    pub fn add_rule(&mut self, rule: &Rule) {
        self.add_relation(&rule.head.relation);
        self.add_body(&rule.body);
    }

    /// Close the set over `rules`.
    pub fn close_over(&mut self, rules: &[Rule]) {
        while let Some(relation) = self.frontier.pop() {
            if !self.relations.insert(relation.clone()) {
                continue;
            }
            for rule in rules.iter().filter(|r| r.head.relation == relation) {
                self.add_body(&rule.body);
            }
        }
    }

    /// Whether `relation` is in the (closed) set.
    pub fn contains(&self, relation: &str) -> bool {
        self.relations.contains(relation)
    }

    /// Relations in the closure, sorted.
    pub fn relations(&self) -> impl Iterator<Item = &str> {
        self.relations.iter().map(String::as_str)
    }

    /// Add every relation of the closed set `other`: the union of two closed
    /// sets over the same rules is closed.
    pub fn merge(&mut self, other: &DependencyClosure) {
        self.relations.extend(other.relations.iter().cloned());
        self.frontier.extend(other.frontier.iter().cloned());
        self.reads_untracked_state |= other.reads_untracked_state;
    }

    /// Whether some body reads state that relation names do not track.
    pub fn reads_untracked_state(&self) -> bool {
        self.reads_untracked_state
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::parser::parse_program;

    fn closure(roots: &[&str], rules: &str) -> Vec<String> {
        let program = parse_program(rules).unwrap();
        let mut c = DependencyClosure::default();
        for r in roots {
            c.add_relation(r);
        }
        c.close_over(&program.rules);
        c.relations().map(str::to_string).collect()
    }

    #[test]
    fn test_closure_follows_rules_transitively_and_skips_unrelated() {
        let rules = "reach(X, Y) <- edge(X, Y)\n\
                     reach(X, Z) <- reach(X, Y), edge(Y, Z)\n\
                     other(X) <- unrelated(X)";
        assert_eq!(closure(&["reach"], rules), vec!["edge", "reach"]);
        assert_eq!(closure(&["edge"], rules), vec!["edge"]);
    }

    #[test]
    fn test_closure_includes_negated_relations() {
        let rules = "safe(X) <- node(X), !blocked(X)";
        assert_eq!(closure(&["safe"], rules), vec!["blocked", "node", "safe"]);
    }
}
