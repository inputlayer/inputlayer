//! What a committed change touched, as standing queries see it.

use std::collections::BTreeSet;

use crate::protocol::handler::Notification;

/// Committed persistent changes in one knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeSet {
    /// These relations (base data, rules defining them, or the relation itself).
    Relations(BTreeSet<String>),
    /// Unknown scope (missed notifications, KG dropped): treat as relevant.
    Everything,
}

impl ChangeSet {
    /// Change to a single relation.
    pub fn relation(name: &str) -> Self {
        Self::Relations(BTreeSet::from([name.to_string()]))
    }

    /// Add `other` to this change.
    pub fn merge(&mut self, other: &ChangeSet) {
        match (&mut *self, other) {
            (Self::Everything, _) => {}
            (_, Self::Everything) => *self = Self::Everything,
            (Self::Relations(mine), Self::Relations(theirs)) => {
                mine.extend(theirs.iter().cloned());
            }
        }
    }
}

/// The knowledge graph a notification is about and what it changed there.
pub fn change_of(notification: &Notification) -> (&str, ChangeSet) {
    match notification {
        Notification::PersistentUpdate {
            knowledge_graph,
            relation,
            ..
        } => (knowledge_graph, ChangeSet::relation(relation)),
        Notification::RuleChange {
            knowledge_graph,
            rule_name,
            ..
        } => (knowledge_graph, ChangeSet::relation(rule_name)),
        Notification::SchemaChange {
            knowledge_graph,
            entity,
            ..
        } => (knowledge_graph, ChangeSet::relation(entity)),
        Notification::KgChange {
            knowledge_graph, ..
        } => (knowledge_graph, ChangeSet::Everything),
    }
}

/// Whether a notification may change a knowledge graph's persistent rules.
pub fn changes_rules(notification: &Notification) -> bool {
    matches!(
        notification,
        Notification::RuleChange { .. } | Notification::KgChange { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_unions_relations_and_everything_absorbs() {
        let mut change = ChangeSet::relation("a");
        change.merge(&ChangeSet::relation("b"));
        assert_eq!(
            change,
            ChangeSet::Relations(BTreeSet::from(["a".to_string(), "b".to_string()]))
        );
        change.merge(&ChangeSet::Everything);
        assert_eq!(change, ChangeSet::Everything);
        change.merge(&ChangeSet::relation("c"));
        assert_eq!(change, ChangeSet::Everything);
    }
}
