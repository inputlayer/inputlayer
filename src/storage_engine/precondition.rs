//! Revision preconditions on commits (`expect_revision`).
//!
//! Every published snapshot carries a [`ChangeLog`]: for each base relation
//! of its knowledge graph, the revision of the last snapshot that changed the
//! relation's tuples (creating or removing it counts), and the revision of the
//! last one that changed the persistent rules. Both are stamped at publish
//! time by comparing the new state with the previous snapshot, so every path
//! that publishes is covered.
//!
//! A [`Precondition`] asks that state in a scope be as it was at a revision
//! `R`: no relation in scope and no persistent rule changed after `R`. The
//! scope is named relations closed over the persistent rules that derive
//! them, or the whole knowledge graph. [`ChangeLog::check`] decides it
//! exactly against the snapshot a program commits on, under the KG's write
//! lock, so the commit and the check see the same state.
//!
//! Revisions restart with the engine, and the change log only knows the
//! knowledge graph from its first snapshot in this engine run
//! ([`ChangeLog::since`]): a revision older than that fails, because what
//! changed before it is unknown here.

use super::snapshot::last_revision;
use super::KnowledgeGraphSnapshot;
use crate::ast::dependencies::DependencyClosure;
use crate::ast::Rule;
use crate::value::RelationMap;
use std::collections::HashMap;
use std::fmt;

/// Commit only if state in scope is unchanged since `revision`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Precondition {
    /// The revision the caller observed the knowledge graph at.
    pub revision: u64,
    /// The scope: these relations and every relation they are derived from
    /// through persistent rules. `None`: the whole knowledge graph.
    pub relations: Option<Vec<String>>,
}

/// Why a [`Precondition`] does not hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreconditionError {
    /// No snapshot of this engine run has this revision yet.
    Unissued { revision: u64, latest: u64 },
    /// The revision is older than this knowledge graph's first snapshot in
    /// this engine run (it was created, re-created or loaded after it).
    Predates { revision: u64, since: u64 },
    /// The persistent rules changed at `at`, after `revision`.
    RulesChanged { revision: u64, at: u64 },
    /// `relation`, in scope, changed at `at`, after `revision`.
    RelationChanged {
        revision: u64,
        relation: String,
        at: u64,
    },
    /// A scope relation that is not a relation, rule or schema of the
    /// knowledge graph: probably a misspelling, which must not pass silently.
    UnknownRelation(String),
}

impl fmt::Display for PreconditionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unissued { revision, latest } => write!(
                f,
                "Precondition failed: revision {revision} has not been issued \
                 (latest is {latest}); it may be from another engine run. Nothing was applied."
            ),
            Self::Predates { revision, since } => write!(
                f,
                "Precondition failed: revision {revision} predates this knowledge graph's \
                 state in this engine run (from revision {since}); read it again. Nothing \
                 was applied."
            ),
            Self::RulesChanged { revision, at } => write!(
                f,
                "Precondition failed: the persistent rules changed at revision {at}, after \
                 revision {revision}. Nothing was applied."
            ),
            Self::RelationChanged {
                revision,
                relation,
                at,
            } => write!(
                f,
                "Precondition failed: relation '{relation}' changed at revision {at}, after \
                 revision {revision}. Nothing was applied."
            ),
            Self::UnknownRelation(name) => write!(
                f,
                "expect_relations names '{name}', which is not a relation, rule or schema \
                 of this knowledge graph. Nothing was applied."
            ),
        }
    }
}

/// When each part of one knowledge graph's state last changed; see the
/// module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChangeLog {
    /// The revision of the knowledge graph's first snapshot in this engine run.
    since: u64,
    /// The last revision that changed the persistent rules.
    rules: u64,
    /// Per base relation present at some revision from `since` on: the last
    /// revision that changed its tuples.
    relations: HashMap<String, u64>,
    /// The latest revision in `rules` and `relations`.
    latest: u64,
}

impl ChangeLog {
    /// The log of a knowledge graph whose first snapshot is `revision`.
    pub(super) fn starting_at(revision: u64) -> Self {
        Self {
            since: revision,
            ..Self::default()
        }
    }

    /// The log of `snapshot` as a knowledge graph's first: everything it
    /// holds (loaded from disk) appeared at its revision.
    pub(super) fn first(snapshot: &KnowledgeGraphSnapshot) -> Self {
        let revision = snapshot.revision;
        let mut log = Self::starting_at(revision);
        for name in snapshot.input_tuples.keys() {
            if !snapshot.is_materialized(name) {
                log.stamp_relation(name, revision);
            }
        }
        if !snapshot.rules.is_empty() {
            log.rules = revision;
            log.latest = revision;
        }
        log
    }

    /// The revision of the knowledge graph's first snapshot in this engine
    /// run.
    pub fn since(&self) -> u64 {
        self.since
    }

    /// The last revision that changed `relation`'s tuples, if it was present
    /// at any revision since [`Self::since`].
    pub fn relation_changed_at(&self, relation: &str) -> Option<u64> {
        self.relations.get(relation).copied()
    }

    /// The log of the snapshot `revision`, which publishes `base` (the base
    /// relations, without materializations) and `rules`, following
    /// `previous`, whose log this is.
    pub(super) fn next(
        &self,
        previous: &KnowledgeGraphSnapshot,
        base: &RelationMap,
        rules: &[Rule],
        revision: u64,
    ) -> Self {
        let mut next = self.clone();
        let was_base = |name: &str| !previous.materialized_relations.contains(name);
        for (name, relation) in base {
            let unchanged = was_base(name)
                && previous
                    .input_tuples
                    .get(name)
                    .is_some_and(|before| before.shares_tuples_with(relation));
            if !unchanged {
                next.stamp_relation(name, revision);
            }
        }
        for name in previous.input_tuples.keys() {
            if was_base(name) && !base.contains_key(name) {
                next.stamp_relation(name, revision);
            }
        }
        if previous.rules.as_slice() != rules {
            next.rules = revision;
            next.latest = revision;
        }
        next
    }

    /// Whether the persistent rules or one of `relations` may have changed
    /// after `revision`: they did, or `revision` predates [`Self::since`].
    pub fn changed_after<'a>(
        &self,
        revision: u64,
        mut relations: impl Iterator<Item = &'a str>,
    ) -> bool {
        if revision < self.since {
            return true;
        }
        self.latest > revision
            && (self.rules > revision
                || relations.any(|name| self.relations.get(name).is_some_and(|&at| at > revision)))
    }

    fn stamp_relation(&mut self, name: &str, revision: u64) {
        match self.relations.get_mut(name) {
            Some(at) => *at = revision,
            None => {
                self.relations.insert(name.to_string(), revision);
            }
        }
        self.latest = revision;
    }

    /// Whether `precondition` holds on `current`, the snapshot this log
    /// belongs to. `declared` tells whether a name has a schema.
    ///
    /// # Errors
    /// The first reason it does not hold, checked in this order: unknown
    /// scope relations, an unissued or too old revision, a rule change, a
    /// relation change in scope.
    pub fn check(
        &self,
        precondition: &Precondition,
        current: &KnowledgeGraphSnapshot,
        declared: impl Fn(&str) -> bool,
    ) -> Result<(), PreconditionError> {
        let revision = precondition.revision;
        let rules = current.rules.as_slice();
        if let Some(names) = &precondition.relations {
            let known = |name: &str| {
                self.relations.contains_key(name)
                    || rules.iter().any(|rule| rule.head.relation == name)
                    || declared(name)
            };
            if let Some(name) = names.iter().find(|name| !known(name)) {
                return Err(PreconditionError::UnknownRelation(name.clone()));
            }
        }
        let latest = last_revision();
        if revision > latest {
            return Err(PreconditionError::Unissued { revision, latest });
        }
        if revision < self.since {
            return Err(PreconditionError::Predates {
                revision,
                since: self.since,
            });
        }
        if self.latest <= revision {
            return Ok(());
        }
        if self.rules > revision {
            return Err(PreconditionError::RulesChanged {
                revision,
                at: self.rules,
            });
        }
        let changed = |relation: &str| {
            self.relations
                .get(relation)
                .filter(|&&at| at > revision)
                .map(|&at| PreconditionError::RelationChanged {
                    revision,
                    relation: relation.to_string(),
                    at,
                })
        };
        let scope = precondition.relations.as_ref().and_then(|names| {
            let mut closure = DependencyClosure::default();
            for name in names {
                closure.add_relation(name);
            }
            closure.close_over(rules);
            // A rule reading an index reads state no relation name tracks.
            (!closure.reads_untracked_state()).then_some(closure)
        });
        let failure = match scope {
            Some(closure) => closure.relations().find_map(changed),
            None => {
                let mut names: Vec<&String> = self.relations.keys().collect();
                names.sort();
                names.into_iter().find_map(|name| changed(name))
            }
        };
        failure.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
#[path = "precondition_tests.rs"]
mod tests;
