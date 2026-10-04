//! Staged write programs: the writes of one program, committed as one
//! transaction at one revision.
//!
//! A caller *stages* a program's writes in statement order, without touching
//! the knowledge graph: fact statements (`+`, `-`, update) resolve to concrete
//! tuple changes, and rule and schema statements to [`CatalogChange`]s.
//! [`StorageEngine::commit_program`] then takes the KG's existing commit
//! ownership (its write lock) once, applies the catalog changes to copies of
//! the KG's catalogs, validates every fact change against those copies and the
//! current data, writes everything as one WAL transaction and publishes one
//! snapshot, with the new rules and data together. Every statement takes
//! effect, or none does.
//!
//! Inserts and deletes of literal tuples are *blind*: their effect is decided
//! under the lock, so they never go stale. A statement that reads the KG while
//! staging (conditional delete, update) evaluates its query on
//! [`WriteProgram::view`], the snapshot plus the changes staged before it, and
//! records the read with [`WriteProgram::read`]: the snapshot and the relations
//! its query reads through persistent rules. The commit refuses with
//! [`CommitError::Stale`] if any of those relations or any rule changed
//! since, and the caller stages again against the new state. Writes to other
//! relations do not conflict.
//!
//! [`StorageEngine::commit_program`]: super::StorageEngine::commit_program

use super::catalog_change::{CatalogChange, CatalogOutcome};
use super::KnowledgeGraphSnapshot;
use crate::ast::dependencies::DependencyClosure;
use crate::ast::Rule;
use crate::execution::Stop;
use crate::rule_catalog::RuleCatalog;
use crate::storage::StorageError;
use crate::value::{RelationMap, Tuple};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// One tuple change a statement makes.
#[derive(Debug, Clone, PartialEq)]
pub enum FactChange {
    /// Insert `tuples` into `relation` (set semantics).
    Insert {
        relation: String,
        tuples: Vec<Tuple>,
    },
    /// Delete `tuples` from `relation`.
    Delete {
        relation: String,
        tuples: Vec<Tuple>,
    },
}

impl FactChange {
    /// The relation this change writes.
    pub fn relation(&self) -> &str {
        match self {
            Self::Insert { relation, .. } | Self::Delete { relation, .. } => relation,
        }
    }
}

/// What one staged statement changes.
#[derive(Debug, Clone, PartialEq)]
pub enum StagedChanges {
    /// Fact changes, applied in order.
    Facts(Vec<FactChange>),
    /// One change to the rule or schema catalog.
    Catalog(CatalogChange),
}

/// The changes of one program statement.
#[derive(Debug, Clone, PartialEq)]
pub struct StagedStatement {
    /// The statement's 0-based index in its program.
    pub index: usize,
    pub changes: StagedChanges,
}

/// A program's writes, staged for one commit.
#[derive(Debug, Clone, Default)]
pub struct WriteProgram {
    statements: Vec<StagedStatement>,
    /// What state-reading statements read, if any.
    read: Option<StagedRead>,
}

/// The KG state that state-reading statements were staged on.
#[derive(Debug, Clone)]
struct StagedRead {
    snapshot: Arc<KnowledgeGraphSnapshot>,
    relations: ReadSet,
}

/// Relations a staged query reads.
#[derive(Debug, Clone)]
enum ReadSet {
    Relations(BTreeSet<String>),
    /// Reads state relation names do not track (HNSW search), or could not
    /// be analysed: any change conflicts.
    Everything,
}

impl ReadSet {
    /// Relations `query` reads, directly or through `rules`.
    fn of(query: &str, rules: &[Rule]) -> Self {
        let Ok(program) = crate::parser::parse_program(query) else {
            return Self::Everything;
        };
        let mut closure = DependencyClosure::default();
        for rule in &program.rules {
            closure.add_rule(rule);
        }
        closure.close_over(rules);
        if closure.reads_untracked_state() {
            return Self::Everything;
        }
        Self::Relations(closure.relations().map(str::to_string).collect())
    }

    fn merge(&mut self, other: ReadSet) {
        match (&mut *self, other) {
            (Self::Relations(mine), Self::Relations(theirs)) => mine.extend(theirs),
            _ => *self = Self::Everything,
        }
    }
}

impl StagedRead {
    /// Whether `current` still reads the same as the staged snapshot: same
    /// rules, and every read relation shares its tuples with the staged one.
    fn holds_in(&self, current: &Arc<KnowledgeGraphSnapshot>) -> bool {
        let staged = &self.snapshot;
        if Arc::ptr_eq(staged, current) {
            return true;
        }
        let ReadSet::Relations(relations) = &self.relations else {
            return false;
        };
        staged.rule_prefix() == current.rule_prefix()
            && relations.iter().all(|relation| {
                match (
                    staged.input_tuples.get(relation),
                    current.input_tuples.get(relation),
                ) {
                    (None, None) => true,
                    (Some(a), Some(b)) => a.shares_tuples_with(b),
                    _ => false,
                }
            })
    }
}

impl WriteProgram {
    /// An empty program.
    pub fn new() -> Self {
        Self::default()
    }

    /// A program of one statement (index 0) making `changes`.
    pub fn single(changes: StagedChanges) -> Self {
        let mut program = Self::new();
        program.push(0, changes);
        program
    }

    /// Append statement `index`, which makes `changes`.
    pub fn push(&mut self, index: usize, changes: StagedChanges) {
        self.statements.push(StagedStatement { index, changes });
    }

    /// Record that a staged statement evaluates `query` on a
    /// [`view`](Self::view) of `snapshot` whose rules are `rules`: the commit
    /// then requires every relation the query reads through `rules`, and every
    /// rule, to be unchanged since `snapshot`.
    ///
    /// # Panics
    /// In debug builds, if the program already read a different snapshot;
    /// every statement of a program must stage against the same one.
    pub fn read(&mut self, snapshot: &Arc<KnowledgeGraphSnapshot>, rules: &[Rule], query: &str) {
        let relations = ReadSet::of(query, rules);
        match &mut self.read {
            Some(read) => {
                debug_assert!(
                    Arc::ptr_eq(&read.snapshot, snapshot),
                    "a fact program stages against one snapshot"
                );
                read.relations.merge(relations);
            }
            None => {
                self.read = Some(StagedRead {
                    snapshot: Arc::clone(snapshot),
                    relations,
                });
            }
        }
    }

    /// Whether what the program read still holds in `current`, the KG's
    /// published snapshot. Always true for programs that read nothing.
    pub fn read_holds_in(&self, current: &Arc<KnowledgeGraphSnapshot>) -> bool {
        self.read.as_ref().is_none_or(|read| read.holds_in(current))
    }

    /// The staged statements, in program order.
    pub fn statements(&self) -> &[StagedStatement] {
        &self.statements
    }

    /// The staged fact changes, in program order.
    pub fn fact_changes(&self) -> impl Iterator<Item = &FactChange> {
        self.statements.iter().flat_map(|s| match &s.changes {
            StagedChanges::Facts(changes) => changes.as_slice(),
            StagedChanges::Catalog(_) => &[],
        })
    }

    /// The staged catalog changes, in program order.
    pub fn catalog_changes(&self) -> impl Iterator<Item = &CatalogChange> {
        self.statements.iter().filter_map(|s| match &s.changes {
            StagedChanges::Catalog(change) => Some(change),
            StagedChanges::Facts(_) => None,
        })
    }

    pub(super) fn into_statements(self) -> Vec<StagedStatement> {
        self.statements
    }

    /// `snapshot` as it reads after the changes staged so far, for evaluating
    /// the next state-reading statement. `rules`, when given, are the
    /// snapshot's rules with the program's staged rule changes applied (see
    /// [`CatalogChange::apply_to_rules`]); the view evaluates with them.
    ///
    /// Only relations the program touches are rebuilt; the others are shared
    /// with `snapshot`. Materialized derived relations are dropped once any
    /// change is staged, since they may be stale; their rules run instead.
    pub fn view(
        &self,
        snapshot: &Arc<KnowledgeGraphSnapshot>,
        rules: Option<&RuleCatalog>,
    ) -> Arc<KnowledgeGraphSnapshot> {
        let overlay = self.overlay();
        if overlay.is_empty() && rules.is_none() {
            return Arc::clone(snapshot);
        }
        let mut inputs: RelationMap = (*snapshot.input_tuples).clone();
        for name in snapshot.materialized_relations.iter() {
            inputs.remove(name);
        }
        for (relation, membership) in overlay {
            let tuples = inputs.entry(relation).or_default();
            tuples.retain(|t| !membership.removed.contains(t) && !membership.added.contains(t));
            tuples.extend(membership.added);
        }

        let mut view = if snapshot.materialized_relations.is_empty() && rules.is_none() {
            (**snapshot).clone()
        } else {
            let rules =
                rules.map_or_else(|| snapshot.rules.as_ref().clone(), RuleCatalog::all_rules);
            let mut view = KnowledgeGraphSnapshot::new_with_workers(
                HashMap::<String, Vec<Tuple>>::new(),
                rules,
                snapshot.num_workers,
            );
            view.max_result_rows = snapshot.max_result_rows;
            view.max_query_cost = snapshot.max_query_cost;
            view.optimization = snapshot.optimization.clone();
            view.hnsw_search_fn.clone_from(&snapshot.hnsw_search_fn);
            view
        };
        view.input_tuples = Arc::new(inputs);
        Arc::new(view)
    }

    /// Final membership of every touched tuple: the last change to a tuple wins.
    fn overlay(&self) -> HashMap<String, Membership> {
        let mut overlay: HashMap<String, Membership> = HashMap::new();
        for change in self.fact_changes() {
            match change {
                FactChange::Insert { relation, tuples } => {
                    let membership = overlay.entry(relation.clone()).or_default();
                    for tuple in tuples {
                        membership.removed.remove(tuple);
                        membership.added.insert(tuple.clone());
                    }
                }
                FactChange::Delete { relation, tuples } => {
                    let membership = overlay.entry(relation.clone()).or_default();
                    for tuple in tuples {
                        membership.added.remove(tuple);
                        membership.removed.insert(tuple.clone());
                    }
                }
            }
        }
        overlay
    }
}

/// Staged membership changes of one relation.
#[derive(Default)]
struct Membership {
    added: HashSet<Tuple>,
    removed: HashSet<Tuple>,
}

/// Effective fact changes made by one committed statement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FactCount {
    /// Tuples that were absent before this statement and present after it.
    pub inserted: usize,
    /// Tuples that were present before this statement and absent after it.
    pub deleted: usize,
}

/// What one committed statement did.
#[derive(Debug, Clone, PartialEq)]
pub enum StatementEffect {
    Facts(FactCount),
    Catalog(CatalogOutcome),
}

/// The effect of one committed statement.
#[derive(Debug, Clone, PartialEq)]
pub struct StatementOutcome {
    /// The statement's index in its program.
    pub index: usize,
    pub effect: StatementEffect,
}

/// Net change of one relation in a commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationChange {
    pub relation: String,
    pub inserted: usize,
    pub deleted: usize,
}

/// A committed write program.
#[derive(Debug, Clone)]
pub struct ProgramCommit {
    /// Per staged statement, in program order.
    pub statements: Vec<StatementOutcome>,
    /// Relations whose contents changed, in first-touch order.
    pub relations: Vec<RelationChange>,
    /// The snapshot the program committed against: the KG as published just
    /// before its changes, on which its reads were validated to still hold.
    pub base: Arc<KnowledgeGraphSnapshot>,
}

/// Why a write program was not committed. In every case but
/// [`CommitError::Unknown`] and [`CommitError::OutcomeUnknown`] nothing was
/// written, published or applied.
#[derive(Debug)]
pub enum CommitError {
    /// The KG published a new snapshot after the program read it. Stage the
    /// program again against the current snapshot.
    Stale,
    /// The request's [`Precondition`](super::Precondition) does not hold on
    /// the KG's current state. Staging again cannot change that.
    Precondition(super::PreconditionError),
    /// The request was stopped (deadline or cancel) before the commit began.
    Cancelled(Stop),
    /// Statement `statement` cannot apply to the KG's current state.
    Rejected {
        statement: usize,
        error: StorageError,
    },
    /// The KG is gone or the transaction could not be persisted.
    Failed(StorageError),
    /// The transaction reached the WAL, so it is durable and replays on
    /// restart, but applying it to the live KG failed: this process may not
    /// show it. The outcome is unknown to the caller.
    Unknown(StorageError),
    /// The WAL write failed and could not be undone, so a restart may still
    /// recover it; the store is read-only until restart recovery.
    OutcomeUnknown(StorageError),
    /// The store is read-only until restart recovery; nothing was written.
    StoreReadOnly,
}

impl From<StorageError> for CommitError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::OutcomeUnknown { .. } => Self::OutcomeUnknown(error),
            StorageError::StoreReadOnly => Self::StoreReadOnly,
            _ => Self::Failed(error),
        }
    }
}

impl CommitError {
    /// This failure as a plain storage error, for single-write APIs.
    pub fn into_storage_error(self) -> StorageError {
        match self {
            Self::Rejected { error, .. } | Self::Failed(error) | Self::OutcomeUnknown(error) => {
                error
            }
            Self::StoreReadOnly => StorageError::StoreReadOnly,
            Self::Unknown(error) => StorageError::Other(format!(
                "The write is durable but failed to apply ({error}); its outcome is \
                 unknown until restart."
            )),
            Self::Stale => StorageError::Other(
                "The knowledge graph changed while the write was staged; nothing was applied."
                    .to_string(),
            ),
            Self::Cancelled(stop) => StorageError::Other(stop.message().to_string()),
            Self::Precondition(error) => StorageError::Other(error.to_string()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn t(a: i32, b: i32) -> Tuple {
        Tuple::from_pair(a, b)
    }

    fn insert(relation: &str, tuples: Vec<Tuple>) -> FactChange {
        FactChange::Insert {
            relation: relation.to_string(),
            tuples,
        }
    }

    fn delete(relation: &str, tuples: Vec<Tuple>) -> FactChange {
        FactChange::Delete {
            relation: relation.to_string(),
            tuples,
        }
    }

    fn facts(changes: Vec<FactChange>) -> StagedChanges {
        StagedChanges::Facts(changes)
    }

    fn snapshot(edges: Vec<Tuple>) -> Arc<KnowledgeGraphSnapshot> {
        let mut inputs = HashMap::new();
        inputs.insert("e".to_string(), edges);
        Arc::new(KnowledgeGraphSnapshot::new(inputs, Vec::new()))
    }

    fn sorted(snapshot: &KnowledgeGraphSnapshot, relation: &str) -> Vec<Tuple> {
        let mut tuples = snapshot
            .input_tuples
            .get(relation)
            .map(|r| r.to_vec())
            .unwrap_or_default();
        tuples.sort();
        tuples
    }

    #[test]
    fn empty_program_views_the_snapshot_itself() {
        let base = snapshot(vec![t(1, 1)]);
        assert!(Arc::ptr_eq(&WriteProgram::new().view(&base, None), &base));
    }

    #[test]
    fn view_applies_staged_changes_in_order_without_touching_the_snapshot() {
        let base = snapshot(vec![t(1, 1), t(2, 2)]);
        let mut program = WriteProgram::new();
        program.push(
            0,
            facts(vec![delete("e", vec![t(1, 1)]), insert("e", vec![t(3, 3)])]),
        );
        // Re-insert of a present tuple stays single; delete of a staged insert wins.
        program.push(
            1,
            facts(vec![insert("e", vec![t(2, 2)]), delete("e", vec![t(3, 3)])]),
        );
        program.push(2, facts(vec![insert("f", vec![t(9, 9)])]));

        let view = program.view(&base, None);
        assert_eq!(sorted(&view, "e"), vec![t(2, 2)]);
        assert_eq!(sorted(&view, "f"), vec![t(9, 9)]);
        assert_eq!(sorted(&base, "e"), vec![t(1, 1), t(2, 2)]);
        assert!(base.input_tuples.get("f").is_none());
    }

    #[test]
    fn view_query_sees_staged_changes() {
        let base = snapshot(vec![t(1, 1)]);
        let mut program = WriteProgram::new();
        program.push(0, facts(vec![insert("e", vec![t(2, 2)])]));
        let mut rows = program
            .view(&base, None)
            .execute_with_rules_tuples("q(X, Y) <- e(X, Y)")
            .unwrap();
        rows.sort();
        assert_eq!(rows, vec![t(1, 1), t(2, 2)]);
    }

    #[test]
    fn view_evaluates_with_staged_rules() {
        let base = snapshot(vec![t(1, 2)]);
        let mut rules = RuleCatalog::empty();
        let change = CatalogChange::RegisterRule(
            crate::statement::parse_rule_definition("swap(Y, X) <- e(X, Y)").unwrap(),
        );
        change.apply_to_rules(&mut rules).unwrap();
        let rows = WriteProgram::new()
            .view(&base, Some(&rules))
            .execute_with_rules_tuples("q(X, Y) <- swap(X, Y)")
            .unwrap();
        assert_eq!(rows, vec![t(2, 1)]);
    }

    #[test]
    fn read_holds_until_a_read_relation_or_rule_changes() {
        let rules = crate::parser::parse_program("p(X) <- e(X, Y)")
            .unwrap()
            .rules;
        let mut inputs = HashMap::new();
        inputs.insert("e".to_string(), vec![t(1, 1)]);
        inputs.insert("other".to_string(), vec![t(5, 5)]);
        let base = Arc::new(KnowledgeGraphSnapshot::new(inputs, rules));
        let mut program = WriteProgram::new();
        assert!(program.read_holds_in(&base));
        program.read(&base, &base.rules, "q(X) <- p(X)");

        let republished = |edit: &dyn Fn(&mut RelationMap)| {
            let mut next = (*base).clone();
            let mut inputs = (*next.input_tuples).clone();
            edit(&mut inputs);
            next.input_tuples = Arc::new(inputs);
            Arc::new(next)
        };
        assert!(program.read_holds_in(&base));
        assert!(program.read_holds_in(&republished(&|_| {})));
        assert!(program.read_holds_in(&republished(&|m| {
            m.get_mut("other").unwrap().push(t(6, 6));
        })));
        // `e` is read through rule `p`.
        assert!(!program.read_holds_in(&republished(&|m| {
            m.get_mut("e").unwrap().push(t(2, 2));
        })));
        assert!(!program.read_holds_in(&republished(&|m| {
            m.insert("p".to_string(), Vec::<Tuple>::new().into());
        })));
        let new_rules = crate::parser::parse_program("p(X) <- e(Y, X)")
            .unwrap()
            .rules;
        let rules_changed = KnowledgeGraphSnapshot::new(
            HashMap::from([("e".to_string(), vec![t(1, 1)])]),
            new_rules,
        );
        assert!(!program.read_holds_in(&Arc::new(rules_changed)));
    }
}
