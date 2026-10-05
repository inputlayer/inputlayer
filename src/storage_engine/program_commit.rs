//! Committing a staged [`WriteProgram`]: one KG write lock, one effective
//! delta of facts and catalogs, one WAL transaction, one snapshot publish.

use super::catalog_change::{CatalogChange, CatalogDelta, CatalogOutcome, StagedCatalog};
use super::relation_store::stored_bytes;
use super::write_program::{
    CommitError, FactChange, FactCount, ProgramCommit, RelationChange, StagedChanges,
    StatementEffect, StatementOutcome, WriteProgram,
};
use super::{validate_names, KnowledgeGraph, StorageEngine};
use crate::execution::RequestControl;
use crate::schema::SchemaCatalog;
use crate::storage::persist::{PersistBackend, Transaction};
use crate::storage::{StorageError, StorageResult};
use crate::value::{Relation, Tuple};
use crate::view_maintainer::{BaseChange, BaseDelta};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::Instant;
use tracing::{info, warn};

impl StorageEngine {
    /// Commit `program` to `kg` as one transaction.
    ///
    /// Under the KG's write lock: check the request's precondition, if
    /// `control` carries one (see [`RequestControl::precondition`]), and that
    /// what the program read is unchanged, finish any pending drop of a
    /// written name, apply the catalog changes to copies of the KG's catalogs
    /// and validate every fact change against those copies and the current
    /// data, in statement order, and compute the effective delta. A non-empty
    /// delta is written as one WAL record at one logical time, applied, and
    /// published as one snapshot, so readers see the program's rules and data
    /// together. An empty delta writes and publishes nothing.
    /// The commit reports the revision its effect is visible at
    /// ([`ProgramCommit::revision`]).
    ///
    /// `control`, the request's deadline and cancellation, enters its commit
    /// under the lock just before the WAL write: a request stopped before then
    /// writes nothing, and once the WAL write starts the commit completes and
    /// a later stop cannot interrupt it.
    ///
    /// # Errors
    /// See [`CommitError`]. On any error but [`CommitError::OutcomeUnknown`]
    /// nothing was written or published; an unknown outcome requires restart
    /// recovery before the caller can determine whether the transaction
    /// persisted.
    pub fn commit_program(
        &self,
        kg: &str,
        program: WriteProgram,
        control: Option<&RequestControl>,
    ) -> Result<ProgramCommit, CommitError> {
        self.persist.check_writable().map_err(CommitError::from)?;
        let mut db = self.lock_kg(kg).map_err(CommitError::from)?;
        let base = db.snapshot.load_full();
        if let Some(precondition) = control.and_then(RequestControl::precondition) {
            base.changes()
                .check(precondition, &base, |name| {
                    db.schema_catalog.get(name).is_some()
                })
                .map_err(CommitError::Precondition)?;
        }
        if !program.read_holds_in(&base) {
            return Err(CommitError::Stale);
        }

        // A committed drop whose cleanup failed must finish before anything
        // reads or writes that relation's name again.
        let written = program
            .fact_changes()
            .map(FactChange::relation)
            .chain(program.catalog_changes().filter_map(|c| c.name()));
        let mut settled = HashSet::new();
        for name in written {
            if settled.insert(name) {
                self.settle_relation_drop(&mut db, kg, name)
                    .map_err(CommitError::from)?;
            }
        }

        let durable = program.fact_changes().next().is_some()
            || program
                .catalog_changes()
                .any(|c| !matches!(c, CatalogChange::DefineSessionSchema(_)));
        let Resolved {
            facts,
            catalog,
            statements,
        } = db.resolve(kg, program)?;
        let budget = self.config.storage.performance.max_graph_memory_bytes;
        if budget > 0 {
            db.check_memory_budget(kg, budget, &facts, &statements)?;
        }
        if facts.is_empty() && !catalog.is_durable() {
            if durable {
                // Its reply reports state that may rest on events not yet
                // on a follower.
                crate::replication::writes::staged();
            }
            if !catalog.is_empty() {
                // Session schemas only: nothing to persist or publish.
                db.install_catalog(catalog, 0);
            }
            return Ok(ProgramCommit {
                statements,
                relations: Vec::new(),
                base,
                revision: db.snapshot.load().revision,
            });
        }
        // A follower stages and answers session-only programs, but writes
        // nothing durable: that comes only from its primary.
        self.check_client_write().map_err(CommitError::from)?;
        if let Some(control) = control {
            control.begin_commit().map_err(CommitError::Cancelled)?;
        }

        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        let mut txn = transaction(kg, time, &facts);
        catalog.write_to(&mut txn, kg);
        let persist_start = Instant::now();
        self.persist.commit(txn).map_err(CommitError::from)?;
        info!(
            kg = %kg,
            relations = facts.len(),
            statements = statements.len(),
            time,
            persist_ms = persist_start.elapsed().as_millis() as u64,
            "program_commit_persisted"
        );

        let catalog_durable = catalog.is_durable();
        let catalog_saved = db.install_catalog(catalog, time);
        let relations = db.apply_delta(facts);
        if catalog_durable && catalog_saved {
            if let Err(e) = self.persist.catalog_saved(kg, time) {
                warn!(kg = %kg, time, error = %e, "catalog_wal_prune_failed");
            }
        }
        // Still under the write lock, so this is the snapshot just published.
        let revision = db.snapshot.load().revision;
        Ok(ProgramCommit {
            statements,
            relations,
            base,
            revision,
        })
    }
}

/// A program resolved against the KG's current state.
struct Resolved {
    /// Net change of each relation whose contents change, in first-touch order.
    facts: Vec<(String, RelationDelta)>,
    catalog: CatalogDelta,
    statements: Vec<StatementOutcome>,
}

impl KnowledgeGraph {
    /// Apply `program`'s changes to staged copies of the catalogs and
    /// validate its fact changes against them and the current data, in
    /// statement order; compute the net change of each relation and catalog
    /// entry, and each statement's effect.
    fn resolve(&self, kg: &str, program: WriteProgram) -> Result<Resolved, CommitError> {
        let mut catalog = StagedCatalog::new(&self.rule_catalog, &self.schema_catalog);
        let mut delta = FactDelta::default();
        let mut index_dims = HashMap::new();
        let mut statements = Vec::with_capacity(program.statements().len());
        // The last statement that edited each rule, to blame for emptying it.
        let mut rule_edited_by = HashMap::new();
        for statement in program.into_statements() {
            let rejected = |error| CommitError::Rejected {
                statement: statement.index,
                error,
            };
            let effect = match statement.changes {
                StagedChanges::Catalog(change) => {
                    let outcome = catalog.apply(kg, &change).map_err(rejected)?;
                    if change.edits_rules() {
                        let edited = match &outcome {
                            CatalogOutcome::RulesDropped(names) => names.clone(),
                            _ => change.name().map(str::to_string).into_iter().collect(),
                        };
                        for name in edited {
                            rule_edited_by.insert(name, statement.index);
                        }
                    }
                    StatementEffect::Catalog(outcome)
                }
                StagedChanges::Facts(changes) => StatementEffect::Facts(
                    self.resolve_facts(kg, changes, &catalog, &mut delta, &mut index_dims)
                        .map_err(rejected)?,
                ),
            };
            statements.push(StatementOutcome {
                index: statement.index,
                effect,
            });
        }
        if let Some((rule, rules)) = catalog.emptied_negated_rule() {
            return Err(CommitError::Rejected {
                statement: rule_edited_by.get(&rule).copied().unwrap_or_default(),
                error: StorageError::RuleNegated { rule, rules },
            });
        }
        Ok(Resolved {
            facts: delta.into_changed(),
            catalog: catalog.into_delta(),
            statements,
        })
    }

    /// Refuse a fact delta that grows the KG's estimated fact bytes past
    /// `budget`. A delta that does not grow them passes even when the KG is
    /// already over, so deletes always work. The refusal is blamed on the
    /// last statement that inserted facts.
    fn check_memory_budget(
        &self,
        kg: &str,
        budget: u64,
        facts: &[(String, RelationDelta)],
        statements: &[StatementOutcome],
    ) -> Result<(), CommitError> {
        let (mut added, mut removed) = (0usize, 0usize);
        for (_, changes) in facts {
            added += changes.added().map(stored_bytes).sum::<usize>();
            removed += changes.removed.iter().map(stored_bytes).sum::<usize>();
        }
        let current = self.store.bytes();
        let projected = (current + added).saturating_sub(removed);
        if projected <= current || projected as u64 <= budget {
            return Ok(());
        }
        let statement = statements
            .iter()
            .rev()
            .find(|s| matches!(&s.effect, StatementEffect::Facts(count) if count.inserted > 0))
            .map_or(0, |s| s.index);
        warn!(kg = %kg, projected, budget, "graph_memory_budget_exceeded");
        Err(CommitError::Rejected {
            statement,
            error: StorageError::MemoryBudgetExceeded {
                kg: kg.to_string(),
                projected,
                budget,
            },
        })
    }

    /// Validate one statement's fact changes and add them to `delta`.
    fn resolve_facts(
        &self,
        kg: &str,
        changes: Vec<FactChange>,
        catalog: &StagedCatalog<'_>,
        delta: &mut FactDelta,
        index_dims: &mut HashMap<String, usize>,
    ) -> StorageResult<FactCount> {
        let mut count = FactCount::default();
        for change in changes {
            let relation = change.relation().to_string();
            match change {
                FactChange::Insert { tuples, .. } => {
                    self.check_insert(kg, &relation, &tuples, catalog, delta, index_dims)?;
                    let Some(first) = tuples.first() else {
                        continue;
                    };
                    let arity = first.arity();
                    let changes = delta.relation(&relation);
                    changes.arity.get_or_insert(arity);
                    for tuple in tuples {
                        let present = self.store.contains(&relation, &tuple);
                        count.inserted += usize::from(changes.insert(tuple, present));
                    }
                }
                FactChange::Delete { tuples, .. } => {
                    validate_names(kg, &relation)?;
                    if tuples.is_empty() {
                        continue;
                    }
                    let changes = delta.relation(&relation);
                    for tuple in &tuples {
                        let present = self.store.contains(&relation, tuple);
                        count.deleted += usize::from(changes.delete(tuple, present));
                    }
                }
            }
        }
        Ok(count)
    }

    /// Reject an insert the current state (plus the changes staged before it)
    /// does not allow. Checks run in the order single inserts always used, so
    /// a write with several faults reports the same one.
    fn check_insert(
        &self,
        kg: &str,
        relation: &str,
        tuples: &[Tuple],
        catalog: &StagedCatalog<'_>,
        delta: &FactDelta,
        index_dims: &mut HashMap<String, usize>,
    ) -> StorageResult<()> {
        let rejected = StorageError::WriteRejected;
        super::validate_tuples(catalog.schemas(), relation, tuples)
            .map_err(|e| rejected(format!("Insert rejected for '{relation}': {e}")))?;
        validate_names(kg, relation)?;
        if tuples.is_empty() {
            return Ok(());
        }
        if catalog.rules().exists(relation) {
            return Err(rejected(format!(
                "Cannot insert into '{relation}': it is a derived relation (view). \
                 Use a base relation or drop the rule first with '.rule drop {relation}'."
            )));
        }
        self.validate_index_rows(relation, tuples, index_dims)
            .map_err(rejected)?;

        let new_arity = tuples[0].arity();
        if let Some(tuple) = tuples.iter().find(|t| t.arity() != new_arity) {
            return Err(rejected(format!(
                "Arity mismatch in insert batch: expected {}, got {}",
                new_arity,
                tuple.arity()
            )));
        }
        if let Some(existing_arity) = self.relation_arity(relation, catalog.schemas(), delta) {
            if existing_arity != new_arity {
                return Err(rejected(format!(
                    "Arity mismatch for relation '{relation}': existing arity is {existing_arity}, but trying to insert tuples with arity {new_arity}"
                )));
            }
        }
        Ok(())
    }

    /// Arity of `relation`: from the KG if it exists there, else from the
    /// first insert staged into it.
    fn relation_arity(
        &self,
        relation: &str,
        schemas: &SchemaCatalog,
        delta: &FactDelta,
    ) -> Option<usize> {
        match self.metadata.relations.get(relation) {
            Some(meta) => Some(
                schemas
                    .get(relation)
                    .map_or(meta.schema.len(), |schema| schema.columns.len()),
            ),
            None => delta.arity(relation),
        }
    }

    /// Apply a persisted fact delta and publish it, with any installed
    /// catalog changes, as one snapshot.
    pub(super) fn apply_delta(
        &mut self,
        delta: Vec<(String, RelationDelta)>,
    ) -> Vec<RelationChange> {
        let mut changed = Vec::with_capacity(delta.len());
        let mut change = BaseChange::default();
        for (relation, changes) in delta {
            let (added, removed) = changes.into_parts();
            let removed = self.store.delete(&relation, &removed);
            self.index_deleted(&relation, &removed);
            let (added, _) = self.store.insert(&relation, added);
            self.index_inserted(&relation, &added);

            let schema = match added.first() {
                Some(first) => (0..first.arity()).map(|i| format!("col{i}")).collect(),
                None => self.metadata.relations.get(&relation).map_or_else(
                    || vec!["col0".to_string(), "col1".to_string()],
                    |meta| meta.schema.clone(),
                ),
            };
            let tuple_count = self.store.get(&relation).map_or(0, Relation::len);
            self.metadata
                .add_relation(relation.clone(), schema, tuple_count);

            let (inserted, deleted) = (added.len(), removed.len());
            if self.views.is_some() {
                change.deltas.push(BaseDelta {
                    relation: relation.clone(),
                    added,
                    removed,
                });
            }
            changed.push(RelationChange {
                relation,
                inserted,
                deleted,
            });
        }
        self.publish_change(change);
        changed
    }
}

/// Net effect of a program on each relation it touches, relative to the
/// store at commit time.
#[derive(Default)]
pub(super) struct FactDelta {
    /// In first-touch order.
    relations: Vec<(String, RelationDelta)>,
    positions: HashMap<String, usize>,
}

impl FactDelta {
    pub(super) fn relation(&mut self, relation: &str) -> &mut RelationDelta {
        let position = match self.positions.get(relation) {
            Some(&position) => position,
            None => {
                self.positions
                    .insert(relation.to_string(), self.relations.len());
                self.relations
                    .push((relation.to_string(), RelationDelta::default()));
                self.relations.len() - 1
            }
        };
        &mut self.relations[position].1
    }

    fn arity(&self, relation: &str) -> Option<usize> {
        let &position = self.positions.get(relation)?;
        self.relations[position].1.arity
    }

    /// The relations whose contents change, in first-touch order.
    pub(super) fn into_changed(self) -> Vec<(String, RelationDelta)> {
        self.relations
            .into_iter()
            .filter(|(_, changes)| !changes.is_empty())
            .collect()
    }
}

/// `changed` as one transaction at `time`: per relation, deletes then inserts.
pub(super) fn transaction(kg: &str, time: u64, changed: &[(String, RelationDelta)]) -> Transaction {
    let mut txn = Transaction::new(time);
    for (relation, changes) in changed {
        let removed = changes.removed.iter().map(|t| (t.clone(), -1));
        let added = changes.added().map(|t| (t.clone(), 1));
        txn.facts(format!("{kg}:{relation}"), removed.chain(added).collect());
    }
    txn
}

/// Net change to one relation. `added` and `removed` are disjoint: a tuple
/// deleted and re-inserted (or the reverse) cancels out.
#[derive(Default)]
pub(super) struct RelationDelta {
    /// Tuples absent from the store that the program inserts, in first-insert
    /// order; `None` where a later statement deleted it again.
    added: Vec<Option<Tuple>>,
    added_at: HashMap<Tuple, usize>,
    /// Tuples present in the store that the program deletes.
    removed: HashSet<Tuple>,
    /// Arity of the first staged insert.
    arity: Option<usize>,
}

impl RelationDelta {
    /// Insert `tuple` (`present`: whether the store holds it). Returns whether
    /// the tuple was absent before this change.
    pub(super) fn insert(&mut self, tuple: Tuple, present: bool) -> bool {
        if self.removed.remove(&tuple) {
            return true;
        }
        if present || self.added_at.contains_key(&tuple) {
            return false;
        }
        self.added_at.insert(tuple.clone(), self.added.len());
        self.added.push(Some(tuple));
        true
    }

    /// Delete `tuple`. Returns whether the tuple was present before this change.
    pub(super) fn delete(&mut self, tuple: &Tuple, present: bool) -> bool {
        if let Some(position) = self.added_at.remove(tuple) {
            self.added[position] = None;
            return true;
        }
        present && self.removed.insert(tuple.clone())
    }

    fn added(&self) -> impl Iterator<Item = &Tuple> {
        self.added.iter().flatten()
    }

    fn is_empty(&self) -> bool {
        self.added_at.is_empty() && self.removed.is_empty()
    }

    fn into_parts(self) -> (Vec<Tuple>, Vec<Tuple>) {
        (
            self.added.into_iter().flatten().collect(),
            self.removed.into_iter().collect(),
        )
    }
}
