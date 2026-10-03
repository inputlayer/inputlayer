//! Committing a staged [`WriteProgram`]: one KG write lock, one effective
//! delta of facts and catalogs, one WAL transaction, one snapshot publish.

use super::catalog_change::{CatalogDelta, StagedCatalog};
use super::write_program::{
    CommitError, FactChange, FactCount, ProgramCommit, RelationChange, StagedChanges,
    StatementEffect, StatementOutcome, WriteProgram,
};
use super::{validate_names, KnowledgeGraph, StorageEngine};
use crate::schema::SchemaCatalog;
use crate::storage::persist::{PersistBackend, Transaction};
use crate::storage::{StorageError, StorageResult};
use crate::value::{Relation, Tuple};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::{error, info, warn};

impl StorageEngine {
    /// Commit `program` to `kg` as one transaction.
    ///
    /// Under the KG's write lock: check that what the program read is
    /// unchanged, finish any pending drop of a written name, apply the
    /// catalog changes to copies of the KG's catalogs and validate every fact
    /// change against those copies and the current data, in statement order,
    /// and compute the effective delta. A non-empty delta is written as one WAL
    /// record at one logical time, applied, and published as one snapshot, so
    /// readers see the program's rules and data together. An empty delta
    /// writes and publishes nothing.
    ///
    /// `cancel` is checked once more under the lock, just before the WAL write;
    /// once that write starts the commit completes.
    ///
    /// # Errors
    /// See [`CommitError`]; on any error nothing was written or published.
    pub fn commit_program(
        &self,
        kg: &str,
        program: WriteProgram,
        cancel: Option<&AtomicBool>,
    ) -> Result<ProgramCommit, CommitError> {
        let handle = self.kg_handle(kg).map_err(CommitError::Failed)?;
        let mut db = Self::lock_live(&handle, kg).map_err(CommitError::Failed)?;
        if !program.read_holds_in(&db.snapshot.load()) {
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
                    .map_err(CommitError::Failed)?;
            }
        }

        let Resolved {
            facts,
            catalog,
            statements,
        } = db.resolve(kg, program)?;
        if facts.is_empty() && !catalog.is_durable() {
            if !catalog.is_empty() {
                // Session schemas only: nothing to persist or publish.
                db.install_catalog(catalog, 0);
            }
            return Ok(ProgramCommit {
                statements,
                relations: Vec::new(),
            });
        }
        if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
            return Err(CommitError::Cancelled);
        }

        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        let mut txn = transaction(kg, time, &facts);
        catalog.write_to(&mut txn, kg);
        let persist_start = Instant::now();
        self.persist.commit(txn).map_err(CommitError::Failed)?;
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
        let relations = db.apply_delta(facts, time);
        if catalog_durable && catalog_saved {
            if let Err(e) = self.persist.catalog_saved(kg, time) {
                warn!(kg = %kg, time, error = %e, "catalog_wal_prune_failed");
            }
        }
        Ok(ProgramCommit {
            statements,
            relations: relations.map_err(CommitError::Failed)?,
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
        for statement in program.into_statements() {
            let rejected = |error| CommitError::Rejected {
                statement: statement.index,
                error,
            };
            let effect = match statement.changes {
                StagedChanges::Catalog(change) => {
                    StatementEffect::Catalog(catalog.apply(kg, &change).map_err(rejected)?)
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
        Ok(Resolved {
            facts: delta.into_changed(),
            catalog: catalog.into_delta(),
            statements,
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

    /// Apply a persisted fact delta at `time` and publish it, with any
    /// installed catalog changes, as one snapshot.
    ///
    /// # Errors
    /// A failed incremental-engine shadow write. The delta is still applied
    /// and published, so memory matches the WAL.
    fn apply_delta(
        &mut self,
        delta: Vec<(String, RelationDelta)>,
        time: u64,
    ) -> StorageResult<Vec<RelationChange>> {
        let mut changed = Vec::with_capacity(delta.len());
        let mut shadow_error = None;
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

            changed.push(RelationChange {
                relation: relation.clone(),
                inserted: added.len(),
                deleted: removed.len(),
            });
            if shadow_error.is_none() {
                shadow_error = self.shadow_write(&relation, added, removed, time).err();
            }
        }
        self.publish_snapshot();
        match shadow_error {
            Some(e) => {
                error!(kg = %self.name, error = %e, "program_commit_shadow_write_failed");
                Err(StorageError::IncrementalEngineError(e))
            }
            None => Ok(changed),
        }
    }

    /// Mirror one relation's committed change into the incremental engine.
    fn shadow_write(
        &self,
        relation: &str,
        added: Vec<Tuple>,
        removed: Vec<Tuple>,
        time: u64,
    ) -> Result<(), String> {
        let Some(dd) = &self.incremental else {
            return Ok(());
        };
        if !removed.is_empty() {
            dd.delete(relation, removed, time)?;
        }
        if !added.is_empty() {
            dd.insert(relation, added, time)?;
        }
        dd.notify_base_update(relation).map(drop)
    }
}

/// Net effect of a program on each relation it touches, relative to the
/// store at commit time.
#[derive(Default)]
struct FactDelta {
    /// In first-touch order.
    relations: Vec<(String, RelationDelta)>,
    positions: HashMap<String, usize>,
}

impl FactDelta {
    fn relation(&mut self, relation: &str) -> &mut RelationDelta {
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
    fn into_changed(self) -> Vec<(String, RelationDelta)> {
        self.relations
            .into_iter()
            .filter(|(_, changes)| !changes.is_empty())
            .collect()
    }
}

/// `changed` as one transaction at `time`: per relation, deletes then inserts.
fn transaction(kg: &str, time: u64, changed: &[(String, RelationDelta)]) -> Transaction {
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
struct RelationDelta {
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
    fn insert(&mut self, tuple: Tuple, present: bool) -> bool {
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
    fn delete(&mut self, tuple: &Tuple, present: bool) -> bool {
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
