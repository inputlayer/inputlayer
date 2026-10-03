//! Committing a staged [`FactProgram`]: one KG write lock, one effective
//! delta, one WAL transaction, one snapshot publish.

use super::fact_program::{
    FactChange, FactCommit, FactCommitError, FactProgram, RelationChange, StatementCount,
};
use super::{validate_names, KnowledgeGraph, StorageEngine};
use crate::storage::persist::{PersistBackend, Transaction};
use crate::storage::{StorageError, StorageResult};
use crate::value::{Relation, Tuple};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tracing::{error, info};

impl StorageEngine {
    /// Commit `program` to `kg` as one transaction.
    ///
    /// Under the KG's write lock: check that what the program read is
    /// unchanged, finish any pending drop of a written relation, validate
    /// every change against the current state in statement order, and compute
    /// the effective delta. A non-empty delta is written as one WAL record at
    /// one logical time, applied, and published as one snapshot. An empty
    /// delta writes and publishes nothing.
    ///
    /// `cancel` is checked once more under the lock, just before the WAL write;
    /// once that write starts the commit completes.
    ///
    /// # Errors
    /// See [`FactCommitError`]; on any error nothing was written or published.
    pub fn commit_facts(
        &self,
        kg: &str,
        program: FactProgram,
        cancel: Option<&AtomicBool>,
    ) -> Result<FactCommit, FactCommitError> {
        let handle = self.kg_handle(kg).map_err(FactCommitError::Failed)?;
        let mut db = Self::lock_live(&handle, kg).map_err(FactCommitError::Failed)?;
        if !program.read_holds_in(&db.snapshot.load()) {
            return Err(FactCommitError::Stale);
        }

        // A committed drop whose cleanup failed must finish before anything
        // reads or writes that relation's name again.
        let mut settled = HashSet::new();
        for change in program.statements().iter().flat_map(|s| &s.changes) {
            if settled.insert(change.relation()) {
                self.settle_relation_drop(&mut db, kg, change.relation())
                    .map_err(FactCommitError::Failed)?;
            }
        }

        let (changed, statements) = db.resolve_delta(kg, program)?;
        if changed.is_empty() {
            return Ok(FactCommit {
                statements,
                relations: Vec::new(),
            });
        }
        if cancel.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
            return Err(FactCommitError::Cancelled);
        }

        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        let persist_start = Instant::now();
        self.persist
            .commit(transaction(kg, time, &changed))
            .map_err(FactCommitError::Failed)?;
        info!(
            kg = %kg,
            relations = changed.len(),
            statements = statements.len(),
            time,
            persist_ms = persist_start.elapsed().as_millis() as u64,
            "fact_commit_persisted"
        );

        let relations = db
            .apply_delta(changed, time)
            .map_err(FactCommitError::Failed)?;
        Ok(FactCommit {
            statements,
            relations,
        })
    }
}

impl KnowledgeGraph {
    /// Validate `program`'s changes against the current state, in order, and
    /// compute the net change of each relation they touch (only relations
    /// that do change) and per-statement counts.
    fn resolve_delta(
        &self,
        kg: &str,
        program: FactProgram,
    ) -> Result<(Vec<(String, RelationDelta)>, Vec<StatementCount>), FactCommitError> {
        let mut delta = FactDelta::default();
        let mut index_dims = HashMap::new();
        let mut counts = Vec::with_capacity(program.statements().len());
        for statement in program.into_statements() {
            let mut count = StatementCount {
                index: statement.index,
                inserted: 0,
                deleted: 0,
            };
            for change in statement.changes {
                let relation = change.relation().to_string();
                let rejected = |error| FactCommitError::Rejected {
                    statement: statement.index,
                    error,
                };
                match change {
                    FactChange::Insert { tuples, .. } => {
                        self.check_insert(kg, &relation, &tuples, &delta, &mut index_dims)
                            .map_err(rejected)?;
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
                        validate_names(kg, &relation).map_err(rejected)?;
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
            counts.push(count);
        }
        Ok((delta.into_changed(), counts))
    }

    /// Reject an insert the current state (plus the changes staged before it)
    /// does not allow. Checks run in the order single inserts always used, so
    /// a write with several faults reports the same one.
    fn check_insert(
        &self,
        kg: &str,
        relation: &str,
        tuples: &[Tuple],
        delta: &FactDelta,
        index_dims: &mut HashMap<String, usize>,
    ) -> StorageResult<()> {
        let rejected = StorageError::WriteRejected;
        self.validate_tuples(relation, tuples)
            .map_err(|e| rejected(format!("Insert rejected for '{relation}': {e}")))?;
        validate_names(kg, relation)?;
        if tuples.is_empty() {
            return Ok(());
        }
        if self.rule_exists(relation) {
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
        if let Some(existing_arity) = self.relation_arity(relation, delta) {
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
    fn relation_arity(&self, relation: &str, delta: &FactDelta) -> Option<usize> {
        match self.metadata.relations.get(relation) {
            Some(meta) => Some(
                self.schema_catalog
                    .get(relation)
                    .map_or(meta.schema.len(), |schema| schema.columns.len()),
            ),
            None => delta.arity(relation),
        }
    }

    /// Apply a persisted delta at `time` and publish it as one snapshot.
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
                error!(kg = %self.name, error = %e, "fact_commit_shadow_write_failed");
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
