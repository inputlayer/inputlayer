//! Applying a primary's replication stream on a follower.
//!
//! Every apply is idempotent: a commit becomes its effective delta against
//! the follower's current state (insert only absent facts, delete only
//! present ones), catalog entries are states rather than edits, and creates
//! and drops of what already exists or is gone change nothing. Replaying a
//! suffix of the stream twice therefore ends in the same state, so the
//! follower may save its position after applying.
//!
//! A follower commits each change at its own next revision, into its own WAL
//! and catalog files, and publishes a snapshot, so it serves queries and
//! standing queries like any engine. A [`GraphState`] from a primary's
//! checkpoint is reconciled the same way: the difference becomes one commit.

use super::catalog_change::CatalogDelta;
use super::program_commit::{transaction, FactDelta};
use super::write_program::RelationChange;
use super::StorageEngine;
use crate::index_manager::RegisteredIndex;
use crate::replication::{EngineEvent, Event};
use crate::storage::persist::{CatalogEntry, PersistBackend, Transaction, TxnOp};
use crate::storage::{StorageError, StorageResult};
use crate::value::Tuple;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use tracing::warn;

/// What applying one replicated change did to one graph, for change
/// notifications on the follower.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicaChange {
    /// The graph.
    pub kg: String,
    /// Relations whose facts changed.
    pub relations: Vec<RelationChange>,
    /// Rules that were set or removed.
    pub rules: Vec<String>,
    /// Relations whose persistent schema was set or removed.
    pub schemas: Vec<String>,
    /// The graph itself was created, dropped, or had a relation or index
    /// dropped or created: anything about it may have changed.
    pub graph: Option<GraphEvent>,
}

/// A whole-graph change on a follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphEvent {
    Created,
    Dropped,
    /// A relation or index changed shape (dropped, created).
    Restructured,
}

impl ReplicaChange {
    fn new(kg: &str) -> Self {
        Self {
            kg: kg.to_string(),
            ..Self::default()
        }
    }

    fn graph(kg: &str, event: GraphEvent) -> Self {
        Self {
            graph: Some(event),
            ..Self::new(kg)
        }
    }

    /// Whether nothing changed.
    pub fn is_empty(&self) -> bool {
        self.relations.is_empty()
            && self.rules.is_empty()
            && self.schemas.is_empty()
            && self.graph.is_none()
    }
}

/// One graph's complete state in a primary's checkpoint.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphState {
    /// Every rule and persistent schema.
    pub catalog: Vec<CatalogEntry>,
    /// Every base fact, by relation.
    pub facts: HashMap<String, HashSet<Tuple>>,
    /// Every vector index definition.
    pub indexes: Vec<RegisteredIndex>,
}

impl GraphState {
    /// Add a checkpoint transaction's content: its catalog entries and its
    /// facts (inserts only).
    ///
    /// # Errors
    /// A fact for another graph, or a delete.
    pub fn absorb(&mut self, kg: &str, txn: Transaction) -> StorageResult<()> {
        for op in txn.into_ops() {
            match op {
                TxnOp::Facts { shard, changes } => {
                    let (owner, relation) = split_shard(&shard)?;
                    if owner != kg {
                        return Err(StorageError::Other(format!(
                            "checkpoint of graph '{kg}' holds facts of shard '{shard}'"
                        )));
                    }
                    let facts = self.facts.entry(relation.to_string()).or_default();
                    for (tuple, diff) in changes {
                        if diff != 1 {
                            return Err(StorageError::Other(format!(
                                "checkpoint of graph '{kg}' holds a change of {diff} to '{shard}'"
                            )));
                        }
                        facts.insert(tuple);
                    }
                }
                TxnOp::Catalog { kg: owner, entry } if owner == kg => self.catalog.push(entry),
                TxnOp::Catalog { kg: owner, .. } => {
                    return Err(StorageError::Other(format!(
                        "checkpoint of graph '{kg}' holds a catalog entry of '{owner}'"
                    )))
                }
            }
        }
        Ok(())
    }
}

/// `kg:relation` split at its first colon (graph names have none).
fn split_shard(shard: &str) -> StorageResult<(&str, &str)> {
    shard
        .split_once(':')
        .ok_or_else(|| StorageError::Other(format!("shard '{shard}' names no graph")))
}

impl StorageEngine {
    /// Whether this engine is a replication follower.
    pub fn is_replica(&self) -> bool {
        self.replica.load(Ordering::SeqCst)
    }

    /// Apply one event of the primary's stream; see the module docs.
    ///
    /// # Errors
    /// The change could not be applied or persisted. The follower should
    /// then bring its state to a fresh checkpoint.
    pub fn apply_replicated(&self, event: Event) -> StorageResult<Vec<ReplicaChange>> {
        match event {
            Event::Commit(txn) => self.apply_replicated_commit(txn),
            Event::Engine(event) => self.apply_engine_event(event).map(|c| vec![c]),
        }
    }

    fn apply_replicated_commit(&self, txn: Transaction) -> StorageResult<Vec<ReplicaChange>> {
        // A commit writes one graph; group by graph anyway, in first-seen order.
        type Facts = Vec<(String, Vec<(Tuple, i64)>)>;
        let mut graphs: Vec<(String, Facts, Vec<CatalogEntry>)> = Vec::new();
        let slot = |graphs: &mut Vec<(String, Facts, Vec<CatalogEntry>)>, kg: &str| match graphs
            .iter()
            .position(|(name, _, _)| name == kg)
        {
            Some(i) => i,
            None => {
                graphs.push((kg.to_string(), Vec::new(), Vec::new()));
                graphs.len() - 1
            }
        };
        for op in txn.into_ops() {
            match op {
                TxnOp::Facts { shard, changes } => {
                    let (kg, relation) = split_shard(&shard)?;
                    let i = slot(&mut graphs, kg);
                    graphs[i].1.push((relation.to_string(), changes));
                }
                TxnOp::Catalog { kg, entry } => {
                    let i = slot(&mut graphs, &kg);
                    graphs[i].2.push(entry);
                }
            }
        }
        let mut changes = Vec::with_capacity(graphs.len());
        for (kg, facts, catalog) in graphs {
            let mut change = self.ensure_replica_graph(&kg)?;
            let applied = self.commit_replicated(&kg, catalog, |db, delta| {
                for (relation, tuples) in facts {
                    let relation_delta = delta.relation(&relation);
                    for (tuple, diff) in tuples {
                        let present = db.store.contains(&relation, &tuple);
                        match diff.signum() {
                            1 => relation_delta.insert(tuple, present),
                            -1 => relation_delta.delete(&tuple, present),
                            _ => false,
                        };
                    }
                }
            })?;
            change.relations = applied.relations;
            change.rules = applied.rules;
            change.schemas = applied.schemas;
            changes.push(change);
        }
        Ok(changes)
    }

    fn apply_engine_event(&self, event: EngineEvent) -> StorageResult<ReplicaChange> {
        match event {
            EngineEvent::CreateGraph { name } => self.ensure_replica_graph(&name),
            EngineEvent::DropGraph { name } => {
                if !self.knowledge_graphs.contains_key(&name) {
                    return Ok(ReplicaChange::new(&name));
                }
                let cleanup = self.prepare_graph_drop(&name)?;
                self.finish_drop_knowledge_graph(cleanup);
                Ok(ReplicaChange::graph(&name, GraphEvent::Dropped))
            }
            EngineEvent::DropRelation { kg, relation } => {
                let present = match self.kg_handle(&kg) {
                    Ok(db) => db.read().has_relation(&relation),
                    Err(StorageError::KnowledgeGraphNotFound(_)) => false,
                    Err(e) => return Err(e),
                };
                if !present {
                    return Ok(ReplicaChange::new(&kg));
                }
                self.drop_relation_unchecked(&kg, &relation)?;
                Ok(ReplicaChange::graph(&kg, GraphEvent::Restructured))
            }
            EngineEvent::CreateIndex { kg, index } => {
                let mut db = self.lock_kg(&kg)?;
                db.install_index(index).map_err(StorageError::Other)?;
                Ok(ReplicaChange::graph(&kg, GraphEvent::Restructured))
            }
            EngineEvent::DropIndex { kg, name } => {
                let mut db = self.lock_kg(&kg)?;
                if !db.indexes.contains(&name) {
                    return Ok(ReplicaChange::new(&kg));
                }
                db.drop_index(&name).map_err(StorageError::Other)?;
                Ok(ReplicaChange::graph(&kg, GraphEvent::Restructured))
            }
        }
    }

    /// Bring graph `kg` to `state` from a primary's checkpoint: create it if
    /// missing, commit the difference in facts, rules and schemas as one
    /// transaction, then make its vector indexes match.
    ///
    /// # Errors
    /// The difference could not be applied or persisted.
    pub fn reconcile_graph(&self, kg: &str, state: GraphState) -> StorageResult<ReplicaChange> {
        let GraphState {
            catalog,
            facts,
            indexes,
        } = state;
        let mut change = self.ensure_replica_graph(kg)?;
        let mut entries = catalog;
        let applied = {
            // Removals for every rule and schema the primary does not have.
            let db = self.kg_handle(kg)?;
            let db = db.read();
            let kept_rules: BTreeSet<&str> = entries
                .iter()
                .filter_map(|e| match e {
                    CatalogEntry::Rule { name, .. } => Some(name.as_str()),
                    CatalogEntry::Schema { .. } => None,
                })
                .collect();
            let kept_schemas: BTreeSet<&str> = entries
                .iter()
                .filter_map(|e| match e {
                    CatalogEntry::Schema { relation, .. } => Some(relation.as_str()),
                    CatalogEntry::Rule { .. } => None,
                })
                .collect();
            let mut removals: Vec<CatalogEntry> = db
                .rule_catalog
                .list()
                .into_iter()
                .filter(|name| !kept_rules.contains(name.as_str()))
                .map(|name| CatalogEntry::Rule {
                    name,
                    definition: None,
                })
                .collect();
            removals.extend(
                db.schema_catalog
                    .persistent_relations()
                    .into_iter()
                    .filter(|relation| !kept_schemas.contains(relation))
                    .map(|relation| CatalogEntry::Schema {
                        relation: relation.to_string(),
                        schema: None,
                    }),
            );
            drop(db);
            entries.extend(removals);
            self.commit_replicated(kg, entries, |db, delta| {
                let mut relations: BTreeSet<String> = db.store.names().cloned().collect();
                relations.extend(facts.keys().cloned());
                let none = HashSet::new();
                for relation in relations {
                    let wanted = facts.get(&relation).unwrap_or(&none);
                    let relation_delta = delta.relation(&relation);
                    if let Some(current) = db.store.get(&relation) {
                        for tuple in current.iter().filter(|t| !wanted.contains(*t)) {
                            relation_delta.delete(tuple, true);
                        }
                    }
                    for tuple in wanted {
                        if !db.store.contains(&relation, tuple) {
                            relation_delta.insert(tuple.clone(), false);
                        }
                    }
                }
            })?
        };
        change.relations = applied.relations;
        change.rules = applied.rules;
        change.schemas = applied.schemas;

        let mut db = self.lock_kg(kg)?;
        for current in db.indexes.definitions() {
            if !indexes.iter().any(|def| def.name == current.name) {
                db.drop_index(&current.name).map_err(StorageError::Other)?;
                change.graph.get_or_insert(GraphEvent::Restructured);
            }
        }
        for def in indexes {
            let unchanged = db
                .indexes
                .get(&def.name)
                .is_some_and(|managed| managed.definition == def);
            if !unchanged {
                db.install_index(def).map_err(StorageError::Other)?;
                change.graph.get_or_insert(GraphEvent::Restructured);
            }
        }
        Ok(change)
    }

    /// Drop every graph not in `keep` (the default graph, which cannot be
    /// dropped, is emptied instead): the graphs a primary's checkpoint no
    /// longer has.
    ///
    /// # Errors
    /// A drop or reconcile failed.
    pub fn retain_replica_graphs(&self, keep: &[String]) -> StorageResult<Vec<ReplicaChange>> {
        let default = self.config.storage.default_knowledge_graph.clone();
        let mut changes = Vec::new();
        for name in self.list_knowledge_graphs() {
            if keep.contains(&name) {
                continue;
            }
            if name == default {
                changes.push(self.reconcile_graph(&name, GraphState::default())?);
            } else {
                changes.push(self.apply_engine_event(EngineEvent::DropGraph { name })?);
            }
        }
        Ok(changes)
    }

    /// Create graph `kg` if it does not exist.
    fn ensure_replica_graph(&self, kg: &str) -> StorageResult<ReplicaChange> {
        if self.knowledge_graphs.contains_key(kg) {
            return Ok(ReplicaChange::new(kg));
        }
        match self.create_graph(kg) {
            Ok(_) => Ok(ReplicaChange::graph(kg, GraphEvent::Created)),
            Err(StorageError::KnowledgeGraphExists(_)) => Ok(ReplicaChange::new(kg)),
            Err(e) => Err(e),
        }
    }

    /// Under `kg`'s write lock: let `stage` fill the fact delta against the
    /// current state, add the catalog entries not already in place, and
    /// commit the result at the follower's next revision as one transaction
    /// (nothing when it is empty), then install and publish it.
    fn commit_replicated(
        &self,
        kg: &str,
        catalog: Vec<CatalogEntry>,
        stage: impl FnOnce(&super::KnowledgeGraph, &mut FactDelta),
    ) -> StorageResult<ReplicaChange> {
        let mut db = self.lock_kg(kg)?;
        let mut delta = FactDelta::default();
        stage(&db, &mut delta);
        let facts = delta.into_changed();
        let catalog = CatalogDelta::replicated(&db.rule_catalog, &db.schema_catalog, catalog);
        let mut change = ReplicaChange::new(kg);
        if facts.is_empty() && !catalog.is_durable() {
            return Ok(change);
        }
        change.rules = catalog.rule_names().map(str::to_string).collect();
        change.schemas = catalog.schema_names().map(str::to_string).collect();

        let time = self.logical_time.fetch_add(1, Ordering::SeqCst);
        let mut txn = transaction(kg, time, &facts);
        catalog.write_to(&mut txn, kg);
        self.persist.commit(txn)?;

        let catalog_durable = catalog.is_durable();
        let catalog_saved = db.install_catalog(catalog, time);
        match db.apply_delta(facts, time) {
            Ok(relations) => change.relations = relations,
            // Durable and applied to the store; only the incremental
            // engine's shadow copy missed it, as on a primary.
            Err(e) => warn!(kg = %kg, time, error = %e, "replica_apply_shadow_write_failed"),
        }
        if catalog_durable && catalog_saved {
            if let Err(e) = self.persist.catalog_saved(kg, time) {
                warn!(kg = %kg, time, error = %e, "catalog_wal_prune_failed");
            }
        }
        Ok(change)
    }
}
