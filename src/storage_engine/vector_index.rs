//! Vector index lifecycle for a knowledge graph.
//!
//! Indexes are built from base data on `.index create`, maintained on every
//! insert/delete under the KG write lock, rebuilt on `.index rebuild` or when
//! tombstones exceed `COMPACT_RATIO`, and rebuilt from base data on restart
//! (only definitions are persisted, in `indexes.json`). Each snapshot captures
//! index views at the epoch matching its base data.
//!
//! `.index create` and `.index rebuild` build without holding the KG lock,
//! under the request's deadline, then take the write lock to apply the writes
//! made during the build and install the index.

use super::{KnowledgeGraph, StorageEngine, StorageError, StorageResult};
use crate::execution::{RequestControl, Stop};
use crate::hnsw_index::{validate_vector, BuildError, HnswIndex};
use crate::index_manager::{
    DistanceMetric, HnswConfig, IdType, IndexStats, IndexType, ManagedIndex, RegisteredIndex,
    TupleId, INDEX_DEFINITIONS_FILE,
};
use crate::replication::EngineEvent;
use crate::schema::SchemaType;
use crate::size_limits::{MAX_EF_CONSTRUCTION, MAX_EF_SEARCH};
use crate::statement::IndexCreateOptions;
use crate::value::{Relation, Tuple};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::OnceLock;

/// Tombstone fraction that triggers an automatic rebuild.
pub const COMPACT_RATIO: f64 = 0.3;

/// Tombstones below this count never trigger a rebuild (avoids churn on tiny indexes).
const COMPACT_MIN_DEAD: usize = 64;

/// Extract `(id, id_type, vector)` from a row for `def`.
fn index_row<'t>(
    def: &RegisteredIndex,
    tuple: &'t Tuple,
) -> Result<(TupleId, IdType, &'t [f32]), String> {
    let id_value = tuple.get(0).ok_or("row is empty")?;
    let (id, id_type) = IdType::of(id_value).ok_or_else(|| {
        format!(
            "index '{}' needs an integer id in the first column of '{}', got {id_value:?}",
            def.name, def.relation
        )
    })?;
    let vector = tuple
        .get(def.column_idx)
        .and_then(crate::value::Value::as_vector)
        .ok_or_else(|| {
            format!(
                "index '{}' needs a vector in column '{}' of '{}' (row id={id})",
                def.name, def.column_name, def.relation
            )
        })?;
    Ok((id, id_type, vector))
}

/// An index built outside the knowledge graph's lock, with the rows it was
/// built from, to catch it up with the writes made since.
struct BuiltIndex {
    managed: ManagedIndex,
    built_from: Relation,
}

/// The pool index builds run in. A build keeps every thread of its pool busy
/// until it ends, so on the global pool it would starve the queries sharing
/// it; this one has half the global pool's threads, leaving queries the rest.
fn build_pool() -> &'static rayon::ThreadPool {
    static POOL: OnceLock<rayon::ThreadPool> = OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads((rayon::current_num_threads() / 2).max(1))
            .thread_name(|n| format!("index-build-{n}"))
            .build()
            .expect("index build thread pool")
    })
}

/// Build an index for `def` from `rows`, giving up when `stop` returns true.
fn build_from(
    def: RegisteredIndex,
    rows: &Relation,
    stop: impl Fn() -> bool + Sync,
) -> Result<ManagedIndex, BuildError> {
    let mut id_type = IdType::default();
    let mut vectors = Vec::with_capacity(rows.len());
    for tuple in rows.iter() {
        let (id, t, vector) = index_row(&def, tuple).map_err(BuildError::Invalid)?;
        id_type = t;
        vectors.push((id, vector.to_vec()));
    }
    let config = def.index_type.hnsw_config().clone();
    let index = build_pool()
        .install(|| HnswIndex::build_unless(config, vectors, &stop))
        .map_err(|e| match e {
            BuildError::Invalid(e) => {
                BuildError::Invalid(format!("Cannot build index '{}': {e}", def.name))
            }
            BuildError::Stopped => BuildError::Stopped,
        })?;
    Ok(ManagedIndex::new(def, index, id_type))
}

/// Build an index for `def` from `rows` on behalf of a request: it stops
/// when `control` stops the request (its deadline passes or it is
/// cancelled).
fn build_for_request(
    def: RegisteredIndex,
    rows: Relation,
    control: Option<&RequestControl>,
) -> StorageResult<BuiltIndex> {
    let stopped = || control.is_some_and(RequestControl::expire_if_due);
    match build_from(def, &rows, stopped) {
        Ok(managed) => Ok(BuiltIndex {
            managed,
            built_from: rows,
        }),
        Err(BuildError::Invalid(message)) => Err(StorageError::Other(message)),
        Err(BuildError::Stopped) => Err(StorageError::Other(stop_message(control))),
    }
}

/// Enter the commit of the request `control` belongs to, if any.
fn begin_commit(control: Option<&RequestControl>) -> Result<(), String> {
    match control {
        Some(control) => control
            .begin_commit()
            .map_err(|stop| stop.message().to_string()),
        None => Ok(()),
    }
}

fn stop_message(control: Option<&RequestControl>) -> String {
    control
        .and_then(RequestControl::stopped)
        .map_or("Index build stopped", Stop::message)
        .to_string()
}

/// Each id's vector in `rows` for `def` (a later row wins over an earlier
/// one with the same id, as in a build), and the type of the last id.
fn vectors_by_id<'r>(
    def: &RegisteredIndex,
    rows: &'r Relation,
) -> Result<(HashMap<TupleId, &'r [f32]>, Option<IdType>), String> {
    let mut vectors = HashMap::with_capacity(rows.len());
    let mut id_type = None;
    for tuple in rows.iter() {
        let (id, t, vector) = index_row(def, tuple)?;
        id_type = Some(t);
        vectors.insert(id, vector);
    }
    Ok((vectors, id_type))
}

/// Bring `managed`, built from `before`, up to date with `after`: index the
/// ids `after` added or whose vector it changed, and drop the ids it lost.
fn catch_up(managed: &mut ManagedIndex, before: &Relation, after: &Relation) -> Result<(), String> {
    if after.shares_tuples_with(before) {
        return Ok(());
    }
    let name = managed.definition.name.clone();
    let (old, _) = vectors_by_id(&managed.definition, before)?;
    let (new, id_type) = vectors_by_id(&managed.definition, after)?;
    let mut deletes: Vec<TupleId> = old
        .keys()
        .filter(|id| !new.contains_key(id))
        .copied()
        .collect();
    let mut upserts: Vec<(TupleId, Vec<f32>)> = new
        .iter()
        .filter(|(id, vector)| old.get(id) != Some(vector))
        .map(|(id, vector)| (*id, vector.to_vec()))
        .collect();
    if let Some(id_type) = id_type {
        managed.id_type = id_type;
    }
    if deletes.is_empty() && upserts.is_empty() {
        return Ok(());
    }
    deletes.sort_unstable();
    upserts.sort_unstable_by_key(|(id, _)| *id);
    managed
        .index
        .apply(&upserts, &deletes)
        .map(drop)
        .map_err(|e| format!("Cannot build index '{name}': {e}"))
}

impl KnowledgeGraph {
    fn index_definitions_path(&self) -> PathBuf {
        self.data_dir.join(INDEX_DEFINITIONS_FILE)
    }

    /// Resolve `.index create` options against this KG's schema.
    fn index_definition(&self, opts: &IndexCreateOptions) -> Result<RegisteredIndex, String> {
        let schema = self.schema_catalog.get(&opts.relation).ok_or_else(|| {
            format!(
                "No schema found for relation '{}'. Register a schema first.",
                opts.relation
            )
        })?;
        let column_idx = schema.column_index(&opts.column).ok_or_else(|| {
            format!(
                "Column '{}' not found in relation '{}'. Available: {:?}",
                opts.column,
                opts.relation,
                schema.column_names()
            )
        })?;
        let column = &schema.columns[column_idx];
        if !matches!(
            column.data_type,
            SchemaType::Vector { .. } | SchemaType::Any
        ) {
            return Err(format!(
                "Column '{}' of '{}' has type {:?}; an HNSW index needs a vector column.",
                opts.column, opts.relation, column.data_type
            ));
        }
        if column_idx == 0 {
            return Err(format!(
                "Column '{}' is the id column of '{}'; index a vector column instead.",
                opts.column, opts.relation
            ));
        }
        if opts.index_type != "hnsw" {
            return Err(format!(
                "Unsupported index type '{}'. Currently only 'hnsw' is supported.",
                opts.index_type
            ));
        }
        let metric = opts
            .metric
            .as_deref()
            .unwrap_or("cosine")
            .parse::<DistanceMetric>()
            .map_err(|e| format!("Invalid metric: {e}"))?;
        let config = HnswConfig {
            m: opts.m.unwrap_or(16),
            ef_construction: opts.ef_construction.unwrap_or(200),
            ef_search: opts.ef_search.unwrap_or(50),
            metric,
        };
        if !(2..=256).contains(&config.m) {
            return Err(format!(
                "HNSW parameter m must be between 2 and 256, got {}",
                config.m
            ));
        }
        if !(1..=MAX_EF_CONSTRUCTION).contains(&config.ef_construction) {
            return Err(format!(
                "HNSW parameter ef_construction must be between 1 and {MAX_EF_CONSTRUCTION}, got {}",
                config.ef_construction
            ));
        }
        if !(1..=MAX_EF_SEARCH).contains(&config.ef_search) {
            return Err(format!(
                "HNSW parameter ef_search must be between 1 and {MAX_EF_SEARCH}, got {}",
                config.ef_search
            ));
        }
        Ok(RegisteredIndex {
            name: opts.name.clone(),
            relation: opts.relation.clone(),
            column_idx,
            column_name: opts.column.clone(),
            index_type: IndexType::Hnsw(config),
        })
    }

    /// Build a fresh index for `def` from the relation's current rows.
    fn build_index(&self, def: RegisteredIndex) -> Result<ManagedIndex, String> {
        let rows = self.relation_rows(&def.relation);
        build_from(def, &rows, || false).map_err(|e| e.to_string())
    }

    /// The rows of `relation` (none if it does not exist), sharing their
    /// storage with the store.
    fn relation_rows(&self, relation: &str) -> Relation {
        self.store.get(relation).cloned().unwrap_or_default()
    }

    fn check_index_absent(&self, name: &str) -> Result<(), String> {
        if self.indexes.contains(name) {
            return Err(format!(
                "Index '{name}' already exists. Drop it first with `.index drop {name}`."
            ));
        }
        Ok(())
    }

    /// The definition `.index create` resolves `opts` to, and the rows to
    /// build it from.
    fn plan_index(&self, opts: &IndexCreateOptions) -> Result<(RegisteredIndex, Relation), String> {
        self.check_index_absent(&opts.name)?;
        let def = self.index_definition(opts)?;
        let rows = self.relation_rows(&def.relation);
        Ok((def, rows))
    }

    /// Install `built`, the index `.index create` built for `opts`, caught
    /// up with the rows written since; persist it and publish a snapshot
    /// that sees it. The request enters its commit only here.
    fn install_created_index(
        &mut self,
        opts: &IndexCreateOptions,
        built: BuiltIndex,
        control: Option<&RequestControl>,
    ) -> Result<IndexStats, String> {
        self.check_index_absent(&opts.name)?;
        let BuiltIndex {
            mut managed,
            built_from,
        } = built;
        if self.index_definition(opts)? != managed.definition {
            return Err(format!(
                "Relation '{}' changed while index '{}' was built; nothing was created. Retry.",
                opts.relation, opts.name
            ));
        }
        let rows = self.relation_rows(&opts.relation);
        catch_up(&mut managed, &built_from, &rows)?;
        begin_commit(control)?;
        let stats = managed.stats();
        self.indexes.insert(managed)?;
        if let Err(e) = self.save_index_definitions() {
            self.indexes.remove(&opts.name)?;
            return Err(e);
        }
        self.publish_snapshot();
        Ok(stats)
    }

    fn save_index_definitions(&self) -> Result<(), String> {
        self.indexes
            .save_definitions(&self.index_definitions_path())
    }

    /// Make index `def` exist exactly as defined, built from the current
    /// facts; an identical index is left alone. For replication followers.
    pub(super) fn install_index(&mut self, def: RegisteredIndex) -> Result<(), String> {
        if self
            .indexes
            .get(&def.name)
            .is_some_and(|managed| managed.definition == def)
        {
            return Ok(());
        }
        if self.indexes.contains(&def.name) {
            self.indexes.remove(&def.name)?;
        }
        let managed = self.build_index(def)?;
        self.indexes.insert(managed)?;
        self.save_index_definitions()?;
        self.publish_snapshot();
        Ok(())
    }

    /// Drop an index and persist the change.
    pub fn drop_index(&mut self, name: &str) -> Result<(), String> {
        self.indexes.remove(name)?;
        self.save_index_definitions()?;
        self.publish_snapshot();
        Ok(())
    }

    /// The definition of index `name` and the rows to rebuild it from.
    fn plan_rebuild(&self, name: &str) -> Result<(RegisteredIndex, Relation), String> {
        let def = self
            .indexes
            .get(name)
            .ok_or_else(|| self.indexes.not_found(name))?
            .definition
            .clone();
        let rows = self.relation_rows(&def.relation);
        Ok((def, rows))
    }

    /// Replace an index with `built`, its rebuild from base data (without
    /// tombstones), caught up with the rows written since. Snapshots already
    /// handed out keep the old index until they are dropped. The request
    /// enters its commit only here.
    fn install_rebuilt_index(
        &mut self,
        built: BuiltIndex,
        control: Option<&RequestControl>,
    ) -> Result<IndexStats, String> {
        let BuiltIndex {
            mut managed,
            built_from,
        } = built;
        let name = managed.definition.name.clone();
        let current = self
            .indexes
            .get(&name)
            .ok_or_else(|| self.indexes.not_found(&name))?;
        if current.definition != managed.definition {
            return Err(format!(
                "Index '{name}' was redefined while it was rebuilt; nothing was rebuilt. Retry."
            ));
        }
        let rows = self.relation_rows(&managed.definition.relation);
        catch_up(&mut managed, &built_from, &rows)?;
        begin_commit(control)?;
        self.indexes.replace(managed);
        self.publish_snapshot();
        self.indexes.stats(&name)
    }

    fn rebuild_index_quiet(&mut self, name: &str) -> Result<(), String> {
        let def = self
            .indexes
            .get(name)
            .ok_or_else(|| self.indexes.not_found(name))?
            .definition
            .clone();
        let managed = self.build_index(def)?;
        self.indexes.replace(managed);
        Ok(())
    }

    /// Stats for one index, or all when `name` is None.
    pub fn index_stats(&self, name: Option<&str>) -> Result<Vec<IndexStats>, String> {
        match name {
            Some(name) => Ok(vec![self.indexes.stats(name)?]),
            None => Ok(self.indexes.all_stats()),
        }
    }

    /// Index metric names by index (for provenance enrichment).
    pub fn index_metrics(&self) -> std::collections::HashMap<String, String> {
        self.indexes
            .iter()
            .map(|m| {
                let metric = m.definition.index_type.hnsw_config().metric;
                (
                    m.definition.name.clone(),
                    format!("{metric:?}").to_lowercase(),
                )
            })
            .collect()
    }

    /// Reject rows that an index on `relation` could not hold, before they
    /// reach persistence.
    ///
    /// `staged` carries each index's dimension across the batches of one
    /// commit: an empty index takes the dimension of the first staged row,
    /// as it would had the earlier batch already been applied.
    pub(super) fn validate_index_rows(
        &self,
        relation: &str,
        tuples: &[Tuple],
        staged: &mut HashMap<String, usize>,
    ) -> Result<(), String> {
        for name in self.indexes.names_for_relation(relation) {
            let Some(managed) = self.indexes.get(&name) else {
                continue;
            };
            let config = managed.definition.index_type.hnsw_config();
            let mut dimension = staged
                .get(&name)
                .copied()
                .unwrap_or_else(|| managed.index.dimension());
            for tuple in tuples {
                let (id, _, vector) = index_row(&managed.definition, tuple)?;
                validate_vector(config, dimension, vector)
                    .map_err(|e| format!("index '{name}': row id={id}: {e}"))?;
                dimension = vector.len();
            }
            staged.insert(name, dimension);
        }
        Ok(())
    }

    /// Add newly inserted rows to every index on `relation`.
    pub(super) fn index_inserted(&mut self, relation: &str, inserted: &[Tuple]) {
        for name in self.indexes.names_for_relation(relation) {
            let Some(managed) = self.indexes.get_mut(&name) else {
                continue;
            };
            let mut upserts = Vec::with_capacity(inserted.len());
            for tuple in inserted {
                match index_row(&managed.definition, tuple) {
                    Ok((id, id_type, vector)) => {
                        managed.id_type = id_type;
                        upserts.push((id, vector.to_vec()));
                    }
                    Err(e) => tracing::warn!(index = %name, error = %e, "index_row_skipped"),
                }
            }
            if let Err(e) = managed.index.apply(&upserts, &[]) {
                tracing::warn!(index = %name, error = %e, "index_insert_failed");
            }
        }
    }

    /// Remove deleted rows from every index on `relation`.
    ///
    /// When another remaining row shares a deleted row's id, that row's
    /// vector becomes the indexed one.
    pub(super) fn index_deleted(&mut self, relation: &str, deleted: &[Tuple]) {
        for name in self.indexes.names_for_relation(relation) {
            let Some(managed) = self.indexes.get(&name) else {
                continue;
            };
            let def = &managed.definition;
            let deletes: Vec<TupleId> = deleted
                .iter()
                .filter_map(|t| index_row(def, t).ok().map(|(id, _, _)| id))
                .collect();
            if deletes.is_empty() {
                continue;
            }
            let deleted_ids: HashSet<TupleId> = deletes.iter().copied().collect();
            let survivors: Vec<(TupleId, Vec<f32>)> = self
                .store
                .get(relation)
                .into_iter()
                .flatten()
                .filter_map(|t| index_row(def, t).ok())
                .filter(|(id, _, _)| deleted_ids.contains(id))
                .map(|(id, _, v)| (id, v.to_vec()))
                .collect();
            if let Err(e) = managed.index.apply(&survivors, &deletes) {
                tracing::warn!(index = %name, error = %e, "index_delete_failed");
            }
            self.compact_index_if_needed(&name);
        }
    }

    /// Rebuild when tombstones dominate (keeps search fast and recall high).
    fn compact_index_if_needed(&mut self, name: &str) {
        let Some(managed) = self.indexes.get(name) else {
            return;
        };
        if managed.index.dead_count() < COMPACT_MIN_DEAD
            || managed.index.dead_ratio() <= COMPACT_RATIO
        {
            return;
        }
        match self.rebuild_index_quiet(name) {
            Ok(()) => tracing::info!(index = %name, "index_compacted"),
            Err(e) => tracing::warn!(index = %name, error = %e, "index_compaction_failed"),
        }
    }

    /// Rebuild every index on `relation` from current data (after bulk clears).
    pub(super) fn rebuild_indexes_for(&mut self, relation: &str) {
        for name in self.indexes.names_for_relation(relation) {
            if let Err(e) = self.rebuild_index_quiet(&name) {
                tracing::warn!(index = %name, error = %e, "index_rebuild_failed");
            }
        }
    }

    /// Drop every index on `relation` (the relation itself was dropped).
    pub(super) fn drop_indexes_for(&mut self, relation: &str) {
        let names = self.indexes.names_for_relation(relation);
        if names.is_empty() {
            return;
        }
        for name in &names {
            let _ = self.indexes.remove(name);
        }
        if let Err(e) = self.save_index_definitions() {
            tracing::warn!(kg = %self.name, error = %e, "index_definitions_save_failed");
        }
    }

    /// Rebuild persisted indexes from loaded base data (startup).
    pub(super) fn restore_indexes(&mut self) {
        let definitions = match crate::index_manager::IndexManager::load_definitions(
            &self.index_definitions_path(),
        ) {
            Ok(defs) => defs,
            Err(e) => {
                tracing::warn!(kg = %self.name, error = %e, "index_definitions_load_failed");
                return;
            }
        };
        for def in definitions {
            let name = def.name.clone();
            match self.build_index(def) {
                Ok(managed) => {
                    let count = managed.index.len();
                    if let Err(e) = self.indexes.insert(managed) {
                        tracing::warn!(index = %name, error = %e, "index_restore_failed");
                    } else {
                        tracing::info!(kg = %self.name, index = %name, count, "index_restored");
                    }
                }
                Err(e) => tracing::warn!(index = %name, error = %e, "index_restore_failed"),
            }
        }
    }
}

impl StorageEngine {
    /// Create and build an HNSW index (`.index create`). Returns its stats
    /// and the revision of the snapshot that sees it.
    ///
    /// The index is built from the relation's rows without holding the
    /// knowledge graph's lock, so reads and writes go on meanwhile; writes
    /// made during the build are applied to it before it is installed. The
    /// build stops when `control` stops the request: then nothing is created.
    pub fn create_index_in(
        &self,
        kg: &str,
        opts: &IndexCreateOptions,
        control: Option<&RequestControl>,
    ) -> StorageResult<(IndexStats, u64)> {
        self.check_client_write()?;
        let _loaded = self.pin_knowledge_graph(kg)?;
        let (def, rows) = self.with_kg_read(kg, |db| db.plan_index(opts))?;
        let built = build_for_request(def, rows, control)?;
        self.with_kg_mut(kg, |db| {
            let stats = db.install_created_index(opts, built, control)?;
            if let Some(managed) = db.indexes.get(&opts.name) {
                self.replicate(&EngineEvent::CreateIndex {
                    kg: kg.to_string(),
                    index: managed.definition.clone(),
                });
            }
            Ok((stats, db.snapshot.load().revision))
        })
    }

    /// Drop an index (`.index drop`). Returns the revision of the snapshot
    /// published without it.
    pub fn drop_index_in(&self, kg: &str, name: &str) -> StorageResult<u64> {
        self.with_kg_mut(kg, |db| {
            db.drop_index(name)?;
            self.replicate(&EngineEvent::DropIndex {
                kg: kg.to_string(),
                name: name.to_string(),
            });
            Ok(db.snapshot.load().revision)
        })
    }

    /// Rebuild an index from base data (`.index rebuild`). Returns its stats
    /// and the revision of the snapshot that sees the rebuilt index.
    ///
    /// Built like [`Self::create_index_in`]: outside the knowledge graph's
    /// lock, caught up with the writes made meanwhile, and stopped with the
    /// request, leaving the old index in place.
    pub fn rebuild_index_in(
        &self,
        kg: &str,
        name: &str,
        control: Option<&RequestControl>,
    ) -> StorageResult<(IndexStats, u64)> {
        self.check_client_write()?;
        let _loaded = self.pin_knowledge_graph(kg)?;
        let (def, rows) = self.with_kg_read(kg, |db| db.plan_rebuild(name))?;
        let built = build_for_request(def, rows, control)?;
        self.with_kg_mut(kg, |db| {
            let stats = db.install_rebuilt_index(built, control)?;
            Ok((stats, db.snapshot.load().revision))
        })
    }

    /// Stats for one index or all indexes (`.index stats` / `.index list`).
    pub fn index_stats_in(&self, kg: &str, name: Option<&str>) -> StorageResult<Vec<IndexStats>> {
        self.with_kg_read(kg, |db| db.index_stats(name))
    }
}

#[cfg(test)]
#[path = "vector_index_tests.rs"]
mod tests;
