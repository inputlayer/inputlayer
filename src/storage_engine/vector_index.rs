//! Vector index lifecycle for a knowledge graph.
//!
//! Indexes are built from base data on `.index create`, maintained on every
//! insert/delete under the KG write lock, rebuilt on `.index rebuild` or when
//! tombstones exceed `COMPACT_RATIO`, and rebuilt from base data on restart
//! (only definitions are persisted, in `indexes.json`). Each snapshot captures
//! index views at the epoch matching its base data.

use super::{KnowledgeGraph, StorageEngine, StorageResult};
use crate::hnsw_index::{validate_vector, HnswIndex};
use crate::index_manager::{
    DistanceMetric, HnswConfig, IdType, IndexStats, IndexType, ManagedIndex, RegisteredIndex,
    TupleId,
};
use crate::schema::SchemaType;
use crate::statement::IndexCreateOptions;
use crate::value::Tuple;
use std::collections::HashSet;
use std::path::PathBuf;

/// Tombstone fraction that triggers an automatic rebuild.
pub const COMPACT_RATIO: f64 = 0.3;

/// Tombstones below this count never trigger a rebuild (avoids churn on tiny indexes).
const COMPACT_MIN_DEAD: usize = 64;

/// File holding this KG's index definitions.
const DEFINITIONS_FILE: &str = "indexes.json";

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

impl KnowledgeGraph {
    fn index_definitions_path(&self) -> PathBuf {
        self.data_dir.join(DEFINITIONS_FILE)
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
        if config.ef_construction < 1 {
            return Err("HNSW parameter ef_construction must be >= 1".to_string());
        }
        if config.ef_search < 1 {
            return Err("HNSW parameter ef_search must be >= 1".to_string());
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
        let tuples = self.store.get(&def.relation);
        let mut id_type = IdType::default();
        let mut rows = Vec::with_capacity(tuples.map_or(0, |t| t.len()));
        for tuple in tuples.into_iter().flatten() {
            let (id, t, vector) = index_row(&def, tuple)?;
            id_type = t;
            rows.push((id, vector.to_vec()));
        }
        let index = HnswIndex::build(def.index_type.hnsw_config().clone(), rows)
            .map_err(|e| format!("Cannot build index '{}': {e}", def.name))?;
        Ok(ManagedIndex::new(def, index, id_type))
    }

    fn save_index_definitions(&self) -> Result<(), String> {
        self.indexes
            .save_definitions(&self.index_definitions_path())
    }

    /// Create, build, and persist an index; publishes a snapshot that sees it.
    pub fn create_index(&mut self, opts: &IndexCreateOptions) -> Result<IndexStats, String> {
        if self.indexes.contains(&opts.name) {
            return Err(format!(
                "Index '{}' already exists. Drop it first with `.index drop {}`.",
                opts.name, opts.name
            ));
        }
        let def = self.index_definition(opts)?;
        let managed = self.build_index(def)?;
        let stats = managed.stats();
        self.indexes.insert(managed)?;
        if let Err(e) = self.save_index_definitions() {
            self.indexes.remove(&opts.name)?;
            return Err(e);
        }
        self.publish_snapshot();
        Ok(stats)
    }

    /// Drop an index and persist the change.
    pub fn drop_index(&mut self, name: &str) -> Result<(), String> {
        self.indexes.remove(name)?;
        self.save_index_definitions()?;
        self.publish_snapshot();
        Ok(())
    }

    /// Rebuild an index from base data (drops tombstones).
    ///
    /// The new index replaces the old one; snapshots already handed out keep
    /// the old one until they are dropped.
    pub fn rebuild_index(&mut self, name: &str) -> Result<IndexStats, String> {
        self.rebuild_index_quiet(name)?;
        self.publish_snapshot();
        self.indexes.stats(name)
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
    pub(super) fn validate_index_rows(
        &self,
        relation: &str,
        tuples: &[Tuple],
    ) -> Result<(), String> {
        for name in self.indexes.names_for_relation(relation) {
            let Some(managed) = self.indexes.get(&name) else {
                continue;
            };
            let config = managed.definition.index_type.hnsw_config();
            let mut dimension = managed.index.dimension();
            for tuple in tuples {
                let (id, _, vector) = index_row(&managed.definition, tuple)?;
                validate_vector(config, dimension, vector)
                    .map_err(|e| format!("index '{name}': row id={id}: {e}"))?;
                dimension = vector.len();
            }
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
    /// Create and build an HNSW index (`.index create`).
    pub fn create_index_in(
        &self,
        kg: &str,
        opts: &IndexCreateOptions,
    ) -> StorageResult<IndexStats> {
        self.with_kg_mut(kg, |db| db.create_index(opts))
    }

    /// Drop an index (`.index drop`).
    pub fn drop_index_in(&self, kg: &str, name: &str) -> StorageResult<()> {
        self.with_kg_mut(kg, |db| db.drop_index(name))
    }

    /// Rebuild an index from base data (`.index rebuild`).
    pub fn rebuild_index_in(&self, kg: &str, name: &str) -> StorageResult<IndexStats> {
        self.with_kg_mut(kg, |db| db.rebuild_index(name))
    }

    /// Stats for one index or all indexes (`.index stats` / `.index list`).
    pub fn index_stats_in(&self, kg: &str, name: Option<&str>) -> StorageResult<Vec<IndexStats>> {
        self.with_kg_read(kg, |db| db.index_stats(name))
    }
}
