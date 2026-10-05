//! Knowledge graph residency: which knowledge graphs are in memory.
//!
//! Startup reads metadata only: the KG listing, one metadata file per persist
//! shard, and the revision of each KG's catalog files. Every KG starts
//! *dormant*, and is loaded from the persist shards on first use (*activation*),
//! so startup costs O(shards) whatever the data, and memory holds only the KGs
//! in use. Activation of one KG reads its shards in parallel; activations of
//! different KGs run in parallel too.
//!
//! A loaded KG can be *unloaded* back to dormant when the engine holds more
//! than `storage.max_loaded_knowledge_graphs` (least recently used first), or
//! when idle for `storage.unload_idle_after_secs`; both are off by default.
//! Unloading is invisible to clients: a KG is unloaded only when nothing uses
//! it, and its reload publishes the same state under the same snapshot
//! revision. A KG is in use while
//!
//! - a request or writer holds its handle,
//! - anything holds its published snapshot (a running query or evaluation, or
//!   a staged write program, whose commit checks that what it read still
//!   shares the current snapshot's tuples), or
//! - a [`KgPin`] is held on it (a subscription holds one for its lifetime).
//!
//! A KG whose state is not all on disk stays loaded: session schemas, vector
//! indexes (rebuilding them on reload would be slow and could change
//! approximate results), and catalog changes whose save to the catalog files
//! failed (only a restart replays them from the WAL).
//!
//! The names of the loaded KGs are kept in a file, rewritten when a KG loads
//! that the file lacks, or is unloaded or dropped. After a restart the server
//! loads the KGs it names in the background ([`StorageEngine::warm_knowledge_graph`]),
//! so their first request does not wait for the load, startup still reads
//! metadata only, and memory holds what it held before the restart. A request
//! for a KG the warm-up has not reached loads it itself.
//!
//! The hot path takes no lock: a lookup is a map read, an atomic pointer load
//! and an atomic store of the access time. Activation, unloading and removal
//! of one KG are serialized by its slot's mutex. A writer that waited for a
//! KG's write lock while the KG was unloaded finds its handle retired and
//! retries on the reloaded KG, so no commit lands on an unloaded copy.

use super::precondition::ChangeLog;
use super::{KnowledgeGraph, StorageEngine};
use crate::storage::persist::{
    consolidate_to_current, into_tuples, set_semantics_corrections, CatalogRecord, PersistBackend,
    Transaction,
};
use crate::storage::{KnowledgeGraphMetadata, RelationTombstone, StorageError, StorageResult};
use crate::value::Tuple;
use arc_swap::ArcSwapOption;
use parking_lot::lock_api::ArcRwLockWriteGuard;
use parking_lot::{Mutex, RawRwLock, RwLock};
use rayon::prelude::*;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// A knowledge graph the engine knows: loaded, or dormant on disk.
pub(super) struct KgSlot {
    /// The loaded graph; `None` while dormant.
    graph: ArcSwapOption<RwLock<KnowledgeGraph>>,
    /// Serializes activation, unloading and removal.
    state: Mutex<SlotState>,
    /// Engine clock second of the last lookup, for unloading. Written without
    /// a lock, and only when the second changes.
    last_used: AtomicU64,
    /// Live [`KgPin`]s; a pinned KG is never unloaded.
    pins: AtomicUsize,
    /// What listings report while the KG is dormant. Apart from `state`, so
    /// stats never wait for an activation.
    listing: Mutex<Listing>,
}

/// A knowledge graph's entry in listings, kept while it is dormant.
#[derive(Debug, Clone, Default)]
pub(super) struct Listing {
    /// Creation time, from the KG listing.
    pub(super) created_at: Option<String>,
    /// Counts as of when it was last loaded or listed.
    pub(super) summary: KgSummary,
}

struct SlotState {
    /// Dropped: the slot left the engine's map, and activation fails.
    removed: bool,
    /// What activation needs, while dormant.
    dormant: Option<Dormant>,
}

/// A knowledge graph on disk, not in memory.
pub(super) struct Dormant {
    /// Its persist shards, each with its relation name.
    pub(super) shards: Vec<(String, String)>,
    /// The revision and change log of the snapshot it last published, if it
    /// was loaded before: its reload publishes the same state, so under the
    /// same revision, with the same record of when each part last changed
    /// (for `expect_revision` preconditions).
    pub(super) last_published: Option<(u64, ChangeLog)>,
    /// Rule and schema changes the WAL held at startup, which its catalog
    /// files may lack (a crash before they were saved). Loading applies them
    /// and saves the files; the WAL keeps them until then.
    pub(super) catalog: Vec<CatalogRecord>,
}

/// Size of a knowledge graph, as reported by listings and stats.
///
/// A loaded KG reports its current counts. A dormant one reports them as of
/// when it was unloaded, or as of the KG listing saved before the restart
/// (saved at every create, drop, compaction and clean shutdown).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KgSummary {
    /// Relations holding facts.
    pub relations: usize,
    /// Facts over all relations.
    pub tuples: usize,
    /// Persistent rules (named derived relations).
    pub rules: usize,
    /// Whether the KG is in memory.
    pub loaded: bool,
}

/// Knowledge graph loads and unloads since the engine started.
#[derive(Debug, Default)]
pub(super) struct Counts {
    activations: AtomicU64,
    unloads: AtomicU64,
}

/// Why a knowledge graph handle no longer takes writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Retired {
    /// The KG was dropped.
    Dropped,
    /// The KG was unloaded; the engine reloads it on next use.
    Unloaded,
}

/// Keeps a knowledge graph loaded while held: it is never unloaded. Pinning
/// does not load a dormant KG; its next use does. Dropping the KG does not
/// wait for pins.
pub struct KgPin {
    slot: Arc<KgSlot>,
}

impl Drop for KgPin {
    fn drop(&mut self) {
        self.slot.pins.fetch_sub(1, Ordering::AcqRel);
    }
}

impl std::fmt::Debug for KgPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KgPin").finish_non_exhaustive()
    }
}

impl KgSlot {
    /// A slot for a KG created now, already loaded.
    pub(super) fn loaded(graph: KnowledgeGraph, now: u64) -> Self {
        KgSlot {
            graph: ArcSwapOption::from_pointee(RwLock::new(graph)),
            state: Mutex::new(SlotState {
                removed: false,
                dormant: None,
            }),
            last_used: AtomicU64::new(now),
            pins: AtomicUsize::new(0),
            listing: Mutex::new(Listing::default()),
        }
    }

    /// A slot for a KG on disk.
    pub(super) fn dormant(dormant: Dormant, listing: Listing, now: u64) -> Self {
        KgSlot {
            graph: ArcSwapOption::empty(),
            state: Mutex::new(SlotState {
                removed: false,
                dormant: Some(dormant),
            }),
            last_used: AtomicU64::new(now),
            pins: AtomicUsize::new(0),
            listing: Mutex::new(listing),
        }
    }

    /// The loaded graph, if any.
    pub(super) fn graph(&self) -> Option<Arc<RwLock<KnowledgeGraph>>> {
        self.graph.load_full()
    }

    fn touch(&self, now: u64) {
        if self.last_used.load(Ordering::Relaxed) != now {
            self.last_used.store(now, Ordering::Relaxed);
        }
    }

    /// Mark the slot removed by a drop and return its loaded graph. Waits for
    /// a running activation, so a drop never races a load into the slot.
    pub(super) fn remove(&self) -> Option<Arc<RwLock<KnowledgeGraph>>> {
        let mut state = self.state.lock();
        state.removed = true;
        state.dormant = None;
        self.graph.swap(None)
    }

    /// The KG's listing entry: from the graph when loaded, else as kept.
    pub(super) fn listing(&self) -> Listing {
        match self.graph() {
            Some(graph) => {
                let db = graph.read();
                Listing {
                    created_at: Some(db.metadata.created_at.clone()),
                    summary: db.summary(),
                }
            }
            None => self.listing.lock().clone(),
        }
    }
}

/// A slot held so that its KG neither loads, unloads nor is dropped.
pub(super) struct HeldSlot<'a> {
    slot: &'a KgSlot,
    state: parking_lot::MutexGuard<'a, SlotState>,
}

impl HeldSlot<'_> {
    /// The loaded graph, if the KG is loaded.
    pub(super) fn graph(&self) -> Option<Arc<RwLock<KnowledgeGraph>>> {
        self.slot.graph()
    }

    /// What loading the KG needs, if it is dormant (not dropped).
    pub(super) fn dormant(&self) -> Option<&Dormant> {
        self.state.dormant.as_ref()
    }

    /// The KG's creation time, as listed.
    pub(super) fn created_at(&self) -> Option<String> {
        self.slot.listing.lock().created_at.clone()
    }
}

impl KgSlot {
    /// Hold this slot: waits for a running activation or unload.
    pub(super) fn hold(&self) -> HeldSlot<'_> {
        HeldSlot {
            slot: self,
            state: self.state.lock(),
        }
    }
}

impl KnowledgeGraph {
    /// Whether everything this KG holds is on disk, so a reload rebuilds it.
    fn reloadable(&self) -> bool {
        self.retired.is_none()
            && self.schema_catalog.session_len() == 0
            && self.indexes.is_empty()
            && !self.catalog_unsaved
    }

    /// This KG's counts.
    pub(super) fn summary(&self) -> KgSummary {
        KgSummary {
            relations: self.metadata.relations.len(),
            tuples: self.metadata.total_tuples(),
            rules: self.rule_catalog.len(),
            loaded: true,
        }
    }

    /// What reloading this KG needs. Its shards are those of its relations:
    /// every committed fact is in its relation's shard, and a relation
    /// without facts has nothing to load.
    fn to_dormant(&self) -> (Dormant, Listing) {
        let mut shards: Vec<(String, String)> = self
            .store
            .relations()
            .keys()
            .map(|relation| (format!("{}:{relation}", self.name), relation.clone()))
            .collect();
        shards.sort();
        let snapshot = self.snapshot.load();
        let dormant = Dormant {
            shards,
            last_published: Some((snapshot.revision, snapshot.changes().clone())),
            catalog: Vec::new(),
        };
        let listing = Listing {
            created_at: Some(self.metadata.created_at.clone()),
            summary: KgSummary {
                loaded: false,
                ..self.summary()
            },
        };
        (dormant, listing)
    }
}

impl StorageEngine {
    /// Seconds on the engine clock, for access times.
    pub(super) fn clock_now(&self) -> u64 {
        self.clock.elapsed().as_secs()
    }

    /// Every knowledge graph's slot, by name.
    pub(super) fn slots(&self) -> Vec<(String, Arc<KgSlot>)> {
        let mut slots: Vec<(String, Arc<KgSlot>)> = self
            .knowledge_graphs
            .iter()
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect();
        slots.sort_by(|a, b| a.0.cmp(&b.0));
        slots
    }

    /// The slot of `kg`.
    fn slot(&self, kg: &str) -> StorageResult<Arc<KgSlot>> {
        self.knowledge_graphs
            .get(kg)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| StorageError::KnowledgeGraphNotFound(kg.to_string()))
    }

    /// `kg`'s handle, activating it if it is dormant. The handle may be
    /// retired by the time the caller locks it: writers lock through
    /// [`Self::lock_kg`].
    pub(super) fn kg_handle(&self, kg: &str) -> StorageResult<Arc<RwLock<KnowledgeGraph>>> {
        let slot = self.slot(kg)?;
        slot.touch(self.clock_now());
        match slot.graph() {
            Some(graph) => Ok(graph),
            None => self.activate(kg, &slot, true),
        }
    }

    /// Write-lock `kg`, activating it if dormant. A handle retired by an
    /// unload while this waited is retried on the reloaded KG; a dropped KG is
    /// not found.
    pub(super) fn lock_kg(
        &self,
        kg: &str,
    ) -> StorageResult<ArcRwLockWriteGuard<RawRwLock, KnowledgeGraph>> {
        loop {
            let db = self.kg_handle(kg)?.write_arc();
            match db.retired {
                None => return Ok(db),
                Some(Retired::Dropped) => {
                    return Err(StorageError::KnowledgeGraphNotFound(kg.to_string()))
                }
                Some(Retired::Unloaded) => {}
            }
        }
    }

    /// Keep `kg` loaded while the returned pin lives.
    ///
    /// # Errors
    /// `kg` does not exist.
    pub fn pin_knowledge_graph(&self, kg: &str) -> StorageResult<KgPin> {
        let slot = self.slot(kg)?;
        slot.pins.fetch_add(1, Ordering::AcqRel);
        Ok(KgPin { slot })
    }

    /// Whether `kg` is in memory (`None` when it does not exist).
    pub fn is_knowledge_graph_loaded(&self, kg: &str) -> Option<bool> {
        self.slot(kg).ok().map(|slot| slot.graph().is_some())
    }

    /// How many knowledge graphs are in memory.
    pub fn loaded_knowledge_graph_count(&self) -> usize {
        self.loaded.load(Ordering::Acquire)
    }

    /// Knowledge graph loads and unloads since the engine started, as
    /// `(loads, unloads)`. Loads for a checkpoint are not counted.
    pub fn knowledge_graph_residency_counts(&self) -> (u64, u64) {
        (
            self.residency_counts.activations.load(Ordering::Relaxed),
            self.residency_counts.unloads.load(Ordering::Relaxed),
        )
    }

    /// Every knowledge graph's counts, sorted by name, without loading any:
    /// for stats, which must not pull every KG into memory.
    pub fn knowledge_graph_summaries(&self) -> Vec<(String, KgSummary)> {
        self.slots()
            .into_iter()
            .map(|(name, slot)| (name, slot.listing().summary))
            .collect()
    }

    /// Load dormant `kg` into `slot`, or return what another activation
    /// loaded. Holds the slot's mutex while loading, so a KG loads once
    /// however many requests wait for it. A load with `trim` then unloads
    /// other KGs over `storage.max_loaded_knowledge_graphs`.
    fn activate(
        &self,
        kg: &str,
        slot: &KgSlot,
        trim: bool,
    ) -> StorageResult<Arc<RwLock<KnowledgeGraph>>> {
        let mut state = slot.state.lock();
        if let Some(graph) = slot.graph() {
            return Ok(graph);
        }
        if state.removed {
            return Err(StorageError::KnowledgeGraphNotFound(kg.to_string()));
        }
        let Some(dormant) = state.dormant.as_ref() else {
            return Err(StorageError::Other(format!(
                "Knowledge graph '{kg}' is neither loaded nor on disk"
            )));
        };
        let start = Instant::now();
        let created_at = slot.listing.lock().created_at.clone();
        let graph = self.load_graph(kg, dormant, created_at)?;
        let summary = graph.summary();
        let graph = Arc::new(RwLock::new(graph));
        slot.graph.store(Some(Arc::clone(&graph)));
        state.dormant = None;
        let loaded = self.loaded.fetch_add(1, Ordering::AcqRel) + 1;
        self.residency_counts
            .activations
            .fetch_add(1, Ordering::Relaxed);
        self.record_resident(kg, true);
        drop(state);
        info!(
            kg = %kg,
            relations = summary.relations,
            tuples = summary.tuples,
            loaded,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "kg_activated"
        );
        if trim {
            self.unload_over_limit(kg);
        }
        Ok(graph)
    }

    /// Build `kg` from its persist shards and catalog files. A shard whose
    /// multiplicities drifted from set membership is clamped first, with a
    /// committed correction.
    pub(super) fn load_graph(
        &self,
        kg: &str,
        dormant: &Dormant,
        created_at: Option<String>,
    ) -> StorageResult<KnowledgeGraph> {
        let data_dir = self.config.storage.data_dir.join(kg);
        std::fs::create_dir_all(&data_dir)?;

        let shards = self.live_shards(kg, dormant);
        let loaded: Vec<LoadedShard> = shards
            .par_iter()
            .map(|(shard, relation)| self.load_shard(shard, relation))
            .collect::<StorageResult<_>>()?;

        let corrections: Vec<&LoadedShard> =
            loaded.iter().filter(|s| !s.fixes.is_empty()).collect();
        if !corrections.is_empty() {
            let mut txn = Transaction::new(self.logical_time.fetch_add(1, Ordering::SeqCst));
            for shard in corrections {
                warn!(shard = %shard.shard, tuples = shard.fixes.len(), "persist_multiplicity_clamped");
                txn.facts(
                    shard.shard.clone(),
                    shard
                        .fixes
                        .iter()
                        .map(|u| (u.data.clone(), u.diff))
                        .collect(),
                );
            }
            self.persist.commit(txn)?;
        }

        let mut metadata = KnowledgeGraphMetadata::new(kg.to_string());
        if let Some(created_at) = created_at {
            metadata.created_at = created_at;
        }
        let mut relations = Vec::with_capacity(loaded.len());
        for shard in loaded {
            if let Some(arity) = shard.tuples.first().map(Tuple::arity) {
                let schema: Vec<String> = (0..arity).map(|i| format!("col{i}")).collect();
                metadata.add_relation(shard.relation.clone(), schema, shard.tuples.len());
                relations.push((shard.relation, shard.tuples));
            }
        }
        let mut graph = KnowledgeGraph::from_disk(
            self,
            kg,
            data_dir,
            metadata,
            relations,
            dormant.last_published.as_ref(),
        )?;
        if !dormant.catalog.is_empty() {
            match graph.replay_catalog(dormant.catalog.iter()) {
                Ok(Some(revision)) => {
                    if let Err(e) = self.persist.catalog_saved(kg, revision) {
                        warn!(kg = %kg, error = %e, "catalog_wal_prune_failed");
                    }
                }
                Ok(None) => {}
                // Memory has the changes and the WAL keeps them.
                Err(e) => {
                    warn!(kg = %kg, error = %e, "catalog_replay_save_failed");
                    graph.catalog_unsaved = true;
                }
            }
            graph.publish_snapshot();
        }
        Ok(graph)
    }

    /// The shards of dormant `kg` whose relation was not dropped.
    pub(super) fn live_shards<'d>(
        &self,
        kg: &str,
        dormant: &'d Dormant,
    ) -> Vec<&'d (String, String)> {
        let tombstones = if self.has_relation_tombstones.load(Ordering::Acquire) {
            self.tombstones.lock().relations.clone()
        } else {
            std::collections::BTreeSet::new()
        };
        dormant
            .shards
            .iter()
            .filter(|(_, relation)| !tombstones.contains(&RelationTombstone::new(kg, relation)))
            .collect()
    }

    /// Read `shard` and consolidate it to its current tuples.
    fn load_shard(&self, shard: &str, relation: &str) -> StorageResult<LoadedShard> {
        let since = match self.persist.shard_info(shard) {
            Ok(info) => info.since,
            // A relation that never committed a fact has no shard.
            Err(_) if !self.persist.has_shard(shard) => {
                return Ok(LoadedShard {
                    shard: shard.to_string(),
                    relation: relation.to_string(),
                    tuples: Vec::new(),
                    fixes: Vec::new(),
                })
            }
            Err(e) => return Err(e),
        };
        let mut updates = self.persist.read(shard, since)?;
        consolidate_to_current(&mut updates);
        let fixes = set_semantics_corrections(&updates, 0);
        Ok(LoadedShard {
            shard: shard.to_string(),
            relation: relation.to_string(),
            tuples: into_tuples(updates),
            fixes,
        })
    }

    /// The knowledge graphs to load after a restart: those in memory when
    /// the engine last ran, sorted by name.
    pub fn resident_knowledge_graphs(&self) -> Vec<String> {
        self.resident.lock().iter().cloned().collect()
    }

    /// Load dormant `kg` ahead of its first request, as the restart warm-up
    /// does for each of [`Self::resident_knowledge_graphs`]. Returns whether
    /// it loaded: not when `kg` is loaded already, was dropped, or
    /// `storage.max_loaded_knowledge_graphs` are loaded. A KG left dormant by
    /// that limit is no longer recorded as in memory, until a request loads
    /// it. The warm-up never unloads another KG: a request loading one at the
    /// same time can leave more than the limit loaded, until the next request
    /// that loads a KG unloads the excess.
    ///
    /// # Errors
    /// The load failed; the KG's first request retries it.
    pub fn warm_knowledge_graph(&self, kg: &str) -> StorageResult<bool> {
        let Ok(slot) = self.slot(kg) else {
            return Ok(false);
        };
        if slot.graph().is_some() {
            return Ok(false);
        }
        let limit = self.config.storage.max_loaded_knowledge_graphs;
        if limit > 0 && self.loaded_knowledge_graph_count() >= limit {
            let _state = slot.state.lock();
            if slot.graph().is_none() {
                self.record_resident(kg, false);
            }
            return Ok(false);
        }
        match self.activate(kg, &slot, false) {
            Ok(_) => Ok(true),
            Err(StorageError::KnowledgeGraphNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Record that `kg` is in memory, or no longer is, and save the names
    /// when that changes them. Called under the slot's mutex when `kg` loads
    /// or unloads, so the record follows the slot. The file only spares the
    /// first request after a restart its wait, so a failed save is logged,
    /// not returned.
    pub(super) fn record_resident(&self, kg: &str, resident: bool) {
        let mut names = self.resident.lock();
        let changed = if resident {
            !names.contains(kg) && names.insert(kg.to_string())
        } else {
            names.remove(kg)
        };
        if !changed {
            return;
        }
        // Saved under the mutex, so the file holds the latest names.
        let path = resident_file(&self.config.storage.data_dir);
        if let Err(e) = write_resident(&path, &names) {
            warn!(path = %path.display(), error = %e, "kg_resident_save_failed");
        }
    }

    /// Unload least recently used KGs until at most
    /// `storage.max_loaded_knowledge_graphs` are loaded, sparing `keep`.
    fn unload_over_limit(&self, keep: &str) {
        let limit = self.config.storage.max_loaded_knowledge_graphs;
        if limit == 0 || self.loaded_knowledge_graph_count() <= limit {
            return;
        }
        let mut candidates = self.unload_candidates(|name, _| name != keep);
        candidates.sort_by_key(|(_, slot)| slot.last_used.load(Ordering::Relaxed));
        let mut unloaded = 0;
        for (name, slot) in candidates {
            if self.loaded_knowledge_graph_count() <= limit {
                break;
            }
            if self.try_unload(&name, &slot) {
                unloaded += 1;
            }
        }
        let loaded = self.loaded_knowledge_graph_count();
        if loaded > limit {
            warn!(
                loaded,
                limit, unloaded, "kg_residency_over_limit: the other loaded KGs are in use"
            );
        }
    }

    /// Unload every KG unused for at least `idle`. Returns how many were
    /// unloaded. The server calls this periodically when
    /// `storage.unload_idle_after_secs` is set.
    pub fn unload_idle_knowledge_graphs(&self, idle: Duration) -> usize {
        let now = self.clock_now();
        let idle = idle.as_secs();
        self.unload_candidates(|_, slot| {
            now.saturating_sub(slot.last_used.load(Ordering::Relaxed)) >= idle
        })
        .into_iter()
        .filter(|(name, slot)| self.try_unload(name, slot))
        .count()
    }

    /// Loaded, unpinned KGs passing `keep`, except `_internal`, which
    /// authentication reads.
    fn unload_candidates(
        &self,
        keep: impl Fn(&str, &KgSlot) -> bool,
    ) -> Vec<(String, Arc<KgSlot>)> {
        self.knowledge_graphs
            .iter()
            .filter(|entry| {
                let slot = entry.value();
                entry.key() != crate::auth::INTERNAL_KG
                    && slot.pins.load(Ordering::Acquire) == 0
                    && slot.graph.load().is_some()
                    && keep(entry.key(), slot)
            })
            .map(|entry| (entry.key().clone(), Arc::clone(entry.value())))
            .collect()
    }

    /// Unload `name` if nothing uses it (see the module documentation).
    /// Returns whether it was unloaded.
    fn try_unload(&self, name: &str, slot: &KgSlot) -> bool {
        let start = Instant::now();
        // A slot busy loading, unloading or dropping is in use; never wait
        // for it on a request's path.
        let Some(mut state) = slot.state.try_lock() else {
            return false;
        };
        if state.removed || slot.pins.load(Ordering::Acquire) > 0 {
            return false;
        }
        let Some(graph) = slot.graph() else {
            return false;
        };
        // The slot holds one reference and `graph` another: nobody else has
        // the handle, and a new lookup waits for the write lock below.
        if Arc::strong_count(&graph) > 2 {
            return false;
        }
        let Some(mut db) = graph.try_write() else {
            return false;
        };
        // The KG holds one reference to its snapshot and this load another:
        // nobody reads it. A reader holding it would keep its tuples alive
        // and the reload would copy them.
        if !db.reloadable() || Arc::strong_count(&db.snapshot.load_full()) > 2 {
            return false;
        }
        let (dormant, listing) = db.to_dormant();
        let summary = listing.summary;
        db.retired = Some(Retired::Unloaded);
        drop(db);
        *slot.listing.lock() = listing;
        slot.graph.store(None);
        state.dormant = Some(dormant);
        let loaded = self.loaded.fetch_sub(1, Ordering::AcqRel) - 1;
        self.residency_counts
            .unloads
            .fetch_add(1, Ordering::Relaxed);
        self.record_resident(name, false);
        drop(state);
        // Free the graph outside the slot's mutex.
        drop(graph);
        info!(
            kg = %name,
            relations = summary.relations,
            tuples = summary.tuples,
            loaded,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "kg_unloaded"
        );
        true
    }

    /// Mark `slot` removed (a drop) and retire its loaded graph. Waits for
    /// in-flight writes to `kg`, which then fail as not found.
    pub(super) fn retire_dropped(&self, slot: &KgSlot) {
        if let Some(graph) = slot.remove() {
            graph.write().retired = Some(Retired::Dropped);
            self.loaded.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// One shard read for activation.
struct LoadedShard {
    shard: String,
    relation: String,
    tuples: Vec<Tuple>,
    fixes: Vec<crate::storage::persist::Update>,
}

/// The file naming the knowledge graphs in memory.
fn resident_file(data_dir: &Path) -> PathBuf {
    data_dir
        .join("metadata")
        .join("resident_knowledge_graphs.json")
}

/// The knowledge graphs that were in memory when the engine last ran. A
/// missing or unreadable file reads as none: each KG then loads on first use.
pub(super) fn read_resident(data_dir: &Path) -> BTreeSet<String> {
    std::fs::read(resident_file(data_dir))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Replace the file at `path` with `names`. Not synced: a process crash
/// keeps the file, and after power loss a stale or empty one costs only the
/// warm-up.
fn write_resident(path: &Path, names: &BTreeSet<String>) -> StorageResult<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec(names)?)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// The revisions of `kg_dir`'s rule and schema catalog files and its rule
/// count, read without parsing rules. Missing files read as zero; an
/// unreadable one is reported by activation, which loads it fully.
pub(super) fn catalog_revision(kg_dir: &Path) -> (u64, usize) {
    #[derive(serde::Deserialize)]
    struct Revision {
        #[serde(default)]
        revision: u64,
    }
    #[derive(serde::Deserialize)]
    struct Rules {
        #[serde(default)]
        revision: u64,
        #[serde(default)]
        // Rule names only; the definitions are skipped unparsed.
        #[allow(clippy::zero_sized_map_values)]
        rules: std::collections::BTreeMap<String, serde::de::IgnoredAny>,
    }
    let read = |path: &Path| std::fs::read(path).ok();
    let (rules_revision, rules) = read(&kg_dir.join("rules").join("catalog.json"))
        .and_then(|bytes| serde_json::from_slice::<Rules>(&bytes).ok())
        .map_or((0, 0), |file| (file.revision, file.rules.len()));
    let schema_revision = read(&kg_dir.join(crate::schema::catalog::SCHEMA_CATALOG_FILE))
        .and_then(|bytes| serde_json::from_slice::<Revision>(&bytes).ok())
        .map_or(0, |file| file.revision);
    (rules_revision.max(schema_revision), rules)
}

#[cfg(test)]
#[path = "residency_tests.rs"]
mod tests;
