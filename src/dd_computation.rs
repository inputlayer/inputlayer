//! Persistent Differential Dataflow Computation
//!
//! `DDComputation` - a long-lived DD computation for one knowledge graph.
//! Owns a timely worker thread, InputSessions for base relations, and shared
//! arrangements for materialized views.
//!
//! ## Architecture
//!
//! ```text
//! Main thread --command_tx--► DD worker thread
//!                              |- Owns timely Worker
//!                              |- Owns InputSessions (one per base relation)
//!                              |- Owns Arrangements (one per relation + view)
//!                              |- Steps worker in event loop
//!                              `- Processes commands between steps
//! ```
//!
//! ## Thread Safety
//!
//! InputSessions and TraceAgents are NOT Send/Sync (they use Rc internally).
//! All DD state lives on the worker thread. The main thread communicates
//! exclusively through the command channel. Queries that need data from
//! arrangements send a response channel and block until the worker replies.

use crate::derived_relations::{CompiledRule, DerivedRelationsManager};
use crate::index_manager::{Index, IndexManager, IndexStats, RegisteredIndex, TupleId};
use crate::value::Tuple;
use crossbeam_channel as channel;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Commands sent from the main thread to the DD worker thread.
pub enum DDCommand {
    /// Feed a batch of updates into a relation's InputSession.
    /// Each update is (data, time, diff) where diff is +1 for insert, -1 for delete.
    InsertDelta {
        relation: String,
        updates: Vec<(Tuple, u64, isize)>,
    },

    /// Advance time on all InputSessions and flush.
    /// The worker will step until computation catches up.
    AdvanceTime(u64),

    /// Block until the probe frontier advances past the given time.
    /// Sends () on the response channel when done.
    WaitUntilCaughtUp {
        time: u64,
        response: channel::Sender<()>,
    },

    /// Read all current tuples from a relation's arrangement.
    /// Returns the tuples via the response channel.
    ReadRelation {
        relation: String,
        response: channel::Sender<Vec<Tuple>>,
    },

    /// Add a new relation dynamically.
    /// Creates a new InputSession and arrangement in a new dataflow.
    AddRelation {
        name: String,
        response: channel::Sender<()>,
    },

    /// Shut down the computation cleanly.
    /// Sends () on the response channel when shutdown is complete.
    Shutdown { response: channel::Sender<()> },

    // === Derived Relations ===
    /// Register a compiled rule for materialization.
    /// The rule is stored but not immediately materialized.
    RegisterRule {
        rule: CompiledRule,
        response: channel::Sender<Result<(), String>>,
    },

    /// Remove a rule and its materialization.
    RemoveRule {
        name: String,
        response: channel::Sender<()>,
    },

    /// Read a derived relation's materialized data.
    /// Returns None if not materialized or invalid.
    ReadDerivedRelation {
        relation: String,
        response: channel::Sender<Option<Vec<Tuple>>>,
    },

    /// Set materialized data for a derived relation.
    /// Called after executing a rule to cache its results.
    SetMaterialized {
        relation: String,
        tuples: Vec<Tuple>,
        response: channel::Sender<()>,
    },

    /// Notify that a base relation has been updated.
    /// Invalidates dependent derived relations.
    NotifyBaseUpdate {
        relation: String,
        response: channel::Sender<Vec<String>>,
    },

    /// Get the current derived relations manager state.
    GetDerivedStats {
        response: channel::Sender<(usize, usize, usize)>, // (total_rules, materialized, invalid)
    },

    // === Index Management Commands ===
    /// Register a new index (metadata only, does not build)
    RegisterIndex {
        index: RegisteredIndex,
        response: channel::Sender<Result<(), String>>,
    },

    /// Remove an index
    RemoveIndex {
        name: String,
        response: channel::Sender<Result<(), String>>,
    },

    /// Store built index data
    SetIndexMaterialized {
        name: String,
        index: Box<dyn Index + Send + Sync>,
        tuple_count: usize,
        response: channel::Sender<()>,
    },

    /// Read an index for query (returns stats, not the index itself)
    GetIndexStats {
        name: Option<String>,
        response: channel::Sender<Vec<IndexStats>>,
    },

    /// Incremental index update (batched inserts and deletes)
    UpdateIndex {
        name: String,
        inserts: Vec<(TupleId, Vec<f32>)>,
        deletes: Vec<TupleId>,
        response: channel::Sender<Result<(), String>>,
    },

    /// Notify indexes that a base relation has been updated
    NotifyIndexesBaseUpdate {
        relation: String,
        response: channel::Sender<Vec<String>>,
    },
}

/// Handle to a persistent DD computation for one knowledge graph.
///
/// Each knowledge graph gets one DDComputation. It owns:
/// - A dedicated worker thread running a timely computation with u64 timestamps
/// - InputSessions for feeding deltas into base relations
/// - Arrangements for each base relation (queryable)
///
/// All DD state is owned by the worker thread (InputSession and TraceAgent
/// are not Send). The main thread communicates through the command channel.
pub struct DDComputation {
    /// Channel for sending commands to the DD worker thread.
    command_tx: channel::Sender<DDCommand>,

    /// Handle to the background worker thread.
    worker_handle: Option<std::thread::JoinHandle<()>>,

    /// Current logical time (monotonically increasing).
    current_time: Arc<AtomicU64>,

    /// Maximum timestamp used in any write (for lazy time advancement on reads).
    max_write_time: Arc<AtomicU64>,

    /// Track which relations have been created (fast path for ensure_relation).
    known_relations: Mutex<HashSet<String>>,

    /// Derived relations manager (shared with worker via Arc<Mutex<>>).
    /// This lives on the main thread but is also accessible from worker.
    derived_relations: Arc<Mutex<DerivedRelationsManager>>,

    /// Index manager for HNSW and other indexes (shared with worker).
    index_manager: Arc<Mutex<IndexManager>>,
}

impl DDComputation {
    /// Create a new persistent DD computation with a single worker thread.
    ///
    /// `relations` specifies the base relations to create InputSessions for.
    /// Each relation gets an InputSession and an arrangement.
    ///
    /// # Design Note
    /// DD uses single-worker execution for simplicity and correctness.
    /// Parallel query execution is handled by Rayon at the query level,
    /// not by DD workers. This design provides:
    /// - Simpler coordination for shadow writes
    /// - Deterministic behavior for materialization
    /// - Lower memory overhead
    ///
    /// # Errors
    /// Returns error if the worker thread fails to spawn.
    pub fn new(relations: Vec<String>) -> Result<Self, String> {
        let (command_tx, command_rx) = channel::unbounded::<DDCommand>();
        let current_time = Arc::new(AtomicU64::new(0));
        let max_write_time = Arc::new(AtomicU64::new(0));
        let known_relations = Mutex::new(relations.iter().cloned().collect());
        let derived_relations = Arc::new(Mutex::new(DerivedRelationsManager::new()));
        let derived_relations_clone = Arc::clone(&derived_relations);
        let index_manager = Arc::new(Mutex::new(IndexManager::new()));
        let index_manager_clone = Arc::clone(&index_manager);

        let worker_handle = std::thread::Builder::new()
            .name("dd-worker".to_string())
            .spawn(move || {
                Self::worker_loop(
                    relations,
                    command_rx,
                    derived_relations_clone,
                    index_manager_clone,
                );
            })
            .map_err(|e| format!("Failed to spawn DD worker thread: {e}"))?;

        Ok(DDComputation {
            command_tx,
            worker_handle: Some(worker_handle),
            current_time,
            max_write_time,
            known_relations,
            derived_relations,
            index_manager,
        })
    }

    /// The DD worker thread's main loop.
    ///
    /// Creates a timely computation with u64 timestamps, InputSessions for
    /// each relation, and arrangements. Then enters a command-processing loop.
    fn worker_loop(
        relations: Vec<String>,
        command_rx: channel::Receiver<DDCommand>,
        derived_relations: Arc<Mutex<DerivedRelationsManager>>,
        index_manager: Arc<Mutex<IndexManager>>,
    ) {
        use differential_dataflow::input::Input;
        use differential_dataflow::operators::arrange::ArrangeBySelf;
        use differential_dataflow::trace::cursor::Cursor;
        use differential_dataflow::trace::TraceReader;
        use timely::dataflow::operators::Probe;
        use timely::dataflow::ProbeHandle;

        timely::execute_directly(move |worker| {
            // Build the dataflow graph
            let mut probe = ProbeHandle::<u64>::new();

            // InputSessions and Traces are created inside the dataflow closure
            // and then moved out for use in the command loop.
            let mut input_sessions: HashMap<
                String,
                differential_dataflow::input::InputSession<u64, Tuple, isize>,
            > = HashMap::new();

            // Traces for reading back data via cursor.
            // Key=Tuple, Val=(), Time=u64, R=isize
            type KeyTrace =
                differential_dataflow::trace::implementations::ord::OrdKeySpine<Tuple, u64, isize>;
            type KeyTraceAgent = differential_dataflow::operators::arrange::TraceAgent<KeyTrace>;
            let mut traces: HashMap<String, KeyTraceAgent> = HashMap::new();

            worker.dataflow::<u64, _, _>(|scope| {
                for relation in &relations {
                    let (session, collection) = scope.new_collection::<Tuple, isize>();
                    input_sessions.insert(relation.clone(), session);

                    // Arrange by self (full tuple as key, () as value).
                    // This gives us a persistent, queryable index.
                    let arranged = collection.arrange_by_self();

                    // Probe for frontier tracking
                    arranged.stream.probe_with(&mut probe);

                    // Clone the trace handle for reading outside the dataflow
                    traces.insert(relation.clone(), arranged.trace.clone());
                }
            });

            // Command processing loop
            //
            // Uses blocking recv to avoid busy-spinning. Only calls worker.step()
            // when explicitly needed (AdvanceTime, WaitUntilCaughtUp, Shutdown).
            // This prevents DD's merge batcher from being triggered when large
            // amounts of data are buffered without time advancement.
            loop {
                // Block until at least one command arrives
                let first_cmd = match command_rx.recv() {
                    Ok(cmd) => cmd,
                    Err(_) => return, // channel disconnected
                };

                // Collect this command and any others that arrived meanwhile
                let mut commands = vec![first_cmd];
                while let Ok(cmd) = command_rx.try_recv() {
                    commands.push(cmd);
                }

                // Process all collected commands
                for cmd in commands {
                    match cmd {
                        DDCommand::InsertDelta { relation, updates } => {
                            if let Some(session) = input_sessions.get_mut(&relation) {
                                for (data, time, diff) in updates {
                                    session.update_at(data, time, diff);
                                }
                            }
                        }

                        DDCommand::AdvanceTime(time) => {
                            for session in input_sessions.values_mut() {
                                session.advance_to(time);
                                session.flush();
                            }
                            // Step once to begin processing flushed data
                            worker.step();
                        }

                        DDCommand::WaitUntilCaughtUp { time, response } => {
                            // Flush all sessions first
                            for session in input_sessions.values_mut() {
                                session.flush();
                            }
                            // Step until the probe frontier passes the requested time
                            while probe.less_than(&time) {
                                worker.step();
                            }
                            let _ = response.send(());
                        }

                        DDCommand::ReadRelation { relation, response } => {
                            let mut result = Vec::new();

                            if let Some(trace) = traces.get_mut(&relation) {
                                let (mut cursor, storage) = trace.cursor();

                                while cursor.key_valid(&storage) {
                                    let key = cursor.key(&storage).clone();
                                    let mut total_diff: isize = 0;
                                    cursor.map_times(&storage, |_time, diff| {
                                        total_diff += diff;
                                    });
                                    if total_diff > 0 {
                                        result.push(key);
                                    }
                                    cursor.step_key(&storage);
                                }
                            }

                            let _ = response.send(result);
                        }

                        DDCommand::AddRelation { name, response } => {
                            if !input_sessions.contains_key(&name) {
                                worker.dataflow::<u64, _, _>(|scope| {
                                    let (session, collection) =
                                        scope.new_collection::<Tuple, isize>();
                                    input_sessions.insert(name.clone(), session);
                                    let arranged = collection.arrange_by_self();
                                    arranged.stream.probe_with(&mut probe);
                                    traces.insert(name.clone(), arranged.trace.clone());
                                });
                            }
                            let _ = response.send(());
                        }

                        DDCommand::Shutdown { response } => {
                            // Drop all sessions without processing remaining data.
                            // Calling worker.step() with large buffered batches can
                            // trigger a DD merge batcher bug. Since we cleanly close
                            // the InputSessions, timely will clean up on scope drop.
                            input_sessions.clear();
                            traces.clear();
                            let _ = response.send(());
                            return;
                        }

                        // === Derived Relations Commands ===
                        DDCommand::RegisterRule { rule, response } => {
                            let mut manager = derived_relations.lock();
                            manager.register_rule(rule);
                            let _ = response.send(Ok(()));
                        }

                        DDCommand::RemoveRule { name, response } => {
                            let mut manager = derived_relations.lock();
                            manager.remove_rule(&name);
                            let _ = response.send(());
                        }

                        DDCommand::ReadDerivedRelation { relation, response } => {
                            let manager = derived_relations.lock();
                            let result = manager
                                .get_materialized(&relation)
                                .map(|m| m.tuples.clone());
                            let _ = response.send(result);
                        }

                        DDCommand::SetMaterialized {
                            relation,
                            tuples,
                            response,
                        } => {
                            let mut manager = derived_relations.lock();
                            manager.set_materialized(&relation, tuples);
                            let _ = response.send(());
                        }

                        DDCommand::NotifyBaseUpdate { relation, response } => {
                            let mut manager = derived_relations.lock();
                            let invalidated = manager.notify_base_update(&relation);
                            let _ = response.send(invalidated);
                        }

                        DDCommand::GetDerivedStats { response } => {
                            let manager = derived_relations.lock();
                            let stats = manager.stats();
                            let _ = response.send((
                                stats.total_rules,
                                stats.materialized_count,
                                stats.invalid_count,
                            ));
                        }

                        // === Index Management Commands ===
                        DDCommand::RegisterIndex { index, response } => {
                            let mut manager = index_manager.lock();
                            let result = manager.register_index(index);
                            let _ = response.send(result);
                        }

                        DDCommand::RemoveIndex { name, response } => {
                            let mut manager = index_manager.lock();
                            let result = manager.remove_index(&name);
                            let _ = response.send(result);
                        }

                        DDCommand::SetIndexMaterialized {
                            name,
                            index,
                            tuple_count,
                            response,
                        } => {
                            let mut manager = index_manager.lock();
                            manager.set_materialized(&name, index, tuple_count);
                            let _ = response.send(());
                        }

                        DDCommand::GetIndexStats { name, response } => {
                            let manager = index_manager.lock();
                            let stats = match name {
                                Some(n) => manager.get_stats(&n).into_iter().collect(),
                                None => manager.get_all_stats(),
                            };
                            let _ = response.send(stats);
                        }

                        DDCommand::UpdateIndex {
                            name,
                            inserts,
                            deletes,
                            response,
                        } => {
                            let mut manager = index_manager.lock();
                            let result = if let Some(mat) = manager.get_materialized_mut(&name) {
                                // Apply incremental updates
                                for id in deletes {
                                    mat.index.delete(id);
                                }
                                let mut insert_result = Ok(());
                                for (id, vector) in inserts {
                                    if let Err(e) = mat.index.insert(id, &vector) {
                                        insert_result = Err(e);
                                        break;
                                    }
                                }
                                mat.tuple_count = mat.index.len();
                                insert_result
                            } else {
                                Err(format!("Index '{name}' not found or invalid"))
                            };
                            let _ = response.send(result);
                        }

                        DDCommand::NotifyIndexesBaseUpdate { relation, response } => {
                            let mut manager = index_manager.lock();
                            let invalidated = manager.notify_base_update(&relation);
                            let _ = response.send(invalidated);
                        }
                    }
                }
            }
        });
    }

    /// Insert a batch of tuples into a relation.
    ///
    /// Each tuple is inserted at the given logical time with diff=+1.
    /// Tracks `max_write_time` for lazy time advancement on reads.
    pub fn insert(&self, relation: &str, tuples: Vec<Tuple>, time: u64) -> Result<(), String> {
        self.ensure_relation(relation)?;
        self.max_write_time.fetch_max(time, Ordering::SeqCst);

        let updates: Vec<(Tuple, u64, isize)> = tuples.into_iter().map(|t| (t, time, 1)).collect();

        self.command_tx
            .send(DDCommand::InsertDelta {
                relation: relation.to_string(),
                updates,
            })
            .map_err(|_| "DD worker disconnected".to_string())
    }

    /// Delete a batch of tuples from a relation.
    ///
    /// Each tuple is retracted at the given logical time with diff=-1.
    /// Tracks `max_write_time` for lazy time advancement on reads.
    pub fn delete(&self, relation: &str, tuples: Vec<Tuple>, time: u64) -> Result<(), String> {
        self.ensure_relation(relation)?;
        self.max_write_time.fetch_max(time, Ordering::SeqCst);

        let updates: Vec<(Tuple, u64, isize)> = tuples.into_iter().map(|t| (t, time, -1)).collect();

        self.command_tx
            .send(DDCommand::InsertDelta {
                relation: relation.to_string(),
                updates,
            })
            .map_err(|_| "DD worker disconnected".to_string())
    }

    /// Advance the logical time and flush all InputSessions.
    pub fn advance_time(&self, time: u64) -> Result<(), String> {
        self.current_time.store(time, Ordering::SeqCst);
        self.command_tx
            .send(DDCommand::AdvanceTime(time))
            .map_err(|_| "DD worker disconnected".to_string())
    }

    /// Block until the computation has processed all updates through the given time.
    ///
    /// This provides strong read consistency: after this returns, any query
    /// will see all inserts that happened before `time`.
    pub fn wait_until_caught_up(&self, time: u64) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::WaitUntilCaughtUp { time, response: tx })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while waiting".to_string())
    }

    /// Read all current tuples from a relation's arrangement.
    ///
    /// Returns tuples that have a positive net diff (i.e., currently present).
    pub fn read_relation(&self, relation: &str) -> Result<Vec<Tuple>, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::ReadRelation {
                relation: relation.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while reading".to_string())
    }

    /// Read all current tuples from a relation's arrangement with consistency.
    ///
    /// This is the preferred read method. It lazily advances time to cover
    /// all pending writes, waits for the computation to catch up, then reads.
    /// After this returns, the result reflects all inserts/deletes that preceded it.
    pub fn read_relation_consistent(&self, relation: &str) -> Result<Vec<Tuple>, String> {
        let max_time = self.max_write_time.load(Ordering::SeqCst);
        let target = max_time + 1;
        self.advance_time(target)?;
        self.wait_until_caught_up(target)?;
        self.read_relation(relation)
    }

    /// Get the current logical time.
    pub fn current_time(&self) -> u64 {
        self.current_time.load(Ordering::SeqCst)
    }

    /// Get the maximum write time seen so far.
    pub fn max_write_time(&self) -> u64 {
        self.max_write_time.load(Ordering::SeqCst)
    }

    /// Ensure a relation exists in the DD computation.
    ///
    /// If the relation doesn't exist yet, creates a new InputSession and
    /// arrangement for it in a new dataflow on the worker thread.
    /// Idempotent  -  calling for an existing relation is a fast no-op.
    pub fn ensure_relation(&self, name: &str) -> Result<(), String> {
        {
            let known = self.known_relations.lock();
            if known.contains(name) {
                return Ok(());
            }
        }
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::AddRelation {
                name: name.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while adding relation".to_string())?;
        self.known_relations.lock().insert(name.to_string());
        Ok(())
    }

    // === Derived Relations API ===

    /// Register a compiled rule for materialization.
    ///
    /// The rule is stored in the DerivedRelationsManager but not immediately
    /// materialized. Materialization happens on first read or explicit request.
    pub fn register_rule(&self, rule: CompiledRule) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::RegisterRule { rule, response: tx })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while registering rule".to_string())?
    }

    /// Remove a rule and its materialization.
    pub fn remove_rule(&self, name: &str) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::RemoveRule {
                name: name.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while removing rule".to_string())
    }

    /// Read materialized data for a derived relation.
    ///
    /// Returns None if the relation is not materialized or the materialization
    /// is invalid (base data has changed since materialization).
    pub fn read_derived_relation(&self, relation: &str) -> Result<Option<Vec<Tuple>>, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::ReadDerivedRelation {
                relation: relation.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while reading derived relation".to_string())
    }

    /// Set materialized data for a derived relation.
    ///
    /// Called after executing a rule to cache its results. The manager tracks
    /// which base relation versions this materialization is based on.
    pub fn set_materialized(&self, relation: &str, tuples: Vec<Tuple>) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::SetMaterialized {
                relation: relation.to_string(),
                tuples,
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while setting materialized".to_string())
    }

    /// Notify that a base relation has been updated.
    ///
    /// This invalidates all derived relations that depend on the base relation.
    /// Returns the names of relations that were invalidated.
    pub fn notify_base_update(&self, relation: &str) -> Result<Vec<String>, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::NotifyBaseUpdate {
                relation: relation.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while notifying base update".to_string())
    }

    /// Get statistics about derived relations.
    ///
    /// Returns (total_rules, materialized_count, invalid_count).
    pub fn get_derived_stats(&self) -> Result<(usize, usize, usize), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::GetDerivedStats { response: tx })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while getting stats".to_string())
    }

    /// Check if a relation is a derived relation (has a registered rule).
    pub fn is_derived_relation(&self, name: &str) -> bool {
        self.derived_relations.lock().is_derived(name)
    }

    /// Get direct access to the derived relations manager (for advanced use).
    ///
    /// This returns an Arc to the manager, allowing direct access without
    /// going through the command channel. Use with care - the manager
    /// is also accessed by the worker thread.
    pub fn derived_relations(&self) -> Arc<Mutex<DerivedRelationsManager>> {
        Arc::clone(&self.derived_relations)
    }

    // === Index Management API ===

    /// Register a new index (metadata only, does not build).
    ///
    /// Returns an error if an index with the same name already exists.
    pub fn register_index(&self, index: RegisteredIndex) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::RegisterIndex {
                index,
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while registering index".to_string())?
    }

    /// Remove an index.
    pub fn remove_index(&self, name: &str) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::RemoveIndex {
                name: name.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while removing index".to_string())?
    }

    /// Store a built index.
    ///
    /// Called after building an index to make it available for queries.
    pub fn set_index_materialized(
        &self,
        name: &str,
        index: Box<dyn Index + Send + Sync>,
        tuple_count: usize,
    ) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::SetIndexMaterialized {
                name: name.to_string(),
                index,
                tuple_count,
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while setting index".to_string())
    }

    /// Get index statistics.
    ///
    /// If `name` is Some, returns stats for that specific index.
    /// If `name` is None, returns stats for all indexes.
    pub fn get_index_stats(&self, name: Option<&str>) -> Result<Vec<IndexStats>, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::GetIndexStats {
                name: name.map(String::from),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while getting index stats".to_string())
    }

    /// Apply incremental updates to an index.
    ///
    /// Used to keep indexes in sync with base relation updates.
    /// `inserts` is a list of (tuple_id, vector) pairs to insert.
    /// `deletes` is a list of tuple_ids to mark as deleted.
    pub fn update_index(
        &self,
        name: &str,
        inserts: Vec<(TupleId, Vec<f32>)>,
        deletes: Vec<TupleId>,
    ) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::UpdateIndex {
                name: name.to_string(),
                inserts,
                deletes,
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while updating index".to_string())?
    }

    /// Notify indexes that a base relation has been updated.
    ///
    /// This invalidates all indexes that depend on the relation.
    /// Returns the names of indexes that were invalidated.
    pub fn notify_indexes_base_update(&self, relation: &str) -> Result<Vec<String>, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(DDCommand::NotifyIndexesBaseUpdate {
                relation: relation.to_string(),
                response: tx,
            })
            .map_err(|_| "DD worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "DD worker disconnected while notifying indexes".to_string())
    }

    /// Check if an index exists.
    pub fn has_index(&self, name: &str) -> bool {
        self.index_manager.lock().has_index(name)
    }

    /// Get direct access to the index manager (for advanced use).
    ///
    /// This returns an Arc to the manager, allowing direct access without
    /// going through the command channel. Use with care - the manager
    /// is also accessed by the worker thread.
    pub fn index_manager(&self) -> Arc<Mutex<IndexManager>> {
        Arc::clone(&self.index_manager)
    }

    /// Shut down the computation cleanly.
    ///
    /// Blocks until the worker thread has finished.
    pub fn shutdown(mut self) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        let _ = self.command_tx.send(DDCommand::Shutdown { response: tx });
        let _ = rx.recv();

        if let Some(handle) = self.worker_handle.take() {
            handle
                .join()
                .map_err(|_| "DD worker thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for DDComputation {
    fn drop(&mut self) {
        // Send shutdown command (best-effort, worker may already be gone)
        let (tx, rx) = channel::bounded(1);
        let _ = self.command_tx.send(DDCommand::Shutdown { response: tx });
        let _ = rx.recv_timeout(std::time::Duration::from_secs(5));

        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_dd_computation_lifecycle() {
        // Create with one relation
        let dd = DDComputation::new(vec!["edge".to_string()]).unwrap();

        // Insert some data
        dd.insert(
            "edge",
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
            1,
        )
        .unwrap();

        // Advance time
        dd.advance_time(2).unwrap();

        // Wait for processing
        dd.wait_until_caught_up(2).unwrap();

        // Read back
        let tuples = dd.read_relation("edge").unwrap();
        assert_eq!(tuples.len(), 2);

        // Shutdown cleanly
        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_empty_relation() {
        let dd = DDComputation::new(vec!["empty".to_string()]).unwrap();
        dd.advance_time(1).unwrap();
        dd.wait_until_caught_up(1).unwrap();

        let tuples = dd.read_relation("empty").unwrap();
        assert_eq!(tuples.len(), 0);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_unknown_relation() {
        let dd = DDComputation::new(vec!["known".to_string()]).unwrap();
        dd.advance_time(1).unwrap();
        dd.wait_until_caught_up(1).unwrap();

        // Reading an unknown relation returns empty (no InputSession for it)
        let tuples = dd.read_relation("unknown").unwrap();
        assert_eq!(tuples.len(), 0);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_insert_and_delete() {
        let dd = DDComputation::new(vec!["data".to_string()]).unwrap();

        let t1 = Tuple::new(vec![Value::Int32(1)]);
        let t2 = Tuple::new(vec![Value::Int32(2)]);

        // Insert two tuples at time 1
        dd.insert("data", vec![t1.clone(), t2.clone()], 1).unwrap();
        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        let tuples = dd.read_relation("data").unwrap();
        assert_eq!(tuples.len(), 2);

        // Delete one tuple at time 2
        dd.delete("data", vec![t1.clone()], 2).unwrap();
        dd.advance_time(3).unwrap();
        dd.wait_until_caught_up(3).unwrap();

        let tuples = dd.read_relation("data").unwrap();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], t2);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_multiple_relations() {
        let dd = DDComputation::new(vec!["authors".to_string(), "papers".to_string()]).unwrap();

        dd.insert("authors", vec![Tuple::new(vec![Value::string("alice")])], 1)
            .unwrap();
        dd.insert(
            "papers",
            vec![
                Tuple::new(vec![Value::string("paper1")]),
                Tuple::new(vec![Value::string("paper2")]),
            ],
            1,
        )
        .unwrap();

        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        assert_eq!(dd.read_relation("authors").unwrap().len(), 1);
        assert_eq!(dd.read_relation("papers").unwrap().len(), 2);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_incremental_updates() {
        let dd = DDComputation::new(vec!["items".to_string()]).unwrap();

        // Insert batch 1 at time 1
        dd.insert("items", vec![Tuple::new(vec![Value::Int32(1)])], 1)
            .unwrap();
        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();
        assert_eq!(dd.read_relation("items").unwrap().len(), 1);

        // Insert batch 2 at time 2
        dd.insert(
            "items",
            vec![
                Tuple::new(vec![Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(3)]),
            ],
            2,
        )
        .unwrap();
        dd.advance_time(3).unwrap();
        dd.wait_until_caught_up(3).unwrap();
        assert_eq!(dd.read_relation("items").unwrap().len(), 3);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_drop_triggers_shutdown() {
        // Verify that dropping DDComputation shuts down the worker thread
        let dd = DDComputation::new(vec!["test".to_string()]).unwrap();
        dd.insert("test", vec![Tuple::new(vec![Value::Int32(42)])], 1)
            .unwrap();

        // Drop should trigger shutdown without hanging
        drop(dd);
        // If we reach here, the drop completed successfully
    }

    #[test]
    fn test_dd_computation_duplicate_inserts() {
        let dd = DDComputation::new(vec!["data".to_string()]).unwrap();

        let t = Tuple::new(vec![Value::Int32(1)]);

        // Insert the same tuple twice at the same time
        dd.insert("data", vec![t.clone(), t.clone()], 1).unwrap();
        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        // DD uses multiset semantics  -  the tuple should appear once
        // because we read with "total_diff > 0" check (not exact count)
        let tuples = dd.read_relation("data").unwrap();
        assert_eq!(tuples.len(), 1);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_string_data() {
        let dd = DDComputation::new(vec!["names".to_string()]).unwrap();

        dd.insert(
            "names",
            vec![
                Tuple::new(vec![Value::string("alice"), Value::Int32(30)]),
                Tuple::new(vec![Value::string("bob"), Value::Int32(25)]),
            ],
            1,
        )
        .unwrap();
        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        let mut tuples = dd.read_relation("names").unwrap();
        tuples.sort();
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0].get(0), Some(&Value::string("alice")));
        assert_eq!(tuples[1].get(0), Some(&Value::string("bob")));

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_vector_data() {
        let dd = DDComputation::new(vec!["embeddings".to_string()]).unwrap();

        dd.insert(
            "embeddings",
            vec![
                Tuple::new(vec![
                    Value::string("doc1"),
                    Value::vector(vec![1.0, 2.0, 3.0]),
                ]),
                Tuple::new(vec![
                    Value::string("doc2"),
                    Value::vector(vec![4.0, 5.0, 6.0]),
                ]),
            ],
            1,
        )
        .unwrap();
        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        let tuples = dd.read_relation("embeddings").unwrap();
        assert_eq!(tuples.len(), 2);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_dynamic_relation() {
        // Create with NO initial relations
        let dd = DDComputation::new(vec![]).unwrap();

        // Insert into a dynamically-created relation (auto-ensures)
        dd.insert(
            "edge",
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
            1,
        )
        .unwrap();

        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        let tuples = dd.read_relation("edge").unwrap();
        assert_eq!(tuples.len(), 2);

        // Add another relation dynamically
        dd.insert("node", vec![Tuple::new(vec![Value::Int32(1)])], 2)
            .unwrap();

        dd.advance_time(3).unwrap();
        dd.wait_until_caught_up(3).unwrap();

        assert_eq!(dd.read_relation("node").unwrap().len(), 1);
        // Original relation still intact
        assert_eq!(dd.read_relation("edge").unwrap().len(), 2);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_ensure_relation_idempotent() {
        let dd = DDComputation::new(vec!["existing".to_string()]).unwrap();

        // Ensure existing relation  -  should be fast no-op
        dd.ensure_relation("existing").unwrap();

        // Ensure new relation
        dd.ensure_relation("new_rel").unwrap();

        // Ensure same new relation again  -  idempotent
        dd.ensure_relation("new_rel").unwrap();

        // Both should work for inserts
        dd.insert("existing", vec![Tuple::new(vec![Value::Int32(1)])], 1)
            .unwrap();
        dd.insert("new_rel", vec![Tuple::new(vec![Value::Int32(2)])], 1)
            .unwrap();

        dd.advance_time(2).unwrap();
        dd.wait_until_caught_up(2).unwrap();

        assert_eq!(dd.read_relation("existing").unwrap().len(), 1);
        assert_eq!(dd.read_relation("new_rel").unwrap().len(), 1);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_max_write_time_tracking() {
        let dd = DDComputation::new(vec![]).unwrap();

        // Initially zero
        assert_eq!(dd.max_write_time(), 0);

        // Insert at time 5
        dd.insert("data", vec![Tuple::new(vec![Value::Int32(1)])], 5)
            .unwrap();
        assert_eq!(dd.max_write_time(), 5);

        // Insert at time 3 (lower)  -  max stays at 5
        dd.insert("data", vec![Tuple::new(vec![Value::Int32(2)])], 3)
            .unwrap();
        assert_eq!(dd.max_write_time(), 5);

        // Delete at time 10  -  max advances to 10
        dd.delete("data", vec![Tuple::new(vec![Value::Int32(1)])], 10)
            .unwrap();
        assert_eq!(dd.max_write_time(), 10);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_read_relation_consistent() {
        let dd = DDComputation::new(vec![]).unwrap();

        // Insert data at various times WITHOUT manually advancing time
        dd.insert(
            "items",
            vec![
                Tuple::new(vec![Value::Int32(1)]),
                Tuple::new(vec![Value::Int32(2)]),
            ],
            1,
        )
        .unwrap();

        dd.insert("items", vec![Tuple::new(vec![Value::Int32(3)])], 2)
            .unwrap();

        // read_relation_consistent() should lazily advance time and return all data
        let tuples = dd.read_relation_consistent("items").unwrap();
        assert_eq!(tuples.len(), 3);

        // Now delete one and read consistently again
        dd.delete("items", vec![Tuple::new(vec![Value::Int32(2)])], 3)
            .unwrap();

        let tuples = dd.read_relation_consistent("items").unwrap();
        assert_eq!(tuples.len(), 2);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_computation_consistent_read_multi_relation() {
        let dd = DDComputation::new(vec![]).unwrap();

        // Insert into multiple relations at different times
        dd.insert(
            "edges",
            vec![
                Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
            ],
            1,
        )
        .unwrap();

        dd.insert(
            "nodes",
            vec![
                Tuple::new(vec![Value::string("a")]),
                Tuple::new(vec![Value::string("b")]),
                Tuple::new(vec![Value::string("c")]),
            ],
            2,
        )
        .unwrap();

        // Consistent reads should see all data
        let edges = dd.read_relation_consistent("edges").unwrap();
        assert_eq!(edges.len(), 2);

        let nodes = dd.read_relation_consistent("nodes").unwrap();
        assert_eq!(nodes.len(), 3);

        // Unknown relation returns empty
        let empty = dd.read_relation_consistent("nonexistent").unwrap();
        assert_eq!(empty.len(), 0);

        dd.shutdown().unwrap();
    }

    // === Derived Relations Tests ===

    fn make_compiled_rule(name: &str, deps: Vec<&str>) -> CompiledRule {
        CompiledRule {
            name: name.to_string(),
            clauses: vec![],
            dependencies: deps.into_iter().map(|s| s.to_string()).collect(),
            is_recursive: false,
            output_schema: vec![],
            stratum: 0,
        }
    }

    #[test]
    fn test_dd_register_rule() {
        let dd = DDComputation::new(vec!["edge".to_string()]).unwrap();

        let rule = make_compiled_rule("path", vec!["edge"]);
        dd.register_rule(rule).unwrap();

        assert!(dd.is_derived_relation("path"));
        assert!(!dd.is_derived_relation("edge"));

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_remove_rule() {
        let dd = DDComputation::new(vec![]).unwrap();

        let rule = make_compiled_rule("derived", vec!["base"]);
        dd.register_rule(rule).unwrap();
        assert!(dd.is_derived_relation("derived"));

        dd.remove_rule("derived").unwrap();
        assert!(!dd.is_derived_relation("derived"));

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_materialization() {
        let dd = DDComputation::new(vec![]).unwrap();

        // Register a rule
        let rule = make_compiled_rule("path", vec!["edge"]);
        dd.register_rule(rule).unwrap();

        // Initially not materialized
        assert!(dd.read_derived_relation("path").unwrap().is_none());

        // Materialize it
        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
            Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
        ];
        dd.set_materialized("path", tuples).unwrap();

        // Now available
        let result = dd.read_derived_relation("path").unwrap().unwrap();
        assert_eq!(result.len(), 2);

        dd.shutdown().unwrap();
    }

    #[test]
    fn test_dd_invalidation() {
        let dd = DDComputation::new(vec![]).unwrap();

        // Register and materialize
        let rule = make_compiled_rule("path", vec!["edge"]);
        dd.register_rule(rule).unwrap();
        dd.set_materialized("path", vec![Tuple::new(vec![Value::Int32(1)])])
            .unwrap();

        // Verify materialized
        assert!(dd.read_derived_relation("path").unwrap().is_some());

        // Notify base update
        let invalidated = dd.notify_base_update("edge").unwrap();
        assert!(invalidated.contains(&"path".to_string()));

        // Now invalid
        assert!(dd.read_derived_relation("path").unwrap().is_none());

        dd.shutdown().unwrap();
    }

