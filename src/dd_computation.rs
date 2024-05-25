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
                                    // TODO: verify this condition
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
