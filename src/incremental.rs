//! Incremental Materialization Engine
//!
//! `IncrementalEngine` provides persistent incremental computation for one
//! knowledge graph. It owns a timely worker thread with Differential Dataflow
//! InputSessions for base relations, and coordinates derived relation
//! materialization.
//!
//! ## Architecture
//!
//! ```text
//! Main thread --command_tx--► Worker thread (timely, idle merging on)
//!                              ├─ InputSessions (one per base relation)
//!                              ├─ Arrangements (queryable via cursor)
//!                              └─ Command loop (steps and compacts per window)
//! Main thread --mutex--------► DerivedRelationsManager (rule tracking)
//! ```
//!
//! ## Thread Safety
//!
//! InputSessions and TraceAgents are NOT Send/Sync (Rc-based internally).
//! All DD state lives on the worker thread and is reached only through the
//! command channel. Writes are fire-and-forget sends.

use crate::derived_relations::{CompiledRule, DerivedRelationsManager};
use crate::value::Tuple;
use crossbeam_channel as channel;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Merge effort each idle arrangement spends per scheduling quantum.
const IDLE_MERGE_EFFORT: isize = 1 << 12;

/// Buffered input updates that close a window even while commands keep arriving.
const SETTLE_UPDATES: usize = 1 << 16;

/// Idle merge steps between commands, so a busy trace cannot starve the queue.
const MAX_IDLE_MERGE_STEPS: usize = 1 << 12;

/// `timely::execute_directly` with idle merging on, so traces compact while
/// no commands arrive.
fn execute_with_idle_merging<F>(func: F)
where
    F: FnOnce(&mut timely::worker::Worker<timely::communication::allocator::thread::Thread>),
{
    let mut config = timely::WorkerConfig::default();
    differential_dataflow::configure(
        &mut config,
        &differential_dataflow::Config::default().idle_merge_effort(Some(IDLE_MERGE_EFFORT)),
    );
    let alloc = timely::communication::allocator::thread::Thread::default();
    let mut worker = timely::worker::Worker::new(config, alloc, Some(std::time::Instant::now()));
    func(&mut worker);
    while worker.has_dataflows() {
        worker.step_or_park(None);
    }
}

/// Commands sent from the main thread to the worker thread.
enum EngineCommand {
    // === Base Relation Operations ===
    InsertDelta {
        relation: String,
        updates: Vec<(Tuple, isize)>,
    },
    AdvanceTime(u64),
    WaitUntilCaughtUp {
        time: u64,
        response: channel::Sender<()>,
    },
    ReadRelation {
        relation: String,
        response: channel::Sender<Vec<Tuple>>,
    },
    AddRelation {
        name: String,
        response: channel::Sender<()>,
    },
    Shutdown {
        response: channel::Sender<()>,
    },
    TraceUpdates {
        relation: String,
        response: channel::Sender<usize>,
    },
}

/// Handle to the incremental computation engine for one knowledge graph.
///
/// Owns a dedicated worker thread running a timely/DD computation. All DD
/// state (InputSessions, TraceAgents) is confined to the worker thread.
/// The main thread communicates exclusively through the command channel.
pub struct IncrementalEngine {
    command_tx: channel::Sender<EngineCommand>,
    worker_handle: Option<std::thread::JoinHandle<()>>,
    current_time: Arc<AtomicU64>,
    max_write_time: Arc<AtomicU64>,
    known_relations: Mutex<HashSet<String>>,
    derived_relations: Arc<Mutex<DerivedRelationsManager>>,
}

impl IncrementalEngine {
    /// Create a new incremental engine with a single worker thread.
    ///
    /// `relations` specifies the initial base relations to create InputSessions for.
    pub fn new(relations: Vec<String>) -> Result<Self, String> {
        let (command_tx, command_rx) = channel::bounded::<EngineCommand>(1024);
        let current_time = Arc::new(AtomicU64::new(0));
        let max_write_time = Arc::new(AtomicU64::new(0));
        let known_relations = Mutex::new(relations.iter().cloned().collect());
        let derived_relations = Arc::new(Mutex::new(DerivedRelationsManager::new()));

        let worker_handle = std::thread::Builder::new()
            .name("incremental-worker".to_string())
            .spawn(move || {
                Self::worker_loop(relations, command_rx);
            })
            .map_err(|e| format!("Failed to spawn worker thread: {e}"))?;

        Ok(IncrementalEngine {
            command_tx,
            worker_handle: Some(worker_handle),
            current_time,
            max_write_time,
            known_relations,
            derived_relations,
        })
    }

    /// The worker thread's main loop.
    ///
    /// Creates a timely computation with u64 timestamps, InputSessions for
    /// each relation, and arrangements. Processes commands via blocking recv.
    fn worker_loop(relations: Vec<String>, command_rx: channel::Receiver<EngineCommand>) {
        use differential_dataflow::input::Input;
        use differential_dataflow::trace::cursor::Cursor;
        use differential_dataflow::trace::{BatchReader, TraceReader};
        use std::time::Duration;
        use timely::dataflow::operators::Probe;
        use timely::dataflow::ProbeHandle;
        use timely::progress::frontier::AntichainRef;
        use timely::scheduling::Scheduler;

        execute_with_idle_merging(move |worker| {
            let probe = ProbeHandle::<u64>::new();

            let mut input_sessions: HashMap<
                String,
                differential_dataflow::input::InputSession<u64, Tuple, isize>,
            > = HashMap::new();

            type KeyTrace = differential_dataflow::trace::implementations::ord_neu::OrdKeySpine<
                Tuple,
                u64,
                isize,
            >;
            type KeyTraceAgent = differential_dataflow::operators::arrange::TraceAgent<KeyTrace>;
            let mut traces: HashMap<String, KeyTraceAgent> = HashMap::new();

            worker.dataflow::<u64, _, _>(|scope| {
                for relation in &relations {
                    let (session, collection) = scope.new_collection::<Tuple, isize>();
                    input_sessions.insert(relation.clone(), session);
                    let arranged = collection.arrange_by_self();
                    arranged.stream.probe_with(&probe);
                    traces.insert(relation.clone(), arranged.trace.clone());
                }
            });

            // Updates land at the inputs' current time, so the batcher cancels
            // retractions within a window. A window closes when the queue
            // drains or grows large: inputs advance, the worker steps, and the
            // traces compact, so memory follows the live collections.
            let mut frontier: u64 = 0;
            let mut pending: usize = 0;
            macro_rules! settle {
                () => {
                    if pending > 0 {
                        for session in input_sessions.values_mut() {
                            if *session.time() < frontier {
                                session.advance_to(frontier);
                            }
                            session.flush();
                        }
                        while probe.less_than(&frontier) {
                            worker.step();
                        }
                        let compact = [frontier];
                        for trace in traces.values_mut() {
                            trace.set_logical_compaction(AntichainRef::new(&compact));
                            trace.set_physical_compaction(AntichainRef::new(&compact));
                        }
                        worker.step();
                        pending = 0;
                    }
                };
            }

            loop {
                let mut idle_steps = 0;
                let cmd = loop {
                    match command_rx.try_recv() {
                        Ok(cmd) => break cmd,
                        Err(channel::TryRecvError::Disconnected) => return,
                        Err(channel::TryRecvError::Empty) => {}
                    }
                    settle!();
                    // Spend idle time merging trace batches until they are compact.
                    let merging = worker.activations().borrow().empty_for() == Some(Duration::ZERO);
                    if merging && idle_steps < MAX_IDLE_MERGE_STEPS {
                        idle_steps += 1;
                        worker.step();
                    } else {
                        match command_rx.recv() {
                            Ok(cmd) => break cmd,
                            Err(_) => return, // channel disconnected
                        }
                    }
                };
                if !matches!(cmd, EngineCommand::InsertDelta { .. }) {
                    settle!();
                }

                match cmd {
                    EngineCommand::InsertDelta { relation, updates } => {
                        if let Some(session) = input_sessions.get_mut(&relation) {
                            frontier = frontier.max(*session.time() + 1);
                            pending += updates.len();
                            for (data, diff) in updates {
                                session.update(data, diff);
                            }
                        }
                        if pending >= SETTLE_UPDATES {
                            settle!();
                        }
                    }

                    EngineCommand::AdvanceTime(time) => {
                        for session in input_sessions.values_mut() {
                            if *session.time() < time {
                                session.advance_to(time);
                            }
                            session.flush();
                        }
                        worker.step();
                    }

                    EngineCommand::WaitUntilCaughtUp { time, response } => {
                        for session in input_sessions.values_mut() {
                            session.flush();
                        }
                        while probe.less_than(&time) {
                            worker.step();
                            std::thread::yield_now();
                        }
                        let _ = response.send(());
                    }

                    EngineCommand::ReadRelation { relation, response } => {
                        let mut result = Vec::new();
                        if let Some(trace) = traces.get_mut(&relation) {
                            let (mut cursor, storage) = trace.cursor();
                            while cursor.key_valid(&storage) {
                                let key = cursor.key(&storage).clone();
                                let mut total_diff: isize = 0;
                                cursor.map_times(&storage, |_time, diff| {
                                    total_diff += *diff;
                                });
                                if total_diff > 0 {
                                    result.push(key);
                                }
                                cursor.step_key(&storage);
                            }
                        }
                        let _ = response.send(result);
                    }

                    EngineCommand::AddRelation { name, response } => {
                        if !input_sessions.contains_key(&name) {
                            worker.dataflow::<u64, _, _>(|scope| {
                                let (mut session, collection) =
                                    scope.new_collection::<Tuple, isize>();
                                session.advance_to(frontier);
                                input_sessions.insert(name.clone(), session);
                                let arranged = collection.arrange_by_self();
                                arranged.stream.probe_with(&probe);
                                traces.insert(name.clone(), arranged.trace.clone());
                            });
                        }
                        let _ = response.send(());
                    }

                    EngineCommand::Shutdown { response } => {
                        // Drop sessions without stepping to avoid merge batcher issues
                        input_sessions.clear();
                        traces.clear();
                        let _ = response.send(());
                        return;
                    }

                    EngineCommand::TraceUpdates { relation, response } => {
                        let mut count = 0;
                        if let Some(trace) = traces.get_mut(&relation) {
                            trace.map_batches(|batch| count += batch.len());
                        }
                        let _ = response.send(count);
                    }
                }
            }
        });
    }

    // === Base Relation Operations ===

    /// Insert tuples into a base relation; `time` feeds `max_write_time`.
    pub fn insert(&self, relation: &str, tuples: Vec<Tuple>, time: u64) -> Result<(), String> {
        self.ensure_relation(relation)?;
        self.max_write_time.fetch_max(time, Ordering::SeqCst);
        let updates: Vec<(Tuple, isize)> = tuples.into_iter().map(|t| (t, 1)).collect();
        self.command_tx
            .send(EngineCommand::InsertDelta {
                relation: relation.to_string(),
                updates,
            })
            .map_err(|_| "Worker disconnected".to_string())
    }

    /// Delete tuples from a base relation; `time` feeds `max_write_time`.
    pub fn delete(&self, relation: &str, tuples: Vec<Tuple>, time: u64) -> Result<(), String> {
        self.ensure_relation(relation)?;
        self.max_write_time.fetch_max(time, Ordering::SeqCst);
        let updates: Vec<(Tuple, isize)> = tuples.into_iter().map(|t| (t, -1)).collect();
        self.command_tx
            .send(EngineCommand::InsertDelta {
                relation: relation.to_string(),
                updates,
            })
            .map_err(|_| "Worker disconnected".to_string())
    }

    /// Advance the logical time and flush all InputSessions.
    pub fn advance_time(&self, time: u64) -> Result<(), String> {
        self.current_time.store(time, Ordering::SeqCst);
        self.command_tx
            .send(EngineCommand::AdvanceTime(time))
            .map_err(|_| "Worker disconnected".to_string())
    }

    /// Block until the computation has processed all updates through `time`.
    pub fn wait_until_caught_up(&self, time: u64) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(EngineCommand::WaitUntilCaughtUp { time, response: tx })
            .map_err(|_| "Worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "Worker disconnected while waiting".to_string())
    }

    /// Read all current tuples from a base relation.
    pub fn read_relation(&self, relation: &str) -> Result<Vec<Tuple>, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(EngineCommand::ReadRelation {
                relation: relation.to_string(),
                response: tx,
            })
            .map_err(|_| "Worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "Worker disconnected while reading".to_string())
    }

    /// Read with consistency: advance time past all writes, wait, then read.
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

    /// Ensure a relation exists (idempotent).
    pub fn ensure_relation(&self, name: &str) -> Result<(), String> {
        {
            let known = self.known_relations.lock();
            if known.contains(name) {
                return Ok(());
            }
        }
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(EngineCommand::AddRelation {
                name: name.to_string(),
                response: tx,
            })
            .map_err(|_| "Worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "Worker disconnected while adding relation".to_string())?;
        self.known_relations.lock().insert(name.to_string());
        Ok(())
    }

    /// Updates held in a relation's trace (memory proxy for tests).
    pub fn trace_updates(&self, relation: &str) -> Result<usize, String> {
        let (tx, rx) = channel::bounded(1);
        self.command_tx
            .send(EngineCommand::TraceUpdates {
                relation: relation.to_string(),
                response: tx,
            })
            .map_err(|_| "Worker disconnected".to_string())?;
        rx.recv()
            .map_err(|_| "Worker disconnected while reading trace".to_string())
    }

    /// Commands queued for the worker.
    pub fn pending_commands(&self) -> usize {
        self.command_tx.len()
    }

    // === Derived Relations API ===

    /// Register a compiled rule for materialization.
    pub fn register_rule(&self, rule: CompiledRule) -> Result<(), String> {
        self.derived_relations.lock().register_rule(rule);
        Ok(())
    }

    /// Remove a rule and its materialization.
    pub fn remove_rule(&self, name: &str) -> Result<(), String> {
        self.derived_relations.lock().remove_rule(name);
        Ok(())
    }

    /// Read materialized data for a derived relation.
    pub fn read_derived_relation(&self, relation: &str) -> Result<Option<Vec<Tuple>>, String> {
        Ok(self
            .derived_relations
            .lock()
            .get_materialized(relation)
            .map(|m| m.tuples.clone()))
    }

    /// Set materialized data for a derived relation.
    pub fn set_materialized(&self, relation: &str, tuples: Vec<Tuple>) -> Result<(), String> {
        self.derived_relations
            .lock()
            .set_materialized(relation, tuples);
        Ok(())
    }

    /// Notify that a base relation was updated. Returns invalidated relation names.
    pub fn notify_base_update(&self, relation: &str) -> Result<Vec<String>, String> {
        Ok(self.derived_relations.lock().notify_base_update(relation))
    }

    /// Get (total_rules, materialized_count, invalid_count).
    pub fn get_derived_stats(&self) -> Result<(usize, usize, usize), String> {
        let stats = self.derived_relations.lock().stats();
        Ok((
            stats.total_rules,
            stats.materialized_count,
            stats.invalid_count,
        ))
    }

    /// Check if a relation is derived (has a registered rule).
    pub fn is_derived_relation(&self, name: &str) -> bool {
        self.derived_relations.lock().is_derived(name)
    }

    /// Get direct access to the derived relations manager.
    pub fn derived_relations(&self) -> Arc<Mutex<DerivedRelationsManager>> {
        Arc::clone(&self.derived_relations)
    }

    /// Shut down the computation cleanly.
    pub fn shutdown(mut self) -> Result<(), String> {
        let (tx, rx) = channel::bounded(1);
        let _ = self
            .command_tx
            .send(EngineCommand::Shutdown { response: tx });
        let _ = rx.recv();
        if let Some(handle) = self.worker_handle.take() {
            handle
                .join()
                .map_err(|_| "Worker thread panicked".to_string())?;
        }
        Ok(())
    }
}

impl Drop for IncrementalEngine {
    fn drop(&mut self) {
        let (tx, rx) = channel::bounded(1);
        let _ = self
            .command_tx
            .send(EngineCommand::Shutdown { response: tx });
        let _ = rx.recv_timeout(std::time::Duration::from_secs(5));
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::value::Value;

    #[test]
    fn test_lifecycle() {
        let engine = IncrementalEngine::new(vec!["edge".to_string()]).unwrap();
        engine
            .insert(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                ],
                1,
            )
            .unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();
        let tuples = engine.read_relation("edge").unwrap();
        assert_eq!(tuples.len(), 2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_empty_relation() {
        let engine = IncrementalEngine::new(vec!["empty".to_string()]).unwrap();
        engine.advance_time(1).unwrap();
        engine.wait_until_caught_up(1).unwrap();
        assert_eq!(engine.read_relation("empty").unwrap().len(), 0);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_insert_and_delete() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        let t1 = Tuple::new(vec![Value::Int32(1)]);
        let t2 = Tuple::new(vec![Value::Int32(2)]);

        engine
            .insert("data", vec![t1.clone(), t2.clone()], 1)
            .unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();
        assert_eq!(engine.read_relation("data").unwrap().len(), 2);

        engine.delete("data", vec![t1], 2).unwrap();
        engine.advance_time(3).unwrap();
        engine.wait_until_caught_up(3).unwrap();
        let tuples = engine.read_relation("data").unwrap();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0], t2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_multiple_relations() {
        let engine =
            IncrementalEngine::new(vec!["authors".to_string(), "papers".to_string()]).unwrap();
        engine
            .insert("authors", vec![Tuple::new(vec![Value::string("alice")])], 1)
            .unwrap();
        engine
            .insert(
                "papers",
                vec![
                    Tuple::new(vec![Value::string("p1")]),
                    Tuple::new(vec![Value::string("p2")]),
                ],
                1,
            )
            .unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();
        assert_eq!(engine.read_relation("authors").unwrap().len(), 1);
        assert_eq!(engine.read_relation("papers").unwrap().len(), 2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_dynamic_relation() {
        let engine = IncrementalEngine::new(vec![]).unwrap();
        engine
            .insert(
                "edge",
                vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
                ],
                1,
            )
            .unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();
        assert_eq!(engine.read_relation("edge").unwrap().len(), 2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_consistent_read() {
        let engine = IncrementalEngine::new(vec![]).unwrap();
        engine
            .insert(
                "items",
                vec![
                    Tuple::new(vec![Value::Int32(1)]),
                    Tuple::new(vec![Value::Int32(2)]),
                ],
                1,
            )
            .unwrap();
        engine
            .insert("items", vec![Tuple::new(vec![Value::Int32(3)])], 2)
            .unwrap();
        let tuples = engine.read_relation_consistent("items").unwrap();
        assert_eq!(tuples.len(), 3);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_max_write_time_tracking() {
        let engine = IncrementalEngine::new(vec![]).unwrap();
        assert_eq!(engine.max_write_time(), 0);
        engine
            .insert("data", vec![Tuple::new(vec![Value::Int32(1)])], 5)
            .unwrap();
        assert_eq!(engine.max_write_time(), 5);
        engine
            .insert("data", vec![Tuple::new(vec![Value::Int32(2)])], 3)
            .unwrap();
        assert_eq!(engine.max_write_time(), 5);
        engine
            .delete("data", vec![Tuple::new(vec![Value::Int32(1)])], 10)
            .unwrap();
        assert_eq!(engine.max_write_time(), 10);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_drop_triggers_shutdown() {
        let engine = IncrementalEngine::new(vec!["test".to_string()]).unwrap();
        engine
            .insert("test", vec![Tuple::new(vec![Value::Int32(42)])], 1)
            .unwrap();
        drop(engine);
    }

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
    fn test_register_rule() {
        let engine = IncrementalEngine::new(vec!["edge".to_string()]).unwrap();
        let rule = make_compiled_rule("path", vec!["edge"]);
        engine.register_rule(rule).unwrap();
        assert!(engine.is_derived_relation("path"));
        assert!(!engine.is_derived_relation("edge"));
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_materialization() {
        let engine = IncrementalEngine::new(vec![]).unwrap();
        let rule = make_compiled_rule("path", vec!["edge"]);
        engine.register_rule(rule).unwrap();
        assert!(engine.read_derived_relation("path").unwrap().is_none());

        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
            Tuple::new(vec![Value::Int32(2), Value::Int32(3)]),
        ];
        engine.set_materialized("path", tuples).unwrap();
        let result = engine.read_derived_relation("path").unwrap().unwrap();
        assert_eq!(result.len(), 2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_invalidation() {
        let engine = IncrementalEngine::new(vec![]).unwrap();
        let rule = make_compiled_rule("path", vec!["edge"]);
        engine.register_rule(rule).unwrap();
        engine
            .set_materialized("path", vec![Tuple::new(vec![Value::Int32(1)])])
            .unwrap();
        assert!(engine.read_derived_relation("path").unwrap().is_some());

        let invalidated = engine.notify_base_update("edge").unwrap();
        assert!(invalidated.contains(&"path".to_string()));
        assert!(engine.read_derived_relation("path").unwrap().is_none());
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_derived_stats() {
        let engine = IncrementalEngine::new(vec![]).unwrap();
        let (total, mat, inv) = engine.get_derived_stats().unwrap();
        assert_eq!((total, mat, inv), (0, 0, 0));

        let rule = make_compiled_rule("path", vec!["edge"]);
        engine.register_rule(rule).unwrap();
        let (total, mat, inv) = engine.get_derived_stats().unwrap();
        assert_eq!((total, mat), (1, 0));
        assert_eq!(inv, 1);

        engine.set_materialized("path", vec![]).unwrap();
        let (total, mat, inv) = engine.get_derived_stats().unwrap();
        assert_eq!((total, mat, inv), (1, 1, 0));
        engine.shutdown().unwrap();
    }

    // =========================================================================
    // Stress Tests: IncrementalEngine High-Throughput & Concurrency
    // =========================================================================

    #[test]
    fn stress_high_throughput_inserts() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();

        // Insert 1000 tuples in 10 batches
        for batch in 0..10u64 {
            let tuples: Vec<Tuple> = (0..100)
                .map(|i| {
                    Tuple::new(vec![
                        Value::Int32((batch * 100 + i) as i32),
                        Value::Int32(((batch * 100 + i) * 2) as i32),
                    ])
                })
                .collect();
            engine.insert("data", tuples, batch + 1).unwrap();
        }

        engine.advance_time(11).unwrap();
        engine.wait_until_caught_up(11).unwrap();

        let tuples = engine.read_relation("data").unwrap();
        assert_eq!(tuples.len(), 1000);
        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_insert_delete_cycles() {
        let engine = IncrementalEngine::new(vec!["cycle".to_string()]).unwrap();

        // Repeatedly insert and delete the same tuples
        let base: Vec<Tuple> = (0..50).map(|i| Tuple::new(vec![Value::Int32(i)])).collect();

        for cycle in 0..10u64 {
            let time = cycle * 2 + 1;
            engine.insert("cycle", base.clone(), time).unwrap();
            engine.advance_time(time + 1).unwrap();
            engine.wait_until_caught_up(time + 1).unwrap();
            assert_eq!(engine.read_relation("cycle").unwrap().len(), 50);

            let del_time = time + 1;
            engine.delete("cycle", base.clone(), del_time).unwrap();
            engine.advance_time(del_time + 1).unwrap();
            engine.wait_until_caught_up(del_time + 1).unwrap();
            assert_eq!(engine.read_relation("cycle").unwrap().len(), 0);
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_sequential_reads_between_writes() {
        // Verify reads are consistent between write batches
        let engine = IncrementalEngine::new(vec!["shared".to_string()]).unwrap();

        for batch in 0..10u64 {
            let tuples: Vec<Tuple> = (0..10)
                .map(|i| Tuple::new(vec![Value::Int32((batch * 10 + i) as i32)]))
                .collect();
            engine.insert("shared", tuples, batch + 1).unwrap();
            engine.advance_time(batch + 2).unwrap();
            engine.wait_until_caught_up(batch + 2).unwrap();

            // Read after each batch: should see accumulated tuples
            let result = engine.read_relation("shared").unwrap();
            assert_eq!(result.len(), ((batch + 1) * 10) as usize);
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_concurrent_inserts_different_relations() {
        use std::sync::Arc;

        let engine = Arc::new(
            IncrementalEngine::new(vec!["a".to_string(), "b".to_string(), "c".to_string()])
                .unwrap(),
        );

        let mut handles = vec![];
        for (idx, rel) in ["a", "b", "c"].iter().enumerate() {
            let engine = Arc::clone(&engine);
            let rel = rel.to_string();
            handles.push(std::thread::spawn(move || {
                for i in 0..20u64 {
                    let time = idx as u64 * 100 + i + 1;
                    engine
                        .insert(&rel, vec![Tuple::new(vec![Value::Int32(i as i32)])], time)
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Advance past all write times and read
        engine.advance_time(301).unwrap();
        engine.wait_until_caught_up(301).unwrap();

        assert_eq!(engine.read_relation("a").unwrap().len(), 20);
        assert_eq!(engine.read_relation("b").unwrap().len(), 20);
        assert_eq!(engine.read_relation("c").unwrap().len(), 20);
    }

    #[test]
    fn stress_many_dynamic_relations() {
        let engine = IncrementalEngine::new(vec![]).unwrap();

        // Create 50 relations dynamically
        for i in 0..50 {
            engine
                .insert(
                    &format!("rel_{i}"),
                    vec![Tuple::new(vec![
                        Value::Int32(i),
                        Value::string(&format!("val_{i}")),
                    ])],
                    1,
                )
                .unwrap();
        }

        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();

        // Verify each relation
        for i in 0..50 {
            let tuples = engine.read_relation(&format!("rel_{i}")).unwrap();
            assert_eq!(tuples.len(), 1, "rel_{i} should have 1 tuple");
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_batched_time_advancement() {
        let engine = IncrementalEngine::new(vec!["fast".to_string()]).unwrap();

        // Insert in batches of 20, advancing time between batches
        for batch in 0..10u64 {
            let tuples: Vec<Tuple> = (0..20)
                .map(|i| Tuple::new(vec![Value::Int32((batch * 20 + i) as i32)]))
                .collect();
            engine.insert("fast", tuples, batch + 1).unwrap();
            engine.advance_time(batch + 2).unwrap();
            engine.wait_until_caught_up(batch + 2).unwrap();
        }

        let tuples = engine.read_relation("fast").unwrap();
        assert_eq!(tuples.len(), 200);
        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_multiple_rules_same_base() {
        let engine = IncrementalEngine::new(vec!["edge".to_string()]).unwrap();

        // Register 5 rules all depending on "edge"
        for i in 0..5 {
            let rule = make_compiled_rule(&format!("derived_{i}"), vec!["edge"]);
            engine.register_rule(rule).unwrap();
        }

        // Materialize all
        for i in 0..5 {
            engine
                .set_materialized(
                    &format!("derived_{i}"),
                    vec![Tuple::new(vec![Value::Int32(i)])],
                )
                .unwrap();
        }

        // Verify all materialized
        for i in 0..5 {
            assert!(engine
                .read_derived_relation(&format!("derived_{i}"))
                .unwrap()
                .is_some());
        }

        // Update base → should invalidate all 5
        let invalidated = engine.notify_base_update("edge").unwrap();
        assert_eq!(invalidated.len(), 5);

        // All should be invalidated now
        for i in 0..5 {
            assert!(engine
                .read_derived_relation(&format!("derived_{i}"))
                .unwrap()
                .is_none());
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_rule_register_remove_cycle() {
        let engine = IncrementalEngine::new(vec!["base".to_string()]).unwrap();

        for i in 0..20 {
            let name = format!("rule_{i}");
            let rule = make_compiled_rule(&name, vec!["base"]);
            engine.register_rule(rule).unwrap();
            assert!(engine.is_derived_relation(&name));

            engine.remove_rule(&name).unwrap();
            assert!(!engine.is_derived_relation(&name));
        }

        let (total, _, _) = engine.get_derived_stats().unwrap();
        assert_eq!(total, 0);
        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_large_tuple_batches() {
        let engine = IncrementalEngine::new(vec!["wide".to_string()]).unwrap();

        // Insert tuples with many columns
        let tuples: Vec<Tuple> = (0..100)
            .map(|i| {
                Tuple::new(vec![
                    Value::Int32(i),
                    Value::string(&format!("name_{i}")),
                    Value::Float64(i as f64 * 1.5),
                    Value::Bool(i % 2 == 0),
                    Value::Int32(i * 100),
                ])
            })
            .collect();

        engine.insert("wide", tuples, 1).unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();

        let result = engine.read_relation("wide").unwrap();
        assert_eq!(result.len(), 100);

        // Each tuple should have 5 values
        for t in &result {
            assert_eq!(t.arity(), 5);
        }

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_consistent_read_during_updates() {
        let engine = IncrementalEngine::new(vec!["stream".to_string()]).unwrap();

        // Insert data at multiple timestamps without explicitly waiting
        for t in 1..=50u64 {
            engine
                .insert("stream", vec![Tuple::new(vec![Value::Int32(t as i32)])], t)
                .unwrap();
        }

        // Consistent read should give us all 50 tuples
        let tuples = engine.read_relation_consistent("stream").unwrap();
        assert_eq!(tuples.len(), 50);

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_max_write_time_under_concurrent_writes() {
        use std::sync::Arc;

        let engine = Arc::new(IncrementalEngine::new(vec!["data".to_string()]).unwrap());
        let mut handles = vec![];

        for i in 0..20u64 {
            let engine = Arc::clone(&engine);
            handles.push(std::thread::spawn(move || {
                engine
                    .insert(
                        "data",
                        vec![Tuple::new(vec![Value::Int32(i as i32)])],
                        i + 1,
                    )
                    .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // max_write_time should be at least 20
        assert!(engine.max_write_time() >= 20);
    }

    #[test]
    fn stress_interleaved_relations() {
        let engine =
            IncrementalEngine::new(vec!["a".to_string(), "b".to_string(), "c".to_string()])
                .unwrap();

        // Interleave inserts across relations
        for i in 0..100u64 {
            let rel = match i % 3 {
                0 => "a",
                1 => "b",
                _ => "c",
            };
            engine
                .insert(rel, vec![Tuple::new(vec![Value::Int32(i as i32)])], i + 1)
                .unwrap();
        }

        engine.advance_time(101).unwrap();
        engine.wait_until_caught_up(101).unwrap();

        let a = engine.read_relation("a").unwrap().len();
        let b = engine.read_relation("b").unwrap().len();
        let c = engine.read_relation("c").unwrap().len();

        assert_eq!(a, 34); // 0,3,6,...,99 = 34 values
        assert_eq!(b, 33); // 1,4,7,...,97 = 33 values
        assert_eq!(c, 33); // 2,5,8,...,98 = 33 values
        assert_eq!(a + b + c, 100);

        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_delete_nonexistent_tuples() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();

        // Insert 10 tuples
        let tuples: Vec<Tuple> = (0..10).map(|i| Tuple::new(vec![Value::Int32(i)])).collect();
        engine.insert("data", tuples, 1).unwrap();

        // Try to delete tuples that don't exist (should not crash)
        let nonexistent: Vec<Tuple> = (100..110)
            .map(|i| Tuple::new(vec![Value::Int32(i)]))
            .collect();
        engine.delete("data", nonexistent, 2).unwrap();

        engine.advance_time(3).unwrap();
        engine.wait_until_caught_up(3).unwrap();

        // Original 10 tuples should still be there
        assert_eq!(engine.read_relation("data").unwrap().len(), 10);
        engine.shutdown().unwrap();
    }

    #[test]
    fn stress_derived_materialization_refresh() {
        let engine = IncrementalEngine::new(vec!["base".to_string()]).unwrap();

        let rule = make_compiled_rule("derived", vec!["base"]);
        engine.register_rule(rule).unwrap();

        // Simulate 10 cycles of: materialize → invalidate → re-materialize
        for cycle in 0..10 {
            let mat_data = vec![Tuple::new(vec![Value::Int32(cycle)])];
            engine.set_materialized("derived", mat_data).unwrap();
            assert!(engine.read_derived_relation("derived").unwrap().is_some());

            engine.notify_base_update("base").unwrap();
            assert!(engine.read_derived_relation("derived").unwrap().is_none());
        }

        engine.shutdown().unwrap();
    }

    // Batch 20: Index lifecycle, accessors, edge cases

    #[test]
    fn test_current_time_after_advance() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        assert_eq!(engine.current_time(), 0);

        engine.advance_time(5).unwrap();
        engine.wait_until_caught_up(5).unwrap();
        assert_eq!(engine.current_time(), 5);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_max_write_time_after_insert() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        assert_eq!(engine.max_write_time(), 0);

        engine
            .insert("data", vec![Tuple::new(vec![Value::Int32(1)])], 3)
            .unwrap();
        assert_eq!(engine.max_write_time(), 3);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_derived_relations_accessor() {
        let engine = IncrementalEngine::new(vec!["base".to_string()]).unwrap();
        let dr = engine.derived_relations();
        // Just verify the accessor returns an Arc and we can lock it
        let _guard = dr.lock();
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_ensure_relation_new() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        engine.ensure_relation("new_rel").unwrap();
        // Should be able to insert into the new relation
        engine
            .insert("new_rel", vec![Tuple::new(vec![Value::Int32(1)])], 1)
            .unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();
        assert_eq!(engine.read_relation("new_rel").unwrap().len(), 1);
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_ensure_relation_existing() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        // Should not error for existing relation
        engine.ensure_relation("data").unwrap();
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_is_derived_relation() {
        let engine = IncrementalEngine::new(vec!["base".to_string()]).unwrap();
        assert!(!engine.is_derived_relation("base"));

        let rule = make_compiled_rule("derived", vec!["base"]);
        engine.register_rule(rule).unwrap();
        assert!(engine.is_derived_relation("derived"));

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_get_derived_stats() {
        let engine = IncrementalEngine::new(vec!["base".to_string()]).unwrap();
        let (total, mat, inval) = engine.get_derived_stats().unwrap();
        assert_eq!(total, 0);
        assert_eq!(mat, 0);
        assert_eq!(inval, 0);

        let rule = make_compiled_rule("derived", vec!["base"]);
        engine.register_rule(rule).unwrap();
        let (total2, _, _) = engine.get_derived_stats().unwrap();
        assert_eq!(total2, 1);

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_remove_rule() {
        let engine = IncrementalEngine::new(vec!["base".to_string()]).unwrap();

        let rule = make_compiled_rule("view", vec!["base"]);
        engine.register_rule(rule).unwrap();
        assert!(engine.is_derived_relation("view"));

        engine.remove_rule("view").unwrap();
        assert!(!engine.is_derived_relation("view"));

        engine.shutdown().unwrap();
    }

    #[test]
    fn test_sustained_writes_keep_queue_and_trace_bounded() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        let rows = |range: std::ops::Range<i64>| -> Vec<Tuple> {
            range.map(|i| Tuple::new(vec![Value::Int64(i)])).collect()
        };
        let mut time = 1;
        for chunk in 0..1_000 {
            engine
                .insert("data", rows(chunk * 100..(chunk + 1) * 100), time)
                .unwrap();
            time += 1;
        }
        for chunk in 0..1_000 {
            engine
                .delete("data", rows(chunk * 100..(chunk + 1) * 100), time)
                .unwrap();
            time += 1;
        }
        engine.insert("data", rows(0..10), time).unwrap();

        // A read round trip is answered only after the queue is drained and stepped.
        assert_eq!(engine.read_relation("data").unwrap().len(), 10);
        assert_eq!(engine.pending_commands(), 0);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let mut held = engine.trace_updates("data").unwrap();
        while held > 1_000 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
            held = engine.trace_updates("data").unwrap();
        }
        assert!(
            held <= 1_000,
            "trace holds {held} of 200010 written updates"
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_read_relation_consistent() {
        let engine = IncrementalEngine::new(vec!["data".to_string()]).unwrap();
        engine
            .insert("data", vec![Tuple::new(vec![Value::Int32(42)])], 1)
            .unwrap();
        engine.advance_time(2).unwrap();
        engine.wait_until_caught_up(2).unwrap();

        let results = engine.read_relation_consistent("data").unwrap();
        assert_eq!(results.len(), 1);

        engine.shutdown().unwrap();
    }
}
