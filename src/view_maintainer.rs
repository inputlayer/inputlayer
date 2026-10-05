//! The view maintainer of one knowledge graph: a long-lived differential
//! dataflow fed from the commit path.
//!
//! A `ViewMaintainer` owns a timely worker thread holding, per base relation,
//! one Differential Dataflow `InputSession` and two arrangements of it: by
//! tuple and by key (the first column). Its timestamps are the knowledge
//! graph's snapshot revisions: a commit published as revision `r` is fed as
//! updates at time `r`, and the maintainer's *frontier* is the last revision
//! its arrangements hold completely.
//!
//! It maintains base relations only. Persistent rules are still evaluated by
//! every read from the base facts (recompute-on-read, see
//! `storage_engine::snapshot`); compiling them into this dataflow is the next
//! step, and reads keep using the recompute path until then. The maintainer
//! runs only with `engine.views = "maintained"`.
//!
//! ## Architecture
//!
//! ```text
//! Commit path (KG write lock) --feed--► unbounded queue --► Worker thread (timely)
//!                                                           ├─ InputSession per base relation
//!                                                           ├─ Arrangements by tuple and by key
//!                                                           └─ apply, advance, step to the revision
//! ```
//!
//! ## Backpressure
//!
//! [`ViewMaintainer::feed`] queues the commit and returns: the queue is
//! unbounded and the commit path never waits for the worker. A worker slower
//! than the writers shows as a frontier that lags the published revision,
//! reported by [`ViewStats`]. The worker applies every queued commit before it
//! steps, so a backlog costs one round of work, not one per commit.
//!
//! ## Failure
//!
//! The store is authoritative; the arrangements are derived from it. A worker
//! that panics (or cannot be started) marks the maintainer unavailable, later
//! commits are no longer fed, and nothing on the commit path fails. A reload
//! of the knowledge graph starts a fresh maintainer from the store.
//!
//! ## Thread Safety
//!
//! InputSessions and TraceAgents are NOT Send/Sync (Rc-based internally).
//! All DD state lives on the worker thread and is reached only through the
//! command queue.

use crate::value::{RelationMap, Tuple, Value};
use crossbeam_channel as channel;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord_neu::{OrdKeySpine, OrdValSpine};
use differential_dataflow::trace::{BatchReader, TraceReader};
use parking_lot::{Condvar, Mutex};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use timely::communication::allocator::thread::Thread;
use timely::dataflow::ProbeHandle;
use timely::progress::frontier::AntichainRef;
use timely::worker::Worker;
use tracing::{error, info};

/// Merge effort each idle arrangement spends per scheduling quantum.
const IDLE_MERGE_EFFORT: isize = 1 << 12;

/// Buffered input updates after which the worker steps even while commits
/// keep arriving.
const SETTLE_UPDATES: usize = 1 << 16;

/// Idle merge steps between commands, so a busy trace cannot starve the queue.
const MAX_IDLE_MERGE_STEPS: usize = 1 << 12;

/// Estimated bytes one update holds besides its data: its time and its diff.
const UPDATE_OVERHEAD_BYTES: usize = std::mem::size_of::<u64>() + std::mem::size_of::<isize>();

type TupleTrace = OrdKeySpine<Tuple, u64, isize>;
type KeyTrace = OrdValSpine<Value, Tuple, u64, isize>;

/// The key a tuple is arranged by: its first column (`Null` without columns).
pub fn key_of(tuple: &Tuple) -> Value {
    tuple.get(0).cloned().unwrap_or(Value::Null)
}

/// What one commit changes in one base relation. `added` and `removed` are
/// disjoint; `added` tuples were absent and `removed` ones present.
#[derive(Debug, Default)]
pub struct BaseDelta {
    pub relation: String,
    pub added: Vec<Tuple>,
    pub removed: Vec<Tuple>,
}

/// What one published snapshot changes in the base relations.
#[derive(Debug, Default)]
pub struct BaseChange {
    pub deltas: Vec<BaseDelta>,
    /// Relations dropped with everything they held.
    pub dropped: Vec<String>,
}

impl BaseChange {
    fn updates(&self) -> usize {
        self.deltas
            .iter()
            .map(|delta| delta.added.len() + delta.removed.len())
            .sum()
    }
}

/// A maintainer's state at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewStats {
    /// The last revision the arrangements hold completely.
    pub frontier: u64,
    /// Snapshots fed and not yet applied.
    pub pending_commits: usize,
    /// How long the oldest of them has waited (zero when none).
    pub frontier_lag: Duration,
    /// Updates held in the arrangements' traces.
    pub trace_rows: u64,
    /// Estimated bytes of those updates.
    pub trace_bytes: u64,
    /// Why the maintainer stopped, if it did: its views are unavailable.
    pub unavailable: Option<String>,
}

/// Commands sent to the worker thread.
enum Command {
    Commit {
        revision: u64,
        change: BaseChange,
    },
    ScanTuples {
        relation: String,
        reply: channel::Sender<BaseRows>,
    },
    ScanKeys {
        relation: String,
        key: Option<Value>,
        reply: channel::Sender<BaseRows>,
    },
    /// Wakes the worker so that it sees the stop flag.
    Stop,
    #[cfg(test)]
    Panic,
    #[cfg(test)]
    Stall(channel::Receiver<()>),
}

/// The rows of one arrangement at `revision`, each with its multiplicity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BaseRows {
    pub revision: u64,
    pub rows: Vec<(Tuple, isize)>,
}

/// State shared by the handle and the worker thread.
struct Shared {
    kg: String,
    /// Snapshots fed and not yet applied, oldest first: revision and when fed.
    pending: Mutex<VecDeque<(u64, Instant)>>,
    frontier: AtomicU64,
    trace_rows: AtomicU64,
    trace_bytes: AtomicU64,
    failed: AtomicBool,
    failure: Mutex<Option<String>>,
    stop: AtomicBool,
    /// Signalled when the frontier advances or the maintainer fails.
    progress: (Mutex<()>, Condvar),
}

impl Shared {
    fn fail(&self, reason: String) {
        error!(kg = %self.kg, reason = %reason, "view_maintainer_failed");
        *self.failure.lock() = Some(reason);
        self.failed.store(true, Ordering::Release);
        self.pending.lock().clear();
        self.notify();
    }

    fn notify(&self) {
        let _guard = self.progress.0.lock();
        self.progress.1.notify_all();
    }

    /// Record that the arrangements hold everything through `revision`.
    fn applied(&self, revision: u64) {
        {
            let mut pending = self.pending.lock();
            while pending.front().is_some_and(|(fed, _)| *fed <= revision) {
                pending.pop_front();
            }
        }
        self.frontier.store(revision, Ordering::Release);
        self.notify();
    }
}

/// Handle to the view maintainer of one knowledge graph.
///
/// All DD state is confined to the worker thread; the handle reaches it
/// through the command queue and reads its progress from shared counters.
pub struct ViewMaintainer {
    commands: channel::Sender<Command>,
    shared: Arc<Shared>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ViewMaintainer {
    /// Start the maintainer of `kg`, whose published snapshot has `revision`
    /// and the base relations `relations`. Returns at once: the worker loads
    /// the relations, and the frontier reaches `revision` when it has.
    ///
    /// A worker that cannot be started leaves the maintainer unavailable.
    pub fn start(kg: &str, revision: u64, relations: RelationMap) -> Self {
        let (commands, queue) = channel::unbounded();
        let shared = Arc::new(Shared {
            kg: kg.to_string(),
            pending: Mutex::new(VecDeque::from([(revision, Instant::now())])),
            // Nothing is held yet; revisions start at 1.
            frontier: AtomicU64::new(0),
            trace_rows: AtomicU64::new(0),
            trace_bytes: AtomicU64::new(0),
            failed: AtomicBool::new(false),
            failure: Mutex::new(None),
            stop: AtomicBool::new(false),
            progress: (Mutex::new(()), Condvar::new()),
        });
        let worker_shared = Arc::clone(&shared);
        let spawned = std::thread::Builder::new()
            .name(format!("views-{kg}"))
            .stack_size(crate::ENGINE_THREAD_STACK_BYTES)
            .spawn(move || {
                let shared = worker_shared;
                let run = std::panic::AssertUnwindSafe(|| {
                    worker_loop(&shared, revision, relations, &queue);
                });
                if let Err(panic) = std::panic::catch_unwind(run) {
                    // The queue goes first: a commit fed from here on finds
                    // it closed and discards its own pending entry.
                    drop(queue);
                    shared.fail(format!("worker panicked: {}", panic_message(&*panic)));
                }
            });
        let worker = match spawned {
            Ok(worker) => {
                info!(kg = %kg, revision, "view_maintainer_started");
                Some(worker)
            }
            Err(e) => {
                shared.fail(format!("worker thread did not start: {e}"));
                None
            }
        };
        ViewMaintainer {
            commands,
            shared,
            worker,
        }
    }

    /// Feed the snapshot published as `revision` and what it changes in the
    /// base relations. Called under the knowledge graph's write lock, so in
    /// commit order, with a revision above every earlier one.
    ///
    /// Queues the change and returns; it never waits for the worker. Does
    /// nothing once the maintainer is unavailable.
    pub fn feed(&self, revision: u64, change: BaseChange) {
        if self.shared.failed.load(Ordering::Acquire) {
            return;
        }
        self.shared
            .pending
            .lock()
            .push_back((revision, Instant::now()));
        if self
            .commands
            .send(Command::Commit { revision, change })
            .is_err()
        {
            // The worker is gone and records why.
            self.shared.pending.lock().clear();
        }
    }

    /// The last revision the arrangements hold completely (0: none yet).
    pub fn frontier(&self) -> u64 {
        self.shared.frontier.load(Ordering::Acquire)
    }

    /// Why the maintainer stopped, if it did.
    pub fn unavailable(&self) -> Option<String> {
        if self.shared.failed.load(Ordering::Acquire) {
            self.shared.failure.lock().clone()
        } else {
            None
        }
    }

    /// The maintainer's progress, backlog and trace size.
    pub fn stats(&self) -> ViewStats {
        let unavailable = self.unavailable();
        let (pending_commits, frontier_lag) = if unavailable.is_some() {
            (0, Duration::ZERO)
        } else {
            let pending = self.shared.pending.lock();
            let lag = pending.front().map_or(Duration::ZERO, |(_, fed)| fed.elapsed());
            (pending.len(), lag)
        };
        ViewStats {
            frontier: self.frontier(),
            pending_commits,
            frontier_lag,
            trace_rows: self.shared.trace_rows.load(Ordering::Relaxed),
            trace_bytes: self.shared.trace_bytes.load(Ordering::Relaxed),
            unavailable,
        }
    }

    /// Wait until the frontier reaches `revision`. Returns whether it did
    /// within `timeout`; false at once when the maintainer is unavailable.
    pub fn wait_for(&self, revision: u64, timeout: Duration) -> bool {
        self.waiter().wait_for(revision, timeout)
    }

    /// A handle that waits for the frontier without borrowing the
    /// maintainer, so without the knowledge graph's lock.
    pub fn waiter(&self) -> FrontierWaiter {
        FrontierWaiter(Arc::clone(&self.shared))
    }

    /// The by-tuple arrangement of `relation` at the frontier; `None` when
    /// the maintainer is unavailable.
    pub fn scan_tuples(&self, relation: &str) -> Option<BaseRows> {
        self.ask(|reply| Command::ScanTuples {
            relation: relation.to_string(),
            reply,
        })
    }

    /// The by-key arrangement of `relation` at the frontier: the tuples
    /// under `key`, or under every key. `None` when the maintainer is
    /// unavailable.
    pub fn scan_keys(&self, relation: &str, key: Option<Value>) -> Option<BaseRows> {
        self.ask(|reply| Command::ScanKeys {
            relation: relation.to_string(),
            key,
            reply,
        })
    }

    fn ask<R>(&self, command: impl FnOnce(channel::Sender<R>) -> Command) -> Option<R> {
        let (reply, answer) = channel::bounded(1);
        self.commands.send(command(reply)).ok()?;
        answer.recv().ok()
    }

    /// Make the worker panic at its next command.
    #[cfg(test)]
    pub(crate) fn inject_panic(&self) {
        let _ = self.commands.send(Command::Panic);
    }

    /// Hold the worker at its next command until the returned sender is
    /// dropped.
    #[cfg(test)]
    pub(crate) fn stall(&self) -> channel::Sender<()> {
        let (release, held) = channel::bounded(0);
        let _ = self.commands.send(Command::Stall(held));
        release
    }
}

/// Waits for a maintainer's frontier; see [`ViewMaintainer::waiter`].
pub struct FrontierWaiter(Arc<Shared>);

impl FrontierWaiter {
    /// See [`ViewMaintainer::wait_for`].
    pub fn wait_for(&self, revision: u64, timeout: Duration) -> bool {
        let shared = &self.0;
        let deadline = Instant::now() + timeout;
        let mut guard = shared.progress.0.lock();
        loop {
            if shared.frontier.load(Ordering::Acquire) >= revision {
                return true;
            }
            if shared.failed.load(Ordering::Acquire) || shared.stop.load(Ordering::Acquire) {
                return false;
            }
            if shared.progress.1.wait_until(&mut guard, deadline).timed_out() {
                return shared.frontier.load(Ordering::Acquire) >= revision;
            }
        }
    }
}

impl Drop for ViewMaintainer {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
        self.shared.notify();
        let _ = self.commands.send(Command::Stop);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        info!(kg = %self.shared.kg, "view_maintainer_stopped");
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// `timely::execute_directly` with idle merging on, so traces compact while
/// no commands arrive.
fn execute_with_idle_merging<F>(func: F)
where
    F: FnOnce(&mut Worker<Thread>),
{
    let mut config = timely::WorkerConfig::default();
    differential_dataflow::configure(
        &mut config,
        &differential_dataflow::Config::default().idle_merge_effort(Some(IDLE_MERGE_EFFORT)),
    );
    let mut worker = Worker::new(config, Thread::default(), Some(Instant::now()));
    func(&mut worker);
    while worker.has_dataflows() {
        worker.step_or_park(None);
    }
}

/// One base relation in the dataflow.
struct Base {
    input: InputSession<u64, Tuple, isize>,
    by_tuple: TraceAgent<TupleTrace>,
    by_key: TraceAgent<KeyTrace>,
    /// Behind both arrangements.
    probe: ProbeHandle<u64>,
    /// Size of each trace batch when last metered, by its bounds.
    metered: HashMap<BatchBounds, BatchSize>,
}

/// A batch's lower, upper and compaction frontiers.
type BatchBounds = (Vec<u64>, Vec<u64>, Vec<u64>);

#[derive(Clone, Copy, Default)]
struct BatchSize {
    rows: u64,
    bytes: u64,
}

fn bounds<B: BatchReader<Time = u64>>(batch: &B) -> BatchBounds {
    let description = batch.description();
    (
        description.lower().elements().to_vec(),
        description.upper().elements().to_vec(),
        description.since().elements().to_vec(),
    )
}

impl Base {
    /// Rows and estimated bytes of both traces. Walks only the batches that
    /// appeared since the last call, so metering costs what merging did.
    fn meter(&mut self) -> BatchSize {
        let mut metered = HashMap::new();
        let mut total = BatchSize::default();
        let previous = &self.metered;
        self.by_tuple.map_batches(|batch| {
            if batch.is_empty() {
                return;
            }
            // The two traces' batches share bounds: keep them apart.
            let mut key = bounds(batch);
            key.0.push(0);
            let size = previous.get(&key).copied().unwrap_or_else(|| {
                let mut bytes = 0;
                let mut cursor = batch.cursor();
                while cursor.key_valid(batch) {
                    bytes += cursor.key(batch).estimated_bytes();
                    cursor.step_key(batch);
                }
                BatchSize {
                    rows: batch.len() as u64,
                    bytes: (bytes + batch.len() * UPDATE_OVERHEAD_BYTES) as u64,
                }
            });
            total.rows += size.rows;
            total.bytes += size.bytes;
            metered.insert(key, size);
        });
        self.by_key.map_batches(|batch| {
            if batch.is_empty() {
                return;
            }
            let mut key = bounds(batch);
            key.0.push(1);
            let size = previous.get(&key).copied().unwrap_or_else(|| {
                let mut bytes = 0;
                let mut cursor = batch.cursor();
                while cursor.key_valid(batch) {
                    // A key shares its heap data with its tuples' first column.
                    bytes += std::mem::size_of::<Value>();
                    while cursor.val_valid(batch) {
                        bytes += cursor.val(batch).estimated_bytes();
                        cursor.step_val(batch);
                    }
                    cursor.step_key(batch);
                }
                BatchSize {
                    rows: batch.len() as u64,
                    bytes: (bytes + batch.len() * UPDATE_OVERHEAD_BYTES) as u64,
                }
            });
            total.rows += size.rows;
            total.bytes += size.bytes;
            metered.insert(key, size);
        });
        self.metered = metered;
        total
    }
}

/// The worker's dataflows: one per base relation.
struct Dataflows<'w> {
    worker: &'w mut Worker<Thread>,
    bases: HashMap<String, Base>,
    /// The inputs' time: one past the last applied revision.
    time: u64,
}

impl Dataflows<'_> {
    /// The dataflow of `relation`, built on first use.
    fn base(&mut self, relation: &str) -> &mut Base {
        use differential_dataflow::input::Input;
        use timely::dataflow::operators::Probe;

        if !self.bases.contains_key(relation) {
            let time = self.time;
            let base = self.worker.dataflow::<u64, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection::<Tuple, isize>();
                input.advance_to(time);
                let probe = ProbeHandle::new();
                let by_tuple = collection.clone().arrange_by_self();
                by_tuple.stream.probe_with(&probe);
                let by_key = collection
                    .map(|tuple| (key_of(&tuple), tuple))
                    .arrange_by_key();
                by_key.stream.probe_with(&probe);
                Base {
                    input,
                    by_tuple: by_tuple.trace,
                    by_key: by_key.trace,
                    probe,
                    metered: HashMap::new(),
                }
            });
            self.bases.insert(relation.to_string(), base);
        }
        self.bases
            .get_mut(relation)
            .unwrap_or_else(|| unreachable!("inserted above"))
    }

    /// Buffer `change` as updates at `revision`.
    fn apply(&mut self, revision: u64, change: BaseChange) {
        // Revisions only grow; an input cannot go back in time.
        let time = revision.max(self.time);
        self.time = time;
        for delta in change.deltas {
            let base = self.base(&delta.relation);
            if *base.input.time() < time {
                base.input.advance_to(time);
            }
            for tuple in delta.removed {
                base.input.update(tuple, -1);
            }
            for tuple in delta.added {
                base.input.update(tuple, 1);
            }
        }
        for relation in change.dropped {
            self.bases.remove(&relation);
        }
    }

    /// Make the arrangements hold everything through `revision`: advance
    /// every input past it and step until each probe has passed it.
    fn settle(&mut self, revision: u64) {
        let next = revision.max(self.time) + 1;
        self.time = next;
        for base in self.bases.values_mut() {
            base.input.advance_to(next);
            base.input.flush();
        }
        while self.bases.values().any(|base| base.probe.less_than(&next)) {
            self.worker.step();
        }
        // Reads are answered as of the frontier; nothing earlier is kept apart.
        let since = [next - 1];
        for base in self.bases.values_mut() {
            base.by_tuple
                .set_logical_compaction(AntichainRef::new(&since));
            base.by_tuple
                .set_physical_compaction(AntichainRef::new(&since));
            base.by_key
                .set_logical_compaction(AntichainRef::new(&since));
            base.by_key
                .set_physical_compaction(AntichainRef::new(&since));
        }
        self.worker.step();
    }

    /// Rows and estimated bytes of every trace.
    fn meter(&mut self) -> BatchSize {
        let mut total = BatchSize::default();
        for base in self.bases.values_mut() {
            let size = base.meter();
            total.rows += size.rows;
            total.bytes += size.bytes;
        }
        total
    }

    fn scan_tuples(&mut self, relation: &str) -> Vec<(Tuple, isize)> {
        let mut rows = Vec::new();
        if let Some(base) = self.bases.get_mut(relation) {
            let (mut cursor, storage) = base.by_tuple.cursor();
            while cursor.key_valid(&storage) {
                let mut count = 0;
                cursor.map_times(&storage, |_, diff| count += *diff);
                if count != 0 {
                    rows.push((cursor.key(&storage).clone(), count));
                }
                cursor.step_key(&storage);
            }
        }
        rows
    }

    fn scan_keys(&mut self, relation: &str, key: Option<&Value>) -> Vec<(Tuple, isize)> {
        let mut rows = Vec::new();
        if let Some(base) = self.bases.get_mut(relation) {
            let (mut cursor, storage) = base.by_key.cursor();
            if let Some(key) = key {
                cursor.seek_key(&storage, key);
            }
            while cursor.key_valid(&storage) {
                if key.is_some_and(|key| cursor.key(&storage) != key) {
                    break;
                }
                while cursor.val_valid(&storage) {
                    let mut count = 0;
                    cursor.map_times(&storage, |_, diff| count += *diff);
                    if count != 0 {
                        rows.push((cursor.val(&storage).clone(), count));
                    }
                    cursor.step_val(&storage);
                }
                cursor.step_key(&storage);
            }
        }
        rows
    }
}

/// The worker thread: load `relations` at `revision`, then apply commands
/// until stopped.
fn worker_loop(
    shared: &Shared,
    revision: u64,
    relations: RelationMap,
    queue: &channel::Receiver<Command>,
) {
    use timely::scheduling::Scheduler;

    execute_with_idle_merging(|worker| {
        let mut dataflows = Dataflows {
            worker,
            bases: HashMap::new(),
            time: revision,
        };
        let publish = |dataflows: &mut Dataflows, revision: u64, started: Instant| {
            let size = dataflows.meter();
            shared.trace_rows.store(size.rows, Ordering::Relaxed);
            shared.trace_bytes.store(size.bytes, Ordering::Relaxed);
            crate::execution::view_counters().record_view_maintenance(started.elapsed());
            shared.applied(revision);
        };

        let started = Instant::now();
        for (relation, tuples) in relations {
            if shared.stop.load(Ordering::Acquire) {
                return;
            }
            let base = dataflows.base(&relation);
            for tuple in tuples.iter() {
                base.input.update(tuple.clone(), 1);
            }
        }
        dataflows.settle(revision);
        publish(&mut dataflows, revision, started);

        // A command taken off the queue while applying commits.
        let mut carried: Option<Command> = None;
        loop {
            let mut idle_steps = 0;
            let command = loop {
                if shared.stop.load(Ordering::Acquire) {
                    return;
                }
                if let Some(command) = carried.take() {
                    break command;
                }
                match queue.try_recv() {
                    Ok(command) => break command,
                    Err(channel::TryRecvError::Disconnected) => return,
                    Err(channel::TryRecvError::Empty) => {}
                }
                // Spend idle time merging trace batches until they are compact.
                let merging =
                    dataflows.worker.activations().borrow().empty_for() == Some(Duration::ZERO);
                if merging && idle_steps < MAX_IDLE_MERGE_STEPS {
                    idle_steps += 1;
                    dataflows.worker.step();
                } else {
                    if idle_steps > 0 {
                        let size = dataflows.meter();
                        shared.trace_rows.store(size.rows, Ordering::Relaxed);
                        shared.trace_bytes.store(size.bytes, Ordering::Relaxed);
                    }
                    match queue.recv() {
                        Ok(command) => break command,
                        Err(_) => return,
                    }
                }
            };

            match command {
                Command::Commit { revision, change } => {
                    // Every commit already queued joins this round.
                    let started = Instant::now();
                    let mut last = revision;
                    let mut buffered = change.updates();
                    dataflows.apply(revision, change);
                    while buffered < SETTLE_UPDATES {
                        match queue.try_recv() {
                            Ok(Command::Commit { revision, change }) => {
                                last = revision;
                                buffered += change.updates();
                                dataflows.apply(revision, change);
                            }
                            Ok(other) => {
                                carried = Some(other);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    dataflows.settle(last);
                    publish(&mut dataflows, last, started);
                }
                Command::ScanTuples { relation, reply } => {
                    let _ = reply.send(BaseRows {
                        revision: shared.frontier.load(Ordering::Acquire),
                        rows: dataflows.scan_tuples(&relation),
                    });
                }
                Command::ScanKeys {
                    relation,
                    key,
                    reply,
                } => {
                    let _ = reply.send(BaseRows {
                        revision: shared.frontier.load(Ordering::Acquire),
                        rows: dataflows.scan_keys(&relation, key.as_ref()),
                    });
                }
                Command::Stop => return,
                #[cfg(test)]
                Command::Panic => panic!("injected view maintainer panic"),
                #[cfg(test)]
                Command::Stall(held) => {
                    let _ = held.recv();
                }
            }
        }
    });
}

#[cfg(test)]
#[path = "view_maintainer_tests.rs"]
mod tests;
