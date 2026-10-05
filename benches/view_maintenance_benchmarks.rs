//! Maintained-view headroom: what keeping deployed rules as long-lived
//! differential dataflow views costs per write, without the engine around it.
//!
//! The incremental-views review's micro-benchmark (its report, Section 4.3 and
//! Appendix A.7) as a Criterion bench: the `perf-gate views` graph (chains of
//! four nodes, every hundredth chain head labelled hot) and its three rules,
//! maintained as arrangements on one timely worker:
//!
//! - `r1(X,Y) <- edge(X,Y), label(X,"hot")`
//! - `path(X,Y) <- edge(X,Y)`; `path(X,Z) <- edge(X,Y), path(Y,Z)`
//! - `hot_reach(X,Y) <- label(X,"hot"), path(X,Y)`
//!
//! Per graph size (10K, 100K and 1M edges; `VIEW_BENCH_EDGES=10000,100000`
//! picks others), each iteration is one commit fully propagated through the
//! views: `insert` adds an edge from a hot head to a fresh node (every view
//! changes), `delete` retracts one (through the recursion), `insert_cold`
//! adds an edge inside a chain that is not hot (only `path` changes);
//! `lookup` reads one key of `hot_reach`. Each change is undone untimed
//! after it is measured, so the views keep their size. The load time, the
//! RSS holding the views and their row counts are printed per size.
//!
//! These are the bars' headroom reference, not the engine's numbers: the
//! engine adds the wire, the commit lock, fsync and delivery.

use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion};
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::Iterate;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord_neu::OrdValSpine;
use differential_dataflow::trace::TraceReader;
use timely::communication::allocator::thread::Thread;
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::Probe;
use timely::progress::frontier::AntichainRef;
use timely::worker::Worker;
use timely::WorkerConfig;

type Trace = TraceAgent<OrdValSpine<u32, u32, u64, isize>>;

/// The label value meaning hot.
const HOT: u8 = 1;

/// The `i`-th hot chain head (as `perf-gate views` numbers them).
fn hot_head(i: usize) -> u32 {
    (4 * 100 * i) as u32
}

/// The three rules as maintained views over the graph, on one worker.
struct Views {
    worker: Worker<Thread>,
    edge: InputSession<u64, (u32, u32), isize>,
    label: InputSession<u64, (u32, u8), isize>,
    probe: Handle<u64>,
    r1: Trace,
    path: Trace,
    hot_reach: Trace,
    time: u64,
    chains: usize,
    hot: usize,
    fresh: u32,
}

impl Views {
    /// Build the views and load a graph of `edges` edges into them.
    fn load(edges: usize) -> (Self, Duration) {
        let mut worker = Worker::new(
            WorkerConfig::default(),
            Thread::default(),
            Some(Instant::now()),
        );
        let probe = Handle::<u64>::new();
        let (edge, label, r1, path, hot_reach) = worker.dataflow::<u64, _, _>(|scope| {
            let (edge_in, edge) = scope.new_collection::<(u32, u32), isize>();
            let (label_in, label) = scope.new_collection::<(u32, u8), isize>();
            let hot = label.filter(|(_, l)| *l == HOT).map(|(x, _)| x);
            let r1 = edge.clone().semijoin(hot.clone());
            let path = edge.clone().iterate(|scope, inner| {
                let edge = edge.enter(&scope);
                let by_target = edge.clone().map(|(x, y)| (y, x));
                inner
                    .join_map(by_target, |_y, z, x| (*x, *z))
                    .concat(edge)
                    .distinct()
            });
            let hot_reach = path.clone().semijoin(hot);
            let r1 = r1.arrange_by_key();
            let path = path.arrange_by_key();
            let hot_reach = hot_reach.arrange_by_key();
            r1.stream.probe_with(&probe);
            path.stream.probe_with(&probe);
            hot_reach.stream.probe_with(&probe);
            (edge_in, label_in, r1.trace, path.trace, hot_reach.trace)
        });
        let chains = edges.div_ceil(3);
        let hot = chains.div_ceil(100);
        let mut views = Self {
            worker,
            edge,
            label,
            probe,
            r1,
            path,
            hot_reach,
            time: 0,
            chains,
            hot,
            fresh: (4 * chains + 1_000) as u32,
        };
        let started = Instant::now();
        for chain in 0..chains {
            let b = (4 * chain) as u32;
            for (from, to) in [(b, b + 1), (b + 1, b + 2), (b + 2, b + 3)] {
                views.edge.insert((from, to));
            }
        }
        for i in 0..hot {
            views.label.insert((hot_head(i), HOT));
        }
        views.commit();
        (views, started.elapsed())
    }

    /// Close the current time and propagate it through every view.
    fn commit(&mut self) {
        self.time += 1;
        let time = self.time;
        self.edge.advance_to(time);
        self.label.advance_to(time);
        self.edge.flush();
        self.label.flush();
        let probe = &self.probe;
        self.worker.step_while(|| probe.less_than(&time));
        let frontier = [time];
        for trace in [&mut self.r1, &mut self.path, &mut self.hot_reach] {
            trace.set_logical_compaction(AntichainRef::new(&frontier));
            trace.set_physical_compaction(AntichainRef::new(&frontier));
        }
    }

    /// A node no edge uses yet.
    fn fresh(&mut self) -> u32 {
        self.fresh += 1;
        self.fresh
    }

    /// The second node of the `i`-th chain that is not hot.
    fn cold_node(&self, i: usize) -> u32 {
        let mut chain = (i * 7 + 1) % self.chains;
        if chain.is_multiple_of(100) {
            chain = (chain + 1) % self.chains;
        }
        (4 * chain + 1) as u32
    }

    /// Time one commit of `change`, then undo it untimed.
    fn timed(&mut self, (from, to): (u32, u32), insert: bool) -> Duration {
        let (apply, undo) = if insert { (1, -1) } else { (-1, 1) };
        if !insert {
            self.edge.update((from, to), undo);
            self.commit();
        }
        let started = Instant::now();
        self.edge.update((from, to), apply);
        self.commit();
        let elapsed = started.elapsed();
        if insert {
            self.edge.update((from, to), undo);
            self.commit();
        }
        elapsed
    }

    /// Rows of `hot_reach` with key `key`.
    fn lookup(&mut self, key: u32) -> usize {
        let (mut cursor, storage) = self.hot_reach.cursor();
        cursor.seek_key(&storage, &key);
        let mut rows = 0;
        if cursor.key_valid(&storage) && *cursor.key(&storage) == key {
            while cursor.val_valid(&storage) {
                let mut diff = 0;
                cursor.map_times(&storage, |_, d| diff += *d);
                if diff > 0 {
                    rows += 1;
                }
                cursor.step_val(&storage);
            }
        }
        rows
    }
}

/// Rows in `trace`.
fn rows(trace: &mut Trace) -> usize {
    let (mut cursor, storage) = trace.cursor();
    let mut rows = 0;
    while cursor.key_valid(&storage) {
        while cursor.val_valid(&storage) {
            let mut diff = 0;
            cursor.map_times(&storage, |_, d| diff += *d);
            if diff > 0 {
                rows += 1;
            }
            cursor.step_val(&storage);
        }
        cursor.step_key(&storage);
    }
    rows
}

fn rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|rest| rest.split_whitespace().next()?.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb / 1024)
}

fn sizes() -> Vec<usize> {
    std::env::var("VIEW_BENCH_EDGES")
        .unwrap_or_else(|_| "10000,100000,1000000".to_string())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

fn bench_size(c: &mut Criterion, edges: usize) {
    let (mut views, load) = Views::load(edges);
    let (r1, path, hot_reach) = (
        rows(&mut views.r1),
        rows(&mut views.path),
        rows(&mut views.hot_reach),
    );
    assert_eq!(
        (r1, path, hot_reach),
        (views.hot, 2 * 3 * views.chains, 3 * views.hot)
    );
    eprintln!(
        "view_maintenance {edges} edges: load {:.0} ms, RSS {} MB holding the views; rows r1={r1} path={path} hot_reach={hot_reach}",
        load.as_secs_f64() * 1e3,
        rss_mb(),
    );

    let mut group = c.benchmark_group("view_maintenance");
    group.bench_function(BenchmarkId::new("insert", edges), |b| {
        b.iter_custom(|iters| {
            (0..iters as usize)
                .map(|i| {
                    let edge = (hot_head(i % views.hot), views.fresh());
                    views.timed(edge, true)
                })
                .sum()
        });
    });
    group.bench_function(BenchmarkId::new("delete", edges), |b| {
        b.iter_custom(|iters| {
            (0..iters as usize)
                .map(|i| {
                    let edge = (hot_head(i % views.hot), views.fresh());
                    views.timed(edge, false)
                })
                .sum()
        });
    });
    group.bench_function(BenchmarkId::new("insert_cold", edges), |b| {
        b.iter_custom(|iters| {
            (0..iters as usize)
                .map(|i| {
                    let edge = (views.cold_node(i), views.fresh());
                    views.timed(edge, true)
                })
                .sum()
        });
    });
    group.bench_function(BenchmarkId::new("lookup", edges), |b| {
        let mut i = 0;
        b.iter(|| {
            i += 1;
            let rows = views.lookup(hot_head(i % views.hot));
            assert_eq!(rows, 3);
        });
    });
    group.finish();
}

fn main() {
    let mut criterion = Criterion::default().configure_from_args();
    for edges in sizes() {
        bench_size(&mut criterion, edges);
    }
    criterion.final_summary();
}
