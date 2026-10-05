//! Restart time at stated fact counts: how long a stopped or crashed engine
//! takes to serve again, and what the first query on a knowledge graph costs.
//!
//! Each point seeds a data directory in a child process, which either shuts
//! down cleanly (shards compacted, WAL empty) or crashes with a WAL tail it
//! never flushed (`abort`, like `kill -9`). Every open then runs in a fresh
//! child process, so its RSS is its own:
//!
//! - `open_ms`: `StorageEngine::new` until it returns, i.e. until the server
//!   could bind and serve.
//! - `first_query_ms`: the first query on `kg_0`, a bound lookup that returns
//!   one row; it pays for loading `kg_0` if the engine did not load it at open.
//! - `all_ms`: then reading a snapshot of every other knowledge graph.
//! - `rss_open_mb`, `rss_query_mb`: resident set after the open and after the
//!   first query.
//!
//! Each open checks the facts it finds against the seeded counts.
//!
//! ```text
//! cargo bench --bench restart_benchmarks                       # full sweep
//! RESTART_BENCH_POINT=1x1000000 cargo bench --bench restart_benchmarks
//! RESTART_BENCH_POINT=1000x10000+crash cargo bench --bench restart_benchmarks
//! ```
//!
//! A point is `{kgs}x{facts_per_kg}`, with `+crash` for a crash with a WAL
//! tail. Data lives under `RESTART_BENCH_DIR` (default: the system temp
//! directory), which should be a real disk: the crash drain pays fsyncs.
//! `RESTART_BENCH_OPENS` sets the opens per point (default 3; the median is
//! reported). `RESTART_BENCH_DROP_CACHES=1` drops the page cache before each
//! open (`sudo -n`), for a node that restarts without the data in memory.

// Benchmark setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::{Config, DurabilityMode, StorageEngine, Tuple, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// Relations per knowledge graph; facts are spread evenly over them.
const RELATIONS: usize = 4;
/// Facts per committed batch while seeding.
const SEED_BATCH: usize = 100_000;
/// Facts per relation in the crash tail: below the default flush threshold
/// (`buffer_size` 10,000), so they stay in the WAL only.
const TAIL_PER_RELATION_ONE_KG: usize = 5_000;
/// Facts per knowledge graph in the crash tail of a many-KG point.
const TAIL_PER_KG_MANY: usize = 200;

/// The default sweep: one knowledge graph at growing fact counts, then the
/// same 10M facts over 1,000 knowledge graphs, each clean and after a crash.
const SWEEP: &[&str] = &[
    "1x0",
    "1x100000",
    "1x1000000",
    "1x1000000+crash",
    "1x10000000",
    "1000x10000",
    "1000x10000+crash",
];

#[derive(Clone, Copy)]
struct Point {
    kgs: usize,
    facts_per_kg: usize,
    crash: bool,
}

impl Point {
    fn parse(text: &str) -> Self {
        let (spec, crash) = match text.strip_suffix("+crash") {
            Some(spec) => (spec, true),
            None => (text, false),
        };
        let (kgs, facts) = spec.split_once('x').expect("point is {kgs}x{facts_per_kg}");
        Point {
            kgs: kgs.parse().unwrap(),
            facts_per_kg: facts.parse().unwrap(),
            crash,
        }
    }

    fn label(self) -> String {
        format!(
            "{}x{}{}",
            self.kgs,
            self.facts_per_kg,
            if self.crash { "+crash" } else { "" }
        )
    }

    /// Facts the crash tail adds to each relation of each KG.
    fn tail_per_relation(self) -> usize {
        match (self.crash, self.kgs) {
            (false, _) => 0,
            (true, 1) => TAIL_PER_RELATION_ONE_KG,
            (true, _) => TAIL_PER_KG_MANY / RELATIONS,
        }
    }

    /// Facts each KG holds after seeding, tail included.
    fn expected_per_kg(self) -> usize {
        self.facts_per_kg / RELATIONS * RELATIONS + self.tail_per_relation() * RELATIONS
    }
}

fn config(dir: &Path) -> Config {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.max_knowledge_graphs = 0;
    config.storage.auto_create_knowledge_graphs = false;
    config.storage.persist.durability_mode = DurabilityMode::Immediate;
    config
}

/// Fact `i` of relation `r`: a unique key and a value derived from it.
fn fact(i: usize, r: usize) -> Tuple {
    Tuple::new(vec![
        Value::Int64(i as i64),
        Value::Int64(((i * 7 + r) % 1009) as i64),
    ])
}

fn seed(dir: &Path, point: Point) {
    let mut bulk = config(dir);
    // Seeding speed only; the tail and the opens use the production default.
    bulk.storage.persist.durability_mode = DurabilityMode::Batched;
    let storage = StorageEngine::new(bulk).unwrap();
    let per_relation = point.facts_per_kg / RELATIONS;
    for k in 0..point.kgs {
        let kg = format!("kg_{k}");
        storage.create_knowledge_graph(&kg).unwrap();
        for r in 0..RELATIONS {
            let relation = format!("rel_{r}");
            let mut start = 0;
            while start < per_relation {
                let end = (start + SEED_BATCH).min(per_relation);
                let tuples = (start..end).map(|i| fact(i, r)).collect();
                storage.insert_tuples_into(&kg, &relation, tuples).unwrap();
                start = end;
            }
        }
    }
    // Steady state: one batch file per shard, empty WAL.
    storage.compact_all().unwrap();
    storage.save_all().unwrap();
    let tail = point.tail_per_relation();
    if tail == 0 {
        return;
    }
    // Crash tail: small durable commits that stay in the WAL, then die
    // without flushing or saving anything.
    drop(storage);
    let storage = StorageEngine::new(config(dir)).unwrap();
    for k in 0..point.kgs {
        let kg = format!("kg_{k}");
        for r in 0..RELATIONS {
            let relation = format!("rel_{r}");
            for chunk in (per_relation..per_relation + tail)
                .collect::<Vec<_>>()
                .chunks(500)
            {
                let tuples = chunk.iter().map(|&i| fact(i, r)).collect();
                storage.insert_tuples_into(&kg, &relation, tuples).unwrap();
            }
        }
    }
    std::process::abort();
}

/// Resident and peak resident set of this process, in MB.
fn rss_mb() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<f64>().ok())
        .map_or(f64::NAN, |kb| kb / 1024.0)
}

fn open(dir: &Path, point: Point) {
    let start = Instant::now();
    let storage = StorageEngine::new(config(dir)).unwrap();
    let open_ms = start.elapsed().as_secs_f64() * 1e3;
    let rss_open_mb = rss_mb();

    let start = Instant::now();
    let rows = storage
        .execute_query_tuples_on("kg_0", "q(Y) <- rel_0(7, Y)")
        .unwrap();
    let first_query_ms = start.elapsed().as_secs_f64() * 1e3;
    let rss_query_mb = rss_mb();
    let expected_rows = usize::from(point.expected_per_kg() > 0);
    assert_eq!(rows.len(), expected_rows, "rows of the first query");

    let start = Instant::now();
    for k in 1..point.kgs {
        storage.get_snapshot_for(&format!("kg_{k}")).unwrap();
    }
    let all_ms = start.elapsed().as_secs_f64() * 1e3;

    // Not timed: every KG holds exactly the facts seeded.
    for k in [0, point.kgs - 1] {
        let facts: usize = storage
            .list_relations_with_metadata(&format!("kg_{k}"))
            .unwrap()
            .iter()
            .map(|(_, _, count)| count)
            .sum();
        assert_eq!(facts, point.expected_per_kg(), "facts of kg_{k}");
    }
    println!(
        "{{\"open_ms\":{open_ms:.1},\"first_query_ms\":{first_query_ms:.1},\"all_ms\":{all_ms:.1},\
         \"rss_open_mb\":{rss_open_mb:.0},\"rss_query_mb\":{rss_query_mb:.0}}}"
    );
}

fn drop_caches() {
    let status = Command::new("sudo")
        .args(["-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches"])
        .status()
        .unwrap();
    assert!(
        status.success(),
        "dropping the page cache needs passwordless sudo"
    );
}

fn child(role: &str, dir: &Path, point: Point) -> String {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--restart-child",
            role,
            dir.to_str().unwrap(),
            &point.label(),
        ])
        .output()
        .unwrap();
    // A crashing seed exits by abort, which is its intended end.
    if role == "open" || !point.crash {
        assert!(
            output.status.success(),
            "{role} child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).unwrap()
}

/// Value of numeric field `key` in a one-line JSON object.
fn field(json: &str, key: &str) -> f64 {
    let at = json.find(&format!("\"{key}\":")).unwrap() + key.len() + 3;
    json[at..]
        .split([',', '}'])
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

fn run_point(root: &Path, point: Point, opens: usize, cold: bool) {
    let tmp = tempfile::tempdir_in(root).unwrap();
    let seed_start = Instant::now();
    child("seed", tmp.path(), point);
    let seed_s = seed_start.elapsed().as_secs_f64();
    let mut runs = Vec::new();
    for _ in 0..opens {
        // Opening a crashed directory recovers it, so each open of a crash
        // point gets its own copy of the crashed state.
        let copy = point.crash.then(|| {
            let copy = tempfile::tempdir_in(root).unwrap();
            copy_dir(tmp.path(), copy.path());
            copy
        });
        let open_dir = copy.as_ref().map_or(tmp.path(), |d| d.path());
        if cold {
            drop_caches();
        }
        runs.push(child("open", open_dir, point));
    }
    let stat = |key: &str| median(runs.iter().map(|run| field(run, key)).collect());
    println!(
        "{:<18} {:>11} {:>9.1} {:>9.1} {:>15.1} {:>10.1} {:>12.0} {:>13.0}",
        point.label(),
        point.kgs * point.expected_per_kg(),
        seed_s,
        stat("open_ms"),
        stat("first_query_ms"),
        stat("all_ms"),
        stat("rss_open_mb"),
        stat("rss_query_mb"),
    );
}

/// Copy a data directory.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(at) = args.iter().position(|a| a == "--restart-child") {
        let dir = PathBuf::from(&args[at + 2]);
        let point = Point::parse(&args[at + 3]);
        match args[at + 1].as_str() {
            "seed" => seed(&dir, point),
            "open" => open(&dir, point),
            other => panic!("unknown child role {other}"),
        }
        return;
    }

    let root = std::env::var_os("RESTART_BENCH_DIR").map_or_else(std::env::temp_dir, PathBuf::from);
    let opens = std::env::var("RESTART_BENCH_OPENS")
        .ok()
        .map_or(3, |v| v.parse().unwrap());
    let cold = std::env::var_os("RESTART_BENCH_DROP_CACHES").is_some();
    let points: Vec<Point> = match std::env::var("RESTART_BENCH_POINT") {
        Ok(points) => points.split(',').map(Point::parse).collect(),
        Err(_) => SWEEP.iter().map(|p| Point::parse(p)).collect(),
    };
    println!(
        "restart: median of {opens} opens, {} page cache, data in {}",
        if cold { "cold" } else { "warm" },
        root.display()
    );
    println!(
        "{:<18} {:>11} {:>9} {:>9} {:>15} {:>10} {:>12} {:>13}",
        "point",
        "facts",
        "seed_s",
        "open_ms",
        "first_query_ms",
        "all_ms",
        "rss_open_mb",
        "rss_query_mb"
    );
    for point in points {
        run_point(&root, point, opens, cold);
    }
}
