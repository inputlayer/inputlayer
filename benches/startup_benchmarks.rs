//! Startup scaling: `StorageEngine::new` over persisted KGs x shards.
//!
//! Each point seeds a data directory with `kgs` knowledge graphs of
//! `shards_per_kg` one-tuple relations, then times fresh engine opens. Shard
//! ownership must not depend on the KG count, so at a fixed shard total the
//! time per shard should stay flat as KGs grow.
//!
//! ```text
//! cargo bench --bench startup_benchmarks                  # full sweep
//! STARTUP_BENCH_POINT=4000,1 cargo bench --bench startup_benchmarks
//! STARTUP_BENCH_LEGACY=1 cargo bench --bench startup_benchmarks
//! ```
//!
//! Data lives on `/dev/shm` unless `STARTUP_BENCH_DIR` names another
//! directory. `STARTUP_BENCH_POINT` runs one point, for an external RSS
//! probe such as `/usr/bin/time -v` on the bench binary.
//! `STARTUP_BENCH_LEGACY=1` names every other KG `tenant:{i}` next to a
//! canonical `tenant`, so every lookup takes the legacy longest-prefix path.

// Benchmark setup aborts on failure; `unwrap` is the intended behavior.
#![allow(clippy::unwrap_used)]

use inputlayer::storage::persist::{FilePersist, PersistBackend, PersistConfig, Transaction};
use inputlayer::storage::{KnowledgeGraphInfo, KnowledgeGraphsMetadata};
use inputlayer::{Config, DurabilityMode, StorageEngine, Tuple, Value};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// (kgs, shards per KG). The first three hold 4,000 shards while the KG
/// count grows 16x; the rest grow the shard total at one shard per KG.
const SWEEP: &[(usize, usize)] = &[
    (250, 16),
    (1_000, 4),
    (4_000, 1),
    (1_000, 1),
    (8_000, 1),
    (16_000, 1),
];

const OPENS: usize = 3;

fn kg_names(kgs: usize, legacy: bool) -> Vec<String> {
    (0..kgs)
        .map(|i| match (legacy, i) {
            (true, 0) => "tenant".to_string(),
            (true, i) if i % 2 == 1 => format!("tenant:{i}"),
            _ => format!("kg_{i}"),
        })
        .collect()
}

fn seed(dir: &Path, kgs: &[String], shards_per_kg: usize) {
    let persist = FilePersist::new(PersistConfig {
        path: dir.join("persist"),
        durability_mode: DurabilityMode::Batched,
        ..Default::default()
    })
    .unwrap();
    // Commit and flush shard by shard: a flush rewrites the WAL, so one big
    // transaction would make seeding quadratic. Opens then see an empty WAL.
    let mut revision = 0;
    for (i, kg) in kgs.iter().enumerate() {
        for r in 0..shards_per_kg {
            let shard = format!("{kg}:rel_{r}");
            revision += 1;
            let mut txn = Transaction::new(revision);
            txn.insert(shard.clone(), [Tuple::new(vec![Value::Int64(i as i64)])]);
            persist.commit(txn).unwrap();
            persist.flush(&shard).unwrap();
        }
    }
    persist.sync().unwrap();
    let metadata = KnowledgeGraphsMetadata {
        version: "1.0".to_string(),
        knowledge_graphs: kgs
            .iter()
            .map(|name| KnowledgeGraphInfo {
                name: name.clone(),
                created_at: String::new(),
                last_accessed: String::new(),
                relations_count: shards_per_kg,
                total_tuples: shards_per_kg,
            })
            .collect(),
    };
    std::fs::create_dir_all(dir.join("metadata")).unwrap();
    metadata
        .save(&dir.join("metadata/knowledge_graphs.json"))
        .unwrap();
}

fn open(dir: &Path, expected_kgs: usize) -> Duration {
    let mut config = Config::default();
    config.storage.data_dir = dir.to_path_buf();
    config.storage.performance.num_threads = 1;
    config.storage.max_knowledge_graphs = 0;
    let start = Instant::now();
    let storage = StorageEngine::new(config).unwrap();
    let elapsed = start.elapsed();
    // `default` is created when absent.
    assert!(storage.list_knowledge_graphs().len() >= expected_kgs);
    elapsed
}

/// `STARTUP_BENCH_DIR`, else tmpfs when present: on a disk, per-shard
/// fsyncs hide the CPU cost of startup.
fn bench_root() -> PathBuf {
    std::env::var_os("STARTUP_BENCH_DIR").map_or_else(
        || {
            let shm = PathBuf::from("/dev/shm");
            if shm.is_dir() {
                shm
            } else {
                std::env::temp_dir()
            }
        },
        PathBuf::from,
    )
}

fn run_point(kgs: usize, shards_per_kg: usize, legacy: bool) {
    let tmp = tempfile::tempdir_in(bench_root()).unwrap();
    let names = kg_names(kgs, legacy);
    seed(tmp.path(), &names, shards_per_kg);
    // The first open creates the per-KG directories; time the rest.
    open(tmp.path(), kgs);
    let mut opens: Vec<Duration> = (0..OPENS).map(|_| open(tmp.path(), kgs)).collect();
    opens.sort();
    let median = opens[OPENS / 2];
    let shards = kgs * shards_per_kg;
    println!(
        "{kgs:>7} {shards_per_kg:>6} {shards:>7} {:>10.1} {:>10.2}",
        median.as_secs_f64() * 1e3,
        median.as_secs_f64() * 1e6 / shards as f64,
    );
}

fn main() {
    let legacy = std::env::var_os("STARTUP_BENCH_LEGACY").is_some();
    let points: Vec<(usize, usize)> = match std::env::var("STARTUP_BENCH_POINT") {
        Ok(point) => {
            let (kgs, per) = point
                .split_once(',')
                .expect("STARTUP_BENCH_POINT=kgs,shards");
            vec![(kgs.parse().unwrap(), per.parse().unwrap())]
        }
        Err(_) => SWEEP.to_vec(),
    };
    println!("startup median of {OPENS} opens (legacy names: {legacy})");
    println!(
        "{:>7} {:>6} {:>7} {:>10} {:>10}",
        "kgs", "per_kg", "shards", "open_ms", "us/shard"
    );
    for (kgs, per) in points {
        run_point(kgs, per, legacy);
    }
}
