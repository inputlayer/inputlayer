//! The stable on-disk record of a gate run.
//!
//! A run file holds every raw sample, so a verdict can be recomputed (or
//! re-judged under a new policy) without rerunning anything. Changing the
//! meaning of a field requires a new [`SCHEMA`] version.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Schema identifier written into, and required from, every run file.
pub const SCHEMA: &str = "inputlayer-perf-gate/v1";

/// One gate run: two arms measured in interleaved rounds on one host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub schema: String,
    pub created_unix: u64,
    pub environment: Environment,
    pub workload: Workload,
    pub arms: Vec<Arm>,
    /// One entry per (round, fixture, arm), in execution order.
    pub runs: Vec<FixtureRun>,
}

/// A server binary under test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Arm {
    /// `baseline` or `candidate`.
    pub name: String,
    /// Commit or other provenance label supplied by the caller.
    pub label: String,
    pub binary: String,
    pub binary_sha256: String,
}

/// Where and how the run was measured.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Environment {
    pub hostname: String,
    pub kernel: String,
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub mem_total_kb: u64,
    pub cpu_governor: String,
    /// `/proc/loadavg` when the run started and ended.
    pub loadavg_start: String,
    pub loadavg_end: String,
    /// Filesystem type holding the servers' data directories.
    pub data_fs: String,
    pub data_root: String,
    /// CPU list the servers were pinned to, if any.
    pub server_cpus: Option<String>,
    /// Free-form toolchain/build notes supplied by the caller.
    pub build: String,
}

/// What was measured: the profile and its full fixture parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workload {
    pub profile: String,
    pub rounds: u32,
    pub fixtures: Vec<String>,
    /// Server settings that differ from a default install, with reasons.
    pub server_overrides: BTreeMap<String, String>,
    /// Profile parameters, as serialized by the profile itself.
    pub parameters: serde_json::Value,
}

/// Raw measurements of one fixture on one arm in one round.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FixtureRun {
    pub arm: String,
    pub round: u32,
    pub fixture: String,
    /// Latency samples in microseconds, by series name.
    pub series: BTreeMap<String, Vec<u64>>,
    /// Throughput counters, by rate name.
    pub rates: BTreeMap<String, Rate>,
    /// Point observations, e.g. server peak RSS.
    pub gauges: BTreeMap<String, u64>,
    /// Set when the fixture failed or saw a wrong result; the run is then
    /// unusable for a verdict.
    pub error: Option<String>,
}

/// `ops` completed in `elapsed_us`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Rate {
    pub ops: u64,
    pub elapsed_us: u64,
}

impl Rate {
    /// Operations per second; zero when nothing was timed.
    pub fn per_sec(&self) -> f64 {
        if self.elapsed_us == 0 {
            return 0.0;
        }
        self.ops as f64 * 1_000_000.0 / self.elapsed_us as f64
    }
}
