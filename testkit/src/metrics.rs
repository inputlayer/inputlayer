//! Raw writer→agent delta latency samples.
//!
//! Every sample is one (committed write, subscriber) pair: when the writer
//! sent the program, when its acknowledgement arrived, and when the agent's
//! `subscription_delta` arrived. Samples are collected in an owned
//! [`SampleLog`] per scenario (no shared lock on the measured path) and
//! written once, as JSON lines, to
//! `$INPUTLAYER_REACTIVE_SAMPLES_DIR/<scenario>.jsonl` when that variable is
//! set. The record layout is versioned by [`SCHEMA`]; the performance gate
//! reads these files as raw samples rather than summary statistics.

use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use serde::Serialize;

use crate::client::Commit;

/// Identifier and version of the sample record layout.
pub const SCHEMA: &str = "inputlayer.reactive.delta_latency.v1";
/// Directory receiving `<scenario>.jsonl`; unset disables writing.
pub const SAMPLES_DIR_ENV: &str = "INPUTLAYER_REACTIVE_SAMPLES_DIR";

/// One writer→agent delivery.
#[derive(Debug, Clone, Serialize)]
pub struct DeltaLatencySample {
    pub schema: &'static str,
    pub scenario: String,
    pub fixture: String,
    /// Agents subscribed to the changed result when the write committed.
    pub subscribers: usize,
    /// What the write changed, e.g. `insert`, `retract`, `rule_add`.
    pub event: String,
    /// Index of the write within the scenario.
    pub sequence: usize,
    /// Which subscriber received the delta.
    pub subscriber: usize,
    /// Write sent → write acknowledged, microseconds.
    pub write_ack_us: u64,
    /// Write sent → delta arrived at the agent, microseconds.
    pub write_to_delta_us: u64,
    /// Build profile of the harness (`debug` or `release`).
    pub profile: &'static str,
}

/// p50/p99/max of `write_to_delta_us` over a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub samples: usize,
    pub p50_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

/// Samples of one scenario.
#[derive(Debug)]
pub struct SampleLog {
    scenario: String,
    fixture: String,
    samples: Vec<DeltaLatencySample>,
}

impl SampleLog {
    /// Empty log for `scenario` over fixture `fixture`.
    pub fn new(scenario: &str, fixture: &str) -> Self {
        Self {
            scenario: scenario.to_string(),
            fixture: fixture.to_string(),
            samples: Vec::new(),
        }
    }

    /// Record that `subscriber` (of `subscribers`) received the delta for
    /// write number `sequence` at `delta_at`.
    pub fn record(
        &mut self,
        event: &str,
        sequence: usize,
        commit: Commit,
        subscriber: usize,
        subscribers: usize,
        delta_at: Instant,
    ) {
        self.samples.push(DeltaLatencySample {
            schema: SCHEMA,
            scenario: self.scenario.clone(),
            fixture: self.fixture.clone(),
            subscribers,
            event: event.to_string(),
            sequence,
            subscriber,
            write_ack_us: micros(commit.acked_at - commit.sent_at),
            write_to_delta_us: micros(delta_at.saturating_duration_since(commit.sent_at)),
            profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        });
    }

    /// The recorded samples.
    pub fn samples(&self) -> &[DeltaLatencySample] {
        &self.samples
    }

    /// Latency distribution; `None` without samples.
    pub fn summary(&self) -> Option<Summary> {
        let mut latencies: Vec<u64> = self.samples.iter().map(|s| s.write_to_delta_us).collect();
        latencies.sort_unstable();
        let last = *latencies.last()?;
        Some(Summary {
            samples: latencies.len(),
            p50_us: percentile(&latencies, 50),
            p99_us: percentile(&latencies, 99),
            max_us: last,
        })
    }

    /// Print the summary and write the samples if [`SAMPLES_DIR_ENV`] is set.
    pub fn finish(self) -> std::io::Result<Option<Summary>> {
        let summary = self.summary();
        if let Some(s) = summary {
            println!(
                "[{}] {} delta sample(s): p50 {} us, p99 {} us, max {} us",
                self.scenario, s.samples, s.p50_us, s.p99_us, s.max_us
            );
        }
        if let Some(dir) = std::env::var_os(SAMPLES_DIR_ENV).map(PathBuf::from) {
            std::fs::create_dir_all(&dir)?;
            let mut out = Vec::new();
            for sample in &self.samples {
                serde_json::to_writer(&mut out, sample)?;
                out.push(b'\n');
            }
            std::fs::File::create(dir.join(format!("{}.jsonl", self.scenario)))?.write_all(&out)?;
        }
        Ok(summary)
    }
}

fn micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Nearest-rank percentile of sorted, non-empty `values`.
fn percentile(values: &[u64], pct: usize) -> u64 {
    let rank = (values.len() * pct).div_ceil(100).max(1);
    values[rank - 1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_percentile_nearest_rank() {
        let values: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&values, 50), 50);
        assert_eq!(percentile(&values, 99), 99);
        assert_eq!(percentile(&[7], 99), 7);
    }

    #[test]
    fn test_sample_records_latencies_from_send() {
        let sent_at = Instant::now();
        let commit = Commit {
            sent_at,
            acked_at: sent_at + Duration::from_micros(300),
        };
        let mut log = SampleLog::new("scenario", "fixture");
        log.record(
            "insert",
            0,
            commit,
            0,
            1,
            sent_at + Duration::from_micros(900),
        );
        let sample = &log.samples()[0];
        assert_eq!(sample.schema, SCHEMA);
        assert_eq!(sample.write_ack_us, 300);
        assert_eq!(sample.write_to_delta_us, 900);
        let summary = log.summary().expect("one sample");
        assert_eq!(
            (summary.p50_us, summary.p99_us, summary.max_us),
            (900, 900, 900)
        );
        assert!(SampleLog::new("s", "f").summary().is_none());
    }
}
