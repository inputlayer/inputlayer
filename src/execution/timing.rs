//! Query Timing & Profiling
//!
//! Per-stage timing breakdown for query execution.
//! Times are in microseconds (us) - many stages are sub-millisecond.

use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Controls the level of timing detail collected during query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimingMode {
    /// No timing overhead - skips all `Instant::now()` calls.
    Off,
    /// Stage-level totals only (default). Adds ~200-350ns overhead.
    #[default]
    Summary,
    /// Per-rule breakdown with rule head names, recursion, and worker count.
    Detailed,
}

/// Per-stage timing breakdown for a query execution.
///
/// All times are in microseconds (us).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimingBreakdown {
    /// Total execution time (us)
    pub total_us: u64,
    /// Source parsing time (us)
    pub parse_us: u64,
    /// SIP rewriting time (us)
    pub sip_us: u64,
    /// Magic Sets transformation time (us)
    pub magic_sets_us: u64,
    /// IR building time (us)
    pub ir_build_us: u64,
    /// Optimization passes time (us)
    pub optimize_us: u64,
    /// Shared views (CSE) execution time (us)
    pub shared_views_us: u64,
    /// Per-rule execution timings (only in Detailed mode)
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub rules: Vec<RuleTiming>,
    /// Detailed optimizer timing (only in Detailed mode)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub optimizer_detail: Option<OptimizerTiming>,
    /// Detailed IR builder timing (only in Detailed mode)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ir_builder_detail: Option<IrBuilderTiming>,
    /// Detailed codegen timing (only in Detailed mode)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub codegen_detail: Option<CodegenTiming>,
}

/// Timing information for a single rule execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleTiming {
    /// Rule head relation name
    pub rule_head: String,
    /// Execution time (us)
    pub execution_us: u64,
    /// Whether this rule was evaluated recursively
    pub is_recursive: bool,
    /// Number of workers used for execution
    pub workers: usize,
}

/// Detailed optimizer timing (only in Detailed mode).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OptimizerTiming {
    /// Number of optimization iterations before fixpoint
    pub iterations: u32,
    /// Total time for iterative rule application (us)
    pub rules_us: u64,
    /// Time for final logic fusion passes (us)
    pub fusion_us: u64,
}

/// Detailed IR builder timing (only in Detailed mode).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IrBuilderTiming {
    /// Time building scan nodes (us)
    pub scans_us: u64,
    /// Time building join tree (us)
    pub joins_us: u64,
    /// Time building computed columns (us)
    pub computed_us: u64,
    /// Time building comparison filters (us)
    pub filters_us: u64,
    /// Time building antijoins (us)
    pub antijoins_us: u64,
    /// Time building projection/aggregation (us)
    pub projection_us: u64,
}

/// Detailed code generation timing (only in Detailed mode).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodegenTiming {
    /// Time for DD dataflow setup (us)
    pub setup_us: u64,
    /// Time for DD computation / fixpoint iteration (us)
    pub computation_us: u64,
    /// Time for result collection from DD (us)
    pub collection_us: u64,
}

/// Helper for collecting timing measurements during execution.
///
/// In `Off` mode, the `time()` method skips `Instant::now()` calls entirely.
pub struct TimingCollector {
    mode: TimingMode,
    start: Option<Instant>,
    pub breakdown: TimingBreakdown,
}

impl TimingCollector {
    /// Create a new collector with the given timing mode.
    pub fn new(mode: TimingMode) -> Self {
        let start = if mode == TimingMode::Off {
            None
        } else {
            Some(Instant::now())
        };
        Self {
            mode,
            start,
            breakdown: TimingBreakdown::default(),
        }
    }

    /// Returns the current timing mode.
    pub fn mode(&self) -> TimingMode {
        self.mode
    }

    /// Returns true if any timing is being collected.
    pub fn is_active(&self) -> bool {
        self.mode != TimingMode::Off
    }

    /// Returns true if per-rule detail is being collected.
    pub fn is_detailed(&self) -> bool {
        self.mode == TimingMode::Detailed
    }

    /// Time a closure, returning its result and the elapsed microseconds.
    /// In `Off` mode, just runs the closure and returns 0.
    pub fn time<F, R>(&self, f: F) -> (R, u64)
    where
        F: FnOnce() -> R,
    {
        if self.mode == TimingMode::Off {
            return (f(), 0);
        }
        let start = Instant::now();
        let result = f();
        let elapsed_us = start.elapsed().as_micros() as u64;
        (result, elapsed_us)
    }

    /// Record a rule execution timing (only in Detailed mode).
    pub fn record_rule(
        &mut self,
        head: String,
        execution_us: u64,
        is_recursive: bool,
        workers: usize,
    ) {
        if self.mode == TimingMode::Detailed {
            self.breakdown.rules.push(RuleTiming {
                rule_head: head,
                execution_us,
                is_recursive,
                workers,
            });
        }
    }

    /// Finalize total time and return the breakdown (if timing is active).
    pub fn finish(mut self) -> Option<TimingBreakdown> {
        match self.mode {
            TimingMode::Off => None,
            _ => {
                if let Some(start) = self.start {
                    self.breakdown.total_us = start.elapsed().as_micros() as u64;
                }
                Some(self.breakdown)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Prometheus histogram buckets for query timing stages
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering};

/// Fixed bucket boundaries in seconds for Prometheus histograms.
const BUCKET_BOUNDS: [f64; 8] = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Number of buckets including the implicit +Inf bucket.
const NUM_BUCKETS: usize = BUCKET_BOUNDS.len() + 1; // 9

/// A single Prometheus-style histogram with fixed buckets.
///
/// Each bucket counter tracks cumulative observations <= that boundary.
/// The last bucket is +Inf (all observations).
struct Histogram {
    /// Cumulative bucket counters. Index `i` counts observations <= BUCKET_BOUNDS[i].
    /// The last index counts all observations (+Inf).
    buckets: [AtomicU64; NUM_BUCKETS],
    /// Sum of all observed values (stored as integer microseconds).
    sum_us: AtomicU64,
    /// Total number of observations.
    count: AtomicU64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record a duration in microseconds.
    fn record_us(&self, us: u64) {
        let secs = us as f64 / 1_000_000.0;
        // Increment all cumulative buckets where the value fits.
        for (i, &bound) in BUCKET_BOUNDS.iter().enumerate() {
            if secs <= bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket always gets incremented.
        self.buckets[NUM_BUCKETS - 1].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Format this histogram as Prometheus text exposition lines.
    /// `name` is the full metric name (e.g. `inputlayer_query_parse_seconds`).
    fn format_prometheus(&self, name: &str, help: &str, out: &mut String) {
        use std::fmt::Write;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (i, &bound) in BUCKET_BOUNDS.iter().enumerate() {
            let cumulative = self.buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let inf_count = self.buckets[NUM_BUCKETS - 1].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {inf_count}");
        let sum_secs = self.sum_us.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_sum {sum_secs}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

/// Accumulated timing histograms for Prometheus export.
///
/// Tracks fixed-bucket histograms for parse, optimize, execute, and total
/// query time. All counters use relaxed atomic operations for lock-free updates.
pub struct TimingHistograms {
    parse: Histogram,
    optimize: Histogram,
    execute: Histogram,
    total: Histogram,
}

impl TimingHistograms {
    /// Create a new set of empty histograms.
    pub fn new() -> Self {
        Self {
            parse: Histogram::new(),
            optimize: Histogram::new(),
            execute: Histogram::new(),
            total: Histogram::new(),
        }
    }

    /// Record a timing breakdown into the histograms.
    pub fn record(&self, breakdown: &TimingBreakdown) {
        self.parse.record_us(breakdown.parse_us);
        self.optimize.record_us(breakdown.optimize_us);
        // Execute time = total minus parsing/rewriting/optimization stages.
        let execute_us = breakdown.total_us.saturating_sub(
            breakdown
                .parse_us
                .saturating_add(breakdown.sip_us)
                .saturating_add(breakdown.magic_sets_us)
                .saturating_add(breakdown.ir_build_us)
                .saturating_add(breakdown.optimize_us),
        );
        self.execute.record_us(execute_us);
        self.total.record_us(breakdown.total_us);
    }

    /// Format all histograms as Prometheus text exposition.
    pub fn format_prometheus(&self) -> String {
        let mut out = String::with_capacity(2048);
        self.parse.format_prometheus(
            "inputlayer_query_parse_seconds",
            "Time spent parsing query source.",
            &mut out,
        );
        self.optimize.format_prometheus(
            "inputlayer_query_optimize_seconds",
            "Time spent optimizing query plan.",
            &mut out,
        );
        self.execute.format_prometheus(
            "inputlayer_query_execute_seconds",
            "Time spent executing query (DD computation).",
            &mut out,
        );
        self.total.format_prometheus(
            "inputlayer_query_total_seconds",
            "Total end-to-end query time.",
            &mut out,
        );
        out
    }
}

impl Default for TimingHistograms {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_timing_mode_default() {
        assert_eq!(TimingMode::default(), TimingMode::Summary);
    }

    #[test]
    fn test_collector_off_mode() {
        let collector = TimingCollector::new(TimingMode::Off);
        assert!(!collector.is_active());
        assert!(!collector.is_detailed());
        let (result, us) = collector.time(|| 42);
        assert_eq!(result, 42);
        assert_eq!(us, 0);
        assert!(collector.finish().is_none());
    }

    #[test]
    fn test_collector_summary_mode() {
        let mut collector = TimingCollector::new(TimingMode::Summary);
        assert!(collector.is_active());
        assert!(!collector.is_detailed());

        let (result, _us) = collector.time(|| "hello");
        assert_eq!(result, "hello");

        // record_rule is a no-op in Summary mode
        collector.record_rule("test".into(), 100, false, 1);
        let breakdown = collector.finish().unwrap();
        assert!(breakdown.rules.is_empty());
    }

    #[test]
    fn test_collector_detailed_mode() {
        let mut collector = TimingCollector::new(TimingMode::Detailed);
        assert!(collector.is_active());
        assert!(collector.is_detailed());

        collector.record_rule("result".into(), 500, false, 1);
        collector.record_rule("tc".into(), 2000, true, 4);

        let breakdown = collector.finish().unwrap();
        assert_eq!(breakdown.rules.len(), 2);
        assert_eq!(breakdown.rules[0].rule_head, "result");
        assert!(!breakdown.rules[0].is_recursive);
        assert_eq!(breakdown.rules[1].rule_head, "tc");
        assert!(breakdown.rules[1].is_recursive);
        assert!(breakdown.total_us > 0);
    }

    #[test]
    fn test_timing_breakdown_serde_roundtrip() {
        let breakdown = TimingBreakdown {
            total_us: 1000,
            parse_us: 100,
            sip_us: 50,
            magic_sets_us: 30,
            ir_build_us: 200,
            optimize_us: 150,
            shared_views_us: 70,
            rules: vec![RuleTiming {
                rule_head: "result".into(),
                execution_us: 400,
                is_recursive: false,
                workers: 1,
            }],
            ..Default::default()
        };

        let json = serde_json::to_string(&breakdown).unwrap();
        let deserialized: TimingBreakdown = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_us, 1000);
        assert_eq!(deserialized.rules.len(), 1);
        assert_eq!(deserialized.rules[0].rule_head, "result");
    }

    #[test]
    fn test_timing_breakdown_empty_rules_skipped() {
        let breakdown = TimingBreakdown::default();
        let json = serde_json::to_string(&breakdown).unwrap();
        assert!(!json.contains("rules"));
    }

    #[test]
    fn test_timing_mode_serde() {
        let json = serde_json::to_string(&TimingMode::Off).unwrap();
        assert_eq!(json, "\"off\"");
        let json = serde_json::to_string(&TimingMode::Summary).unwrap();
        assert_eq!(json, "\"summary\"");
        let json = serde_json::to_string(&TimingMode::Detailed).unwrap();
        assert_eq!(json, "\"detailed\"");

        let mode: TimingMode = serde_json::from_str("\"off\"").unwrap();
        assert_eq!(mode, TimingMode::Off);
    }

    #[test]
    fn test_timing_histograms_record_and_format() {
        let histograms = TimingHistograms::new();
        let breakdown = TimingBreakdown {
            total_us: 50_000,   // 50ms total
            parse_us: 1_000,    // 1ms parse
            sip_us: 500,        // 0.5ms sip
            magic_sets_us: 200, // 0.2ms magic sets
            ir_build_us: 2_000, // 2ms ir build
            optimize_us: 3_000, // 3ms optimize
            shared_views_us: 0,
            rules: vec![],
            ..Default::default()
        };
        histograms.record(&breakdown);

        let prom = histograms.format_prometheus();
        assert!(prom.contains("inputlayer_query_parse_seconds"));
        assert!(prom.contains("inputlayer_query_optimize_seconds"));
        assert!(prom.contains("inputlayer_query_execute_seconds"));
        assert!(prom.contains("inputlayer_query_total_seconds"));
        // Each histogram should have _count 1 after one recording
        assert!(prom.contains("inputlayer_query_total_seconds_count 1"));
        assert!(prom.contains("inputlayer_query_parse_seconds_count 1"));
        // +Inf bucket should always have 1
        assert!(prom.contains("inputlayer_query_total_seconds_bucket{le=\"+Inf\"} 1"));
    }

    #[test]
    fn test_timing_histograms_empty() {
        let histograms = TimingHistograms::new();
        let prom = histograms.format_prometheus();
        // All counts should be 0
        assert!(prom.contains("inputlayer_query_total_seconds_count 0"));
        assert!(prom.contains("inputlayer_query_total_seconds_bucket{le=\"+Inf\"} 0"));
    }
}
