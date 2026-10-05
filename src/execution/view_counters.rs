//! Counters that tell how reads of persistent rules were answered.
//!
//! A read of a persistent rule is answered one of two ways: by evaluating the
//! rule (and every persistent rule it depends on) from the base facts, or by
//! reading a view that already holds the rule's rows. Today every such read
//! evaluates: `rule_evaluations` grows by one per read and `view_reads` and
//! `view_maintenance_us` stay at zero. The view maintainer (#309) records the
//! time it spends keeping views current, and queries that read views (#315)
//! record view reads instead of evaluations.
//!
//! The counters are process-wide (the server runs one storage engine) and
//! exported on `/metrics/prometheus`.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static COUNTERS: ViewCounters = ViewCounters::new();

/// The process-wide counters.
pub fn view_counters() -> &'static ViewCounters {
    &COUNTERS
}

/// Lock-free counters of how reads of persistent rules were answered.
#[derive(Debug)]
pub struct ViewCounters {
    view_reads: AtomicU64,
    rule_evaluations: AtomicU64,
    view_maintenance_us: AtomicU64,
}

/// The counters' values at one moment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ViewCounts {
    /// Reads answered from a view, without evaluating a rule.
    pub view_reads: u64,
    /// Reads answered by evaluating persistent rules from the base facts.
    pub rule_evaluations: u64,
    /// Microseconds spent keeping views current with commits.
    pub view_maintenance_us: u64,
}

impl ViewCounters {
    const fn new() -> Self {
        Self {
            view_reads: AtomicU64::new(0),
            rule_evaluations: AtomicU64::new(0),
            view_maintenance_us: AtomicU64::new(0),
        }
    }

    /// One read answered from a view.
    pub fn record_view_read(&self) {
        self.view_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// One read answered by evaluating persistent rules: a query, a standing
    /// query's refresh, a proof or a guarded write whose program depends on
    /// at least one persistent rule. Counted once per read, however many
    /// rules its dependency closure holds.
    pub fn record_rule_evaluation(&self) {
        self.rule_evaluations.fetch_add(1, Ordering::Relaxed);
    }

    /// Time spent applying commits to views.
    pub fn record_view_maintenance(&self, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.view_maintenance_us.fetch_add(us, Ordering::Relaxed);
    }

    /// The current values.
    pub fn counts(&self) -> ViewCounts {
        ViewCounts {
            view_reads: self.view_reads.load(Ordering::Relaxed),
            rule_evaluations: self.rule_evaluations.load(Ordering::Relaxed),
            view_maintenance_us: self.view_maintenance_us.load(Ordering::Relaxed),
        }
    }

    /// The counters in Prometheus text exposition format.
    pub fn format_prometheus(&self) -> String {
        let counts = self.counts();
        let mut out = String::with_capacity(512);
        for (name, help, value) in [
            (
                "inputlayer_view_reads_total",
                "Reads of persistent rules answered from a view, without evaluating a rule.",
                counts.view_reads,
            ),
            (
                "inputlayer_rule_evaluations_total",
                "Reads of persistent rules answered by evaluating the rules from base facts.",
                counts.rule_evaluations,
            ),
            (
                "inputlayer_view_maintenance_us_total",
                "Microseconds spent keeping views current with commits.",
                counts.view_maintenance_us,
            ),
        ] {
            let _ = writeln!(out, "# HELP {name} {help}");
            let _ = writeln!(out, "# TYPE {name} counter");
            let _ = writeln!(out, "{name} {value}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_exports_each_counter() {
        let counters = ViewCounters::new();
        counters.record_view_read();
        counters.record_rule_evaluation();
        counters.record_rule_evaluation();
        counters.record_view_maintenance(Duration::from_micros(1_500));
        assert_eq!(
            counters.counts(),
            ViewCounts {
                view_reads: 1,
                rule_evaluations: 2,
                view_maintenance_us: 1_500,
            }
        );
        let text = counters.format_prometheus();
        assert!(text.contains("# TYPE inputlayer_view_reads_total counter\n"));
        assert!(text.contains("\ninputlayer_view_reads_total 1\n"));
        assert!(text.contains("\ninputlayer_rule_evaluations_total 2\n"));
        assert!(text.contains("\ninputlayer_view_maintenance_us_total 1500\n"));
    }
}
