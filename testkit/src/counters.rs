//! Engine work counters read from `/metrics/prometheus`.
//!
//! Scenarios assert on what the engine did, not only on what it answered:
//! whether a read of a deployed rule evaluated it, how many view reads and
//! subscription evaluations a write caused. Every field is an `Option`: a
//! counter the running engine does not export yet reads as `None`, and an
//! assertion on it is an expected failure until the issue that adds it lands,
//! never a silent pass.

use std::collections::BTreeMap;

use crate::contract::{Checked, Violation};

/// Work counters at one scrape. Take one before and one after the step under
/// test and compare them with [`Counters::delta`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Counters {
    /// `inputlayer_queries_total`: queries executed.
    pub queries: Option<u64>,
    /// `inputlayer_rule_evaluations_total`: deployed rules evaluated to
    /// answer a read (V1 #308; 0 per read once V9 #315 serves views).
    pub rule_evaluations: Option<u64>,
    /// `inputlayer_view_reads_total`: reads served from a deployed view (V1 #308).
    pub view_reads: Option<u64>,
    /// `inputlayer_subscription_evaluations_total`: standing-query evaluations.
    pub subscription_evaluations: Option<u64>,
    /// `inputlayer_view_maintenance_us_total`: time spent maintaining views (V1 #308).
    pub view_maintenance_us: Option<u64>,
}

impl Counters {
    /// Parse a Prometheus text exposition. Unknown metrics, comments and
    /// labelled samples are ignored.
    pub fn parse(text: &str) -> Self {
        let samples = samples(text);
        let counter = |name: &str| {
            samples
                .get(&format!("inputlayer_{name}_total"))
                .or_else(|| samples.get(&format!("inputlayer_{name}")))
                .copied()
        };
        Self {
            queries: counter("queries"),
            rule_evaluations: counter("rule_evaluations"),
            view_reads: counter("view_reads"),
            subscription_evaluations: counter("subscription_evaluations"),
            view_maintenance_us: counter("view_maintenance_us"),
        }
    }

    /// `value` of counter `name`, or [`Violation::NotMeasurable`] when the
    /// engine does not export it: `Counters::require("rule_evaluations",
    /// delta.rule_evaluations)?`.
    pub fn require(name: &str, value: Option<u64>) -> Checked<u64> {
        value.ok_or_else(|| Violation::NotMeasurable(name.to_string()))
    }

    /// Work done between two scrapes, `after - before`, per counter. `None`
    /// where either scrape lacks the counter or it went backwards (an engine
    /// restart in between).
    pub fn delta(before: &Self, after: &Self) -> Self {
        let diff = |b: Option<u64>, a: Option<u64>| a?.checked_sub(b?);
        Self {
            queries: diff(before.queries, after.queries),
            rule_evaluations: diff(before.rule_evaluations, after.rule_evaluations),
            view_reads: diff(before.view_reads, after.view_reads),
            subscription_evaluations: diff(
                before.subscription_evaluations,
                after.subscription_evaluations,
            ),
            view_maintenance_us: diff(before.view_maintenance_us, after.view_maintenance_us),
        }
    }
}

/// Unlabelled whole-number samples by metric name.
fn samples(text: &str) -> BTreeMap<String, u64> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let name = parts.next()?;
            if name.contains('{') {
                return None;
            }
            // Counters are whole numbers; a gauge with a fraction is not one.
            let value = parts.next()?.parse::<u64>().ok()?;
            Some((name.to_string(), value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scrape of today's engine: no view counters yet.
    const TODAY: &str = "\
# HELP inputlayer_uptime_seconds Server uptime in seconds.
# TYPE inputlayer_uptime_seconds gauge
inputlayer_uptime_seconds 12
# HELP inputlayer_queries_total Total queries executed.
# TYPE inputlayer_queries_total counter
inputlayer_queries_total 41
inputlayer_memory_usage_bytes 1.5e6
";

    #[test]
    fn todays_engine_has_no_rule_evaluation_counter() {
        let counters = Counters::parse(TODAY);
        assert_eq!(counters.queries, Some(41));
        assert_eq!(counters.rule_evaluations, None);
        assert_eq!(counters.view_reads, None);
        assert_eq!(counters.subscription_evaluations, None);
        assert_eq!(counters.view_maintenance_us, None);
    }

    #[test]
    fn view_counters_parse_and_delta() {
        let before = Counters::parse(
            "inputlayer_queries_total 10\n\
             inputlayer_rule_evaluations_total 7\n\
             inputlayer_view_reads_total 3\n\
             inputlayer_view_maintenance_us_total 900\n\
             inputlayer_view_reads_total{kg=\"shop\"} 99\n",
        );
        let after = Counters::parse(
            "inputlayer_queries_total 60\n\
             inputlayer_rule_evaluations_total 7\n\
             inputlayer_view_reads_total 53\n\
             inputlayer_view_maintenance_us_total 1000\n\
             inputlayer_subscription_evaluations_total 2\n",
        );
        assert_eq!(before.view_reads, Some(3), "labelled samples are ignored");
        assert_eq!(
            Counters::delta(&before, &after),
            Counters {
                queries: Some(50),
                rule_evaluations: Some(0),
                view_reads: Some(50),
                subscription_evaluations: None,
                view_maintenance_us: Some(100),
            }
        );
    }

    #[test]
    fn a_missing_counter_is_not_measurable() {
        let counters = Counters::parse(TODAY);
        assert_eq!(Counters::require("queries", counters.queries), Ok(41));
        assert_eq!(
            Counters::require("rule_evaluations", counters.rule_evaluations),
            Err(Violation::NotMeasurable("rule_evaluations".to_string()))
        );
    }

    #[test]
    fn a_counter_that_went_backwards_is_not_a_delta() {
        let before = Counters::parse("inputlayer_queries_total 10\n");
        let after = Counters::parse("inputlayer_queries_total 2\n");
        assert_eq!(Counters::delta(&before, &after).queries, None);
    }
}
