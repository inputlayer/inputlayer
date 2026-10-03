//! The acceptance policy: which metrics are required and how much a candidate
//! may cost relative to the baseline. Loaded from `perf-gate/policy.toml`.

use std::fmt;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// `provisional` until the budgets are approved; shown on every report.
    pub status: String,
    pub note: String,
    /// Fewer rounds per arm than this make a metric invalid.
    pub min_rounds: usize,
    /// Fewer samples in a round than this make that round's p50 invalid.
    pub min_samples_p50: usize,
    /// Fewer samples in a round than this make that round's p99 invalid.
    pub min_samples_p99: usize,
    /// Two-sided confidence level of the median interval, e.g. 0.95.
    pub confidence: f64,
    pub tolerance: Tolerance,
    /// Metrics that must be present, valid and within budget.
    pub required: Vec<String>,
}

/// Largest allowed relative cost increase, e.g. 0.10 = 10% slower.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tolerance {
    pub p50: f64,
    pub p99: f64,
    pub rate: f64,
}

impl Policy {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read policy {}", path.display()))?;
        let policy: Self =
            toml::from_str(&text).with_context(|| format!("parse policy {}", path.display()))?;
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> Result<()> {
        if !(0.5..1.0).contains(&self.confidence) {
            bail!("confidence must be in [0.5, 1)");
        }
        if self.min_rounds < 2 {
            bail!("need min_rounds >= 2");
        }
        let t = self.tolerance;
        if [t.p50, t.p99, t.rate]
            .iter()
            .any(|v| !(0.0..1.0).contains(v))
        {
            bail!("tolerances must be in [0, 1)");
        }
        for key in &self.required {
            MetricKey::parse(key)?;
        }
        Ok(())
    }

    /// Allowed cost ratio above 1.0 for `key`.
    pub fn tolerance_for(&self, key: &MetricKey) -> f64 {
        match key.stat {
            Stat::P50 => self.tolerance.p50,
            Stat::P99 => self.tolerance.p99,
            Stat::Rate => self.tolerance.rate,
        }
    }
}

/// A statistic of a fixture: `fixture.series.p50|p99` or `fixture.rate`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricKey {
    pub fixture: String,
    pub name: String,
    pub stat: Stat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stat {
    P50,
    P99,
    /// Throughput: higher is better.
    Rate,
}

impl Stat {
    pub fn quantile(self) -> Option<f64> {
        match self {
            Stat::P50 => Some(0.50),
            Stat::P99 => Some(0.99),
            Stat::Rate => None,
        }
    }
}

impl MetricKey {
    pub fn parse(key: &str) -> Result<Self> {
        let parts: Vec<&str> = key.split('.').collect();
        let (fixture, name, stat) = match parts.as_slice() {
            [fixture, series, "p50"] => (fixture, series, Stat::P50),
            [fixture, series, "p99"] => (fixture, series, Stat::P99),
            [fixture, rate] => (fixture, rate, Stat::Rate),
            _ => bail!("bad metric key '{key}': want fixture.series.p50|p99 or fixture.rate"),
        };
        Ok(Self {
            fixture: (*fixture).to_string(),
            name: (*name).to_string(),
            stat,
        })
    }
}

impl fmt::Display for MetricKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.stat {
            Stat::P50 => write!(f, "{}.{}.p50", self.fixture, self.name),
            Stat::P99 => write!(f, "{}.{}.p99", self.fixture, self.name),
            Stat::Rate => write!(f, "{}.{}", self.fixture, self.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip() {
        for key in ["bound_query.latency_us.p99", "insert_single.facts_per_sec"] {
            assert_eq!(MetricKey::parse(key).unwrap().to_string(), key);
        }
        assert!(MetricKey::parse("bound_query.latency_us.p95").is_err());
        assert!(MetricKey::parse("bound_query").is_err());
    }

    #[test]
    fn repository_policy_is_valid() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("policy.toml");
        let policy = Policy::load(&path).unwrap();
        assert!(!policy.required.is_empty());
    }
}
