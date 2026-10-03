//! Per-stage query timing, reported in `result.timing_breakdown`.
//!
//! All times are in microseconds (us).

use serde::{Deserialize, Serialize};

/// Per-stage timing breakdown for a query execution.
///
/// All times are in microseconds (us).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// Timing information for a single rule execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptimizerTiming {
    /// Number of optimization iterations before fixpoint
    pub iterations: u32,
    /// Total time for iterative rule application (us)
    pub rules_us: u64,
    /// Time for final logic fusion passes (us)
    pub fusion_us: u64,
}

/// Detailed IR builder timing (only in Detailed mode).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
