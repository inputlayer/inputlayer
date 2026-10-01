//! `[optimization]` config reaches every engine the server builds.

use inputlayer::execution::TimingMode;
use inputlayer::protocol::Handler;
use inputlayer::{Config, OptimizationConfig};
use tempfile::TempDir;

const SETUP: &[&str] = &[
    "+a[(1, 2), (2, 3)]",
    "+b[(2, 5), (3, 6)]",
    "+c[(5, 7), (6, 8)]",
    "+chain(X, W) <- a(X, Y), b(Y, Z), c(Z, W)",
];

fn handler_in(dir: &TempDir, optimization: OptimizationConfig) -> Handler {
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    config.storage.performance.timing_mode = TimingMode::Detailed;
    config.optimization = optimization;
    Handler::new(inputlayer::StorageEngine::new(config).expect("storage engine"))
}

fn handler_with(optimization: OptimizationConfig) -> (Handler, TempDir) {
    let temp = TempDir::new().expect("temp dir");
    (handler_in(&temp, optimization), temp)
}

fn sip_off() -> OptimizationConfig {
    OptimizationConfig {
        enable_sip_rewriting: false,
        ..OptimizationConfig::default()
    }
}

async fn setup(handler: &Handler) {
    for stmt in SETUP {
        handler
            .query_program(None, (*stmt).to_string())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute '{stmt}': {e}"));
    }
}

/// Rule heads the engine evaluated for `?chain(X, W)`.
async fn evaluated_rules(handler: &Handler) -> Vec<String> {
    let result = handler
        .query_program(None, "?chain(X, W)".to_string())
        .await
        .expect("query");
    let mut rows: Vec<String> = result
        .rows
        .iter()
        .map(|r| format!("{:?}", r.values))
        .collect();
    rows.sort();
    assert_eq!(rows.len(), 2, "results must not depend on the optimizer");
    result
        .timing_breakdown
        .expect("detailed timing")
        .rules
        .into_iter()
        .map(|r| r.rule_head)
        .collect()
}

#[tokio::test]
async fn test_optimizer_config_sip_flag_changes_query_execution() {
    let (on, _t1) = handler_with(OptimizationConfig::default());
    let (off, _t2) = handler_with(sip_off());
    setup(&on).await;
    setup(&off).await;

    let with_sip = evaluated_rules(&on).await;
    assert!(
        with_sip.iter().any(|r| r.contains("_sip")),
        "SIP on should add semijoin rules: {with_sip:?}"
    );
    assert_eq!(evaluated_rules(&off).await, ["chain", "__query__"]);
}

#[tokio::test]
async fn test_optimizer_config_applies_to_kg_loaded_from_disk() {
    let temp = TempDir::new().expect("temp dir");
    setup(&handler_in(&temp, sip_off())).await;

    let reopened = handler_in(&temp, sip_off());
    assert_eq!(evaluated_rules(&reopened).await, ["chain", "__query__"]);
}

#[tokio::test]
async fn test_optimizer_config_sip_flag_changes_debug_plan() {
    let (on, _t1) = handler_with(OptimizationConfig::default());
    let (off, _t2) = handler_with(sip_off());
    setup(&on).await;
    setup(&off).await;

    let (plan_on, passes_on) = on
        .debug_query(None, "__q__(X, W) <- chain(X, W)".into())
        .expect("debug");
    let (plan_off, passes_off) = off
        .debug_query(None, "__q__(X, W) <- chain(X, W)".into())
        .expect("debug");
    assert!(plan_on.contains("_sip"), "{plan_on}");
    assert!(!plan_off.contains("_sip"), "{plan_off}");
    assert!(passes_on.iter().any(|p| p.starts_with("SIP")));
    assert!(!passes_off.iter().any(|p| p.starts_with("SIP")));
}
