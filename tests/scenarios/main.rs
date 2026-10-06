//! Scenarios against a real `inputlayer-server` process.
//!
//! Each scenario starts an engine with its own data directory, connects
//! agents and writers over `/ws` the way a deployment does, and checks the
//! related changes together: rows, deltas, revisions, structured errors and
//! the engine's work counters. Plain `cargo test` runs the suite in debug
//! (all but `saturation`, whose timing bounds need a release engine, and the
//! quarantined scenarios listed in TESTING.md); `make e2e-reactive` runs all
//! of it in release and writes writer→agent latency samples when
//! `INPUTLAYER_REACTIVE_SAMPLES_DIR` is set.
//!
//! `INPUTLAYER_SCENARIO_VIEWS` (`recompute`, the default, or `maintained`)
//! selects how the engine answers reads of persistent rules, so CI can run the
//! suite once per mode. Until V2 (#309) defines `engine.views`, `maintained`
//! fails every scenario before an engine starts.
//!
//! Modules: `reactive` (the agent subscription path), `stream` (the
//! notification stream and snapshot handoff), `delivery` (results and deltas
//! too large for one frame), `saturation` (sessions' deltas while writers
//! overload the engine), `views` (how reads of deployed rules are answered:
//! the view work counters), `wire` (request correlation) and `harness`
//! (the testkit pieces the scenarios build on). The strategy's catalogue
//! scenarios on the shop pack, each naming its captain's table row and the
//! milestone 9 issues it gates: `lifecycle` (S1), `claims` (S7),
//! `retraction` (S9, also through the differential oracle via
//! `oracle_check`), `restart` (S12), `limits` (S14) and `tenancy` (S15), and
//! the ones that hold milestone 9's contract as expected failures: `reads`
//! (S2, S3), `consistency` (S4), `fanout` (S5), `subscribe_rules` (S6),
//! `generations` (S8), `vectors` (S10, S11), `failover` (S13), `modes`
//! (S16) and `burst` (S17), sharing the assertions in `support`. Expected failures for tracked defects use
//! `inputlayer_testkit::KnownDefect`, naming the issue that fixes them; an
//! XPASS fails the run.
//!
//! Agents use the testkit's thin `/ws` client until an SDK subscribe API
//! exists; switch them to the SDK then.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod burst;
mod claims;
mod consistency;
mod delivery;
mod failover;
mod fanout;
mod generations;
mod harness;
mod lifecycle;
mod limits;
mod modes;
mod oracle_check;
mod reactive;
mod reads;
mod restart;
mod retraction;
mod saturation;
mod stream;
mod subscribe_rules;
mod support;
mod tenancy;
mod vectors;
mod views;
mod wire;

// The differential oracle's adapters, for `oracle_check`. They name each
// other as `crate::...`, so they sit at the crate root as in their own binary.
#[path = "../differential_oracle/adapter.rs"]
mod adapter;
#[path = "../differential_oracle/engine.rs"]
#[allow(dead_code)]
mod engine;
#[path = "../differential_oracle/group.rs"]
mod group;
#[path = "../differential_oracle/minimize.rs"]
mod minimize;
#[path = "../differential_oracle/model.rs"]
#[allow(dead_code)]
mod model;
#[path = "../differential_oracle/oracle.rs"]
#[allow(dead_code)]
mod oracle;
#[path = "../differential_oracle/recompute.rs"]
#[allow(dead_code)]
mod recompute;
#[path = "../differential_oracle/reference/mod.rs"]
mod reference;
#[path = "../differential_oracle/shop.rs"]
#[allow(dead_code)]
mod shop;
#[path = "../differential_oracle/subscription.rs"]
#[allow(dead_code)]
mod subscription;

use inputlayer_testkit::{EngineBuilder, Mode};

/// Environment variable selecting the [`Mode`] every scenario runs in.
const VIEWS_ENV: &str = "INPUTLAYER_SCENARIO_VIEWS";

/// Builder for an engine running this crate's server binary in the selected
/// views mode.
fn engine() -> EngineBuilder {
    EngineBuilder::new(env!("CARGO_BIN_EXE_inputlayer-server")).views(Mode::from_env(VIEWS_ENV))
}
