//! Runs every fixture for both arms in interleaved rounds.
//!
//! Each (round, fixture, arm) gets a fresh server process. The arm order
//! alternates per round and per fixture, so slow drift on the host (another
//! build, thermal state) lands on both arms instead of biasing one.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;

use crate::environment;
use crate::fixtures::{Fixture, Measurement};
use crate::profile::Profile;
use crate::schema::{Arm, Environment, FixtureRun, RunRecord, Workload, SCHEMA};
use crate::server::{recorded_overrides, RunningServer, ServerSpec};
use crate::stats::percentile;

/// Upper bound for a single fixture run, startup included.
const FIXTURE_TIMEOUT: Duration = Duration::from_secs(600);

/// One side of the comparison.
pub struct ArmSpec {
    pub arm: Arm,
    pub server: ServerSpec,
}

pub struct Plan {
    pub profile: Profile,
    pub rounds: u32,
    pub fixtures: Vec<Fixture>,
    /// Exactly two arms: baseline first, candidate second.
    pub arms: [ArmSpec; 2],
    pub data_root: PathBuf,
}

/// Execute `plan`; fixture failures are recorded, not raised.
pub async fn run(plan: &Plan, mut environment: Environment) -> Result<RunRecord> {
    let mut runs = Vec::new();
    for round in 0..plan.rounds {
        for (index, fixture) in plan.fixtures.iter().enumerate() {
            let flip = (round as usize + index) % 2 == 1;
            let order: [&ArmSpec; 2] = if flip {
                [&plan.arms[1], &plan.arms[0]]
            } else {
                [&plan.arms[0], &plan.arms[1]]
            };
            for spec in order {
                let run = run_one(plan, spec, round, *fixture).await;
                eprintln!("{}", progress_line(plan.rounds, &run));
                runs.push(run);
            }
        }
    }
    environment.loadavg_end = environment::loadavg();
    Ok(RunRecord {
        schema: SCHEMA.to_string(),
        created_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs()),
        environment,
        workload: Workload {
            profile: plan.profile.name.to_string(),
            rounds: plan.rounds,
            fixtures: plan.fixtures.iter().map(|f| f.name().to_string()).collect(),
            server_overrides: recorded_overrides(),
            parameters: serde_json::to_value(&plan.profile)?,
        },
        arms: plan.arms.iter().map(|spec| spec.arm.clone()).collect(),
        runs,
    })
}

async fn run_one(plan: &Plan, spec: &ArmSpec, round: u32, fixture: Fixture) -> FixtureRun {
    let mut run = FixtureRun {
        arm: spec.arm.name.clone(),
        round,
        fixture: fixture.name().to_string(),
        ..FixtureRun::default()
    };
    let name = format!("r{round}-{}-{}", fixture.name(), spec.arm.name);
    let outcome = tokio::time::timeout(FIXTURE_TIMEOUT, async {
        let server = RunningServer::start(&spec.server, &plan.data_root, &name).await?;
        fixture.run(&server, &plan.profile).await
    })
    .await
    .unwrap_or_else(|_| Err(anyhow::anyhow!("timed out after {FIXTURE_TIMEOUT:?}")));
    match outcome {
        Ok(Measurement {
            series,
            rates,
            gauges,
        }) => {
            run.series = series;
            run.rates = rates;
            run.gauges = gauges;
        }
        Err(e) => run.error = Some(format!("{e:#}")),
    }
    run
}

fn progress_line(rounds: u32, run: &FixtureRun) -> String {
    let head = format!(
        "round {}/{rounds} {:<14} {:<9}",
        run.round + 1,
        run.fixture,
        run.arm
    );
    if let Some(error) = &run.error {
        return format!("{head} ERROR {error}");
    }
    let series = run.series.iter().map(|(name, samples)| {
        let p50 = percentile(samples, 0.50).unwrap_or(0.0) / 1000.0;
        let p99 = percentile(samples, 0.99).unwrap_or(0.0) / 1000.0;
        format!("{name} p50 {p50:.2}ms p99 {p99:.2}ms")
    });
    let rates = run
        .rates
        .iter()
        .map(|(name, rate)| format!("{name} {:.0}", rate.per_sec()));
    let detail: Vec<String> = series.chain(rates).collect();
    format!("{head} {}", detail.join(" | "))
}
