//! `shop_install`: the scenario suite's shop pack (`Fixture::shop_pack` in
//! `inputlayer-testkit`, `Size::Small`) installed into a fresh knowledge
//! graph, the way every catalogue scenario starts. Each sample runs from
//! `.kg create` through connecting to the graph to the last program's reply,
//! as the testkit's `Fixture::install` does, and is checked by reading the
//! pack's anchor rows. `policy.toml` holds its p50 to an absolute ceiling of
//! 200 ms, so the scenario suite's setup cost cannot grow unnoticed.

use anyhow::{Context, Result};
use inputlayer_testkit::{Fixture as Pack, Size};

use super::{elapsed_us, expect_rows, Measurement};
use crate::profile::ShopParams;
use crate::server::RunningServer;

/// o-42's eligible items in the pack: i1 and i5.
const ANCHOR: &str = r#"?eligible("o-42", Item, Why)"#;

pub async fn install(server: &RunningServer, params: &ShopParams) -> Result<Measurement> {
    let programs = Pack::shop_pack(Size::Small).programs();
    let mut admin = server.client("default").await?;
    let mut samples = Vec::with_capacity(params.installs);
    for n in 0..params.installs {
        let kg = format!("shop_{n}");
        let (started, _) = admin
            .execute(&format!(".kg create {kg}"))
            .await
            .context("create the shop graph")?;
        let mut writer = server.client(&kg).await?;
        let mut done = started;
        for program in &programs {
            let (_, reply) = writer
                .execute(program)
                .await
                .context("install a shop pack program")?;
            done = reply.at;
        }
        samples.push(elapsed_us(started, done));
        let (_, answer) = writer.query(ANCHOR).await?;
        expect_rows(ANCHOR, answer.rows.len(), 2)?;
    }
    let mut measurement = Measurement::default();
    measurement.series("install_us", samples);
    Ok(measurement)
}
