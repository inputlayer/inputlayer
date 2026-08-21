//! Deploying an ontology pack into a knowledge graph: the shared routine
//! behind `il install` and the gateway's install endpoint.

use crate::registry::validate_component;
use crate::ws::Engine;
use anyhow::{Context, Result};

/// Deploy a pack's rules program into `kg` and pin it in `pack_meta`.
///
/// The caller supplies the assembled rules program (the pack's rules files
/// concatenated) plus the identity to pin. With `create`, an existing KG is
/// tolerated so installs stay idempotent. Returns the number of deployed
/// statements.
///
/// # Errors
///
/// Fails when the KG cannot be created/selected, when the engine reports
/// parse or per-statement failures (earlier statements may remain applied -
/// inspect the KG), or when the pack_meta pin cannot be confirmed by
/// read-back.
pub async fn deploy_pack(
    engine: &mut Engine,
    kg: &str,
    create: bool,
    name: &str,
    version: &str,
    digest: &str,
    rules_program: &str,
) -> Result<usize> {
    validate_component("ontology name", name)?;
    validate_component("version", version)?;
    let statement_count = rules_program
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("//"))
        .count();

    if create {
        // Tolerate an existing KG so install is idempotent under create.
        if let Err(err) = engine.execute(&format!(".kg create {kg}")).await {
            let msg = err.to_string().to_lowercase();
            if !msg.contains("exist") {
                return Err(err.context(format!("failed to create knowledge graph '{kg}'")));
            }
        }
    }
    engine
        .execute(&format!(".kg use {kg}"))
        .await
        .with_context(|| format!("failed to switch to knowledge graph '{kg}'"))?;

    // Parse errors reject the whole program up front; runtime failures are
    // per-statement and reported as message rows, so both paths are checked.
    let deploy = engine.execute(rules_program).await.context(
        "pack deployment failed (parse errors reject the whole program; a mid-program \
         runtime failure may leave earlier statements applied - inspect the KG)",
    )?;
    let problems = deploy.soft_errors();
    if !problems.is_empty() {
        anyhow::bail!(
            "pack deployment reported {} failed statement(s); earlier statements may \
             remain applied - inspect the KG:\n  {}",
            problems.len(),
            problems.join("\n  ")
        );
    }

    // Pin the install so findings are attributable to an exact rule set.
    // The decl may legitimately already exist; any other decl failure
    // surfaces via the read-back below.
    let _ = engine
        .execute("+pack_meta(name: string, version: string, digest: string)")
        .await;
    let pin = engine
        .execute(&format!(
            "+pack_meta[(\"{name}\", \"{version}\", \"{digest}\")]"
        ))
        .await
        .context("failed to pin pack_meta")?;
    if let Some(problem) = pin.soft_errors().first() {
        anyhow::bail!("failed to pin pack_meta: {problem}");
    }

    // Trust the read-back, not the insert's silence: the engine reports
    // some rejections as plain messages.
    let pinned = engine
        .execute("?pack_meta(Name, Version, Digest)")
        .await
        .context("failed to verify pack_meta pin")?;
    let confirmed = pinned.rows.iter().any(|row| {
        row.first().and_then(|v| v.as_str()) == Some(name)
            && row.get(1).and_then(|v| v.as_str()) == Some(version)
            && row.get(2).and_then(|v| v.as_str()) == Some(digest)
    });
    anyhow::ensure!(
        confirmed,
        "pack_meta pin not confirmed by read-back - deployment state is uncertain"
    );
    Ok(statement_count)
}
