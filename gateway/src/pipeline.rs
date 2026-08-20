//! The verify pipeline: validate -> map -> deploy -> insert -> report.
//!
//! Stateless M0: each request gets an ephemeral verify KG, the ontology's
//! rules deployed atomically, the extracted facts inserted, the report views
//! queried, and the KG dropped. Extraction and engine failures fail OPEN:
//! the response says `unverified` with a reason rather than failing the
//! request - a verifier outage must not take down the caller's traffic.

use crate::mapper::{map_extraction, MapOutcome};
use crate::ontology::LoadedOntology;
use anyhow::{Context, Result};
use inputlayer_ontology_client::ws::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct EngineConfig {
    pub url: String,
    pub api_key: String,
}

/// Per-process sequence keeping verify KG names unique across concurrent
/// requests (see `run_verify`).
static REQUEST_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub struct VerifyOutcome {
    pub status: &'static str,
    pub findings: Vec<Value>,
    pub dropped: Vec<String>,
}

/// Drop extraction rows whose quote is not verbatim in the message it cites.
/// Extraction noise becomes a MISSED finding, never a false one.
pub fn validate_quotes(
    manifest: &crate::ontology::Manifest,
    extraction: &mut Value,
    messages: &[(String, String)],
) -> Vec<String> {
    let Some(rule) = &manifest.validate.quote else {
        return Vec::new();
    };
    let mut dropped = Vec::new();
    if let Some(map) = extraction.as_object_mut() {
        for (section, value) in map.iter_mut() {
            let Some(rows) = value.as_array_mut() else {
                continue;
            };
            rows.retain(|row| {
                let surface_value = row.get(&rule.field);
                let msg_value = row.get(&rule.within);
                if surface_value.is_none() && msg_value.is_none() {
                    return true; // rows without quote fields are not quote-gated
                }
                // A row that carries quote fields must carry them WELL-TYPED:
                // a stringly msg or a non-string surface would otherwise skip
                // validation entirely while the mapper still inserts the row.
                let (Some(surface), Some(msg)) = (
                    surface_value.and_then(Value::as_str),
                    msg_value.and_then(Value::as_u64),
                ) else {
                    dropped.push(format!("{section}: malformed quote fields: {row}"));
                    return false;
                };
                // An empty surface is verbatim in everything; it proves
                // nothing and must not pass as a quoted span.
                #[allow(clippy::cast_possible_truncation)]
                let ok = !surface.trim().is_empty()
                    && messages
                        .get(msg as usize)
                        .is_some_and(|(_, content)| content.contains(surface));
                if !ok {
                    dropped.push(format!(
                        "{section}: quote not verbatim in message {msg}: {surface:?}"
                    ));
                }
                ok
            });
        }
    }
    dropped
}

pub async fn run_verify(
    engine_config: &EngineConfig,
    ontology: &LoadedOntology,
    mut extraction: Value,
    messages: &[(String, String)],
) -> Result<VerifyOutcome> {
    let dropped = validate_quotes(&ontology.manifest, &mut extraction, messages);
    let MapOutcome {
        statements,
        skipped,
    } = map_extraction(&ontology.manifest, &extraction);
    // A mapping skip means the extraction and the manifest disagree (schema
    // drift, a field the templates expect but the schema cannot produce).
    // Silently verifying over partially mapped facts would be a false
    // "verified" - exactly what this product must never emit. Fail open.
    if !skipped.is_empty() {
        anyhow::bail!(
            "extraction-to-ontology mapping failed (pack drift?): {}",
            skipped.join("; ")
        );
    }

    // The KG name must be unique PER REQUEST, not per content: two
    // concurrent identical requests sharing a name would race each other's
    // create/insert/drop, and the loser's empty queries would read as a
    // false "verified". The content hash stays for log correlation only.
    let seq = REQUEST_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut hasher = Sha256::new();
    hasher.update(ontology.name.as_bytes());
    hasher.update(serde_json::to_vec(messages).unwrap_or_default());
    let digest = hasher.finalize();
    let kg = format!(
        "_verify_{}_{}_{seq}",
        hex::encode(&digest[..6]),
        std::process::id()
    );

    let mut engine = Engine::connect(&engine_config.url, &engine_config.api_key)
        .await
        .context("engine unreachable")?;

    // Ephemeral KG lifecycle; cleanup is best-effort on every exit path.
    let result = verify_in_kg(&mut engine, &kg, ontology, &statements).await;
    let _ = engine.execute(".kg use default").await;
    let _ = engine.execute(&format!(".kg drop {kg}")).await;

    let findings = result?;
    Ok(VerifyOutcome {
        status: if findings.is_empty() {
            "verified"
        } else {
            "conflicts_found"
        },
        findings,
        dropped,
    })
}

async fn verify_in_kg(
    engine: &mut Engine,
    kg: &str,
    ontology: &LoadedOntology,
    statements: &[String],
) -> Result<Vec<Value>> {
    // The name is unique per request, so an existing KG can only be debris
    // from a crashed earlier process; its stale facts must not contaminate
    // this run - drop it and start clean.
    if let Err(err) = engine.execute(&format!(".kg create {kg}")).await {
        let msg = err.to_string().to_lowercase();
        if !msg.contains("exist") {
            return Err(err.context("creating verify KG"));
        }
        engine
            .execute(&format!(".kg drop {kg}"))
            .await
            .context("dropping stale verify KG")?;
        engine
            .execute(&format!(".kg create {kg}"))
            .await
            .context("recreating verify KG")?;
    }
    engine
        .execute(&format!(".kg use {kg}"))
        .await
        .context("switching to verify KG")?;

    let deploy = engine
        .execute(&ontology.rules_program)
        .await
        .context("deploying ontology rules")?;
    let problems = deploy.soft_errors();
    if !problems.is_empty() {
        anyhow::bail!("ontology deploy reported failures: {}", problems.join("; "));
    }

    if !statements.is_empty() {
        let insert = engine
            .execute(&statements.join("\n"))
            .await
            .context("inserting extracted facts")?;
        // A rejected insert may be exactly the claim that would have
        // conflicted; reporting "verified" over a partial fact base would be
        // a false verification. Bail so the caller answers "unverified".
        let problems = insert.soft_errors();
        if !problems.is_empty() {
            anyhow::bail!("fact insert reported failures: {}", problems.join("; "));
        }
    }

    let mut findings = Vec::new();
    for watch in &ontology.manifest.report.watch {
        let result = engine
            .execute(&format!("?{}", watch.view))
            .await
            .with_context(|| format!("querying {}", watch.view))?;
        let columns = column_names(&watch.view);
        let mut seen = std::collections::HashSet::new();
        for row in &result.rows {
            let mut cells: Vec<String> = row
                .iter()
                .map(|cell| {
                    cell.as_str()
                        .map_or_else(|| cell.to_string(), str::to_string)
                })
                .collect();
            cells.resize(columns.len(), String::new());
            if watch.symmetric_dedup {
                let mut key: Vec<String> = cells.clone();
                key.sort();
                if !seen.insert(key.join("\u{1}")) {
                    continue;
                }
            }
            let value_of = |name: &str| {
                columns
                    .iter()
                    .position(|c| c == name)
                    .map(|i| cells[i].clone())
                    .unwrap_or_default()
            };
            let title = watch.title.as_ref().map(|template| {
                let mut rendered = template.clone();
                for column in &columns {
                    rendered = rendered.replace(&format!("{{{column}}}"), &value_of(column));
                }
                rendered
            });
            let spans: Vec<Value> = watch
                .spans
                .iter()
                .filter(|pair| pair.len() == 2)
                .map(|pair| json!({ "message": value_of(&pair[0]), "surface": value_of(&pair[1]) }))
                .collect();
            findings.push(json!({
                "view": watch.view,
                "title": title,
                "spans": spans,
                "row": cells,
            }));
        }
    }
    Ok(findings)
}

/// Column names from a view spec like `finding_src(K, Sev, C1, ...)`.
fn column_names(view: &str) -> Vec<String> {
    view.split_once('(')
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .map(|args| args.split(',').map(|a| a.trim().to_string()).collect())
        .unwrap_or_default()
}

mod hex {
    use std::fmt::Write;

    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_names_parse() {
        assert_eq!(
            column_names("finding_src(K, Sev, C1)"),
            vec!["K", "Sev", "C1"]
        );
    }

    fn quote_manifest() -> crate::ontology::Manifest {
        toml::from_str(
            r#"
[ontology]
name = "t"
version = "0"
rules = []

[validate.quote]
field = "surface"
within = "msg"
"#,
        )
        .expect("manifest")
    }

    fn geneva_messages() -> Vec<(String, String)> {
        vec![("user".to_string(), "We leave on August 14th.".to_string())]
    }

    #[test]
    fn quote_gate_drops_malformed_msg_types() {
        let manifest = quote_manifest();
        // A stringly, negative, or fractional msg must DROP the row, not
        // skip validation: the mapper would otherwise still insert it.
        let mut extraction = json!({ "claims": [
            {"id": "a", "surface": "August 14th", "msg": "0"},
            {"id": "b", "surface": "August 14th", "msg": -1},
            {"id": "c", "surface": "August 14th", "msg": 0.5},
            {"id": "d", "surface": 7, "msg": 0},
            {"id": "e", "surface": "August 14th"},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages());
        assert_eq!(dropped.len(), 5, "{dropped:?}");
        assert_eq!(extraction["claims"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn quote_gate_rejects_empty_surface() {
        let manifest = quote_manifest();
        let mut extraction = json!({ "claims": [
            {"id": "a", "surface": "", "msg": 0},
            {"id": "b", "surface": "   ", "msg": 0},
            {"id": "c", "surface": "August 14th", "msg": 0},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages());
        assert_eq!(dropped.len(), 2, "{dropped:?}");
        assert_eq!(extraction["claims"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn quote_gate_leaves_ungated_rows_alone() {
        let manifest = quote_manifest();
        let mut extraction = json!({ "constraints": [
            {"id": "k1", "type": "max_value", "attr": "price", "value": "2000"},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages());
        assert!(dropped.is_empty());
        assert_eq!(extraction["constraints"].as_array().map(Vec::len), Some(1));
    }
}
