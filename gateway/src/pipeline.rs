//! The evaluation pipeline: validate -> prefix -> map -> store -> report.
//!
//! Conversations are prefixed namespaces inside an operator-designated KG
//! whose ontology was installed by the engine's `.ontology install` (D10):
//! the gateway NEVER deploys rules. It verifies the KG's pack_meta pin
//! matches the pack it translates with, inserts conversation-prefixed
//! tuples, and reads the pack's watch views (findings filtered to the
//! conversation) plus engine-produced proof trees for the events stream.

use crate::mapper::{map_extraction, MapOutcome};
use crate::ontology::LoadedOntology;
use anyhow::{Context, Result};
use inputlayer_ontology_client::ws::Engine;
use serde_json::{json, Value};

pub struct EngineConfig {
    pub url: String,
    pub api_key: String,
}

pub struct EvalOutcome {
    pub findings: Vec<Value>,
    pub dropped: Vec<String>,
    /// Operational problems that do not invalidate the evaluation itself
    /// (for example an incomplete one-shot retraction).
    pub notes: Vec<String>,
    /// The stored tuples with their provenance, for the translation event.
    pub tuples: Vec<Value>,
    pub trace: Option<Value>,
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

/// Prefix every identifier field the pack declares with the conversation
/// id, so conversations are isolated namespaces inside the shared KG:
/// identifiers never collide across conversations, and the pack's rules
/// (which join through identifiers) evaluate unchanged.
///
/// FAILS CLOSED: a declared identifier field that is missing, empty, or
/// not a string cannot be namespaced, and an unprefixed identifier would
/// join across conversations (an empty entity in two conversations makes
/// their claims collide and fabricates cross-conversation findings). Such
/// rows are dropped, not passed through - extraction noise becomes a
/// missed finding, never a false one.
pub fn prefix_identifiers(
    manifest: &crate::ontology::Manifest,
    extraction: &mut Value,
    prefix: &str,
) -> Vec<String> {
    let mut dropped = Vec::new();
    for (section, fields) in &manifest.extraction.identifier_fields {
        let Some(rows) = extraction.get_mut(section).and_then(Value::as_array_mut) else {
            continue;
        };
        rows.retain_mut(|row| {
            let Some(object) = row.as_object_mut() else {
                dropped.push(format!("{section}: row is not an object"));
                return false;
            };
            for field in fields {
                match object.get(field) {
                    Some(Value::String(value)) if !value.trim().is_empty() => {}
                    _ => {
                        dropped.push(format!(
                            "{section}: identifier field {field:?} missing, empty, or not a string"
                        ));
                        return false;
                    }
                }
            }
            for field in fields {
                if let Some(Value::String(value)) = object.get_mut(field) {
                    *value = format!("{prefix}:{value}");
                }
            }
            true
        });
    }
    dropped
}

/// A finding row belongs to a conversation only when EVERY column the pack
/// declares as conversation-scoped carries that conversation's prefix.
///
/// Attribution must never rest on free-text columns: values and quoted
/// surfaces are model-controlled, so a caller who picks a conversation id
/// like `http` would otherwise match any other conversation's row
/// containing a URL - leaking its spans, ids, and proof tree.
fn row_in_conversation(
    cells: &[String],
    columns: &[String],
    scope: &[String],
    prefix: &str,
) -> bool {
    if scope.is_empty() {
        return false; // undeclared scope: cannot attribute safely
    }
    let marker = format!("{prefix}:");
    scope.iter().all(|name| {
        columns
            .iter()
            .position(|c| c == name)
            .and_then(|i| cells.get(i))
            .is_some_and(|cell| cell.starts_with(&marker))
    })
}

/// Evaluate one (kg, ontology) pair for a conversation turn.
///
/// Preconditions enforced here: the KG must have the ontology installed
/// (`pack_meta` pin matching name, version, AND digest - a mismatched rule
/// set would attribute findings to rules that are not the ones deployed).
#[allow(clippy::too_many_arguments)]
pub async fn evaluate(
    engine_config: &EngineConfig,
    ontology: &LoadedOntology,
    kg: &str,
    prefix: &str,
    mut extraction: Value,
    messages: &[(String, String)],
    want_trace: bool,
    retract_after: bool,
) -> Result<EvalOutcome> {
    let mut dropped = validate_quotes(&ontology.manifest, &mut extraction, messages);
    dropped.extend(prefix_identifiers(
        &ontology.manifest,
        &mut extraction,
        prefix,
    ));
    let MapOutcome {
        statements,
        skipped,
    } = map_extraction(&ontology.manifest, &extraction);
    // A mapping skip means the extraction and the manifest disagree (schema
    // drift). Evaluating over partially mapped facts would misreport;
    // fail closed into "incomplete".
    if !skipped.is_empty() {
        anyhow::bail!(
            "extraction-to-ontology mapping failed (pack drift?): {}",
            skipped.join("; ")
        );
    }

    let mut engine = Engine::connect(&engine_config.url, &engine_config.api_key)
        .await
        .context("engine unreachable")?;
    engine
        .execute(&format!(".kg use {kg}"))
        .await
        .with_context(|| format!("knowledge graph '{kg}' not available"))?;

    ensure_pack_pinned(&mut engine, ontology, kg).await?;

    let mut notes: Vec<String> = Vec::new();
    let started = std::time::Instant::now();
    // Conversation tracking: instantiated and queryable like anything else.
    let _ = engine
        .execute("+il_conversation(id: string, ontology: string)")
        .await;
    let tracking = engine
        .execute(&format!(
            "+il_conversation[(\"{prefix}\", \"{}@{}\")]",
            ontology.name, ontology.version
        ))
        .await
        .context("recording conversation")?;
    if let Some(problem) = tracking.soft_errors().first() {
        anyhow::bail!("recording conversation failed: {problem}");
    }

    if !statements.is_empty() {
        let insert = engine
            .execute(&statements.join("\n"))
            .await
            .context("storing extracted tuples")?;
        let problems = insert.soft_errors();
        if !problems.is_empty() {
            anyhow::bail!("tuple store reported failures: {}", problems.join("; "));
        }
    }

    let findings = read_findings(&mut engine, ontology, prefix).await;

    // One-shot requests (no conversation id) leave no residue: retract the
    // stored tuples and the tracking row.
    if retract_after {
        let mut retract = String::new();
        for statement in &statements {
            if let Some(rest) = statement.strip_prefix('+') {
                retract.push_str(&format!("-{rest}\n"));
            }
        }
        retract.push_str(&format!(
            "-il_conversation(I, O) <- il_conversation(I, O), I = \"{prefix}\"\n"
        ));
        match engine.execute(&retract).await {
            Ok(result) => {
                for problem in result.soft_errors() {
                    notes.push(format!("one-shot retraction incomplete: {problem}"));
                }
            }
            Err(err) => notes.push(format!("one-shot retraction failed: {err}")),
        }
    }

    let findings = findings?;
    let engine_ms = started.elapsed().as_millis();

    // Translation provenance for the events stream: statement + the quote
    // fields of the row it came from are already inside the statement
    // literals; expose the statements verbatim.
    let tuples: Vec<Value> = statements.iter().map(|s| json!(s)).collect();
    let trace = want_trace.then(|| {
        json!({
            "extraction": extraction,
            "statements": statements,
            "kg": kg,
            "conversation": prefix,
            "engine_ms": engine_ms,
        })
    });
    Ok(EvalOutcome {
        findings,
        dropped,
        notes,
        tuples,
        trace,
    })
}

/// The KG must run exactly the rule set of the pack the conversation was
/// translated with: name, version, AND digest. A mismatch refuses rather
/// than proceeding, because findings attributed to a rule set that is not
/// the deployed one are worse than no findings.
async fn ensure_pack_pinned(
    engine: &mut Engine,
    ontology: &LoadedOntology,
    kg: &str,
) -> Result<()> {
    let pins = engine
        .execute("?pack_meta(N, V, D)")
        .await
        .with_context(|| format!("'{kg}' has no pack_meta - install the ontology first"))?;
    let pinned = pins.rows.iter().any(|row| {
        row.first().and_then(Value::as_str) == Some(ontology.name.as_str())
            && row.get(1).and_then(Value::as_str) == Some(ontology.version.as_str())
            && row.get(2).and_then(Value::as_str) == Some(ontology.digest.as_str())
    });
    anyhow::ensure!(
        pinned,
        "ontology {}@{} (digest {}) is not installed in '{kg}' - run .ontology install \
         (and ensure engine and gateway use the same registry source)",
        ontology.name,
        ontology.version,
        ontology.digest
    );
    Ok(())
}

async fn read_findings(
    engine: &mut Engine,
    ontology: &LoadedOntology,
    prefix: &str,
) -> Result<Vec<Value>> {
    let mut findings = Vec::new();
    for watch in &ontology.manifest.report.watch {
        anyhow::ensure!(
            !watch.scope.is_empty(),
            "pack {} declares no scope columns for watch view {} - conversation \
             attribution would be unsafe (pack must be 1.0.4 or newer)",
            ontology.name,
            watch.view
        );
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
            if !row_in_conversation(&cells, &columns, &watch.scope, prefix) {
                continue;
            }
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
            // The engine's explainability produces the proof; the gateway
            // only instantiates the pack's goal template and relays.
            // Cell values are model-controlled: escape them the same way
            // the mapper does before they enter the proof goal, or a value
            // containing a quote could reshape the goal into one that
            // matches a different row (and returns its proof tree).
            let proof = match &watch.proof {
                Some(template) => {
                    let mut goal = template.clone();
                    for column in &columns {
                        goal = goal.replace(
                            &format!("{{{column}}}"),
                            &crate::mapper::esc(&value_of(column)),
                        );
                    }
                    match engine.execute(&format!(".why ?{goal}")).await {
                        Ok(result) => result.proof_trees.unwrap_or(Value::Null),
                        Err(err) => json!({ "error": err.to_string() }),
                    }
                }
                None => Value::Null,
            };
            findings.push(json!({
                "view": watch.view,
                "title": title,
                "blocking": watch.blocking,
                "spans": spans,
                "row": cells,
                "proof": proof,
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

    #[test]
    fn conversation_attribution_uses_only_scope_columns() {
        let columns: Vec<String> = ["K", "C1", "S1", "C2"]
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let scope: Vec<String> = ["C1", "C2"].iter().map(|s| (*s).to_string()).collect();
        let cells = vec![
            "functional".to_string(),
            "c42:c_m0_1".to_string(),
            "see http://x".to_string(),
            "c42:c_m2_1".to_string(),
        ];
        assert!(row_in_conversation(&cells, &columns, &scope, "c42"));
        // Prefix-of-a-prefix and unrelated ids do not match.
        assert!(!row_in_conversation(&cells, &columns, &scope, "c4"));
        assert!(!row_in_conversation(&cells, &columns, &scope, "other"));
        // A conversation id crafted to match FREE TEXT ("http") must not
        // capture another conversation's row - only scope columns count.
        assert!(!row_in_conversation(&cells, &columns, &scope, "http"));
        // A row with only one scope column in the conversation is not ours.
        let mixed = vec![
            "functional".to_string(),
            "c42:c_m0_1".to_string(),
            "x".to_string(),
            "cOTHER:c_m2_1".to_string(),
        ];
        assert!(!row_in_conversation(&mixed, &columns, &scope, "c42"));
        // Undeclared scope cannot attribute at all.
        assert!(!row_in_conversation(&cells, &columns, &[], "c42"));
    }

    fn manifest(toml_text: &str) -> crate::ontology::Manifest {
        toml::from_str(toml_text).expect("manifest")
    }

    const QUOTE_TOML: &str = r#"
[ontology]
name = "t"
version = "0"
rules = []

[validate.quote]
field = "surface"
within = "msg"
"#;

    fn geneva_messages() -> Vec<(String, String)> {
        vec![("user".to_string(), "We leave on August 14th.".to_string())]
    }

    #[test]
    fn quote_gate_drops_malformed_msg_types() {
        let manifest = manifest(QUOTE_TOML);
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
        let manifest = manifest(QUOTE_TOML);
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
        let manifest = manifest(QUOTE_TOML);
        let mut extraction = json!({ "constraints": [
            {"id": "k1", "type": "max_value", "attr": "price", "value": "2000"},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages());
        assert!(dropped.is_empty());
        assert_eq!(extraction["constraints"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn identifier_prefixing_drops_unnamespaceable_rows() {
        let manifest = manifest(
            r#"
[ontology]
name = "t"
version = "0"
rules = []

[extraction]
identifier_fields = { claims = ["id", "entity"] }
"#,
        );
        // Empty, missing, and non-string identifiers cannot be namespaced:
        // an unprefixed entity would join across conversations.
        let mut extraction = json!({ "claims": [
            {"id": "ok", "entity": "trip"},
            {"id": "e1", "entity": ""},
            {"id": "e2", "entity": "   "},
            {"id": "e3"},
            {"id": 5, "entity": "trip"},
        ]});
        let dropped = prefix_identifiers(&manifest, &mut extraction, "c42");
        assert_eq!(dropped.len(), 4, "{dropped:?}");
        let claims = extraction["claims"].as_array().expect("claims");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0]["id"], "c42:ok");
        assert_eq!(claims[0]["entity"], "c42:trip");
    }

    #[test]
    fn identifier_prefixing_by_declared_fields() {
        let manifest = manifest(
            r#"
[ontology]
name = "t"
version = "0"
rules = []

[extraction]
identifier_fields = { claims = ["id", "entity"] }
"#,
        );
        let mut extraction = json!({ "claims": [
            {"id": "c1", "entity": "trip", "value": "2026-08-14", "surface": "s"},
        ], "constraints": [ {"id": "k1"} ]});
        let dropped = prefix_identifiers(&manifest, &mut extraction, "c42");
        assert!(dropped.is_empty(), "{dropped:?}");
        assert_eq!(extraction["claims"][0]["id"], "c42:c1");
        assert_eq!(extraction["claims"][0]["entity"], "c42:trip");
        // Non-identifier fields untouched; undeclared sections untouched.
        assert_eq!(extraction["claims"][0]["value"], "2026-08-14");
        assert_eq!(extraction["constraints"][0]["id"], "k1");
    }
}
