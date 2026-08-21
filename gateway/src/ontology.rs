//! Loaded ontology entries: manifest parsing, extraction contract, and the
//! prompt/schema preparation for the model call.
//!
//! An entry is resolved from the registry (digest-verified) at startup and
//! loaded read-only. Everything the pipeline needs per request comes from
//! here: the rules program to deploy, the extraction system prompt (which
//! embeds the ontology), the normalized JSON schema for structured outputs,
//! and the mapping/validation/report specs.

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub ontology: OntologySection,
    #[serde(default)]
    pub extraction: ExtractionSection,
    #[serde(default)]
    pub validate: ValidateSection,
    #[serde(default)]
    pub map: BTreeMap<String, Vec<MapRule>>,
    #[serde(default)]
    pub report: ReportSection,
}

#[derive(Debug, Deserialize)]
pub struct OntologySection {
    pub name: String,
    pub version: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub title: String,
    pub rules: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ExtractionSection {
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    /// Per-section list of extracted fields whose values are
    /// conversation-scoped identifiers; the gateway prefixes them with the
    /// conversation id to isolate conversations inside a shared KG.
    #[serde(default)]
    pub identifier_fields: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ValidateSection {
    #[serde(default)]
    pub quote: Option<QuoteRule>,
}

#[derive(Debug, Deserialize)]
pub struct QuoteRule {
    pub field: String,
    pub within: String,
}

#[derive(Debug, Deserialize)]
pub struct MapRule {
    #[serde(default)]
    pub when: Option<String>,
    #[serde(default)]
    pub insert: Vec<String>,
    #[serde(default)]
    pub insert_by_key: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub extra: Vec<MapRule>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ReportSection {
    #[serde(default)]
    pub watch: Vec<WatchSpec>,
}

#[derive(Debug, Deserialize)]
pub struct WatchSpec {
    pub view: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub spans: Vec<Vec<String>>,
    #[serde(default)]
    pub symmetric_dedup: bool,
    /// Rows from a blocking view refuse completions in enforce mode; the
    /// pack declares its enforcement semantics (D4).
    #[serde(default)]
    pub blocking: bool,
    /// Proof goal template ({Col} placeholders); instantiated per finding
    /// and run through the engine's `.why` for the events stream.
    #[serde(default)]
    pub proof: Option<String>,
}

/// A fully loaded, request-ready ontology entry.
pub struct LoadedOntology {
    pub name: String,
    pub version: String,
    pub digest: String,
    pub manifest: Manifest,
    /// The whole rules program, deployed atomically per verify KG.
    pub rules_program: String,
    /// The extraction system prompt with per-call slots still unfilled.
    pub prompt_template: String,
    /// Structured-outputs schema, normalized (additionalProperties: false).
    pub schema: serde_json::Value,
    pub extraction_model: String,
}

impl LoadedOntology {
    pub fn load(entry_dir: &Path, digest: &str) -> Result<Self> {
        let manifest_text = std::fs::read_to_string(entry_dir.join("ontology.toml"))
            .context("cannot read ontology.toml")?;
        let manifest: Manifest = toml::from_str(&manifest_text).context("invalid ontology.toml")?;

        let mut rules_program = String::new();
        for rel in &manifest.ontology.rules {
            let text = std::fs::read_to_string(entry_dir.join(rel))
                .with_context(|| format!("cannot read rules file {rel}"))?;
            rules_program.push_str(&text);
            rules_program.push('\n');
        }

        let prompt_rel = manifest
            .extraction
            .prompt
            .as_deref()
            .ok_or_else(|| anyhow!("entry has no [extraction].prompt - cannot extract"))?;
        let prompt_doc = std::fs::read_to_string(entry_dir.join(prompt_rel))
            .with_context(|| format!("cannot read {prompt_rel}"))?;
        let prompt_template = fenced_block(&prompt_doc)
            .ok_or_else(|| anyhow!("{prompt_rel} contains no fenced prompt block"))?;

        let schema_rel = manifest
            .extraction
            .schema
            .as_deref()
            .ok_or_else(|| anyhow!("entry has no [extraction].schema - cannot extract"))?;
        let schema_text = std::fs::read_to_string(entry_dir.join(schema_rel))
            .with_context(|| format!("cannot read {schema_rel}"))?;
        let mut schema: serde_json::Value =
            serde_json::from_str(&schema_text).context("extraction schema is not valid JSON")?;
        normalize_schema(&mut schema);

        Ok(Self {
            name: manifest.ontology.name.clone(),
            version: manifest.ontology.version.clone(),
            digest: digest.to_string(),
            extraction_model: manifest
                .extraction
                .model
                .clone()
                .unwrap_or_else(|| "claude-haiku-4-5".to_string()),
            manifest,
            rules_program,
            prompt_template,
            schema,
        })
    }

    /// Fill the prompt's per-call slots. M0 is stateless: the whole
    /// conversation is extracted in one call, so prior-state slots are empty.
    pub fn render_prompt(&self, current_date: &str) -> String {
        self.prompt_template
            .replace("{{current_date}}", current_date)
            .replace(
                "{{extract_prompt_suffix | extract_assistant_output}}",
                "Extract claims from ALL messages listed below.",
            )
            .replace("{{claims_digest}}", "(none - first extraction)")
            .replace("{{prior_messages}}", "(none)")
    }
}

/// First ``` fenced block of a markdown document (the pack convention for
/// where the actual prompt lives).
fn fenced_block(doc: &str) -> Option<String> {
    let mut parts = doc.splitn(3, "```");
    let _before = parts.next()?;
    let block = parts.next()?;
    // Strip an info string on the opening fence line.
    let block = block.split_once('\n').map_or(block, |(_, rest)| rest);
    Some(block.to_string())
}

/// Structured outputs requires `additionalProperties: false` on every object;
/// the SDKs strip unsupported constraints client-side but raw REST does not,
/// so the pack schema is normalized here. The downstream validator re-checks
/// everything, so tightening the schema can only reduce noise.
fn normalize_schema(value: &mut serde_json::Value) {
    if let Some(obj) = value.as_object_mut() {
        let is_object_schema = obj.get("type").and_then(serde_json::Value::as_str)
            == Some("object")
            || obj.contains_key("properties");
        if is_object_schema && !obj.contains_key("additionalProperties") {
            obj.insert("additionalProperties".to_string(), serde_json::json!(false));
        }
        obj.remove("$comment");
        for child in obj.values_mut() {
            normalize_schema(child);
        }
    } else if let Some(arr) = value.as_array_mut() {
        for child in arr {
            normalize_schema(child);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_adds_additional_properties_recursively() {
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": {
                "claims": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"id": {"type": "string"}}}
                }
            }
        });
        normalize_schema(&mut schema);
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
        assert_eq!(
            schema["properties"]["claims"]["items"]["additionalProperties"],
            serde_json::json!(false)
        );
    }

    #[test]
    fn fenced_block_strips_info_string() {
        let doc = "intro\n```text\nTHE PROMPT\nline 2\n```\nafter";
        assert_eq!(fenced_block(doc).expect("block"), "THE PROMPT\nline 2\n");
    }
}
