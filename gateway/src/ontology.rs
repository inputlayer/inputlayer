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
    /// Field of a `retractions` row naming the id of a previously inserted
    /// object to retract. Unset: the pack has no retractions, and any the
    /// model emits are ignored (reported as dropped).
    #[serde(default)]
    pub retract_by: Option<String>,
}

/// Extraction section carrying retractions (the prompt contract).
pub const RETRACTIONS: &str = "retractions";
/// Field holding an extracted object's own id: its tuples' owner.
pub const OWNER_FIELD: &str = "id";

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
    /// Columns of this view that hold conversation-scoped identifiers. A
    /// row belongs to a conversation only when EVERY one of these carries
    /// that conversation's prefix - free-text columns (values, quoted
    /// surfaces) are never used for attribution, because their content is
    /// model-controlled and could be made to look like another
    /// conversation's prefix. A pack that does not declare `scope` cannot
    /// be evaluated in conversation mode at all (fail closed).
    #[serde(default)]
    pub scope: Vec<String>,
}

/// A fully loaded, request-ready ontology entry.
pub struct LoadedOntology {
    pub name: String,
    pub version: String,
    pub digest: String,
    pub manifest: Manifest,
    /// The whole rules program, deployed atomically per verify KG.
    pub rules_program: String,
    /// The static head of the extraction prompt: everything before the
    /// first per-call slot. Byte-identical across calls, so it is the
    /// prompt-cached system block.
    pub prompt_static: String,
    /// The per-call tail (`{{slot}}` lines), rendered into the user turn.
    pub prompt_slots: String,
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
        let (prompt_static, prompt_slots) = split_slots(&prompt_template);

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
            prompt_static,
            prompt_slots,
            schema,
        })
    }

    /// Render the extraction prompt for one call: the static system block
    /// plus a user turn carrying the filled per-call slots. Slots are filled
    /// in one pass, so slot markers inside filled text stay literal.
    /// Messages always reach the model: a template without
    /// `{{new_messages_with_indices}}` gets them appended.
    pub fn render_prompt(&self, slots: &PromptSlots<'_>) -> RenderedPrompt {
        let mut user = String::with_capacity(self.prompt_slots.len());
        let mut delivered = false;
        let mut rest = self.prompt_slots.as_str();
        while let Some(start) = rest.find("{{") {
            let Some(len) = rest[start..].find("}}") else {
                break;
            };
            let name = &rest[start + 2..start + len];
            let value = match name {
                "current_date" => slots.current_date,
                "extract_prompt_suffix | extract_assistant_output" => {
                    "Extract from every message listed under MESSAGES_TO_EXTRACT."
                }
                "claims_digest" => or_none(slots.claims_digest),
                "prior_messages" => or_none(slots.prior_messages),
                "new_messages_with_indices" => {
                    delivered = true;
                    slots.new_messages
                }
                _ => &rest[start..start + len + 2],
            };
            user.push_str(&rest[..start]);
            user.push_str(value);
            rest = &rest[start + len + 2..];
        }
        user.push_str(rest);
        if !delivered {
            if !user.is_empty() && !user.ends_with('\n') {
                user.push('\n');
            }
            user.push_str("MESSAGES_TO_EXTRACT:\n");
            user.push_str(slots.new_messages);
        }
        RenderedPrompt {
            system: self.prompt_static.clone(),
            user,
        }
    }
}

/// Per-call inputs to the extraction prompt.
pub struct PromptSlots<'a> {
    pub current_date: &'a str,
    /// Live prior rows (retraction targets); empty on a first extraction.
    pub claims_digest: &'a str,
    /// Read-only context messages, already extracted; empty when none.
    pub prior_messages: &'a str,
    /// The messages to extract, rendered with their global indices.
    pub new_messages: &'a str,
}

pub struct RenderedPrompt {
    pub system: String,
    pub user: String,
}

fn or_none(text: &str) -> &str {
    if text.trim().is_empty() {
        "(none)"
    } else {
        text
    }
}

/// Split a prompt at the start of the line holding its first `{{slot}}`:
/// the head is static (cacheable), the tail is per-call.
fn split_slots(template: &str) -> (String, String) {
    match template.find("{{") {
        Some(at) => {
            let line_start = template[..at].rfind('\n').map_or(0, |i| i + 1);
            (
                template[..line_start].to_string(),
                template[line_start..].to_string(),
            )
        }
        None => (template.to_string(), String::new()),
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
    fn slots_split_at_first_slot_line() {
        let (head, tail) = split_slots("rules\n# slots\nDATE: {{current_date}}\nMSGS:\n{{x}}\n");
        assert_eq!(head, "rules\n# slots\n");
        assert_eq!(tail, "DATE: {{current_date}}\nMSGS:\n{{x}}\n");
        let (head, tail) = split_slots("no slots");
        assert_eq!((head.as_str(), tail.as_str()), ("no slots", ""));
    }

    fn ontology_with(prompt_slots: &str) -> LoadedOntology {
        LoadedOntology {
            name: "t".to_string(),
            version: "0".to_string(),
            digest: "d".to_string(),
            manifest: toml::from_str("[ontology]\nname = \"t\"\nversion = \"0\"\nrules = []\n")
                .expect("manifest"),
            rules_program: String::new(),
            prompt_static: "STATIC".to_string(),
            prompt_slots: prompt_slots.to_string(),
            schema: serde_json::json!({}),
            extraction_model: "m".to_string(),
        }
    }

    #[test]
    fn prompt_renders_prior_state_and_new_messages() {
        let ontology = ontology_with(
            "CURRENT_DATE: {{current_date}}\nCLAIMS_SO_FAR:\n{{claims_digest}}\n\
             CONTEXT:\n{{prior_messages}}\nMESSAGES_TO_EXTRACT:\n{{new_messages_with_indices}}\n",
        );
        let rendered = ontology.render_prompt(&PromptSlots {
            current_date: "2026-10-01",
            claims_digest: "claims: c_m0_1 | trip | departure_date | 2026-08-14",
            prior_messages: "[0] user: We fly on August 14th.",
            new_messages: "[1] user: Since we leave on the 12th...",
        });
        assert_eq!(rendered.system, "STATIC");
        assert!(rendered.user.contains("CURRENT_DATE: 2026-10-01"));
        assert!(rendered.user.contains("c_m0_1 | trip"));
        assert!(rendered.user.contains("[0] user: We fly"));
        assert!(rendered.user.contains("[1] user: Since we leave"));
        assert!(!rendered.user.contains("{{"), "{}", rendered.user);
        // First extraction: empty prior state renders as (none).
        let first = ontology.render_prompt(&PromptSlots {
            current_date: "2026-10-01",
            claims_digest: "",
            prior_messages: "",
            new_messages: "[0] user: hi",
        });
        assert!(first.user.contains("CLAIMS_SO_FAR:\n(none)"));
        assert!(first.user.contains("CONTEXT:\n(none)"));
    }

    #[test]
    fn slot_markers_in_filled_text_stay_literal() {
        let ontology = ontology_with(
            "CLAIMS_SO_FAR:\n{{claims_digest}}\nCONTEXT:\n{{prior_messages}}\n\
             MESSAGES_TO_EXTRACT:\n{{new_messages_with_indices}}\n{{unknown}}",
        );
        let rendered = ontology.render_prompt(&PromptSlots {
            current_date: "d",
            claims_digest: "claims: c1 | {{prior_messages}}",
            prior_messages: "[0] user: see {{new_messages_with_indices}}",
            new_messages: "[1] user: {{claims_digest}}",
        });
        assert_eq!(
            rendered.user,
            "CLAIMS_SO_FAR:\nclaims: c1 | {{prior_messages}}\n\
             CONTEXT:\n[0] user: see {{new_messages_with_indices}}\n\
             MESSAGES_TO_EXTRACT:\n[1] user: {{claims_digest}}\n{{unknown}}"
        );
    }

    #[test]
    fn prompt_without_message_slot_still_delivers_messages() {
        let ontology = ontology_with("");
        let rendered = ontology.render_prompt(&PromptSlots {
            current_date: "d",
            claims_digest: "",
            prior_messages: "",
            new_messages: "[0] user: hi",
        });
        assert_eq!(rendered.user, "MESSAGES_TO_EXTRACT:\n[0] user: hi");
    }

    #[test]
    fn fenced_block_strips_info_string() {
        let doc = "intro\n```text\nTHE PROMPT\nline 2\n```\nafter";
        assert_eq!(fenced_block(doc).expect("block"), "THE PROMPT\nline 2\n");
    }
}
