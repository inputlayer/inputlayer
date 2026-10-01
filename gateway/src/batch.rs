//! Per-request batch preparation that is pure (no engine): retraction
//! extraction, id renaming for incremental turns, and ledger rows.
//!
//! Retraction semantics: a `retractions` row names, in the manifest's
//! `retract_by` field, the id of an object extracted earlier in the same
//! conversation. The target is namespaced like any id, so a retraction can
//! only ever reach its own conversation. The pipeline then deletes every
//! tuple the KG ledger recorded for that id and removes the id from the
//! ledger (or, when the target was extracted in the same batch, never
//! inserts it). Rows are quote-gated before they get here, exactly like
//! claims: a retraction that cannot quote its revision marker verbatim is
//! dropped, never applied.

use crate::ledger::LedgerRow;
use crate::ontology::{Manifest, OWNER_FIELD, RETRACTIONS};
use serde_json::Value;
use std::collections::HashSet;

/// Retractions requested by an extraction, after quote validation and
/// namespacing: (namespaced target id, as the model named it).
pub fn take_retractions(
    manifest: &Manifest,
    extraction: &mut Value,
    prefix: &str,
    dropped: &mut Vec<String>,
) -> Vec<(String, String)> {
    let Some(rows) = extraction
        .as_object_mut()
        .and_then(|m| m.remove(RETRACTIONS))
        .and_then(|v| v.as_array().cloned())
    else {
        return Vec::new();
    };
    let Some(field) = manifest.extraction.retract_by.as_deref() else {
        if !rows.is_empty() {
            dropped.push(format!(
                "{RETRACTIONS}: pack declares no retract_by; {} retraction(s) ignored",
                rows.len()
            ));
        }
        return Vec::new();
    };
    let mut targets = Vec::new();
    for row in rows {
        match row.get(field).and_then(Value::as_str).map(str::trim) {
            Some(target) if !target.is_empty() && !target.chars().any(char::is_control) => {
                let namespaced = format!("{prefix}:{target}");
                if !targets.iter().any(|(t, _)| t == &namespaced) {
                    targets.push((namespaced, target.to_string()));
                }
            }
            _ => dropped.push(format!(
                "{RETRACTIONS}: {field:?} missing, empty, or not a string: {row}"
            )),
        }
    }
    targets
}

/// Rename object ids that are already taken in the conversation (or
/// repeated within the batch), so a turn can never overwrite or merge
/// into an earlier turn's object: `c_m3_1` becomes `c_m3_1_r2`. Returns
/// notes describing each rename.
pub fn dedupe_ids<S: std::hash::BuildHasher>(
    manifest: &Manifest,
    extraction: &mut Value,
    taken: &mut HashSet<String, S>,
) -> Vec<String> {
    let mut notes = Vec::new();
    for section in manifest.map.keys() {
        let Some(rows) = extraction.get_mut(section).and_then(Value::as_array_mut) else {
            continue;
        };
        for row in rows {
            let Some(Value::String(id)) = row.get_mut(OWNER_FIELD) else {
                continue;
            };
            if taken.insert(id.clone()) {
                continue;
            }
            let mut k = 2;
            let renamed = loop {
                let candidate = format!("{id}_r{k}");
                if taken.insert(candidate.clone()) {
                    break candidate;
                }
                k += 1;
            };
            notes.push(format!(
                "{section}: id {id:?} already used in this conversation; renamed {renamed:?}"
            ));
            *id = renamed;
        }
    }
    notes
}

/// Ledger rows for every mapped object with an owner id.
pub fn ledger_rows(
    manifest: &Manifest,
    extraction: &Value,
    msg_field: Option<&str>,
) -> Vec<LedgerRow> {
    let mut rows = Vec::new();
    for section in manifest.map.keys().filter(|s| s.as_str() != "ontology") {
        let Some(objects) = extraction.get(section).and_then(Value::as_array) else {
            continue;
        };
        for object in objects {
            let Some(owner) = object.get(OWNER_FIELD).and_then(Value::as_str) else {
                continue;
            };
            rows.push(LedgerRow {
                owner: owner.to_string(),
                section: section.clone(),
                msg: msg_field
                    .and_then(|f| object.get(f))
                    .and_then(Value::as_u64)
                    .unwrap_or(0),
                row: object.clone(),
            });
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn manifest(retract_by: bool) -> Manifest {
        let mut text =
            String::from("[ontology]\nname = \"t\"\nversion = \"0\"\nrules = []\n[extraction]\n");
        if retract_by {
            text.push_str("retract_by = \"target\"\n");
        }
        text.push_str("[[map.claims]]\ninsert = ['+claim[(\"{id}\")]']\n");
        toml::from_str(&text).expect("manifest")
    }

    #[test]
    fn retractions_are_taken_out_and_namespaced() {
        let mut dropped = Vec::new();
        let mut extraction = json!({
            "claims": [{"id": "c_m5_1"}],
            "retractions": [
                {"target": "c_m3_1", "kind": "claim", "msg": 5, "surface": "Actually"},
                {"target": "c_m3_1", "kind": "claim", "msg": 5, "surface": "Actually"},
                {"target": "", "kind": "claim", "msg": 5, "surface": "x"},
                {"kind": "claim", "msg": 5, "surface": "x"}
            ]
        });
        let targets = take_retractions(&manifest(true), &mut extraction, "c9", &mut dropped);
        assert_eq!(
            targets,
            vec![("c9:c_m3_1".to_string(), "c_m3_1".to_string())]
        );
        assert_eq!(dropped.len(), 2, "{dropped:?}");
        assert!(
            extraction.get(RETRACTIONS).is_none(),
            "never reaches the mapper"
        );
    }

    #[test]
    fn retractions_without_retract_by_are_dropped() {
        let mut dropped = Vec::new();
        let mut extraction = json!({"retractions": [{"target": "c1"}]});
        let targets = take_retractions(&manifest(false), &mut extraction, "c9", &mut dropped);
        assert!(targets.is_empty());
        assert_eq!(dropped.len(), 1);
    }

    #[test]
    fn taken_ids_are_renamed_apart() {
        let mut extraction =
            json!({"claims": [{"id": "c_m0_1"}, {"id": "c_m2_1"}, {"id": "c_m2_1"}]});
        let mut taken: HashSet<String> = ["c_m0_1".to_string(), "c_m0_1_r2".to_string()].into();
        let notes = dedupe_ids(&manifest(true), &mut extraction, &mut taken);
        assert_eq!(extraction["claims"][0]["id"], "c_m0_1_r3");
        assert_eq!(extraction["claims"][1]["id"], "c_m2_1");
        assert_eq!(extraction["claims"][2]["id"], "c_m2_1_r2");
        assert_eq!(notes.len(), 2);
    }

    #[test]
    fn ledger_rows_carry_owner_section_and_msg() {
        let extraction = json!({"claims": [{"id": "c9:c_m4_1", "msg": 4}, {"no_id": true}]});
        let rows = ledger_rows(&manifest(true), &extraction, Some("msg"));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].owner, "c9:c_m4_1");
        assert_eq!(rows[0].section, "claims");
        assert_eq!(rows[0].msg, 4);
    }
}
