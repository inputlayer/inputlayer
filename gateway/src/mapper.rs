//! Manifest-driven mapping from extraction output to IQL fact statements.
//!
//! The mapping is an allowlist: only extraction sections the manifest
//! declares produce statements, and only through its templates - one of the
//! enforcement layers keeping text-to-facts bound to the ontology.

use crate::ontology::{Manifest, MapRule};
use serde_json::Value;

pub struct MapOutcome {
    pub statements: Vec<String>,
    pub skipped: Vec<String>,
}

/// Escape a string for interpolation into an IQL string literal.
pub fn esc(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Integer mirror for ordered comparisons: ISO dates become YYYYMMDD, bare
/// years on date-like attributes become Jan 1, quantities keep their leading
/// integer. The engine's `<` is int-only, so ordered checks ride these.
pub fn numeric_mirror(attribute: &str, value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() >= 10
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
    {
        let y: i64 = value[..4].parse().ok()?;
        let m: i64 = value[5..7].parse().ok()?;
        let d: i64 = value[8..10].parse().ok()?;
        // An impossible date must not become an ordinal: it would sort
        // arbitrarily and fabricate (or mask) an ordering conflict.
        if y == 0 || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        return Some(y * 10_000 + m * 100 + d);
    }
    let date_like =
        attribute.ends_with("_date") || attribute == "check_in" || attribute == "check_out";
    if date_like && bytes.len() == 4 && bytes.iter().all(u8::is_ascii_digit) {
        let y: i64 = value.parse().ok()?;
        if y == 0 {
            return None;
        }
        return Some(y * 10_000 + 101);
    }
    leading_int(value)
}

/// Leading integer of a quantity string: "2600", "2600 EUR" -> 2600;
/// "150x" -> None. This is the pack convention for numeric bounds that
/// carry a unit.
fn leading_int(value: &str) -> Option<i64> {
    let digits: String = value.chars().take_while(char::is_ascii_digit).collect();
    if !digits.is_empty() {
        let rest = &value[digits.len()..];
        if rest.is_empty() || rest.starts_with(' ') {
            return digits.parse().ok();
        }
    }
    None
}

/// Substitute {field} placeholders from the object. Slots are TYPED by the
/// template: a placeholder wrapped in double quotes is a string slot (value
/// escaped), a bare placeholder is a numeric slot and accepts only JSON
/// integers - a model-controlled string in a bare slot would sit unquoted
/// inside the statement, where no escaping can contain it. Strings with
/// control characters are rejected outright: statements are newline-joined
/// into one program, so an embedded newline would split the batch. Returns
/// None when a slot cannot be filled safely (row skipped: extraction noise
/// becomes a missed finding, never a false or forged one).
fn fill(template: &str, object: &Value, num: Option<i64>) -> Option<String> {
    let mut out = String::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let end = rest[start..].find('}')? + start;
        let key = &rest[start + 1..end];
        let quoted = rest[..start].ends_with('"') && rest[end + 1..].starts_with('"');
        if key == "num" {
            out.push_str(&num?.to_string());
        } else if quoted {
            match object.get(key)? {
                Value::String(s) if !s.chars().any(char::is_control) => out.push_str(&esc(s)),
                Value::Number(n) => out.push_str(&n.to_string()),
                Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
                _ => return None,
            }
        } else {
            match object.get(key)? {
                Value::Number(n) if n.is_i64() || n.is_u64() => out.push_str(&n.to_string()),
                // The pack schema carries numerics as strings, with an
                // optional unit ("2000", "2000 EUR"); the leading-integer
                // parse is the only accepted coercion - its output is a
                // parsed i64, so nothing hostile can reach the bare slot.
                Value::String(s) => out.push_str(&leading_int(s.trim())?.to_string()),
                _ => return None,
            }
        }
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    Some(out)
}

/// Evaluate a manifest `when` clause. Supported forms:
/// `numeric_mirror(<attr_field>, <value_field>)` (binds num on success) and
/// `<field> in [..]` (membership test).
fn when_matches(clause: &str, object: &Value) -> Option<Option<i64>> {
    let clause = clause.trim();
    if let Some(args) = clause
        .strip_prefix("numeric_mirror(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let mut parts = args.split(',').map(str::trim);
        let attr_field = parts.next()?;
        let value_field = parts.next()?;
        let attr = object.get(attr_field)?.as_str()?;
        let value = object.get(value_field)?.as_str()?;
        return numeric_mirror(attr, value).map(Some);
    }
    if let Some((field, list)) = clause.split_once(" in ") {
        let field_value = object.get(field.trim())?.as_str()?;
        let list: Vec<String> = serde_json::from_str(&list.trim().replace('\'', "\"")).ok()?;
        return if list.iter().any(|item| item == field_value) {
            Some(None)
        } else {
            None
        };
    }
    None
}

/// Map one extraction document to IQL statements per the manifest.
pub fn map_extraction(manifest: &Manifest, extraction: &Value) -> MapOutcome {
    let mut statements = Vec::new();
    let mut skipped = Vec::new();

    for (section, rules) in &manifest.map {
        let Some(section_value) = extraction.get(section) else {
            continue;
        };
        if section == "ontology" {
            map_ontology(rules, section_value, &mut statements, &mut skipped);
            continue;
        }
        let Some(objects) = section_value.as_array() else {
            skipped.push(format!("{section}: not an array"));
            continue;
        };
        for object in objects {
            map_object(section, rules, object, &mut statements, &mut skipped);
        }
    }

    MapOutcome {
        statements,
        skipped,
    }
}

fn map_object(
    section: &str,
    rules: &[MapRule],
    object: &Value,
    statements: &mut Vec<String>,
    skipped: &mut Vec<String>,
) {
    for rule in rules {
        let num = match &rule.when {
            Some(clause) => match when_matches(clause, object) {
                Some(bound) => bound,
                None => continue, // condition not met: try the next rule
            },
            None => None,
        };
        let mut rendered = Vec::new();
        let mut ok = true;
        for template in &rule.insert {
            if let Some(statement) = fill(template, object, num) {
                rendered.push(statement);
            } else {
                ok = false;
                break;
            }
        }
        if !ok {
            skipped.push(format!("{section}: object missing a mapped field"));
            return;
        }
        statements.extend(rendered);
        for extra in &rule.extra {
            if let Some(clause) = &extra.when {
                if let Some(bound) = when_matches(clause, object) {
                    for template in &extra.insert {
                        if let Some(statement) = fill(template, object, bound) {
                            statements.push(statement);
                        } else {
                            // An extra whose condition matched but whose
                            // template cannot fill is the same drift class as
                            // a failed main template: it must surface (and
                            // bail the request), not silently omit a fact.
                            skipped.push(format!("{section}: extra template failed to fill"));
                        }
                    }
                }
            }
        }
        return; // first matching rule wins
    }
    skipped.push(format!("{section}: no mapping rule matched"));
}

fn map_ontology(
    rules: &[MapRule],
    section_value: &Value,
    statements: &mut Vec<String>,
    skipped: &mut Vec<String>,
) {
    for rule in rules {
        let Some(by_key) = &rule.insert_by_key else {
            continue;
        };
        for (key, template) in by_key {
            let Some(items) = section_value.get(key).and_then(Value::as_array) else {
                continue;
            };
            for item in items {
                let object = match item {
                    Value::String(s) => serde_json::json!({ "item": s }),
                    Value::Object(_) => item.clone(),
                    _ => {
                        skipped.push(format!("ontology.{key}: unsupported item"));
                        continue;
                    }
                };
                match fill(template, &object, None) {
                    Some(statement) => statements.push(statement),
                    None => skipped.push(format!("ontology.{key}: item missing a field")),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(toml_text: &str) -> Manifest {
        toml::from_str(toml_text).expect("manifest")
    }

    const MAP_TOML: &str = r#"
[ontology]
name = "t"
version = "0"
rules = []

[[map.claims]]
insert = ['+claim[("{id}", "{entity}", "{attribute}", "{value}")]',
          '+claim_source[("{id}", {msg}, "{surface}")]']
[[map.claims.extra]]
when = "numeric_mirror(attribute, value)"
insert = ['+claim_num[("{id}", "{entity}", "{attribute}", {num})]']

[[map.constraints]]
when = 'type in ["max_value", "min_value"]'
insert = ['+constraint_num[("{id}", "{type}", "{attr}", {value})]']
[[map.constraints]]
insert = ['+constraint[("{id}", "{type}", "{attr}", "{value}")]']
"#;

    #[test]
    fn maps_claims_with_numeric_mirror_extra() {
        let m = manifest(MAP_TOML);
        let extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "trip", "attribute": "departure_date",
                        "value": "2026-08-14", "msg": 1, "surface": "on August 14th"}]
        });
        let out = map_extraction(&m, &extraction);
        assert_eq!(
            out.statements,
            vec![
                "+claim[(\"c1\", \"trip\", \"departure_date\", \"2026-08-14\")]",
                "+claim_source[(\"c1\", 1, \"on August 14th\")]",
                "+claim_num[(\"c1\", \"trip\", \"departure_date\", 20260814)]",
            ]
        );
        assert!(out.skipped.is_empty());
    }

    #[test]
    fn constraint_discriminator_picks_numeric_rule() {
        let m = manifest(MAP_TOML);
        let extraction = serde_json::json!({
            "constraints": [
                {"id": "k1", "type": "max_value", "attr": "total_price", "value": "2000"},
                {"id": "k2", "type": "forbid", "attr": "pricing", "value": ""}
            ]
        });
        let out = map_extraction(&m, &extraction);
        assert_eq!(
            out.statements,
            vec![
                "+constraint_num[(\"k1\", \"max_value\", \"total_price\", 2000)]",
                "+constraint[(\"k2\", \"forbid\", \"pricing\", \"\")]",
            ]
        );
    }

    #[test]
    fn quote_injection_is_escaped() {
        let m = manifest(MAP_TOML);
        let extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "e", "attribute": "a",
                        "value": "x\"), +evil[(\"y", "msg": 0, "surface": "s"}]
        });
        let out = map_extraction(&m, &extraction);
        // The hostile text stays INSIDE the escaped literal: the unescaped
        // breakout sequence x"), must not exist, the escaped one must.
        assert!(
            out.statements[0].contains("x\\\"),"),
            "escaped: {}",
            out.statements[0]
        );
        assert!(
            !out.statements[0].contains("x\"),"),
            "breakout: {}",
            out.statements[0]
        );
        assert_eq!(out.statements.len(), 2);
    }

    #[test]
    fn bare_slot_coerces_unit_bearing_bounds() {
        let m = manifest(MAP_TOML);
        let extraction = serde_json::json!({
            "constraints": [{"id": "k1", "type": "max_value", "attr": "total_price",
                             "value": "2000 EUR"}]
        });
        let out = map_extraction(&m, &extraction);
        assert_eq!(
            out.statements,
            vec!["+constraint_num[(\"k1\", \"max_value\", \"total_price\", 2000)]"]
        );
        assert!(out.skipped.is_empty());
    }

    #[test]
    fn bare_slot_rejects_hostile_strings() {
        let m = manifest(MAP_TOML);
        // {msg} sits unquoted in the claim_source template; a string there
        // could not be contained by escaping, so the row must be skipped.
        let extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "e", "attribute": "a", "value": "v",
                        "msg": "0)], +evil[(1", "surface": "s"}]
        });
        let out = map_extraction(&m, &extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
        assert_eq!(out.skipped.len(), 1);
        // {value} sits unquoted in the constraint_num template; only a full
        // integer parse is accepted.
        let extraction = serde_json::json!({
            "constraints": [{"id": "k1", "type": "max_value", "attr": "a",
                             "value": "0)], +evil[(1"}]
        });
        let out = map_extraction(&m, &extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
    }

    #[test]
    fn control_characters_reject_the_row() {
        let m = manifest(MAP_TOML);
        // Statements are newline-joined into one program; an embedded
        // newline would split the batch even inside a quoted literal.
        let extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "e", "attribute": "a",
                        "value": "x\n+evil[(\"y\")]", "msg": 0, "surface": "s"}]
        });
        let out = map_extraction(&m, &extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
    }

    #[test]
    fn extra_fill_failure_surfaces_as_skip() {
        let m = manifest(
            r#"
[ontology]
name = "t"
version = "0"
rules = []
[[map.claims]]
insert = ['+claim[("{id}")]']
[[map.claims.extra]]
when = "numeric_mirror(attribute, value)"
insert = ['+mirror[("{nonexistent}")]']
"#,
        );
        let extraction = serde_json::json!({
            "claims": [{"id": "c1", "attribute": "age", "value": "42"}]
        });
        let out = map_extraction(&m, &extraction);
        assert_eq!(out.statements, vec!["+claim[(\"c1\")]"]);
        // The failed extra must be reported so run_verify bails, not
        // silently omitted.
        assert_eq!(out.skipped.len(), 1, "{:?}", out.skipped);
    }

    #[test]
    fn numeric_mirror_rejects_impossible_dates() {
        assert_eq!(numeric_mirror("departure_date", "2026-13-45"), None);
        assert_eq!(numeric_mirror("departure_date", "2026-00-10"), None);
        assert_eq!(numeric_mirror("departure_date", "2026-01-32"), None);
        assert_eq!(numeric_mirror("departure_date", "0000-01-01"), None);
        assert_eq!(numeric_mirror("start_date", "0000"), None);
    }

    #[test]
    fn numeric_mirror_shapes() {
        assert_eq!(
            numeric_mirror("departure_date", "2026-08-14"),
            Some(20_260_814)
        );
        assert_eq!(numeric_mirror("start_date", "2026"), Some(20_260_101));
        assert_eq!(numeric_mirror("age", "150"), Some(150));
        assert_eq!(numeric_mirror("total_price", "2600 USD"), Some(2600));
        assert_eq!(numeric_mirror("name", "gold"), None);
        assert_eq!(numeric_mirror("age", "150x"), None);
    }
}
