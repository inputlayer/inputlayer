//! Manifest-driven mapping from extraction output to IQL fact statements.
//!
//! The mapping is an allowlist: only extraction sections the manifest
//! declares produce statements, and only through its templates - one of the
//! enforcement layers keeping text-to-facts bound to the ontology.
//!
//! A template's slots become parameters (see [`crate::iql`]): the statement
//! text is the template, the extracted values travel beside it, and no value
//! is ever IQL syntax.

use crate::iql::Stmt;
use crate::ontology::{Manifest, MapRule};
use serde_json::Value;

pub struct MapOutcome {
    pub statements: Vec<Stmt>,
    /// Per statement: the `id` of the extracted object it came from (the
    /// owner a retraction targets). Same length as `statements`.
    pub owners: Vec<Option<String>>,
    /// Objects removed from the extraction because a value cannot be
    /// stored safely. The rest of the request still evaluates.
    pub dropped: Vec<String>,
    /// Extraction and manifest disagree (schema drift): evaluating the
    /// partial mapping would misreport, so the request must fail.
    pub drift: Vec<String>,
}

/// Why a template slot could not be filled.
#[derive(Debug, PartialEq)]
enum FillError {
    /// Field absent or structurally the wrong JSON type: schema drift.
    Drift,
    /// Value present but not storable (control characters, not an integer).
    Unsafe,
}

/// Escape a string for interpolation into an IQL string literal. Only for
/// what takes no parameters (a `.why` goal) and for display.
pub fn esc(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
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

/// Whether a string value is stored: no control characters. Values travel
/// as parameters, so no character can reach IQL syntax; a line break or
/// control character would still reshape the line-per-row renderings of
/// stored rows (the extraction prompt's digest), so such a row is dropped.
pub fn storable(value: &str) -> bool {
    !value.chars().any(char::is_control)
}

/// Fill the template's {field} slots from the object, each value a
/// parameter. Slots are TYPED by the template: a placeholder wrapped in
/// double quotes is a string slot (its quotes become the parameter), a bare
/// placeholder is a numeric slot and accepts only integers.
fn fill(template: &str, object: &Value, num: Option<i64>) -> Result<Stmt, FillError> {
    let mut out = Stmt::new();
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        let end = rest[start..].find('}').ok_or(FillError::Drift)? + start;
        let key = &rest[start + 1..end];
        let quoted = rest[..start].ends_with('"') && rest[end + 1..].starts_with('"');
        if quoted {
            // The quotes are the string slot's; the parameter replaces them.
            out = out.text(&rest[..start - 1]);
        } else {
            out = out.text(&rest[..start]);
        }
        if key == "num" {
            out = out.value(num.ok_or(FillError::Drift)?);
        } else if quoted {
            let text = match object.get(key).ok_or(FillError::Drift)? {
                Value::String(s) if storable(s) => s.clone(),
                Value::String(_) => return Err(FillError::Unsafe),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => return Err(FillError::Drift),
            };
            out = out.value(text);
        } else {
            let n = match object.get(key).ok_or(FillError::Drift)? {
                Value::Number(n) => n.as_i64().ok_or(FillError::Unsafe)?,
                // The pack schema carries numerics as strings, with an
                // optional unit ("2000", "2000 EUR"); the leading-integer
                // parse is the only accepted coercion.
                Value::String(s) => leading_int(s.trim()).ok_or(FillError::Unsafe)?,
                _ => return Err(FillError::Drift),
            };
            out = out.value(n);
        }
        rest = &rest[end + 1 + usize::from(quoted)..];
    }
    Ok(out.text(rest))
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
///
/// An object with a value that cannot be stored is removed from the
/// extraction (so it is never ledgered either) and reported in `dropped`.
pub fn map_extraction(manifest: &Manifest, extraction: &mut Value) -> MapOutcome {
    let mut out = MapOutcome {
        statements: Vec::new(),
        owners: Vec::new(),
        dropped: Vec::new(),
        drift: Vec::new(),
    };

    for (section, rules) in &manifest.map {
        let Some(section_value) = extraction.get_mut(section) else {
            continue;
        };
        if section == "ontology" {
            map_ontology(rules, section_value, &mut out);
            out.owners.resize(out.statements.len(), None);
            continue;
        }
        let Some(objects) = section_value.as_array_mut() else {
            out.drift.push(format!("{section}: not an array"));
            continue;
        };
        objects.retain(|object| match map_object(rules, object) {
            Ok(rendered) => {
                let owner = object
                    .get(crate::ontology::OWNER_FIELD)
                    .and_then(Value::as_str)
                    .map(str::to_string);
                out.statements.extend(rendered);
                out.owners.resize(out.statements.len(), owner);
                true
            }
            Err((FillError::Unsafe, reason)) => {
                out.dropped.push(format!("{section}: {reason}: {object}"));
                false
            }
            Err((FillError::Drift, reason)) => {
                out.drift.push(format!("{section}: {reason}"));
                true
            }
        });
    }
    out
}

/// Render every statement for one object (first matching rule, plus its
/// matching extras), all or nothing.
fn map_object(rules: &[MapRule], object: &Value) -> Result<Vec<Stmt>, (FillError, &'static str)> {
    for rule in rules {
        let num = match &rule.when {
            Some(clause) => match when_matches(clause, object) {
                Some(bound) => bound,
                None => continue, // condition not met: try the next rule
            },
            None => None,
        };
        let mut rendered = Vec::new();
        for template in &rule.insert {
            rendered.push(fill(template, object, num).map_err(|e| match e {
                FillError::Drift => (e, "object missing a mapped field"),
                FillError::Unsafe => (e, "value cannot be stored"),
            })?);
        }
        for extra in &rule.extra {
            let Some(bound) = extra.when.as_ref().and_then(|c| when_matches(c, object)) else {
                continue;
            };
            for template in &extra.insert {
                // A matched extra that cannot fill is the same class as a
                // failed main template: it never silently omits a fact.
                rendered.push(fill(template, object, bound).map_err(|e| match e {
                    FillError::Drift => (e, "extra template failed to fill"),
                    FillError::Unsafe => (e, "value cannot be stored"),
                })?);
            }
        }
        return Ok(rendered); // first matching rule wins
    }
    Err((FillError::Drift, "no mapping rule matched"))
}

fn map_ontology(rules: &[MapRule], section_value: &Value, out: &mut MapOutcome) {
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
                        out.drift.push(format!("ontology.{key}: unsupported item"));
                        continue;
                    }
                };
                match fill(template, &object, None) {
                    Ok(statement) => out.statements.push(statement),
                    Err(FillError::Unsafe) => out
                        .dropped
                        .push(format!("ontology.{key}: value cannot be stored: {item}")),
                    Err(FillError::Drift) => {
                        out.drift
                            .push(format!("ontology.{key}: item missing a field"));
                    }
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

    /// The statements with their values written as literals.
    fn shown(out: &MapOutcome) -> Vec<String> {
        out.statements.iter().map(Stmt::display).collect()
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
        let mut extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "trip", "attribute": "departure_date",
                        "value": "2026-08-14", "msg": 1, "surface": "on August 14th"}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert_eq!(
            shown(&out),
            vec![
                "+claim[(\"c1\", \"trip\", \"departure_date\", \"2026-08-14\")]",
                "+claim_source[(\"c1\", 1, \"on August 14th\")]",
                "+claim_num[(\"c1\", \"trip\", \"departure_date\", 20260814)]",
            ]
        );
        // The text is the template; the values are parameters.
        for stmt in &out.statements {
            assert!(!stmt.iql().contains('"'), "{}", stmt.iql());
        }
        assert!(out.dropped.is_empty() && out.drift.is_empty());
        assert_eq!(out.owners, vec![Some("c1".to_string()); 3]);
    }

    #[test]
    fn owners_track_each_object() {
        let m = manifest(MAP_TOML);
        let mut extraction = serde_json::json!({
            "claims": [
                {"id": "c1", "entity": "e", "attribute": "a", "value": "v", "msg": 0, "surface": "s"},
                {"id": "c2", "entity": "e", "attribute": "a", "value": "w", "msg": 1, "surface": "t"}
            ],
            "retractions": [{"target": "c1", "kind": "claim", "msg": 1, "surface": "t"}]
        });
        let out = map_extraction(&m, &mut extraction);
        // Retractions are not a mapped section: they never become inserts.
        assert_eq!(out.statements.len(), 4);
        assert_eq!(
            out.owners,
            vec![
                Some("c1".to_string()),
                Some("c1".to_string()),
                Some("c2".to_string()),
                Some("c2".to_string()),
            ]
        );
    }

    #[test]
    fn constraint_discriminator_picks_numeric_rule() {
        let m = manifest(MAP_TOML);
        let mut extraction = serde_json::json!({
            "constraints": [
                {"id": "k1", "type": "max_value", "attr": "total_price", "value": "2000"},
                {"id": "k2", "type": "forbid", "attr": "pricing", "value": ""}
            ]
        });
        let out = map_extraction(&m, &mut extraction);
        assert_eq!(
            shown(&out),
            vec![
                "+constraint_num[(\"k1\", \"max_value\", \"total_price\", 2000)]",
                "+constraint[(\"k2\", \"forbid\", \"pricing\", \"\")]",
            ]
        );
    }

    #[test]
    fn esc_encodes_control_characters() {
        assert_eq!(esc("a\nb\r\tc \"q\" \\"), r#"a\nb\r\tc \"q\" \\"#);
    }

    #[test]
    fn iql_syntax_in_a_value_is_stored_verbatim() {
        let m = manifest(MAP_TOML);
        for hostile in [
            "x\"), +evil[(\"y",
            "Option 1) Paris, option 2) Rome",
            "a [b",
            "a := b",
            "a <- b",
            "trail\\",
            "$h00 % // /*",
        ] {
            let mut extraction = serde_json::json!({
                "claims": [
                    {"id": "c1", "entity": "e", "attribute": "a", "value": "v", "msg": 0,
                     "surface": hostile},
                ]
            });
            let out = map_extraction(&m, &mut extraction);
            assert!(out.dropped.is_empty(), "{hostile:?}: {:?}", out.dropped);
            assert!(out.drift.is_empty(), "{:?}", out.drift);
            assert_eq!(out.statements.len(), 2, "{hostile:?}");
            let source = &out.statements[1];
            assert_eq!(source.iql().matches('$').count(), 3, "{}", source.iql());
            assert!(source.strings().any(|s| s == hostile), "{hostile:?}");
        }
    }

    #[test]
    fn missing_field_is_drift() {
        let m = manifest(MAP_TOML);
        let mut extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "e", "attribute": "a", "value": "v", "msg": 0}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert_eq!(out.drift.len(), 1, "{:?}", out.drift);
        assert!(out.statements.is_empty() && out.dropped.is_empty());
    }

    #[test]
    fn bare_slot_coerces_unit_bearing_bounds() {
        let m = manifest(MAP_TOML);
        let mut extraction = serde_json::json!({
            "constraints": [{"id": "k1", "type": "max_value", "attr": "total_price",
                             "value": "2000 EUR"}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert_eq!(
            shown(&out),
            vec!["+constraint_num[(\"k1\", \"max_value\", \"total_price\", 2000)]"]
        );
        assert!(out.dropped.is_empty() && out.drift.is_empty());
    }

    #[test]
    fn bare_slot_rejects_hostile_strings() {
        let m = manifest(MAP_TOML);
        // {msg} sits unquoted in the claim_source template; a string there
        // could not be contained by escaping, so the row is dropped.
        let mut extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "e", "attribute": "a", "value": "v",
                        "msg": "0)], +evil[(1", "surface": "s"}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
        assert_eq!(out.dropped.len(), 1);
        // {value} sits unquoted in the constraint_num template; only a full
        // integer parse is accepted.
        let mut extraction = serde_json::json!({
            "constraints": [{"id": "k1", "type": "max_value", "attr": "a",
                             "value": "0)], +evil[(1"}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
        // An integer beyond i64 cannot fill a bare slot either.
        let mut extraction = serde_json::json!({
            "constraints": [{"id": "k1", "type": "max_value", "attr": "a",
                             "value": "100000000000000000000"}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
        assert_eq!(out.dropped.len(), 1);
        assert!(out.drift.is_empty());
    }

    #[test]
    fn control_characters_reject_the_row() {
        let m = manifest(MAP_TOML);
        // A line break would reshape the line-per-row digest of stored rows.
        let mut extraction = serde_json::json!({
            "claims": [{"id": "c1", "entity": "e", "attribute": "a",
                        "value": "x\n+evil[(\"y\")]", "msg": 0, "surface": "s"}]
        });
        let out = map_extraction(&m, &mut extraction);
        assert!(out.statements.is_empty(), "{:?}", out.statements);
        assert_eq!(out.dropped.len(), 1);
    }

    #[test]
    fn extra_fill_failure_is_drift() {
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
        let mut extraction = serde_json::json!({
            "claims": [{"id": "c1", "attribute": "age", "value": "42"}]
        });
        let out = map_extraction(&m, &mut extraction);
        // The failed extra fails the whole object and bails the request.
        assert!(out.statements.is_empty(), "{:?}", out.statements);
        assert_eq!(out.drift.len(), 1, "{:?}", out.drift);
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
