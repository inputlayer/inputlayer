//! The evaluation pipeline: validate -> prefix -> map -> store -> report.
//!
//! Conversations are prefixed namespaces inside an operator-designated KG
//! whose ontology was installed by the engine's `.ontology install` (D10):
//! the gateway NEVER deploys rules. It verifies the KG's pack_meta pin
//! matches the pack it translates with, inserts conversation-prefixed
//! tuples, and reads the pack's watch views (findings filtered to the
//! conversation) plus engine-produced proof trees for the events stream.

use crate::batch::{dedupe_ids, ledger_rows, take_retractions};
use crate::engine_pool::{EnginePool, PooledEngine};
use crate::iql::{Program, Stmt};
use crate::ledger::{self, PriorState};
use crate::mapper::{map_extraction, MapOutcome};
use crate::ontology::{LoadedOntology, RETRACTIONS};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::collections::HashSet;

pub struct EvalOutcome {
    pub findings: Vec<Value>,
    pub dropped: Vec<String>,
    /// Operational problems that do not invalidate the evaluation itself
    /// (for example an incomplete one-shot retraction).
    pub notes: Vec<String>,
    /// Fact statements inserted (ledger rows excluded).
    pub inserted: usize,
    /// Retraction targets applied, as the model named them.
    pub retracted: Vec<String>,
    /// The stored tuples with their provenance, for the translation event.
    pub tuples: Vec<Value>,
    pub trace: Option<Value>,
}

/// Collapse every whitespace run to one space and trim: a quote that spans
/// a line break stays verbatim and becomes storable.
pub fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Drop extraction rows whose quote is not verbatim in the message it cites
/// (both whitespace-normalized; the stored surface is the normalized one).
/// Extraction noise becomes a MISSED finding, never a false one.
pub fn validate_quotes(
    manifest: &crate::ontology::Manifest,
    extraction: &mut Value,
    messages: &[(String, String)],
    first_index: usize,
) -> Vec<String> {
    let Some(rule) = &manifest.validate.quote else {
        return Vec::new();
    };
    let mut dropped = Vec::new();
    let messages: Vec<String> = messages
        .iter()
        .map(|(_, content)| normalize_whitespace(content))
        .collect();
    if let Some(map) = extraction.as_object_mut() {
        for (section, value) in map.iter_mut() {
            let Some(rows) = value.as_array_mut() else {
                continue;
            };
            rows.retain_mut(|row| {
                if let Some(Value::String(surface)) = row.get_mut(&rule.field) {
                    *surface = normalize_whitespace(surface);
                }
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
                let Some(local) = usize::try_from(msg)
                    .ok()
                    .and_then(|m| m.checked_sub(first_index))
                    .filter(|m| *m < messages.len())
                else {
                    dropped.push(format!(
                        "{section}: cites message {msg}, which is not in this request"
                    ));
                    return false;
                };
                let ok = !surface.is_empty() && messages[local].contains(surface);
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

/// How a request relates to the conversation's state in the KG.
pub enum Mode<'a> {
    /// No conversation id: synthetic prefix, every fact retracted after
    /// the findings are read. No ledger.
    OneShot,
    /// Chat with a conversation id: the request carries the whole
    /// conversation (indices from 0). Facts are ledgered so retractions
    /// replay from the KG.
    Conversation,
    /// Turns endpoint: only new messages, starting at the ledger's next
    /// global index. Messages are appended to the ledger, and ids already
    /// used in the conversation are renamed apart.
    Turn { prior: &'a PriorState },
}

pub struct EvalRequest<'a> {
    pub kg: &'a str,
    /// Conversation namespace (the conversation id, or a one-shot id).
    pub prefix: &'a str,
    /// Messages the extraction may cite; `messages[i]` is global index
    /// `first_index + i`.
    pub messages: &'a [(String, String)],
    pub first_index: usize,
    pub mode: Mode<'a>,
    pub want_trace: bool,
}

/// Checkout, verify the pack pin, and read the conversation's ledger
/// (turns: before extraction, so a KG without the pack never costs a
/// model call).
pub async fn read_prior(
    pool: &EnginePool,
    ontology: &LoadedOntology,
    kg: &str,
    conversation: &str,
) -> Result<PriorState> {
    let mut engine = pool.checkout(kg).await?;
    ensure_pack_pinned(&mut engine, ontology, kg).await?;
    ledger::declare(&mut engine).await;
    ledger::read_prior(&mut engine, conversation).await
}

/// Evaluate one (kg, ontology) pair for a request.
///
/// Order: quote gate (claims AND retractions, against the global index
/// they cite) -> retractions taken out -> ids renamed apart (turns) ->
/// namespacing -> mapping -> ONE write batch (retracted owners' replayed
/// deletes, ledger rows, facts) -> watch views.
///
/// Preconditions enforced here: the KG must have the ontology installed
/// (`pack_meta` pin matching name, version, AND digest - a mismatched rule
/// set would attribute findings to rules that are not the ones deployed).
#[allow(clippy::too_many_lines)]
pub async fn evaluate(
    pool: &EnginePool,
    ontology: &LoadedOntology,
    request: &EvalRequest<'_>,
    mut extraction: Value,
) -> Result<EvalOutcome> {
    let manifest = &ontology.manifest;
    let prefix = request.prefix;
    let kg = request.kg;
    let mut dropped = validate_quotes(
        manifest,
        &mut extraction,
        request.messages,
        request.first_index,
    );
    let targets = take_retractions(manifest, &mut extraction, prefix, &mut dropped);
    let mut notes: Vec<String> = Vec::new();
    if let Mode::Turn { prior } = &request.mode {
        let marker = format!("{prefix}:");
        let mut taken: HashSet<String> = prior
            .owners()
            .iter()
            .map(|o| o.strip_prefix(&marker).unwrap_or(o).to_string())
            .collect();
        notes.extend(dedupe_ids(manifest, &mut extraction, &mut taken));
    }
    dropped.extend(prefix_identifiers(manifest, &mut extraction, prefix));
    let MapOutcome {
        statements,
        owners,
        dropped: unstorable,
        drift,
    } = map_extraction(manifest, &mut extraction);
    // Drift means the extraction and the manifest disagree. Evaluating over
    // partially mapped facts would misreport; fail closed into "incomplete".
    if !drift.is_empty() {
        anyhow::bail!(
            "extraction-to-ontology mapping failed (pack drift?): {}",
            drift.join("; ")
        );
    }
    dropped.extend(unstorable);
    let ledgered = !matches!(request.mode, Mode::OneShot);
    let msg_field = manifest.validate.quote.as_ref().map(|q| q.within.as_str());
    let mut rows = if ledgered {
        ledger_rows(manifest, &extraction, msg_field)
    } else {
        Vec::new()
    };

    // A retraction of an object in this same batch (chat re-extracts the
    // whole conversation, so the revised claim reappears): never insert it.
    let target_ids: HashSet<&str> = targets.iter().map(|(t, _)| t.as_str()).collect();
    let mut facts: Vec<(Option<String>, Stmt)> = Vec::new();
    let mut in_batch: HashSet<String> = HashSet::new();
    for (statement, owner) in statements.into_iter().zip(owners) {
        match owner {
            Some(o) if target_ids.contains(o.as_str()) => {
                in_batch.insert(o);
            }
            owner => facts.push((owner, statement)),
        }
    }
    rows.retain(|r| !target_ids.contains(r.owner.as_str()));

    let mut engine = pool.checkout(kg).await?;
    ensure_pack_pinned(&mut engine, ontology, kg).await?;
    let started = std::time::Instant::now();
    // Conversation tracking: instantiated and queryable like anything else.
    let _ = engine
        .execute("+il_conversation(id: string, ontology: string)")
        .await;
    if ledgered {
        ledger::declare(&mut engine).await;
    }

    // Retractions against earlier requests replay the target's ledgered
    // statements, negated. Unknown targets are dropped, never guessed.
    // Every value in the program is a parameter (see `crate::iql`).
    let mut program = Program::new();
    let mut retracted: Vec<String> = Vec::new();
    for (target, named) in &targets {
        let prior = if ledgered {
            ledger::owner_facts(&mut engine, prefix, target).await?
        } else {
            Vec::new()
        };
        if prior.is_empty() && !in_batch.contains(target) {
            dropped.push(format!(
                "{RETRACTIONS}: target {named:?} is not a live object of this conversation"
            ));
            continue;
        }
        for stmt in &prior {
            program.push(&Stmt::from_parts(
                format!("-{}", stmt.iql()),
                stmt.params().clone(),
            ))?;
        }
        if ledgered {
            for stmt in ledger::owner_deletes(prefix, target) {
                program.push(&stmt)?;
            }
        }
        retracted.push(named.clone());
    }
    program.push(
        &Stmt::new()
            .text("+il_conversation[(")
            .value(ledger::encode(prefix))
            .text(", ")
            .value(ledger::encode(&format!(
                "{}@{}",
                ontology.name, ontology.version
            )))
            .text(")]"),
    )?;
    if let Mode::Turn { .. } = request.mode {
        for (offset, (role, content)) in request.messages.iter().enumerate() {
            program.push(&ledger::message_insert(
                prefix,
                request.first_index + offset,
                role,
                content,
            ))?;
        }
    }
    for row in &rows {
        program.push(&ledger::row_insert(prefix, row))?;
    }
    if ledgered {
        for (owner, statement) in &facts {
            if let Some(owner) = owner {
                program.push(&ledger::fact_insert(prefix, owner, statement))?;
            }
        }
    }
    let statements: Vec<Stmt> = facts.into_iter().map(|(_, s)| s).collect();
    for statement in &statements {
        program.push(statement)?;
    }

    let write = engine
        .run_program(&program)
        .await
        .context("storing extracted tuples")?;
    let problems = write.soft_errors();
    if !problems.is_empty() {
        anyhow::bail!("tuple store reported failures: {}", problems.join("; "));
    }

    let findings = read_findings(&mut engine, ontology, prefix, &mut notes).await;

    if matches!(request.mode, Mode::OneShot) {
        notes.extend(retract_conversation(&mut engine, prefix, &statements).await);
    }

    let findings = findings?;
    let engine_ms = started.elapsed().as_millis();

    // Translation provenance for the events stream: statement + the quote
    // fields of the row it came from; the statements as people read them,
    // values written as literals (display only, never executed).
    let shown: Vec<String> = statements.iter().map(Stmt::display).collect();
    let tuples: Vec<Value> = shown.iter().map(|s| json!(s)).collect();
    let trace = request.want_trace.then(|| {
        json!({
            "extraction": extraction,
            "statements": shown,
            "retracted": retracted,
            "kg": kg,
            "conversation": prefix,
            "first_index": request.first_index,
            "engine_ms": engine_ms,
        })
    });
    Ok(EvalOutcome {
        findings,
        dropped,
        notes,
        inserted: statements.len(),
        retracted,
        tuples,
        trace,
    })
}

/// Retract a one-shot request's facts so it leaves no residue.
///
/// ONLY statements carrying this conversation's prefix are retracted. A
/// pack may map a section to shared, ontology-level relations (seed lists
/// a KG's rules quantify over); flipping those inserts into deletes would
/// delete the pack's own seeds and silently disable detection for every
/// other conversation in the knowledge graph, permanently.
async fn retract_conversation(
    engine: &mut PooledEngine<'_>,
    prefix: &str,
    statements: &[Stmt],
) -> Vec<String> {
    let mut notes = Vec::new();
    let marker = format!("{prefix}:");
    let mut retract = Program::new();
    let mut skipped = 0usize;
    for statement in statements {
        match statement.negated() {
            Some(delete) if statement.strings().any(|s| s.starts_with(&marker)) => {
                if let Err(err) = retract.push(&delete) {
                    notes.push(format!("one-shot retraction failed: {err}"));
                    return notes;
                }
            }
            _ => skipped += 1,
        }
    }
    if skipped > 0 {
        notes.push(format!(
            "one-shot retraction skipped {skipped} statement(s) that are not \
             conversation-scoped (shared or ontology-level facts are never retracted)"
        ));
    }
    let conv = ledger::encode(prefix);
    let conversation = Stmt::new()
        .text("-il_conversation(")
        .value(conv.clone())
        .text(", O) <- il_conversation(")
        .value(conv)
        .text(", O)");
    if let Err(err) = retract.push(&conversation) {
        notes.push(format!("one-shot retraction failed: {err}"));
        return notes;
    }
    match engine.run_program(&retract).await {
        Ok(result) => {
            for problem in result.soft_errors() {
                notes.push(format!("one-shot retraction incomplete: {problem}"));
            }
        }
        Err(err) => notes.push(format!("one-shot retraction failed: {err}")),
    }
    // Trust the read-back, not the delete messages ("Deleted 0 facts" is a
    // success phrase): confirm nothing prefixed survives.
    if let Ok(check) = engine.execute("?claim(C, E, A, V)").await {
        let residue = check
            .rows
            .iter()
            .filter(|row| {
                row.first()
                    .and_then(Value::as_str)
                    .is_some_and(|c| c.starts_with(&format!("{prefix}:")))
            })
            .count();
        if residue > 0 {
            notes.push(format!(
                "one-shot retraction left {residue} claim(s) in the knowledge graph"
            ));
        }
    }
    notes
}

/// The KG must run exactly the rule set of the pack the conversation was
/// translated with: name, version, AND digest. A mismatch refuses rather
/// than proceeding, because findings attributed to a rule set that is not
/// the deployed one are worse than no findings.
async fn ensure_pack_pinned(
    engine: &mut PooledEngine<'_>,
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
    engine: &mut PooledEngine<'_>,
    ontology: &LoadedOntology,
    prefix: &str,
    notes: &mut Vec<String>,
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
        let mut filtered = 0usize;
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
                filtered += 1;
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
                        let value = value_of(column);
                        if value.chars().any(char::is_control) {
                            goal.clear();
                            break;
                        }
                        goal = goal.replace(&format!("{{{column}}}"), &crate::mapper::esc(&value));
                    }
                    if goal.is_empty() {
                        // A cell carrying control characters could split the
                        // proof query into a second statement: skip the
                        // proof, keep the finding.
                        json!({ "error": "proof skipped: unsafe cell value" })
                    } else {
                        match engine.execute(&format!(".why ?{goal}")).await {
                            Ok(result) => result.proof_trees.unwrap_or(Value::Null),
                            Err(err) => json!({ "error": err.to_string() }),
                        }
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
        // A view that returned rows of which NONE belong to this
        // conversation is worth saying out loud: it is the signature of a
        // pack whose scope columns are bound to something unprefixed, and
        // silence there would read as "nothing found".
        if filtered > 0 && !result.rows.is_empty() {
            notes.push(format!(
                "{}: {filtered} row(s) belong to other conversations",
                watch.view
            ));
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
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages(), 0);
        assert_eq!(dropped.len(), 5, "{dropped:?}");
        assert_eq!(extraction["claims"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn quote_gate_uses_global_message_indices() {
        let manifest = manifest(QUOTE_TOML);
        // This request carries messages 7 and 8 of the conversation.
        let messages = vec![
            ("user".to_string(), "We leave on the 12th.".to_string()),
            ("user".to_string(), "Actually, scratch that.".to_string()),
        ];
        let mut extraction = json!({
            "claims": [
                {"id": "a", "surface": "on the 12th", "msg": 7},
                {"id": "b", "surface": "on the 12th", "msg": 0},
                {"id": "c", "surface": "on the 12th", "msg": 8},
                {"id": "d", "surface": "x", "msg": 9},
            ],
            "retractions": [
                {"target": "a", "msg": 8, "surface": "Actually, scratch that"},
                {"target": "a", "msg": 8, "surface": "not quoted"},
            ]
        });
        let dropped = validate_quotes(&manifest, &mut extraction, &messages, 7);
        assert_eq!(dropped.len(), 4, "{dropped:?}");
        assert_eq!(extraction["claims"].as_array().map(Vec::len), Some(1));
        assert_eq!(extraction["claims"][0]["id"], "a");
        assert_eq!(extraction["retractions"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn quote_gate_rejects_empty_surface() {
        let manifest = manifest(QUOTE_TOML);
        let mut extraction = json!({ "claims": [
            {"id": "a", "surface": "", "msg": 0},
            {"id": "b", "surface": "   ", "msg": 0},
            {"id": "c", "surface": "August 14th", "msg": 0},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages(), 0);
        assert_eq!(dropped.len(), 2, "{dropped:?}");
        assert_eq!(extraction["claims"].as_array().map(Vec::len), Some(1));
    }

    #[test]
    fn quote_gate_normalizes_whitespace_in_surfaces() {
        let manifest = manifest(QUOTE_TOML);
        let messages = vec![(
            "user".to_string(),
            "Ship to 12 Main St\r\n   Springfield, please.".to_string(),
        )];
        let mut extraction = json!({ "claims": [
            {"id": "a", "surface": "12 Main St\nSpringfield", "msg": 0},
            {"id": "b", "surface": " 12  Main\tSt ", "msg": 0},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &messages, 0);
        assert!(dropped.is_empty(), "{dropped:?}");
        assert_eq!(extraction["claims"][0]["surface"], "12 Main St Springfield");
        assert_eq!(extraction["claims"][1]["surface"], "12 Main St");
    }

    #[test]
    fn quote_gate_leaves_ungated_rows_alone() {
        let manifest = manifest(QUOTE_TOML);
        let mut extraction = json!({ "constraints": [
            {"id": "k1", "type": "max_value", "attr": "price", "value": "2000"},
        ]});
        let dropped = validate_quotes(&manifest, &mut extraction, &geneva_messages(), 0);
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
