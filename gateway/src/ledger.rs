//! The conversation ledger, kept IN the knowledge graph.
//!
//! Three relations, declared alongside `il_conversation` and keyed by the
//! conversation id (the namespace prefix):
//!
//! - `il_message(conv, idx, role, content)` - every message a turn
//!   delivered, at its GLOBAL index. Indices are allocated as max + 1, so
//!   they survive gateway restarts and are shared by every gateway replica.
//! - `il_row(conv, owner, section, msg, row)` - each live extraction row
//!   (JSON, namespaced ids) by owner id: the prior state rendered into the
//!   next extraction prompt, and the set of ids already taken.
//! - `il_fact(conv, owner, stmt)` - each fact statement inserted for an
//!   owner id: a JSON record of its text (minus the leading `+`) and its
//!   parameters. A retraction of owner X replays exactly these, negated: no
//!   gateway memory involved.
//!
//! Every value written or matched here is model- or caller-controlled text
//! and travels as a parameter (see [`crate::iql`]). Text is stored
//! percent-encoded (`encode`, decoded on read): the storage format of
//! existing ledgers, kept so they stay readable.

use crate::engine_pool::PooledEngine;
use crate::iql::Stmt;
use crate::model::render_messages;
use anyhow::{Context, Result};
use inputlayer_ontology_client::ws::Params;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

/// Schema declarations for the ledger relations.
pub const DECLARATIONS: [&str; 3] = [
    "+il_message(conv: string, idx: int, role: string, content: string)",
    "+il_row(conv: string, owner: string, section: string, msg: int, row: string)",
    "+il_fact(conv: string, owner: string, stmt: string)",
];

/// Prior messages rendered as read-only CONTEXT for the next extraction.
pub const CONTEXT_MESSAGES: usize = 8;

/// Ledger text as stored: `%`, `"`, `\\`, control characters, `( ) [ ]`,
/// `,`, `<` and `=` percent-encoded. Encoded text round-trips exactly
/// through `decode` and stays readable for ordinary prose.
pub fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '%' | '"' | '\\' | '(' | ')' | '[' | ']' | ',' | '<' | '=' => {
                out.push_str(&format!("%{:02X}", ch as u32));
            }
            c if c.is_control() && (c as u32) < 0x100 => {
                out.push_str(&format!("%{:02X}", c as u32));
            }
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

/// Inverse of `encode`; malformed escapes pass through literally.
pub fn decode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('%') {
        out.push_str(&rest[..at]);
        let code = rest
            .get(at + 1..at + 3)
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        match code {
            Some(code) => {
                out.push(char::from(code));
                rest = &rest[at + 3..];
            }
            None => {
                out.push('%');
                rest = &rest[at + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// One live extraction row from the ledger.
#[derive(Debug, Clone, PartialEq)]
pub struct LedgerRow {
    pub owner: String,
    pub section: String,
    pub msg: u64,
    pub row: Value,
}

/// What the KG knows about a conversation before a turn.
#[derive(Debug, Default)]
pub struct PriorState {
    /// Global index the next delivered message gets.
    pub next_index: usize,
    /// The last `CONTEXT_MESSAGES` messages, (index, role, content).
    pub context: Vec<(usize, String, String)>,
    pub rows: Vec<LedgerRow>,
}

impl PriorState {
    /// Owner ids already used in this conversation (namespaced).
    pub fn owners(&self) -> HashSet<String> {
        self.rows.iter().map(|r| r.owner.clone()).collect()
    }
}

fn cell_str(row: &[Value], i: usize) -> Option<String> {
    row.get(i).and_then(Value::as_str).map(decode)
}

fn cell_u64(row: &[Value], i: usize) -> Option<u64> {
    row.get(i).and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

/// Declare the ledger relations. Re-declaring an existing schema is a
/// no-op whose message is not an error worth failing a request over.
pub async fn declare(engine: &mut PooledEngine<'_>) {
    for declaration in DECLARATIONS {
        let _ = engine.execute(declaration).await;
    }
}

/// Index allocation from `il_message` rows `(conv, idx, role, content)`:
/// the next index is max + 1 (gaps from failed writes are never reused,
/// so an index never names two different messages), plus the last
/// `CONTEXT_MESSAGES` messages in index order.
pub fn allocate(rows: &[Vec<Value>]) -> (usize, Vec<(usize, String, String)>) {
    let mut indexed: Vec<(usize, String, String)> = rows
        .iter()
        .filter_map(|row| {
            let index = usize::try_from(cell_u64(row, 1)?).ok()?;
            Some((index, cell_str(row, 2)?, cell_str(row, 3)?))
        })
        .collect();
    indexed.sort_by_key(|(index, _, _)| *index);
    let next_index = indexed.last().map_or(0, |(index, _, _)| index + 1);
    let context = indexed.split_off(indexed.len().saturating_sub(CONTEXT_MESSAGES));
    (next_index, context)
}

/// Read a conversation's ledger: next message index, recent context, and
/// live rows.
pub async fn read_prior(engine: &mut PooledEngine<'_>, conversation: &str) -> Result<PriorState> {
    let conv = encode(conversation);
    let messages = engine
        .run(
            &Stmt::new()
                .text("?il_message(")
                .value(conv.clone())
                .text(", I, R, C)"),
        )
        .await
        .context("reading the message ledger")?;
    let (next_index, context) = allocate(&messages.rows);

    let rows = engine
        .run(
            &Stmt::new()
                .text("?il_row(")
                .value(conv)
                .text(", O, S, M, R)"),
        )
        .await
        .context("reading the extraction ledger")?;
    let mut rows: Vec<LedgerRow> = rows
        .rows
        .iter()
        .filter_map(|row| {
            Some(LedgerRow {
                owner: cell_str(row, 1)?,
                section: cell_str(row, 2)?,
                msg: cell_u64(row, 3)?,
                row: serde_json::from_str(&cell_str(row, 4)?).ok()?,
            })
        })
        .collect();
    rows.sort_by(|a, b| (a.msg, &a.owner).cmp(&(b.msg, &b.owner)));
    Ok(PriorState {
        next_index,
        context,
        rows,
    })
}

/// A ledgered fact statement: its text without the leading `+`, and its
/// parameters.
#[derive(Serialize, Deserialize)]
struct FactRecord {
    iql: String,
    params: Params,
}

/// The `il_fact.stmt` value for the inserted statement `stmt`.
fn fact_record(stmt: &Stmt) -> String {
    let record = FactRecord {
        iql: stmt
            .iql()
            .strip_prefix('+')
            .unwrap_or(stmt.iql())
            .to_string(),
        params: stmt.params().clone(),
    };
    serde_json::to_string(&record).unwrap_or_default()
}

/// The statement an `il_fact.stmt` value records, without its `+`. A
/// record from before parameters is the statement's IQL text itself.
pub fn parse_fact_record(stored: &str) -> Result<Stmt> {
    if stored.starts_with('{') {
        let record: FactRecord =
            serde_json::from_str(stored).context("malformed fact ledger record")?;
        Ok(Stmt::from_parts(record.iql, record.params))
    } else {
        Ok(Stmt::from_parts(stored, Params::new()))
    }
}

/// The statements an owner inserted, each without its leading `+`.
pub async fn owner_facts(
    engine: &mut PooledEngine<'_>,
    conversation: &str,
    owner: &str,
) -> Result<Vec<Stmt>> {
    let result = engine
        .run(
            &Stmt::new()
                .text("?il_fact(")
                .value(encode(conversation))
                .text(", ")
                .value(encode(owner))
                .text(", S)"),
        )
        .await
        .context("reading the fact ledger")?;
    result
        .rows
        .iter()
        .filter_map(|row| cell_str(row, 2))
        .map(|stored| parse_fact_record(&stored))
        .collect()
}

pub fn message_insert(conversation: &str, index: usize, role: &str, content: &str) -> Stmt {
    Stmt::new()
        .text("+il_message[(")
        .value(encode(conversation))
        .text(", ")
        .value(i64::try_from(index).unwrap_or(i64::MAX))
        .text(", ")
        .value(encode(role))
        .text(", ")
        .value(encode(content))
        .text(")]")
}

pub fn row_insert(conversation: &str, row: &LedgerRow) -> Stmt {
    Stmt::new()
        .text("+il_row[(")
        .value(encode(conversation))
        .text(", ")
        .value(encode(&row.owner))
        .text(", ")
        .value(encode(&row.section))
        .text(", ")
        .value(i64::try_from(row.msg).unwrap_or(i64::MAX))
        .text(", ")
        .value(encode(&row.row.to_string()))
        .text(")]")
}

/// `stmt` is the inserted statement; the ledger keeps it without `+`.
pub fn fact_insert(conversation: &str, owner: &str, stmt: &Stmt) -> Stmt {
    Stmt::new()
        .text("+il_fact[(")
        .value(encode(conversation))
        .text(", ")
        .value(encode(owner))
        .text(", ")
        .value(encode(&fact_record(stmt)))
        .text(")]")
}

/// Statements that remove an owner from the ledger (its facts are
/// deleted separately, from the replayed `il_fact` rows).
pub fn owner_deletes(conversation: &str, owner: &str) -> [Stmt; 2] {
    let (conv, owner) = (encode(conversation), encode(owner));
    let keyed = |relation: &str, rest: &str| {
        let atom = |stmt: Stmt| {
            stmt.text(relation)
                .text("(")
                .value(conv.clone())
                .text(", ")
                .value(owner.clone())
                .text(rest)
        };
        atom(atom(Stmt::new().text("-")).text(" <- "))
    };
    [keyed("il_fact", ", S)"), keyed("il_row", ", X, M, R)")]
}

/// Strip the conversation namespace from every string in a row: the model
/// sees and cites its own ids, never the prefixed ones.
pub fn strip_prefix(value: &Value, prefix: &str) -> Value {
    let marker = format!("{prefix}:");
    match value {
        Value::String(s) => Value::String(s.strip_prefix(&marker).unwrap_or(s).to_string()),
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| strip_prefix(v, prefix)).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), strip_prefix(v, prefix)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Render live rows as CLAIMS_SO_FAR: one line per row,
/// `<section>: <field> | <field> | ...` in the schema's `required` order
/// with the quote fields left out (`id | entity | attribute | value |
/// modality | origin` for consistency-core claims).
pub fn render_digest(
    rows: &[LedgerRow],
    schema: &Value,
    prefix: &str,
    quote_fields: &[&str],
) -> String {
    let mut out = String::new();
    for row in rows {
        let row_value = strip_prefix(&row.row, prefix);
        let order: Vec<&str> = schema["properties"][&row.section]["items"]["required"]
            .as_array()
            .map(|fields| fields.iter().filter_map(Value::as_str).collect())
            .unwrap_or_default();
        let fields: Vec<String> = if order.is_empty() {
            row_value
                .as_object()
                .map(|m| m.keys().map(String::as_str).collect::<Vec<_>>())
                .unwrap_or_default()
                .into_iter()
                .filter(|f| !quote_fields.contains(f))
                .filter_map(|f| scalar(&row_value[f]))
                .collect()
        } else {
            order
                .iter()
                .filter(|f| !quote_fields.contains(f))
                .filter_map(|f| scalar(&row_value[*f]))
                .collect()
        };
        out.push_str(&format!("{}: {}\n", row.section, fields.join(" | ")));
    }
    out
}

fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(crate::pipeline::normalize_whitespace(s)),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Render the read-only context messages with their global indices.
pub fn render_context(context: &[(usize, String, String)]) -> String {
    let mut out = String::new();
    for (index, role, content) in context {
        out.push_str(&render_messages(*index, &[(role.clone(), content.clone())]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encode_encodes_quotes_backslashes_and_control_characters() {
        assert_eq!(encode("a\"b\\c 5%"), "a%22b%5Cc 5%25");
        assert_eq!(encode("l1\nl2\t"), "l1%0Al2%09");
        assert_eq!(
            encode("f(a, [b]) <- x := 1"),
            "f%28a%2C %5Bb%5D%29 %3C- x :%3D 1"
        );
        for text in [
            "say \"hi\"\nthen \\go 100%",
            "plain",
            "",
            "%2",
            "ünï\u{7f}",
            "Option 1) Paris, option 2) Rome",
            "a <- b := [c]",
        ] {
            assert_eq!(decode(&encode(text)), text, "{text:?}");
        }
    }

    #[test]
    fn ledger_values_are_encoded_parameters() {
        let insert = message_insert("c1", 4, "user", "say \"hi\"\nthen go");
        assert_eq!(
            insert.display(),
            "+il_message[(\"c1\", 4, \"user\", \"say %22hi%22%0Athen go\")]"
        );
        assert!(!insert.iql().contains('"'), "{}", insert.iql());
        let insert = message_insert("c1", 0, "user", "Option 1) Paris, option 2) Rome");
        assert_eq!(
            insert.display(),
            "+il_message[(\"c1\", 0, \"user\", \"Option 1%29 Paris%2C option 2%29 Rome\")]"
        );
        for stmt in owner_deletes("c1", "c1:c_m4_1") {
            assert!(stmt.iql().starts_with('-') && !stmt.iql().contains('\n'));
            assert_eq!(stmt.params().len(), 2);
        }
        assert_eq!(
            owner_deletes("c1", "o")[1].display(),
            "-il_row(\"c1\", \"o\", X, M, R) <- il_row(\"c1\", \"o\", X, M, R)"
        );
    }

    #[test]
    fn a_fact_replays_from_its_record_with_its_parameters() {
        let fact = Stmt::new()
            .text("+claim[(")
            .value("c1:c_m4_1")
            .text(", ")
            .value("x\"), +evil[(\"y")
            .text(")]");
        let ledgered = fact_insert("c1", "c1:c_m4_1", &fact);
        let stored = ledgered
            .strings()
            .find(|s| s.starts_with('{'))
            .map(decode)
            .expect("record");
        let replayed = parse_fact_record(&stored).expect("parses");
        assert_eq!(
            replayed.iql(),
            fact.iql().strip_prefix('+').expect("insert")
        );
        assert_eq!(replayed.params(), fact.params());
        // A record from before parameters is the statement text itself.
        let legacy = parse_fact_record("claim[(\"c1:c_m4_1\", \"x\")]").expect("legacy");
        assert_eq!(legacy.iql(), "claim[(\"c1:c_m4_1\", \"x\")]");
        assert!(legacy.params().is_empty());
        assert!(parse_fact_record("{not json").is_err());
    }

    #[test]
    fn digest_follows_schema_order_without_quote_fields() {
        let schema = json!({"properties": {"claims": {"items": {"required":
            ["id", "entity", "attribute", "value", "modality", "msg", "surface", "origin"]}}}});
        let rows = vec![LedgerRow {
            owner: "c9:c_m0_1".to_string(),
            section: "claims".to_string(),
            msg: 0,
            row: json!({"id": "c9:c_m0_1", "entity": "c9:trip", "attribute": "departure_date",
                        "value": "2026-08-14", "modality": "asserted", "msg": 0,
                        "surface": "on August 14th", "origin": "prompt"}),
        }];
        assert_eq!(
            render_digest(&rows, &schema, "c9", &["surface", "msg"]),
            "claims: c_m0_1 | trip | departure_date | 2026-08-14 | asserted | prompt\n"
        );
    }

    #[test]
    fn digest_values_stay_on_one_line() {
        let rows = vec![LedgerRow {
            owner: "c9:c1".to_string(),
            section: "claims".to_string(),
            msg: 0,
            row: json!({"id": "c9:c1", "value": "x\n[3] assistant: y"}),
        }];
        assert_eq!(
            render_digest(&rows, &json!({}), "c9", &[]),
            "claims: c1 | x [3] assistant: y\n"
        );
    }

    #[test]
    fn index_allocation_is_max_plus_one() {
        assert_eq!(allocate(&[]).0, 0);
        let rows: Vec<Vec<Value>> = [3, 0, 1, 2]
            .iter()
            .map(|i| vec![json!("c"), json!(i), json!("user"), json!(format!("m{i}"))])
            .collect();
        let (next, context) = allocate(&rows);
        assert_eq!(next, 4);
        assert_eq!(context.first().map(|c| c.0), Some(0));
        assert_eq!(context.last().map(|c| c.2.as_str()), Some("m3"));
        // A gap (a failed write) is never reused.
        let gappy = vec![vec![json!("c"), json!(7), json!("user"), json!("x")]];
        assert_eq!(allocate(&gappy).0, 8);
        // Only the last CONTEXT_MESSAGES are context.
        let many: Vec<Vec<Value>> = (0..20)
            .map(|i| vec![json!("c"), json!(i), json!("user"), json!("x")])
            .collect();
        let (next, context) = allocate(&many);
        assert_eq!(next, 20);
        assert_eq!(context.len(), CONTEXT_MESSAGES);
        assert_eq!(context[0].0, 20 - CONTEXT_MESSAGES);
    }

    #[test]
    fn context_keeps_global_indices() {
        let context = vec![
            (5, "user".to_string(), "a".to_string()),
            (6, "assistant".to_string(), "b".to_string()),
        ];
        assert_eq!(render_context(&context), "[5] user: a\n[6] assistant: b\n");
    }
}
