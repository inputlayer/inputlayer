//! On-disk framing of WAL records.
//!
//! One record holds one [`Transaction`] on one line:
//!
//! ```text
//! <crc32 of json, 8 hex digits>:<transaction json>\n
//! ```
//!
//! The trailing newline is the commit boundary: a record without it, or whose
//! checksum does not match, is torn and holds no committed data. JSON escapes
//! newlines, so a record never contains one.
//!
//! The JSON is `{"rev":7,"ops":[{"facts":{"shard":"kg:edge","changes":[[t,1]]}}]}`,
//! where each tuple `t` is its [`codec`] encoding in base64: lossless for every
//! value, including non-finite floats, which JSON numbers cannot hold. Catalog
//! changes are `{"rule":{"kg":..,"name":..,"definition":d}}` and
//! `{"schema":{"kg":..,"relation":..,"schema":s}}`, where `d` and `s` are a rule
//! definition and a relation schema in their catalog-file JSON, or `null` when the
//! commit removes them. Unknown fields and operations are rejected, never ignored.
//!
//! Recovery replays the longest prefix of intact records, which is the state after
//! some exact number of commits. It never skips a damaged record to replay later
//! ones, since that would produce a state no commit sequence ever reached.

use super::batch::Update;
use super::codec;
use super::transaction::{CatalogEntry, Transaction, TxnOp};
use crate::rule_catalog::RuleDefinition;
use crate::schema::RelationSchema;
use crate::storage::{StorageError, StorageResult};
use crate::value::Tuple;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::ser::{SerializeMap, SerializeSeq, SerializeStruct};
use serde::{Deserialize, Serialize, Serializer};
use std::path::Path;

/// Encode `txn` as one complete record, newline included.
pub(super) fn encode(txn: &Transaction) -> StorageResult<Vec<u8>> {
    let json = serde_json::to_vec(&RecordOut(txn))
        .map_err(|e| StorageError::Other(format!("WAL serialization failed: {e}")))?;
    let mut line = Vec::with_capacity(json.len() + 10);
    line.extend_from_slice(format!("{:08x}:", crc32fast::hash(&json)).as_bytes());
    line.extend_from_slice(&json);
    line.push(b'\n');
    Ok(line)
}

/// What [`scan`] found in a WAL file.
#[derive(Debug)]
pub(super) struct Scan {
    /// Transactions of the intact prefix, in commit order.
    pub txns: Vec<Transaction>,
    /// Length of the intact prefix; bytes from here on are not replayed.
    pub valid_end: usize,
    /// Intact records after the first damaged one: committed, but not replayed.
    pub stranded: usize,
    /// Why the first damaged record is invalid, if there is one.
    pub damage: Option<String>,
    /// The damage is only an unterminated last line: a write torn by a crash. Any
    /// other damage (a complete line that fails its checksum) may hide committed data.
    pub torn_tail: bool,
}

/// Scan WAL bytes into the intact prefix of transactions.
///
/// # Errors
/// [`StorageError::WalUnreadable`] if a record's checksum matches but its content
/// does not decode: it is intact data in a foreign format, so neither replaying nor
/// discarding it is safe.
pub(super) fn scan(file: &Path, bytes: &[u8]) -> StorageResult<Scan> {
    let mut scan = Scan {
        txns: Vec::new(),
        valid_end: 0,
        stranded: 0,
        damage: None,
        torn_tail: false,
    };
    for line in lines(bytes) {
        match decode(line.content, line.terminated) {
            Ok(txn) if scan.damage.is_none() => {
                scan.txns.push(txn);
                scan.valid_end = line.end;
            }
            Ok(_) => scan.stranded += 1,
            Err(Invalid::Torn(reason)) => {
                if scan.damage.is_none() {
                    scan.damage = Some(reason);
                    scan.torn_tail = !line.terminated;
                }
            }
            Err(Invalid::Unreadable(reason)) => {
                return Err(StorageError::WalUnreadable {
                    file: file.to_path_buf(),
                    offset: line.start,
                    reason,
                })
            }
        }
    }
    Ok(scan)
}

/// One non-blank line of a WAL file.
struct Line<'a> {
    start: usize,
    /// Offset just past the line, including its newline if it has one.
    end: usize,
    content: &'a [u8],
    terminated: bool,
}

/// Split bytes into non-blank lines.
fn lines(bytes: &[u8]) -> impl Iterator<Item = Line<'_>> {
    let mut start = 0;
    std::iter::from_fn(move || {
        while start < bytes.len() {
            let line = match bytes[start..].iter().position(|&b| b == b'\n') {
                Some(n) => Line {
                    start,
                    end: start + n + 1,
                    content: &bytes[start..start + n],
                    terminated: true,
                },
                None => Line {
                    start,
                    end: bytes.len(),
                    content: &bytes[start..],
                    terminated: false,
                },
            };
            start = line.end;
            if !line.content.trim_ascii().is_empty() {
                return Some(line);
            }
        }
        None
    })
}

enum Invalid {
    /// Not a complete record: a torn write or corruption.
    Torn(String),
    /// A complete record whose content this server cannot decode.
    Unreadable(String),
}

fn decode(content: &[u8], terminated: bool) -> Result<Transaction, Invalid> {
    if !terminated {
        return Err(Invalid::Torn("record has no commit boundary".into()));
    }
    let (crc, json) = match content.split_at_checked(9) {
        Some((head, json)) if head[8] == b':' => (&head[..8], json),
        _ => return Err(Invalid::Torn("record has no checksum".into())),
    };
    let expected = std::str::from_utf8(crc)
        .ok()
        .and_then(|hex| u32::from_str_radix(hex, 16).ok())
        .ok_or_else(|| Invalid::Torn("record checksum is not hex".into()))?;
    let actual = crc32fast::hash(json);
    if actual != expected {
        return Err(Invalid::Torn(format!(
            "checksum mismatch: expected {expected:08x}, got {actual:08x}"
        )));
    }
    decode_payload(json).map_err(Invalid::Unreadable)
}

/// Serializes a borrowed transaction in the record's JSON shape.
struct RecordOut<'a>(&'a Transaction);

impl Serialize for RecordOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut record = serializer.serialize_struct("Record", 2)?;
        record.serialize_field("rev", &self.0.revision())?;
        record.serialize_field("ops", &OpsOut(self.0.ops()))?;
        record.end()
    }
}

struct OpsOut<'a>(&'a [TxnOp]);

impl Serialize for OpsOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for op in self.0 {
            match op {
                TxnOp::Facts { shard, changes } => seq.serialize_element(&Tagged(
                    "facts",
                    FactsOut {
                        shard,
                        changes: ChangesOut(changes),
                    },
                ))?,
                TxnOp::Catalog {
                    kg,
                    entry: CatalogEntry::Rule { name, definition },
                } => seq.serialize_element(&Tagged(
                    "rule",
                    RuleOut {
                        kg,
                        name,
                        definition: definition.as_ref(),
                    },
                ))?,
                TxnOp::Catalog {
                    kg,
                    entry: CatalogEntry::Schema { relation, schema },
                } => seq.serialize_element(&Tagged(
                    "schema",
                    SchemaOut {
                        kg,
                        relation,
                        schema: schema.as_ref(),
                    },
                ))?,
            }
        }
        seq.end()
    }
}

/// `{"<tag>": value}`
struct Tagged<T>(&'static str, T);

impl<T: Serialize> Serialize for Tagged<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.0, &self.1)?;
        map.end()
    }
}

#[derive(Serialize)]
struct FactsOut<'a> {
    shard: &'a str,
    changes: ChangesOut<'a>,
}

#[derive(Serialize)]
struct RuleOut<'a> {
    kg: &'a str,
    name: &'a str,
    definition: Option<&'a RuleDefinition>,
}

#[derive(Serialize)]
struct SchemaOut<'a> {
    kg: &'a str,
    relation: &'a str,
    schema: Option<&'a RelationSchema>,
}

struct ChangesOut<'a>(&'a [(Tuple, i64)]);

impl Serialize for ChangesOut<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut buf = Vec::new();
        serializer.collect_seq(self.0.iter().map(|(tuple, diff)| {
            buf.clear();
            codec::encode_tuple(tuple, &mut buf);
            (BASE64.encode(&buf), *diff)
        }))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordIn {
    rev: u64,
    ops: Vec<OpIn>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
enum OpIn {
    Facts {
        shard: String,
        changes: Vec<(String, i64)>,
    },
    Rule {
        kg: String,
        name: String,
        definition: Option<RuleDefinition>,
    },
    Schema {
        kg: String,
        relation: String,
        schema: Option<RelationSchema>,
    },
}

/// A record written before transactions existed: one update to one shard. Read so
/// a v1 data directory migrates with its WAL; never written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V1Entry {
    shard: String,
    update: Update,
}

fn decode_payload(json: &[u8]) -> Result<Transaction, String> {
    let record: RecordIn = match serde_json::from_slice(json) {
        Ok(record) => record,
        Err(e) => {
            let v1: V1Entry = serde_json::from_slice(json).map_err(|_| e.to_string())?;
            let mut txn = Transaction::new(v1.update.time);
            txn.facts(v1.shard, vec![(v1.update.data, v1.update.diff)]);
            return Ok(txn);
        }
    };
    let mut txn = Transaction::new(record.rev);
    for op in record.ops {
        match op {
            OpIn::Facts { shard, changes } => {
                let changes = changes
                    .into_iter()
                    .map(|(tuple, diff)| Ok((decode_tuple(&tuple)?, diff)))
                    .collect::<Result<Vec<_>, String>>()?;
                txn.facts(shard, changes);
            }
            OpIn::Rule {
                kg,
                name,
                definition,
            } => {
                txn.catalog(kg, CatalogEntry::Rule { name, definition });
            }
            OpIn::Schema {
                kg,
                relation,
                schema,
            } => {
                txn.catalog(kg, CatalogEntry::Schema { relation, schema });
            }
        }
    }
    Ok(txn)
}

fn decode_tuple(base64: &str) -> Result<Tuple, String> {
    let bytes = BASE64
        .decode(base64)
        .map_err(|e| format!("tuple is not base64: {e}"))?;
    codec::decode_tuple(&bytes).map_err(|e| format!("tuple does not decode: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn txn(rev: u64) -> Transaction {
        let mut txn = Transaction::new(rev);
        txn.insert("kg:a", [Tuple::from_pair(1, 2)])
            .delete("kg:b", [Tuple::from_pair(3, 4)]);
        txn
    }

    fn catalog_txn(rev: u64) -> Transaction {
        let rule = crate::statement::parse_rule_definition("p(X) <- a(X, Y), Y > 1.5").unwrap();
        let schema = RelationSchema::new("a")
            .with_column(crate::schema::ColumnSchema::new(
                "x",
                crate::schema::SchemaType::Int,
            ))
            .with_column(crate::schema::ColumnSchema::new(
                "y",
                crate::schema::SchemaType::Float,
            ));
        let mut txn = Transaction::new(rev);
        txn.catalog(
            "kg",
            CatalogEntry::Schema {
                relation: "a".into(),
                schema: Some(schema),
            },
        )
        .insert("kg:a", [Tuple::from_pair(1, 2)])
        .catalog(
            "kg",
            CatalogEntry::Rule {
                name: "p".into(),
                definition: Some(RuleDefinition::new("p".into(), rule.rule)),
            },
        )
        .catalog(
            "kg",
            CatalogEntry::Rule {
                name: "q".into(),
                definition: None,
            },
        )
        .catalog(
            "kg",
            CatalogEntry::Schema {
                relation: "b".into(),
                schema: None,
            },
        );
        txn
    }

    fn file(records: &[Transaction]) -> Vec<u8> {
        records.iter().flat_map(|t| encode(t).unwrap()).collect()
    }

    fn scan_ok(bytes: &[u8]) -> Scan {
        scan(Path::new("current.wal"), bytes).unwrap()
    }

    #[test]
    fn round_trip() {
        let bytes = file(&[txn(1), txn(2)]);
        let scan = scan_ok(&bytes);
        assert!(scan.damage.is_none());
        assert_eq!(scan.txns, vec![txn(1), txn(2)]);
        assert_eq!(scan.valid_end, bytes.len());
    }

    #[test]
    fn catalog_changes_round_trip_in_commit_order() {
        let expected = vec![catalog_txn(4), txn(5)];
        let scan = scan_ok(&file(&expected));
        assert!(scan.damage.is_none());
        assert_eq!(scan.txns, expected);
    }

    #[test]
    fn missing_newline_is_torn() {
        let mut bytes = file(&[txn(1), txn(2)]);
        bytes.pop();
        let scan = scan_ok(&bytes);
        assert_eq!(scan.txns, vec![txn(1)]);
        assert_eq!(scan.valid_end, encode(&txn(1)).unwrap().len());
        assert_eq!(scan.stranded, 0);
        assert!(scan.damage.is_some());
        assert!(scan.torn_tail);
    }

    #[test]
    fn damage_stops_replay_and_counts_stranded_records() {
        let first = encode(&txn(1)).unwrap();
        let mut bytes = file(&[txn(1), txn(2), txn(3)]);
        bytes[first.len() + 12] ^= 0x01;
        let scan = scan_ok(&bytes);
        assert_eq!(scan.txns, vec![txn(1)]);
        assert_eq!(scan.valid_end, first.len());
        assert_eq!(scan.stranded, 1);
        assert!(!scan.torn_tail);
        assert!(scan.damage.unwrap().contains("checksum mismatch"));
    }

    #[test]
    fn blank_lines_are_ignored() {
        let mut bytes = b"\n  \n".to_vec();
        bytes.extend(file(&[txn(1)]));
        bytes.extend_from_slice(b"\n\n");
        let scan = scan_ok(&bytes);
        assert!(scan.damage.is_none());
        assert_eq!(scan.txns, vec![txn(1)]);
    }

    fn framed(json: &[u8]) -> Vec<u8> {
        let mut bytes = format!("{:08x}:", crc32fast::hash(json)).into_bytes();
        bytes.extend_from_slice(json);
        bytes.push(b'\n');
        bytes
    }

    #[test]
    fn intact_record_in_foreign_format_is_an_error() {
        for json in [
            &br#"{"rev":1,"ops":[{"index":{"name":"r"}}]}"#[..],
            br#"{"rev":1,"ops":[{"rule":{"name":"r"}}]}"#,
            br#"{"rev":1,"ops":[{"schema":{"kg":"k","relation":"r","schema":null,"x":1}}]}"#,
            br#"{"rev":1,"ops":[],"epoch":2}"#,
            br#"{"rev":1,"ops":[{"facts":{"shard":"kg:a","changes":[["!!",1]]}}]}"#,
        ] {
            let err = scan(Path::new("current.wal"), &framed(json)).unwrap_err();
            assert!(
                matches!(err, StorageError::WalUnreadable { offset: 0, .. }),
                "{err}"
            );
        }
    }

    #[test]
    fn v1_entry_is_a_one_operation_transaction() {
        let json = br#"{"shard":"db:r","update":{"data":{"values":[{"type":"Int32","value":9}]},"time":8,"diff":-1}}"#;
        let mut expected = Transaction::new(8);
        expected.delete("db:r", [Tuple::new(vec![Value::Int32(9)])]);
        assert_eq!(scan_ok(&framed(json)).txns, [expected]);
    }

    #[test]
    fn every_value_round_trips_exactly() {
        let values = vec![
            Value::Float64(f64::NAN),
            Value::Float64(f64::NEG_INFINITY),
            Value::Float64(-0.0),
            Value::String("line\nbreak \"quoted\" é".into()),
            Value::Null,
            Value::Int64(i64::MIN),
        ];
        let mut txn = Transaction::new(1);
        txn.insert("kg:a", [Tuple::new(values.clone())]);
        let scan = scan_ok(&file(&[txn]));
        let TxnOp::Facts { changes, .. } = &scan.txns[0].ops()[0] else {
            panic!("expected facts");
        };
        let got = changes[0].0.values();
        assert_eq!(got.len(), values.len());
        for (got, want) in got.iter().zip(&values) {
            match (got, want) {
                (Value::Float64(a), Value::Float64(b)) => assert_eq!(a.to_bits(), b.to_bits()),
                _ => assert_eq!(got, want),
            }
        }
    }

    #[test]
    fn unchecksummed_line_is_torn() {
        let scan = scan_ok(b"{\"rev\":1,\"ops\":[]}\n");
        assert!(scan.txns.is_empty());
        assert!(scan.damage.is_some());
    }
}
