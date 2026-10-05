//! IQL statements whose values travel as parameters.
//!
//! Every value the gateway writes into a program (extracted claims, quotes,
//! messages, conversation ids, ledger rows) is a parameter: the statement
//! text names it `$h<hash>` and the value rides in the request's `params`,
//! which the engine binds to the parsed program without parsing them. The
//! text is the pack's template or the gateway's own IQL; no value is ever
//! IQL syntax, whatever it holds.
//!
//! A parameter is named by a SHA-256 of its typed value, so equal values
//! share one name and different values never do: statements compose into
//! one program, and replay from the ledger, without renaming.

use std::fmt::Write as _;

use anyhow::{bail, Result};
use inputlayer_ontology_client::ws::{ParamValue, Params};
use sha2::{Digest, Sha256};

/// Hex digits of a parameter name's hash (128 bits).
const NAME_HEX: usize = 32;

/// One statement: IQL text naming its values, and the values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Stmt {
    iql: String,
    params: Params,
}

impl Stmt {
    pub fn new() -> Self {
        Self::default()
    }

    /// A statement from stored parts (the ledger's replay record).
    pub fn from_parts(iql: impl Into<String>, params: Params) -> Self {
        Self {
            iql: iql.into(),
            params,
        }
    }

    /// Append IQL text. Only the gateway's own IQL and pack templates go
    /// here, never a value.
    #[must_use]
    pub fn text(mut self, iql: &str) -> Self {
        self.iql.push_str(iql);
        self
    }

    /// Append a reference to `value`, sent as a parameter.
    #[must_use]
    pub fn value(mut self, value: impl Into<ParamValue>) -> Self {
        let value = value.into();
        let name = param_name(&value);
        let _ = write!(self.iql, "${name}");
        // A content-derived name is always a valid identifier.
        let _ = self.params.insert(name, value);
        self
    }

    pub fn iql(&self) -> &str {
        &self.iql
    }

    pub fn params(&self) -> &Params {
        &self.params
    }

    /// The statement with its leading `+` turned into `-`: the delete of the
    /// facts an insert stored. None for anything that is not an insert.
    pub fn negated(&self) -> Option<Self> {
        self.iql.strip_prefix('+').map(|rest| Self {
            iql: format!("-{rest}"),
            params: self.params.clone(),
        })
    }

    /// The string values of the statement.
    pub fn strings(&self) -> impl Iterator<Item = &str> {
        self.params.iter().filter_map(|(_, value)| match value {
            ParamValue::String(s) => Some(s.as_str()),
            _ => None,
        })
    }

    /// The statement with each value written as an IQL literal: for people
    /// (traces, events), never sent to an engine.
    pub fn display(&self) -> String {
        let mut out = String::with_capacity(self.iql.len());
        let mut rest = self.iql.as_str();
        while let Some(at) = rest.find("$h") {
            out.push_str(&rest[..at]);
            let name_end = (at + 2 + NAME_HEX).min(rest.len());
            match self.params.get(&rest[at + 1..name_end]) {
                Some(value) => {
                    out.push_str(&literal(value));
                    rest = &rest[name_end..];
                }
                None => {
                    out.push_str("$h");
                    rest = &rest[at + 2..];
                }
            }
        }
        out.push_str(rest);
        out
    }
}

/// The name of the parameter holding `value`: `h` and 128 bits of the
/// SHA-256 of its type and canonical JSON.
pub fn param_name(value: &ParamValue) -> String {
    let canonical = serde_json::to_string(value).unwrap_or_default();
    let digest = Sha256::new()
        .chain_update(value.type_name())
        .chain_update(b":")
        .chain_update(canonical)
        .finalize();
    let mut name = String::with_capacity(1 + NAME_HEX);
    name.push('h');
    for byte in &digest[..NAME_HEX / 2] {
        let _ = write!(name, "{byte:02x}");
    }
    name
}

/// `value` as an IQL literal, for display.
fn literal(value: &ParamValue) -> String {
    match value {
        ParamValue::Int(n) => n.to_string(),
        ParamValue::Float(f) => format!("{f:?}"),
        ParamValue::String(s) => format!("\"{}\"", crate::mapper::esc(s)),
        ParamValue::Bool(b) => b.to_string(),
        ParamValue::Vector(v) => {
            let items: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
            format!("[{}]", items.join(", "))
        }
    }
}

/// Statements sent as one request: their text joined by newlines, their
/// parameters merged.
#[derive(Debug, Default)]
pub struct Program {
    lines: Vec<String>,
    params: Params,
}

impl Program {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `stmt`. Fails when one of its names is bound to a different
    /// value already, which only a hash collision (or a forged replay
    /// record) could cause: never a silent rebinding.
    pub fn push(&mut self, stmt: &Stmt) -> Result<()> {
        for (name, value) in stmt.params.iter() {
            match self.params.get(name) {
                Some(bound) if bound != value => {
                    bail!("parameter ${name} is bound to two different values")
                }
                Some(_) => {}
                None => {
                    let _ = self.params.insert(name, value.clone());
                }
            }
        }
        self.lines.push(stmt.iql.clone());
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// The program text.
    pub fn iql(&self) -> String {
        self.lines.join("\n")
    }

    pub fn params(&self) -> &Params {
        &self.params
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn values_never_enter_the_text() {
        let hostile = "x\"), +evil[(\"y\n?z(X) $h00";
        let stmt = Stmt::new()
            .text("+claim[(")
            .value(hostile)
            .text(", ")
            .value(7_i64)
            .text(")]");
        assert!(!stmt.iql().contains("evil"), "{}", stmt.iql());
        assert!(!stmt.iql().contains('\n'));
        assert_eq!(stmt.params().len(), 2);
        assert_eq!(
            stmt.display(),
            "+claim[(\"x\\\"), +evil[(\\\"y\\n?z(X) $h00\", 7)]"
        );
    }

    #[test]
    fn names_follow_the_typed_value() {
        let a = param_name(&"1".into());
        assert_eq!(a, param_name(&"1".into()));
        assert_ne!(a, param_name(&ParamValue::Int(1)));
        assert_ne!(
            param_name(&ParamValue::Int(1)),
            param_name(&ParamValue::Float(1.0))
        );
        assert_ne!(
            param_name(&"true".into()),
            param_name(&ParamValue::Bool(true))
        );
        assert_eq!(a.len(), 1 + NAME_HEX);
        assert!(inputlayer_ontology_client::ws::Params::new()
            .with(a, 1_i64)
            .is_ok());
    }

    #[test]
    fn programs_merge_shared_values_and_refuse_a_rebinding() {
        let one = Stmt::new().text("+a(").value("x").text(")");
        let two = Stmt::new()
            .text("+b(")
            .value("x")
            .text(", ")
            .value(2_i64)
            .text(")");
        let mut program = Program::new();
        program.push(&one).unwrap();
        program.push(&two).unwrap();
        assert_eq!(program.params().len(), 2);
        assert_eq!(program.iql().lines().count(), 2);

        let name = param_name(&"x".into());
        let forged = Stmt::from_parts(
            format!("+c(${name})"),
            Params::new().with(name, "not x").unwrap(),
        );
        assert!(program.push(&forged).is_err());
    }

    #[test]
    fn negation_turns_an_insert_into_its_delete() {
        let stmt = Stmt::new().text("+a(").value("x").text(")");
        let deleted = stmt.negated().unwrap();
        assert!(deleted.iql().starts_with("-a($h"));
        assert_eq!(deleted.params(), stmt.params());
        assert!(Stmt::new().text("?a(X)").negated().is_none());
    }
}
