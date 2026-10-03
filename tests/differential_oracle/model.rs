//! Shared vocabulary of the oracle: histories, revisions and observed results.

use std::collections::BTreeMap;
use std::fmt;

/// One value in a result row, normalized so every adapter's output compares
/// by value (an engine `Int32(1)` and a reference `1` are the same cell).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cell {
    Int(i64),
    Str(String),
    Bool(bool),
    /// Any other wire value, kept as canonical JSON (floats, vectors, null).
    Other(String),
}

impl Cell {
    /// Normalize a wire JSON value.
    pub fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Number(n) => n
                .as_i64()
                .map_or_else(|| Self::Other(n.to_string()), Self::Int),
            serde_json::Value::String(s) => Self::Str(s.clone()),
            serde_json::Value::Bool(b) => Self::Bool(*b),
            other => Self::Other(other.to_string()),
        }
    }
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(n) => write!(f, "{n}"),
            Self::Str(s) => write!(f, "{s:?}"),
            Self::Bool(b) => write!(f, "{b}"),
            Self::Other(s) => f.write_str(s),
        }
    }
}

pub type Row = Vec<Cell>;

/// Render a row as an IQL-like tuple, e.g. `(1, "a")`.
pub fn show_row(row: &Row) -> String {
    let cells: Vec<String> = row.iter().map(ToString::to_string).collect();
    format!("({})", cells.join(", "))
}

/// A query result as a Z-set: each row with its multiplicity.
///
/// Query results have set semantics, so a correct observation holds every
/// row exactly once. Keeping multiplicities instead of a set lets the oracle
/// see an adapter that reports a row twice, or retracts one it never had.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observation {
    rows: BTreeMap<Row, i64>,
}

impl Observation {
    /// Count each row once per occurrence.
    pub fn from_rows(rows: impl IntoIterator<Item = Row>) -> Self {
        let mut observation = Self::default();
        for row in rows {
            observation.add(row, 1);
        }
        observation
    }

    /// Add `weight` to `row`'s multiplicity (negative to retract).
    pub fn add(&mut self, row: Row, weight: i64) {
        let entry = self.rows.entry(row).or_insert(0);
        *entry += weight;
        if *entry == 0 {
            self.rows.retain(|_, w| *w != 0);
        }
    }

    /// Rows whose multiplicity is not exactly one.
    pub fn non_set_rows(&self) -> Vec<(Row, i64)> {
        self.rows
            .iter()
            .filter(|(_, w)| **w != 1)
            .map(|(r, w)| (r.clone(), *w))
            .collect()
    }

    /// Rows with positive multiplicity.
    pub fn support(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter().filter(|(_, w)| **w > 0).map(|(r, _)| r)
    }
}

/// One step of a history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// One IQL statement against the knowledge graph (insert, delete, rule
    /// or schema change, rule drop, ...).
    Execute(String),
    /// Stop the engine and reopen it from its data directory.
    Restart,
    /// Observe every query of the history at this named revision.
    Checkpoint(String),
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execute(statement) => f.write_str(statement),
            Self::Restart => f.write_str("// -- restart --"),
            Self::Checkpoint(name) => write!(f, "// -- checkpoint {name} --"),
        }
    }
}

/// Position in a history: the number of steps applied so far.
///
/// Every adapter is observed at the same revision; an adapter with
/// asynchronous maintenance must answer as of exactly this point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Revision(pub usize);

/// Statements plus the standing queries observed at every checkpoint.
///
/// Queries are fixed for the whole history so that an incremental adapter can
/// subscribe before the first step and build every result from deltas alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct History {
    pub queries: Vec<String>,
    pub steps: Vec<Step>,
}

impl History {
    /// The history as a commented IQL script, for reproducing a failure.
    pub fn script(&self) -> String {
        let mut out = String::new();
        for query in &self.queries {
            out.push_str(&format!("// observed: {query}\n"));
        }
        for step in &self.steps {
            out.push_str(&format!("{step}\n"));
        }
        out
    }

    /// Number of state-changing statements.
    pub fn statements(&self) -> usize {
        self.steps
            .iter()
            .filter(|s| matches!(s, Step::Execute(_)))
            .count()
    }
}

/// Result of applying one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Applied,
    Rejected(String),
    /// Applied on the assumption that the statement is valid: the adapter
    /// does not model this kind of validation. If the baseline rejects it,
    /// the adapter's state can no longer be trusted (see `Adapter::desync`).
    Assumed,
}

impl Outcome {
    /// Whether the statement changed (or may have changed) state.
    pub fn accepted(&self) -> bool {
        !matches!(self, Self::Rejected(_))
    }
}

/// Why an adapter could not answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterError {
    /// The adapter does not implement this construct; the comparison is
    /// skipped and reported, never counted as agreement.
    Unsupported(String),
    /// The adapter broke (I/O, internal error, a failed subscription refresh).
    Failed(String),
}
