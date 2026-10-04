//! Resolving one fact statement (`+`, `-`, update) to the tuple changes it
//! makes, without touching the knowledge graph.
//!
//! Literal inserts and deletes resolve from their own terms. Conditional
//! deletes and updates evaluate their query on a view of the KG that already
//! includes the changes staged before them (see
//! [`crate::storage_engine::WriteProgram::view`]). The query is built from
//! the statement's syntax tree and evaluated as one, never written back into
//! IQL text: its constants, bound parameters included, are never re-parsed.

use super::term_to_value;
use crate::ast::{Atom, BodyPredicate, Program, Rule, Term};
use crate::protocol::wire::{ErrorCode, StatementKind};
use crate::statement::{DeleteOp, DeletePattern, InsertOp, UpdateOp};
use crate::storage_engine::{FactChange, KnowledgeGraphSnapshot};
use crate::value::{Tuple, Value};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

/// Size limits on inserted data (0 = unlimited).
pub(super) struct InsertLimits {
    pub max_string_bytes: usize,
    pub max_tuples: usize,
}

/// Why a statement could not be staged.
pub(super) struct StageError {
    pub code: ErrorCode,
    pub message: String,
}

/// A statement that changes persistent facts.
#[derive(Debug)]
pub(super) enum FactStatement {
    Insert(InsertOp),
    Delete(DeleteOp),
    Update(UpdateOp),
}

impl FactStatement {
    /// The tuple changes this statement makes, in order.
    ///
    /// `view` is called only by statements that read the KG (conditional
    /// delete, update), with the query they evaluate, and returns the KG as of
    /// the changes staged so far.
    pub fn changes(
        &self,
        limits: &InsertLimits,
        view: impl FnOnce(&Program) -> Result<Arc<KnowledgeGraphSnapshot>, StageError>,
    ) -> Result<Vec<FactChange>, StageError> {
        let invalid = |message| StageError {
            code: ErrorCode::Validation,
            message,
        };
        let failed = |message| StageError {
            code: super::supervise::computation_failure_code(ErrorCode::Validation),
            message,
        };
        match self {
            Self::Insert(op) => insert_tuples(op, limits)
                .map(|tuples| {
                    vec![FactChange::Insert {
                        relation: op.relation.clone(),
                        tuples,
                    }]
                })
                .map_err(invalid),
            Self::Delete(op) => {
                let tuples = match &op.pattern {
                    DeletePattern::SingleTuple(terms) => terms
                        .iter()
                        .map(term_to_value)
                        .collect::<Result<Vec<_>, _>>()
                        .map(|values| {
                            if values.is_empty() {
                                Vec::new()
                            } else {
                                vec![Tuple::new(values)]
                            }
                        })
                        .map_err(|e| invalid(format!("Delete error: {e}")))?,
                    // Rows with non-constant terms match nothing.
                    DeletePattern::BulkTuples(rows) => rows
                        .iter()
                        .filter_map(|terms| {
                            terms
                                .iter()
                                .map(term_to_value)
                                .collect::<Result<Vec<_>, _>>()
                                .ok()
                        })
                        .map(Tuple::new)
                        .collect(),
                    DeletePattern::Conditional { head_args, body } => {
                        let (query, vars) = conditional_delete_query(&op.relation, head_args, body);
                        let view = view(&query)?;
                        conditional_delete_tuples(head_args, &vars, query, &view)
                            .map_err(|e| failed(self.failure_message(e)))?
                    }
                };
                Ok(vec![FactChange::Delete {
                    relation: op.relation.clone(),
                    tuples,
                }])
            }
            Self::Update(op) => {
                let (query, vars) = update_query(op);
                let view = view(&query)?;
                update_changes(op, &vars, query, &view).map_err(|e| failed(self.failure_message(e)))
            }
        }
    }

    /// The form of this statement, as reported in a result's counts.
    pub fn kind(&self) -> StatementKind {
        match self {
            Self::Insert(_) => StatementKind::Insert,
            Self::Delete(_) => StatementKind::Delete,
            Self::Update(_) => StatementKind::Update,
        }
    }

    /// Whether staging this statement reads the knowledge graph.
    pub fn reads_state(&self) -> bool {
        matches!(
            self,
            Self::Update(_)
                | Self::Delete(DeleteOp {
                    pattern: DeletePattern::Conditional { .. },
                    ..
                })
        )
    }

    /// The message reporting that this statement failed with `error`.
    pub fn failure_message(&self, error: impl Display) -> String {
        match self {
            Self::Insert(_) => error.to_string(),
            Self::Delete(_) => format!("Delete failed: {error}"),
            Self::Update(_) => format!("Update failed: {error}"),
        }
    }

    /// The message reporting that this statement committed with these
    /// effective counts, or `None` if it reports nothing.
    pub fn success_message(&self, inserted: usize, deleted: usize) -> Option<String> {
        match self {
            Self::Insert(op) => Some(format!(
                "Inserted {inserted} fact(s) into '{}'.",
                op.relation
            )),
            Self::Delete(op) => match &op.pattern {
                DeletePattern::SingleTuple(terms) if terms.is_empty() => None,
                DeletePattern::SingleTuple(_) => {
                    Some(format!("Deleted {deleted} facts from '{}'.", op.relation))
                }
                DeletePattern::BulkTuples(_) => {
                    Some(format!("Deleted {deleted} fact(s) from '{}'.", op.relation))
                }
                DeletePattern::Conditional { .. } => Some(format!(
                    "Conditional delete: {deleted} fact(s) deleted from '{}'.",
                    op.relation
                )),
            },
            Self::Update(_) => Some(format!("Update: {deleted} deleted, {inserted} inserted.")),
        }
    }
}

/// Convert an insert's terms to tuples, enforcing `limits`.
fn insert_tuples(op: &InsertOp, limits: &InsertLimits) -> Result<Vec<Tuple>, String> {
    let mut tuples = Vec::with_capacity(op.tuples.len());
    for terms in op.tuples.iter().filter(|terms| !terms.is_empty()) {
        let mut values = Vec::with_capacity(terms.len());
        for term in terms {
            match term_to_value(term)? {
                Value::String(s)
                    if limits.max_string_bytes > 0 && s.len() > limits.max_string_bytes =>
                {
                    return Err(format!(
                        "String value too long: {} bytes (max {})",
                        s.len(),
                        limits.max_string_bytes
                    ));
                }
                value => values.push(value),
            }
        }
        tuples.push(Tuple::new(values));
    }
    if limits.max_tuples > 0 && tuples.len() > limits.max_tuples {
        return Err(format!(
            "Insert rejected for '{}': {} tuples exceeds max {}",
            op.relation,
            tuples.len(),
            limits.max_tuples
        ));
    }
    Ok(tuples)
}

/// Variables of `terms`, in first-occurrence order.
fn collect_vars<'a>(terms: impl IntoIterator<Item = &'a Term>, vars: &mut Vec<String>) {
    for term in terms {
        if let Term::Variable(v) = term {
            if !vars.contains(v) {
                vars.push(v.clone());
            }
        }
    }
}

/// Run `query` on `view` without the result-row cap: every match must be
/// changed, not a page of them.
fn evaluate(view: &KnowledgeGraphSnapshot, query: Program) -> Result<Vec<Tuple>, String> {
    crate::without_result_cap(|| view.execute_program_with_rules(query))
        .map_err(|e| format!("Query execution failed: {e}"))
}

/// The one-rule program `head(vars) <- body`.
fn query_program(head: &str, vars: &[String], body: Vec<BodyPredicate>) -> Program {
    let args = vars.iter().cloned().map(Term::Variable).collect();
    Program {
        rules: vec![Rule::new(Atom::new(head.to_string(), args), body)],
    }
}

/// The query finding matches of `-relation(head_args) <- body`, and the head
/// variables its result columns bind.
fn conditional_delete_query(
    relation: &str,
    head_args: &[Term],
    body: &[BodyPredicate],
) -> (Program, Vec<String>) {
    let mut vars = Vec::new();
    collect_vars(head_args, &mut vars);
    // The target relation binds every head variable.
    let body = std::iter::once(BodyPredicate::Positive(Atom::new(
        relation.to_string(),
        head_args.to_vec(),
    )))
    .chain(body.iter().cloned())
    .collect();
    (query_program("__cond_del_query__", &vars, body), vars)
}

/// Tuples to delete: `head_args` under each match of `query` on `view`.
fn conditional_delete_tuples(
    head_args: &[Term],
    vars: &[String],
    query: Program,
    view: &KnowledgeGraphSnapshot,
) -> Result<Vec<Tuple>, String> {
    let mut tuples = Vec::new();
    for row in evaluate(view, query)? {
        let bindings: HashMap<&str, &Value> = vars
            .iter()
            .map(String::as_str)
            .zip(row.values().iter())
            .collect();
        let values: Option<Vec<Value>> = head_args
            .iter()
            .map(|arg| match arg {
                Term::Variable(v) => bindings.get(v.as_str()).map(|&value| value.clone()),
                Term::Constant(c) => Some(Value::Int64(*c)),
                Term::StringConstant(s) => Some(Value::string(s)),
                Term::FloatConstant(f) => Some(Value::Float64(*f)),
                Term::BoolConstant(b) => Some(Value::Bool(*b)),
                _ => None,
            })
            .collect();
        if let Some(values) = values.filter(|values| !values.is_empty()) {
            tuples.push(Tuple::new(values));
        }
    }
    Ok(tuples)
}

/// The query finding matches of an update's body, and the target variables
/// its result columns bind.
fn update_query(op: &UpdateOp) -> (Program, Vec<String>) {
    let mut vars = Vec::new();
    collect_vars(op.deletes.iter().flat_map(|t| &t.args), &mut vars);
    collect_vars(op.inserts.iter().flat_map(|t| &t.args), &mut vars);
    (query_program("__upd_query__", &vars, op.body.clone()), vars)
}

/// Changes of `-old, +new <- body`: for each match of `query` on `view`, in
/// order, its deletes then its inserts. Targets with an unbound term are
/// skipped.
fn update_changes(
    op: &UpdateOp,
    vars: &[String],
    query: Program,
    view: &KnowledgeGraphSnapshot,
) -> Result<Vec<FactChange>, String> {
    let mut changes = Vec::new();
    for row in evaluate(view, query)? {
        let bindings: HashMap<&str, &Value> = vars
            .iter()
            .map(String::as_str)
            .zip(row.values().iter())
            .collect();
        let resolve = |args: &[Term]| -> Option<Tuple> {
            args.iter()
                .map(|arg| match arg {
                    Term::Variable(v) => bindings.get(v.as_str()).map(|&value| value.clone()),
                    other => term_to_value(other).ok(),
                })
                .collect::<Option<Vec<Value>>>()
                .map(Tuple::new)
        };
        for target in &op.deletes {
            if let Some(tuple) = resolve(&target.args) {
                push_change(&mut changes, false, &target.relation, tuple);
            }
        }
        for target in &op.inserts {
            if let Some(tuple) = resolve(&target.args) {
                push_change(&mut changes, true, &target.relation, tuple);
            }
        }
    }
    Ok(changes)
}

/// Append `tuple` to the last change if it has the same kind and relation,
/// else start a new change.
fn push_change(changes: &mut Vec<FactChange>, insert: bool, relation: &str, tuple: Tuple) {
    match changes.last_mut() {
        Some(FactChange::Insert {
            relation: last,
            tuples,
        }) if insert && last == relation => tuples.push(tuple),
        Some(FactChange::Delete {
            relation: last,
            tuples,
        }) if !insert && last == relation => tuples.push(tuple),
        _ => {
            let relation = relation.to_string();
            let tuples = vec![tuple];
            changes.push(if insert {
                FactChange::Insert { relation, tuples }
            } else {
                FactChange::Delete { relation, tuples }
            });
        }
    }
}
