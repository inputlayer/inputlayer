//! Naive finite reference: the meaning of a history, computed without the
//! engine's evaluator.
//!
//! It shares only the IQL parser with the engine. It models acceptance of
//! rule drops and clears; every other statement is applied as
//! [`Outcome::Assumed`]. Constructs outside its fragment desynchronize it with
//! an explicit reason, after which every observation is `Unsupported`.

mod eval;
mod program;

use std::collections::BTreeSet;

use inputlayer::ast::{Atom, BodyPredicate, Rule, Term};
use inputlayer::statement::{parse_query, parse_statement, DeletePattern, MetaCommand, Statement};

use crate::adapter::Adapter;
use crate::model::{AdapterError, Observation, Outcome, Revision, Row};
use eval::{bindings, check_rule, constant, project, Budget, Relations};
use program::{model, strata, Rules};

#[derive(Default)]
pub struct ReferenceAdapter {
    facts: Relations,
    rules: Rules,
    /// Facts plus derived relations, computed once per state.
    model: Option<Relations>,
    /// Why the reference stopped tracking the history, if it did.
    desynced: Option<String>,
}

impl ReferenceAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    fn apply(&mut self, statement: &str) -> Result<Outcome, AdapterError> {
        let parsed = match parse_statement(statement) {
            Ok(parsed) => parsed,
            Err(e) => return Ok(Outcome::Rejected(format!("parse error: {e}"))),
        };
        match parsed {
            Statement::Insert(op) => {
                let rows = constant_rows(&op.tuples)?;
                self.ensure_base(&op.relation)?;
                self.facts.entry(op.relation).or_default().extend(rows);
                Ok(Outcome::Assumed)
            }
            Statement::Delete(op) => {
                let rows = match op.pattern {
                    DeletePattern::SingleTuple(tuple) => constant_rows(&[tuple])?,
                    DeletePattern::BulkTuples(tuples) => constant_rows(&tuples)?,
                    other => return unsupported(format!("delete pattern {other:?}")),
                };
                self.ensure_base(&op.relation)?;
                if let Some(facts) = self.facts.get_mut(&op.relation) {
                    for row in &rows {
                        facts.remove(row);
                    }
                }
                Ok(Outcome::Assumed)
            }
            // Declares a base relation; the engine checks the types of its
            // facts, which the reference assumes valid.
            Statement::SchemaDecl(decl) if decl.persistent => {
                self.ensure_base(&decl.name)?;
                Ok(Outcome::Assumed)
            }
            Statement::PersistentRule(rule) => self.add_clause(rule),
            Statement::Meta(MetaCommand::RuleDrop(name))
            | Statement::DeleteRelationOrRule(name) => Ok(match self.rules.remove(&name) {
                Some(_) => Outcome::Applied,
                None => Outcome::Rejected(format!("rule '{name}' does not exist")),
            }),
            Statement::Meta(MetaCommand::RuleClear(name)) => Ok(match self.rules.get_mut(&name) {
                Some(clauses) => {
                    clauses.clear();
                    Outcome::Applied
                }
                None => Outcome::Rejected(format!("rule '{name}' does not exist")),
            }),
            other => unsupported(format!("statement {}", kind(&other))),
        }
    }

    /// Facts and rules never share a relation in the modeled fragment.
    fn ensure_base(&self, relation: &str) -> Result<(), AdapterError> {
        if self.rules.contains_key(relation) {
            return unsupported(format!("facts for rule-defined relation '{relation}'"));
        }
        Ok(())
    }

    fn add_clause(&mut self, rule: Rule) -> Result<Outcome, AdapterError> {
        check_rule(&rule)?;
        let name = rule.head.relation.clone();
        if self.facts.get(&name).is_some_and(|f| !f.is_empty()) {
            return unsupported(format!("rule '{name}' over a relation with base facts"));
        }
        let clauses = self.rules.entry(name).or_default();
        if !clauses.contains(&rule) {
            clauses.push(rule);
        }
        strata(&self.rules)?;
        Ok(Outcome::Assumed)
    }

    fn query(&mut self, query: &str) -> Result<Observation, AdapterError> {
        let body = query
            .trim()
            .strip_prefix('?')
            .map_or_else(|| unsupported(format!("not a query: {query}")), Ok)?;
        let goal = parse_query(body)
            .map_err(|e| AdapterError::Unsupported(format!("query parse: {e}")))?;
        if goal.limit.is_some() || goal.offset.is_some() {
            return unsupported("limit/offset");
        }
        let Some(atom) = goal.goal else {
            return unsupported("query without a goal atom");
        };
        // Each `_` in the goal is an output column with its own variable.
        let terms: Vec<Term> = atom
            .args
            .iter()
            .enumerate()
            .map(|(i, t)| match t {
                Term::Placeholder => Term::Variable(format!("_q#{i}")),
                other => other.clone(),
            })
            .collect();
        let mut predicates = vec![BodyPredicate::Positive(Atom::new(
            atom.relation.clone(),
            terms.clone(),
        ))];
        predicates.extend(goal.body);
        if self.model.is_none() {
            // A state the reference cannot evaluate stays that way until
            // more statements arrive, which it no longer tracks reliably.
            let computed = model(&self.facts, &self.rules).inspect_err(|e| {
                if let AdapterError::Unsupported(reason) = e {
                    self.desynced = Some(format!("reference stopped evaluating: {reason}"));
                }
            })?;
            self.model = Some(computed);
        }
        let db = self.model.as_ref().expect("model computed above");
        // The engine answers a goal whose arity differs from the relation's
        // with the relation's rows; the meaning of that is not modeled.
        if let Some(arity) = db.get(&atom.relation).and_then(|t| t.first()).map(Vec::len) {
            if arity != terms.len() {
                return unsupported(format!(
                    "query arity {} for '{}' of arity {arity}",
                    terms.len(),
                    atom.relation
                ));
            }
        }
        let rows = project(&terms, &bindings(&predicates, db, &mut Budget::new())?)?;
        Ok(Observation::from_rows(rows))
    }
}

fn unsupported<T>(what: impl Into<String>) -> Result<T, AdapterError> {
    Err(AdapterError::Unsupported(what.into()))
}

fn constant_rows(tuples: &[Vec<Term>]) -> Result<BTreeSet<Row>, AdapterError> {
    tuples
        .iter()
        .map(|tuple| {
            tuple
                .iter()
                .map(|t| constant(t).map_or_else(|| unsupported(format!("fact value {t:?}")), Ok))
                .collect()
        })
        .collect()
}

fn kind(statement: &Statement) -> String {
    match statement {
        Statement::Meta(meta) => format!("meta {meta:?}"),
        Statement::Update(_) => "update".into(),
        Statement::TypeDecl(_) => "type declaration".into(),
        Statement::SessionRule(_) => "session rule".into(),
        Statement::Fact(_) => "session fact".into(),
        Statement::Query(_) => "query".into(),
        Statement::SchemaDecl(_) => "schema declaration".into(),
        other => format!("{other:?}"),
    }
}

impl Adapter for ReferenceAdapter {
    fn name(&self) -> &'static str {
        "reference"
    }

    fn execute(&mut self, statement: &str, _revision: Revision) -> Result<Outcome, AdapterError> {
        if let Some(reason) = &self.desynced {
            return Err(AdapterError::Unsupported(reason.clone()));
        }
        self.model = None;
        self.apply(statement).inspect_err(|e| {
            if let AdapterError::Unsupported(reason) = e {
                self.desynced = Some(format!("reference stopped at `{statement}`: {reason}"));
            }
        })
    }

    /// Restarting must not change meaning, so the reference has nothing to do.
    fn restart(&mut self, _revision: Revision) -> Result<(), AdapterError> {
        Ok(())
    }

    fn observe(&mut self, query: &str, _revision: Revision) -> Result<Observation, AdapterError> {
        match &self.desynced {
            Some(reason) => Err(AdapterError::Unsupported(reason.clone())),
            None => self.query(query),
        }
    }

    fn desync(&mut self, reason: String) {
        self.desynced.get_or_insert(reason);
    }
}
