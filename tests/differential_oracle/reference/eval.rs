//! Naive bottom-up evaluation over finite sets.
//!
//! Deliberately simple and independent of the engine's planner, optimizer and
//! dataflow code: each stratum is evaluated by re-running every clause over
//! the whole database until nothing changes. Only the parser is shared with
//! the engine. Anything outside the modeled fragment is `Unsupported`, never
//! approximated.
//!
//! Semantics modeled (matching the `.iql.out` specs):
//! - set semantics for facts, rule results and query results;
//! - each `_` is a fresh variable, so it distinguishes bindings;
//! - aggregates range over the distinct full bindings of the body, grouped by
//!   the head's non-aggregate arguments; an empty group produces no row;
//! - a query returns its goal arguments (constants and `_` included).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use inputlayer::ast::{AggregateFunc, Atom, BodyPredicate, ComparisonOp, Rule, Term};

use crate::model::{AdapterError, Cell, Row};

pub type Relations = BTreeMap<String, BTreeSet<Row>>;
type Binding = BTreeMap<String, Cell>;

fn unsupported<T>(what: impl Into<String>) -> Result<T, AdapterError> {
    Err(AdapterError::Unsupported(what.into()))
}

/// A constant term's value, if it is one the reference models.
pub fn constant(term: &Term) -> Option<Cell> {
    match term {
        Term::Constant(n) => Some(Cell::Int(*n)),
        Term::StringConstant(s) => Some(Cell::Str(s.clone())),
        Term::BoolConstant(b) => Some(Cell::Bool(*b)),
        _ => None,
    }
}

/// Atom argument after renaming each `_` to a fresh variable.
enum Arg {
    Var(String),
    Value(Cell),
}

fn args(atom: &Atom, fresh: &mut usize) -> Result<Vec<Arg>, AdapterError> {
    atom.args
        .iter()
        .map(|term| match term {
            Term::Variable(v) => Ok(Arg::Var(v.clone())),
            Term::Placeholder => {
                *fresh += 1;
                Ok(Arg::Var(format!("_#{fresh}")))
            }
            other => constant(other).map(Arg::Value).map_or_else(
                || unsupported(format!("term {other:?} in {}", atom.relation)),
                Ok,
            ),
        })
        .collect()
}

/// Bound on naive evaluation work per model, in binding extensions.
///
/// Naive evaluation re-derives everything each round, so a long recursive
/// chain costs rounds x closure size. Past this bound the reference reports
/// the case as unsupported instead of stalling the test run.
pub struct Budget(usize);

impl Budget {
    pub const STEPS: usize = 2_000_000;

    pub fn new() -> Self {
        Self(Self::STEPS)
    }

    fn spend(&mut self, steps: usize) -> Result<(), AdapterError> {
        self.0 = self.0.checked_sub(steps).map_or_else(
            || {
                unsupported(format!(
                    "naive evaluation needs more than {} binding steps",
                    Self::STEPS
                ))
            },
            Ok,
        )?;
        Ok(())
    }
}

/// Extend each binding with every matching tuple of `atom`, through a hash
/// index on the argument positions already known before matching.
fn join(
    bindings: BTreeSet<Binding>,
    atom: &[Arg],
    tuples: &BTreeSet<Row>,
    budget: &mut Budget,
) -> Result<BTreeSet<Binding>, AdapterError> {
    // Every binding binds the same variables, so the first one decides.
    let Some(first) = bindings.iter().next() else {
        return Ok(BTreeSet::new());
    };
    let known: Vec<usize> = atom
        .iter()
        .enumerate()
        .filter(|(_, arg)| match arg {
            Arg::Value(_) => true,
            Arg::Var(v) => first.contains_key(v),
        })
        .map(|(i, _)| i)
        .collect();
    let mut index: HashMap<Vec<&Cell>, Vec<&Row>> = HashMap::new();
    for tuple in tuples.iter().filter(|t| t.len() == atom.len()) {
        index
            .entry(known.iter().map(|&i| &tuple[i]).collect())
            .or_default()
            .push(tuple);
    }
    let mut out = BTreeSet::new();
    for binding in &bindings {
        let key: Vec<&Cell> = known
            .iter()
            .map(|&i| match &atom[i] {
                Arg::Value(c) => c,
                Arg::Var(v) => &binding[v],
            })
            .collect();
        let matches = index.get(&key).map_or(&[][..], Vec::as_slice);
        budget.spend(matches.len())?;
        'tuple: for tuple in matches {
            let mut extended = binding.clone();
            for (arg, value) in atom.iter().zip(tuple.iter()) {
                if let Arg::Var(v) = arg {
                    // A variable repeated within the atom must match itself.
                    match extended.get(v) {
                        Some(bound) if bound != value => continue 'tuple,
                        Some(_) => {}
                        None => {
                            extended.insert(v.clone(), value.clone());
                        }
                    }
                }
            }
            out.insert(extended);
        }
    }
    Ok(out)
}

fn value(term: &Term, binding: &Binding) -> Result<Cell, AdapterError> {
    match term {
        Term::Variable(v) => binding.get(v).cloned().map_or_else(
            || unsupported(format!("unbound variable {v} in comparison")),
            Ok,
        ),
        other => {
            constant(other).map_or_else(|| unsupported(format!("comparison term {other:?}")), Ok)
        }
    }
}

fn compare(left: &Cell, op: &ComparisonOp, right: &Cell) -> Result<bool, AdapterError> {
    let ordering = match (left, right) {
        (Cell::Int(a), Cell::Int(b)) => a.cmp(b),
        (Cell::Str(a), Cell::Str(b)) => a.cmp(b),
        _ => return unsupported(format!("comparison of {left} with {right}")),
    };
    Ok(match op {
        ComparisonOp::Equal => ordering.is_eq(),
        ComparisonOp::NotEqual => ordering.is_ne(),
        ComparisonOp::LessThan => ordering.is_lt(),
        ComparisonOp::LessOrEqual => ordering.is_le(),
        ComparisonOp::GreaterThan => ordering.is_gt(),
        ComparisonOp::GreaterOrEqual => ordering.is_ge(),
    })
}

/// All distinct full bindings of a conjunctive body over `db`.
///
/// Positive atoms bind left to right; comparisons and negations are then
/// checked on fully bound values (a body needing them earlier is unsafe for
/// the reference and reported as unsupported).
pub fn bindings(
    body: &[BodyPredicate],
    db: &Relations,
    budget: &mut Budget,
) -> Result<BTreeSet<Binding>, AdapterError> {
    let empty = BTreeSet::new();
    let mut fresh = 0;
    let mut result = BTreeSet::from([Binding::new()]);
    for predicate in body {
        if let BodyPredicate::Positive(atom) = predicate {
            let atom_args = args(atom, &mut fresh)?;
            result = join(
                result,
                &atom_args,
                db.get(&atom.relation).unwrap_or(&empty),
                budget,
            )?;
        }
    }
    for predicate in body {
        match predicate {
            BodyPredicate::Positive(_) => {}
            BodyPredicate::Negated(atom) => {
                if atom.args.iter().any(|t| matches!(t, Term::Placeholder)) {
                    return unsupported(format!("'_' inside negated {}", atom.relation));
                }
                let atom_args = args(atom, &mut fresh)?;
                let tuples = db.get(&atom.relation).unwrap_or(&empty);
                let mut kept = BTreeSet::new();
                for binding in result {
                    let mut tuple = Vec::with_capacity(atom_args.len());
                    for arg in &atom_args {
                        tuple.push(match arg {
                            Arg::Value(c) => c.clone(),
                            Arg::Var(v) => value(&Term::Variable(v.clone()), &binding)?,
                        });
                    }
                    if !tuples.contains(&tuple) {
                        kept.insert(binding);
                    }
                }
                result = kept;
            }
            BodyPredicate::Comparison(left, op, right) => {
                let mut kept = BTreeSet::new();
                for binding in result {
                    if compare(&value(left, &binding)?, op, &value(right, &binding)?)? {
                        kept.insert(binding);
                    }
                }
                result = kept;
            }
            BodyPredicate::HnswNearest { .. } => return unsupported("hnsw_nearest"),
        }
    }
    Ok(result)
}

/// Project bindings onto output terms (variables and constants).
pub fn project(
    terms: &[Term],
    bindings: &BTreeSet<Binding>,
) -> Result<BTreeSet<Row>, AdapterError> {
    bindings
        .iter()
        .map(|binding| terms.iter().map(|t| value(t, binding)).collect())
        .collect()
}

/// Tuples produced by one clause over `db`.
pub fn clause(
    rule: &Rule,
    db: &Relations,
    budget: &mut Budget,
) -> Result<BTreeSet<Row>, AdapterError> {
    let found = bindings(&rule.body, db, budget)?;
    if !rule.head.args.iter().any(Term::is_aggregate) {
        return project(&rule.head.args, &found);
    }
    let keys: Vec<&Term> = rule
        .head
        .args
        .iter()
        .filter(|t| !t.is_aggregate())
        .collect();
    let mut groups: BTreeMap<Vec<Cell>, Vec<&Binding>> = BTreeMap::new();
    for binding in &found {
        let key = keys
            .iter()
            .map(|t| value(t, binding))
            .collect::<Result<_, _>>()?;
        groups.entry(key).or_default().push(binding);
    }
    let mut rows = BTreeSet::new();
    for (key, members) in groups {
        let mut key = key.into_iter();
        let mut row = Vec::with_capacity(rule.head.args.len());
        for term in &rule.head.args {
            row.push(match term {
                Term::Aggregate(func, var) => aggregate(func, var, &members)?,
                _ => key
                    .next()
                    .expect("one key cell per non-aggregate head term"),
            });
        }
        rows.insert(row);
    }
    Ok(rows)
}

fn aggregate(func: &AggregateFunc, var: &str, members: &[&Binding]) -> Result<Cell, AdapterError> {
    let values: Vec<&Cell> = members
        .iter()
        .map(|b| {
            b.get(var).map_or_else(
                || unsupported(format!("unbound aggregate variable {var}")),
                Ok,
            )
        })
        .collect::<Result<_, _>>()?;
    let count = |n: usize| {
        i64::try_from(n)
            .map(Cell::Int)
            .map_err(|e| AdapterError::Failed(e.to_string()))
    };
    match func {
        AggregateFunc::Count => count(values.len()),
        AggregateFunc::CountDistinct => count(values.iter().collect::<BTreeSet<_>>().len()),
        AggregateFunc::Sum => {
            let mut total: i64 = 0;
            for v in values {
                let Cell::Int(n) = v else {
                    return unsupported(format!("sum over {v}"));
                };
                total = total
                    .checked_add(*n)
                    .map_or_else(|| unsupported("sum overflow"), Ok)?;
            }
            Ok(Cell::Int(total))
        }
        AggregateFunc::Min | AggregateFunc::Max => {
            let ints = values.iter().all(|v| matches!(v, Cell::Int(_)));
            let strs = values.iter().all(|v| matches!(v, Cell::Str(_)));
            if !(ints || strs) {
                return unsupported(format!("{func:?} over mixed or non-ordered values"));
            }
            let pick = if matches!(func, AggregateFunc::Min) {
                values.into_iter().min()
            } else {
                values.into_iter().max()
            };
            Ok(pick.cloned().expect("groups are never empty"))
        }
        other => unsupported(format!("aggregate {other:?}")),
    }
}

/// Fail unless every construct of `rule` is in the modeled fragment and every
/// variable outside positive atoms is bound by one.
pub fn check_rule(rule: &Rule) -> Result<(), AdapterError> {
    let mut bound = BTreeSet::new();
    for predicate in &rule.body {
        match predicate {
            BodyPredicate::Positive(atom) => {
                args(atom, &mut 0)?;
                bound.extend(atom.args.iter().filter_map(|t| match t {
                    Term::Variable(v) => Some(v.clone()),
                    _ => None,
                }));
            }
            BodyPredicate::Negated(atom) => {
                args(atom, &mut 0)?;
            }
            BodyPredicate::Comparison(..) => {}
            BodyPredicate::HnswNearest { .. } => return unsupported("hnsw_nearest"),
        }
    }
    let needs_binding = |term: &Term| -> Result<(), AdapterError> {
        match term {
            Term::Variable(v) if bound.contains(v) => Ok(()),
            Term::Variable(v) => unsupported(format!("variable {v} not bound by a positive atom")),
            Term::Placeholder => Ok(()),
            other => constant(other)
                .map(|_| ())
                .map_or_else(|| unsupported(format!("term {other:?}")), Ok),
        }
    };
    for predicate in &rule.body {
        match predicate {
            BodyPredicate::Negated(atom) => atom.args.iter().try_for_each(needs_binding)?,
            BodyPredicate::Comparison(left, _, right) => {
                needs_binding(left)?;
                needs_binding(right)?;
            }
            _ => {}
        }
    }
    for term in &rule.head.args {
        match term {
            Term::Aggregate(
                AggregateFunc::Count
                | AggregateFunc::CountDistinct
                | AggregateFunc::Sum
                | AggregateFunc::Min
                | AggregateFunc::Max,
                var,
            ) => needs_binding(&Term::Variable(var.clone()))?,
            Term::Aggregate(func, _) => return unsupported(format!("aggregate {func:?}")),
            Term::Placeholder => return unsupported("'_' in a rule head"),
            other => needs_binding(other)?,
        }
    }
    Ok(())
}
