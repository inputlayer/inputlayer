//! Term unification and substitution for backward chaining.
//!
//! Matches concrete tuples against rule head patterns to extract variable
//! bindings, then substitutes bindings into body atoms for evaluation.

use crate::ast::{Atom, ComparisonOp, Term};
use crate::value::{Tuple, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Maps variable names to their bound concrete values.
pub type Bindings = HashMap<String, Value>;

/// A term with some variables resolved to concrete values.
#[derive(Debug, Clone)]
pub enum BoundTerm {
    /// Fully resolved to a concrete value
    Concrete(Value),
    /// Still an unbound variable
    Unbound(String),
}

/// Try to unify a concrete tuple against an Atom's head arguments.
///
/// Returns `Some(bindings)` if the tuple matches the head pattern,
/// `None` if there is a mismatch (constant conflict, arity mismatch, etc).
pub fn unify_head(tuple: &Tuple, head: &Atom) -> Option<Bindings> {
    if tuple.arity() != head.args.len() {
        return None;
    }
    let mut bindings = Bindings::new();
    for (i, term) in head.args.iter().enumerate() {
        let value = tuple.get(i)?;
        match term {
            Term::Variable(name) => {
                if let Some(existing) = bindings.get(name) {
                    if existing != value {
                        return None; // Repeated variable with conflicting values
                    }
                } else {
                    bindings.insert(name.clone(), value.clone());
                }
            }
            Term::Placeholder => {} // Wildcard, matches anything
            _ => {
                // Constant term - must match the tuple value
                let expected = term_to_value(term)?;
                if !values_equal(&expected, value) {
                    return None;
                }
            }
        }
    }
    Some(bindings)
}

/// Substitute known bindings into an Atom's arguments.
///
/// Returns a list of `BoundTerm` where known variables are replaced
/// with concrete values and unknown variables remain as `Unbound`.
pub fn substitute_atom(atom: &Atom, bindings: &Bindings) -> Vec<BoundTerm> {
    atom.args
        .iter()
        .map(|t| resolve_term(t, bindings))
        .collect()
}

/// Find all tuples in a relation that match a partially-bound pattern.
///
/// For each matching tuple, extends the bindings with newly discovered
/// variable values and returns the tuple + extended bindings.
pub fn find_matching_tuples(
    relation: &str,
    bound_terms: &[BoundTerm],
    base_data: &HashMap<String, Vec<Tuple>>,
) -> Vec<(Tuple, Bindings)> {
    let tuples = match base_data.get(relation) {
        Some(t) => t,
        None => return Vec::new(),
    };

    let mut results = Vec::new();
    for tuple in tuples {
        if tuple.arity() != bound_terms.len() {
            continue;
        }
        let mut new_bindings = Bindings::new();
        let mut matches = true;
        for (i, bt) in bound_terms.iter().enumerate() {
            let value = match tuple.get(i) {
                Some(v) => v,
                None => {
                    matches = false;
                    break;
                }
            };
            match bt {
                BoundTerm::Concrete(expected) => {
                    if !values_equal(expected, value) {
                        matches = false;
                        break;
                    }
                }
                BoundTerm::Unbound(var_name) => {
                    if let Some(existing) = new_bindings.get(var_name) {
                        if !values_equal(existing, value) {
                            matches = false;
                            break;
                        }
                    } else {
                        new_bindings.insert(var_name.clone(), value.clone());
                    }
                }
            }
        }
        if matches {
            results.push((tuple.clone(), new_bindings));
        }
    }
    results
}

/// Evaluate a comparison predicate with the given bindings.
///
/// Both sides must be fully bound (no unresolved variables).
pub fn evaluate_comparison(
    lhs: &Term,
    op: &ComparisonOp,
    rhs: &Term,
    bindings: &Bindings,
) -> Result<bool, String> {
    let left = resolve_to_value(lhs, bindings)?;
    let right = resolve_to_value(rhs, bindings)?;

    let result = match op {
        ComparisonOp::Equal => values_equal(&left, &right),
        ComparisonOp::NotEqual => !values_equal(&left, &right),
        ComparisonOp::LessThan => value_cmp(&left, &right)? == std::cmp::Ordering::Less,
        ComparisonOp::LessOrEqual => value_cmp(&left, &right)? != std::cmp::Ordering::Greater,
        ComparisonOp::GreaterThan => value_cmp(&left, &right)? == std::cmp::Ordering::Greater,
        ComparisonOp::GreaterOrEqual => value_cmp(&left, &right)? != std::cmp::Ordering::Less,
    };
    Ok(result)
}

/// Resolve a term to a concrete Value using bindings.
fn resolve_to_value(term: &Term, bindings: &Bindings) -> Result<Value, String> {
    match resolve_term(term, bindings) {
        BoundTerm::Concrete(v) => Ok(v),
        BoundTerm::Unbound(var) => Err(format!(
            "Variable {var} is unbound - cannot evaluate comparison"
        )),
    }
}

/// Resolve a single term: replace variables with their bindings.
fn resolve_term(term: &Term, bindings: &Bindings) -> BoundTerm {
    match term {
        Term::Variable(name) => match bindings.get(name) {
            Some(val) => BoundTerm::Concrete(val.clone()),
            None => BoundTerm::Unbound(name.clone()),
        },
        Term::Placeholder => BoundTerm::Unbound("_".to_string()),
        _ => match term_to_value(term) {
            Some(v) => BoundTerm::Concrete(v),
            None => BoundTerm::Unbound(format!("{term:?}")),
        },
    }
}

/// Convert an AST Term constant to a runtime Value (public accessor).
pub fn term_to_value_pub(term: &Term) -> Option<Value> {
    term_to_value(term)
}

/// Resolve a term using bindings and format for display (public accessor).
pub fn resolve_term_pub(term: &Term, bindings: &Bindings) -> String {
    match resolve_term(term, bindings) {
        BoundTerm::Concrete(v) => format!("{v}"),
        BoundTerm::Unbound(var) => var,
    }
}

/// Convert an AST Term constant to a runtime Value.
fn term_to_value(term: &Term) -> Option<Value> {
    match term {
        Term::Constant(n) => {
            // Match engine behavior: small values as Int32, large as Int64
            if i32::try_from(*n).is_ok() {
                Some(Value::Int32(*n as i32))
            } else {
                Some(Value::Int64(*n))
            }
        }
        Term::FloatConstant(f) => Some(Value::Float64(*f)),
        Term::StringConstant(s) => Some(Value::String(Arc::from(s.as_str()))),
        Term::BoolConstant(b) => Some(Value::Bool(*b)),
        Term::VectorLiteral(v) => {
            let f32_vec: Vec<f32> = v.iter().map(|x| *x as f32).collect();
            Some(Value::Vector(Arc::new(f32_vec)))
        }
        // Function calls, aggregates, arithmetic cannot be directly converted
        other => {
            tracing::debug!("term_to_value: unhandled term variant {:?}", other);
            None
        }
    }
}

/// Compare two Values for equality, handling cross-type numeric comparisons.
#[allow(clippy::float_cmp)]
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => x == y,
        (Value::Int64(x), Value::Int64(y)) => x == y,
        (Value::Int32(x), Value::Int64(y)) => i64::from(*x) == *y,
        (Value::Int64(x), Value::Int32(y)) => *x == i64::from(*y),
        (Value::Float64(x), Value::Float64(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Null, Value::Null) => true,
        (Value::Timestamp(x), Value::Timestamp(y)) => x == y,
        _ => a == b,
    }
}

/// Compare two Values for ordering.
fn value_cmp(a: &Value, b: &Value) -> Result<std::cmp::Ordering, String> {
    match (a, b) {
        (Value::Int32(x), Value::Int32(y)) => Ok(x.cmp(y)),
        (Value::Int64(x), Value::Int64(y)) => Ok(x.cmp(y)),
        (Value::Int32(x), Value::Int64(y)) => Ok(i64::from(*x).cmp(y)),
        (Value::Int64(x), Value::Int32(y)) => Ok(x.cmp(&i64::from(*y))),
        (Value::Float64(x), Value::Float64(y)) => {
            x.partial_cmp(y).ok_or_else(|| "NaN comparison".to_string())
        }
        (Value::String(x), Value::String(y)) => Ok(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Ok(x.cmp(y)),
        _ => Ok(a.cmp(b)),
    }
}

/// Format a list of BoundTerms for display in proof trees.
pub fn format_bound_terms(terms: &[BoundTerm]) -> String {
    terms
        .iter()
        .map(|bt| match bt {
            BoundTerm::Concrete(v) => format!("{v}"),
            BoundTerm::Unbound(var) => var.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tuple(vals: Vec<Value>) -> Tuple {
        Tuple::new(vals)
    }

    fn int_val(v: i32) -> Value {
        Value::Int32(v)
    }

    fn base_data_with(entries: Vec<(&str, Vec<Vec<Value>>)>) -> HashMap<String, Vec<Tuple>> {
        entries
            .into_iter()
            .map(|(name, rows)| {
                (
                    name.to_string(),
                    rows.into_iter().map(|r| Tuple::new(r)).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn test_unify_all_variables() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
            ],
        };
        let tuple = make_tuple(vec![int_val(1), int_val(2)]);
        let bindings = unify_head(&tuple, &head).expect("should unify");
        assert_eq!(bindings.get("X"), Some(&int_val(1)));
        assert_eq!(bindings.get("Y"), Some(&int_val(2)));
    }

    #[test]
    fn test_unify_with_constants() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![Term::Constant(1), Term::Variable("Y".to_string())],
        };
        let tuple = make_tuple(vec![int_val(1), int_val(42)]);
        let bindings = unify_head(&tuple, &head).expect("should unify");
        assert_eq!(bindings.get("Y"), Some(&int_val(42)));
        assert!(bindings.get("X").is_none());
    }

    #[test]
    fn test_unify_constant_mismatch() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![Term::Constant(1), Term::Variable("Y".to_string())],
        };
        let tuple = make_tuple(vec![int_val(2), int_val(3)]);
        assert!(unify_head(&tuple, &head).is_none());
    }

    #[test]
    fn test_unify_repeated_variable() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![
                Term::Variable("X".to_string()),
                Term::Variable("X".to_string()),
            ],
        };
        let tuple = make_tuple(vec![int_val(5), int_val(5)]);
        let bindings = unify_head(&tuple, &head).expect("should unify");
        assert_eq!(bindings.get("X"), Some(&int_val(5)));
    }

    #[test]
    fn test_unify_repeated_variable_conflict() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![
                Term::Variable("X".to_string()),
                Term::Variable("X".to_string()),
            ],
        };
        let tuple = make_tuple(vec![int_val(1), int_val(2)]);
        assert!(unify_head(&tuple, &head).is_none());
    }

    #[test]
    fn test_unify_arity_mismatch() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![Term::Variable("X".to_string())],
        };
        let tuple = make_tuple(vec![int_val(1), int_val(2)]);
        assert!(unify_head(&tuple, &head).is_none());
    }

    #[test]
    fn test_unify_with_placeholder() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![Term::Placeholder, Term::Variable("Y".to_string())],
        };
        let tuple = make_tuple(vec![int_val(99), int_val(42)]);
        let bindings = unify_head(&tuple, &head).expect("should unify");
        assert_eq!(bindings.get("Y"), Some(&int_val(42)));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn test_unify_with_string_constant() {
        let head = Atom {
            relation: "r".to_string(),
            args: vec![
                Term::StringConstant("alice".to_string()),
                Term::Variable("Y".to_string()),
            ],
        };
        let tuple = make_tuple(vec![Value::String(Arc::from("alice")), int_val(1)]);
        let bindings = unify_head(&tuple, &head).expect("should unify");
        assert_eq!(bindings.get("Y"), Some(&int_val(1)));
    }

    #[test]
    fn test_substitute_atom_partial() {
        let atom = Atom {
            relation: "edge".to_string(),
            args: vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
            ],
        };
        let mut bindings = Bindings::new();
        bindings.insert("X".to_string(), int_val(1));

        let bound = substitute_atom(&atom, &bindings);
        assert!(matches!(bound[0], BoundTerm::Concrete(Value::Int32(1))));
        assert!(matches!(bound[1], BoundTerm::Unbound(ref s) if s == "Y"));
    }

    #[test]
    fn test_substitute_atom_fully_bound() {
        let atom = Atom {
            relation: "edge".to_string(),
            args: vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
            ],
        };
        let mut bindings = Bindings::new();
        bindings.insert("X".to_string(), int_val(1));
        bindings.insert("Y".to_string(), int_val(2));

        let bound = substitute_atom(&atom, &bindings);
        assert!(matches!(bound[0], BoundTerm::Concrete(Value::Int32(1))));
        assert!(matches!(bound[1], BoundTerm::Concrete(Value::Int32(2))));
    }

    #[test]
    fn test_evaluate_comparison_eq() {
        let mut bindings = Bindings::new();
        bindings.insert("X".to_string(), int_val(5));
        let result = evaluate_comparison(
            &Term::Variable("X".to_string()),
            &ComparisonOp::Equal,
            &Term::Constant(5),
            &bindings,
        )
        .expect("should evaluate");
        assert!(result);
    }

    #[test]
    fn test_evaluate_comparison_neq() {
        let mut bindings = Bindings::new();
        bindings.insert("X".to_string(), int_val(1));
        bindings.insert("Y".to_string(), int_val(2));
        let result = evaluate_comparison(
            &Term::Variable("X".to_string()),
            &ComparisonOp::NotEqual,
            &Term::Variable("Y".to_string()),
            &bindings,
        )
        .expect("should evaluate");
        assert!(result);
    }

    #[test]
    fn test_evaluate_comparison_lt_gt() {
        let mut bindings = Bindings::new();
        bindings.insert("X".to_string(), int_val(3));

        assert!(evaluate_comparison(
            &Term::Variable("X".to_string()),
            &ComparisonOp::GreaterThan,
            &Term::Constant(2),
            &bindings,
        )
        .expect("should evaluate"));

        assert!(evaluate_comparison(
            &Term::Variable("X".to_string()),
            &ComparisonOp::LessThan,
            &Term::Constant(10),
            &bindings,
        )
        .expect("should evaluate"));
    }

    #[test]
    fn test_evaluate_comparison_unbound_error() {
        let bindings = Bindings::new();
        let result = evaluate_comparison(
            &Term::Variable("X".to_string()),
            &ComparisonOp::Equal,
            &Term::Constant(5),
            &bindings,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unbound"));
    }

    #[test]
    fn test_find_matching_fully_bound() {
        let data = base_data_with(vec![(
            "edge",
            vec![
                vec![int_val(1), int_val(2)],
                vec![int_val(2), int_val(3)],
                vec![int_val(1), int_val(3)],
            ],
        )]);

        let pattern = vec![
            BoundTerm::Concrete(int_val(1)),
            BoundTerm::Concrete(int_val(2)),
        ];
        let results = find_matching_tuples("edge", &pattern, &data);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_find_matching_partially_bound() {
        let data = base_data_with(vec![(
            "edge",
            vec![
                vec![int_val(1), int_val(2)],
                vec![int_val(2), int_val(3)],
                vec![int_val(1), int_val(3)],
            ],
        )]);

        let pattern = vec![
            BoundTerm::Concrete(int_val(1)),
            BoundTerm::Unbound("Y".to_string()),
        ];
        let results = find_matching_tuples("edge", &pattern, &data);
        assert_eq!(results.len(), 2);
        // Both (1,2) and (1,3) should match
        let y_values: Vec<&Value> = results.iter().map(|(_, b)| b.get("Y").unwrap()).collect();
        assert!(y_values.contains(&&int_val(2)));
        assert!(y_values.contains(&&int_val(3)));
    }

    #[test]
    fn test_find_matching_empty_relation() {
        let data = HashMap::new();
        let pattern = vec![BoundTerm::Concrete(int_val(1))];
        let results = find_matching_tuples("nonexistent", &pattern, &data);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_matching_all_unbound() {
        let data = base_data_with(vec![(
            "edge",
            vec![vec![int_val(1), int_val(2)], vec![int_val(3), int_val(4)]],
        )]);

        let pattern = vec![
            BoundTerm::Unbound("X".to_string()),
            BoundTerm::Unbound("Y".to_string()),
        ];
        let results = find_matching_tuples("edge", &pattern, &data);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_cross_type_numeric_equality() {
        // Int32(1) should equal Int64(1) for matching purposes
        assert!(values_equal(&Value::Int32(1), &Value::Int64(1)));
        assert!(values_equal(&Value::Int64(42), &Value::Int32(42)));
        assert!(!values_equal(&Value::Int32(1), &Value::Int64(2)));
    }

    #[test]
    fn test_format_bound_terms() {
        let terms = vec![
            BoundTerm::Concrete(int_val(1)),
            BoundTerm::Unbound("Y".to_string()),
            BoundTerm::Concrete(Value::String(Arc::from("hello"))),
        ];
        let s = format_bound_terms(&terms);
        assert!(s.contains("1"), "got: {s}");
        assert!(s.contains("Y"), "got: {s}");
        assert!(s.contains("\"hello\""), "got: {s}");
    }
}
