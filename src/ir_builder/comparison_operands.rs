//! Comparison operand hoisting.
//!
//! IR lowering turns a body comparison into one of two things:
//!
//! - an **assignment** `V = <expr>` that binds a variable not bound yet (a
//!   `Compute` column), or
//! - a **filter** whose sides are columns and literals, plus the runtime
//!   column-vs-arithmetic and arithmetic-vs-integer shapes.
//!
//! A filter whose side is a function call (or arithmetic opposite anything
//! but a column or an integer) has no direct filter shape. This pass rewrites
//! such a comparison so each expression side is bound to a fresh variable by
//! an assignment, and the comparison then filters on the fresh columns:
//!
//! ```text
//! concat(A, "|", P) != concat(B, "|", Q)
//!   =>  $cmp0 = concat(A, "|", P), $cmp1 = concat(B, "|", Q), $cmp0 != $cmp1
//! ```
//!
//! That is exactly how the explicitly bound form lowers, so both spellings
//! share one evaluation path. A bound variable equated to a function call
//! (`Y = upper(X)` with `Y` from a body atom) is a filter too and is hoisted
//! the same way instead of being mistaken for an assignment.
//!
//! [`assignment_target`] is the single classifier of assignments: the hoisting
//! decision here and `IRBuilder::build_computed_columns` both use it, so a
//! comparison is never both skipped as an assignment and left unfiltered.

use crate::ast::{BodyPredicate, ComparisonOp, Rule, Term};
use std::borrow::Cow;

/// Prefix of the variables this pass introduces. `$` cannot start an IQL
/// variable, so a hoisted column never captures or shadows a user variable.
const HOISTED_PREFIX: &str = "$cmp";

/// The variable a comparison assigns, given the variables bound before it.
///
/// Mirrors the order-sensitive binding of computed columns: `V = <function
/// call | arithmetic | literal>` with `V` unbound, or `V1 = V2` with exactly
/// one side bound. Every other comparison is a filter.
pub(super) fn assignment_target<'a>(
    left: &'a Term,
    op: &ComparisonOp,
    right: &'a Term,
    bound: &[String],
) -> Option<&'a str> {
    if !matches!(op, ComparisonOp::Equal) {
        return None;
    }
    let is_bound = |v: &String| bound.contains(v);
    match (left, right) {
        (Term::Variable(a), Term::Variable(b)) => match (is_bound(a), is_bound(b)) {
            (false, true) => Some(a),
            (true, false) => Some(b),
            _ => None,
        },
        (Term::Variable(v), value) | (value, Term::Variable(v))
            if !is_bound(v) && is_assignable(value) =>
        {
            Some(v)
        }
        _ => None,
    }
}

/// Terms a computed column can be assigned from.
fn is_assignable(term: &Term) -> bool {
    matches!(
        term,
        Term::FunctionCall(..)
            | Term::Arithmetic(_)
            | Term::Constant(_)
            | Term::FloatConstant(_)
            | Term::StringConstant(_)
            | Term::BoolConstant(_)
    )
}

/// Whether `term` must be bound to a column before comparing it with `other`.
fn needs_hoisting(term: &Term, other: &Term) -> bool {
    match term {
        Term::FunctionCall(..) => true,
        Term::Arithmetic(_) => !matches!(other, Term::Variable(_) | Term::Constant(_)),
        _ => false,
    }
}

/// Rewrite filter comparisons with expression operands into assignments of
/// fresh variables plus a filter over them. `bound` holds the variables bound
/// by the body atoms (the schema before computed columns). Rules without such
/// a comparison are returned unchanged and unallocated.
///
/// Each rewritten filter moves to the end of the body, preceded by its
/// hoisting assignments, so every operand variable is bound when the
/// assignments are computed. Filters apply after all computed columns, so the
/// move does not change the rule's meaning.
pub(super) fn hoist_comparison_operands<'a>(rule: &'a Rule, bound: &[String]) -> Cow<'a, Rule> {
    let has_expression_operand = rule.body.iter().any(|pred| {
        matches!(pred, BodyPredicate::Comparison(left, _, right)
            if needs_hoisting(left, right) || needs_hoisting(right, left))
    });
    if !has_expression_operand {
        return Cow::Borrowed(rule);
    }

    let mut bound = bound.to_vec();
    let mut kept = Vec::with_capacity(rule.body.len());
    let mut hoisted = Vec::new();
    let mut fresh = 0usize;

    for pred in &rule.body {
        let BodyPredicate::Comparison(left, op, right) = pred else {
            kept.push(pred.clone());
            continue;
        };
        if let Some(target) = assignment_target(left, op, right, &bound) {
            bound.push(target.to_string());
            kept.push(pred.clone());
            continue;
        }
        let hoist_left = needs_hoisting(left, right);
        let hoist_right = needs_hoisting(right, left);
        if !hoist_left && !hoist_right {
            kept.push(pred.clone());
            continue;
        }
        let mut operand = |term: &Term, hoist: bool| -> Term {
            if !hoist {
                return term.clone();
            }
            let var = Term::Variable(format!("{HOISTED_PREFIX}{fresh}"));
            fresh += 1;
            hoisted.push(BodyPredicate::Comparison(
                var.clone(),
                ComparisonOp::Equal,
                term.clone(),
            ));
            var
        };
        let left = operand(left, hoist_left);
        let right = operand(right, hoist_right);
        hoisted.push(BodyPredicate::Comparison(left, op.clone(), right));
    }

    if hoisted.is_empty() {
        return Cow::Borrowed(rule);
    }
    kept.extend(hoisted);
    let mut rewritten = rule.clone();
    rewritten.body = kept;
    Cow::Owned(rewritten)
}

/// Variables introduced by [`hoist_comparison_operands`] in `rule`.
#[cfg(test)]
fn hoisted_variables(rule: &Rule) -> std::collections::HashSet<String> {
    rule.body
        .iter()
        .flat_map(BodyPredicate::variables)
        .filter(|v| v.starts_with(HOISTED_PREFIX))
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::parser::parse_rule;

    fn hoist(rule_str: &str, bound: &[&str]) -> (Rule, Rule) {
        let rule = parse_rule(rule_str).unwrap();
        let bound: Vec<String> = bound.iter().map(|v| v.to_string()).collect();
        let rewritten = hoist_comparison_operands(&rule, &bound).into_owned();
        (rule, rewritten)
    }

    /// Parse an expected rule where `#cmpN` spells a hoisted variable. `$` is
    /// not valid IQL, so parse a placeholder spelling and rename it.
    fn expect(rule_str: &str) -> Rule {
        let mut rule = parse_rule(&rule_str.replace("#cmp", "H_")).unwrap();
        rename(&mut rule, "H_", HOISTED_PREFIX);
        rule
    }

    fn rename(rule: &mut Rule, from: &str, to: &str) {
        fn term(t: &mut Term, from: &str, to: &str) {
            match t {
                Term::Variable(v) if v.starts_with(from) => *v = v.replacen(from, to, 1),
                Term::FunctionCall(_, args) => args.iter_mut().for_each(|a| term(a, from, to)),
                _ => {}
            }
        }
        for pred in &mut rule.body {
            if let BodyPredicate::Comparison(l, _, r) = pred {
                term(l, from, to);
                term(r, from, to);
            }
        }
    }

    #[test]
    fn function_calls_on_both_sides_are_hoisted() {
        let (_, rewritten) = hoist(
            r#"c(S) <- e(S, A, P), e(S, B, Q), concat(A, "|", P) != concat(B, "|", Q)"#,
            &["S", "A", "P", "B", "Q"],
        );
        assert_eq!(
            rewritten,
            expect(
                r#"c(S) <- e(S, A, P), e(S, B, Q), #cmp0 = concat(A, "|", P), #cmp1 = concat(B, "|", Q), #cmp0 != #cmp1"#
            )
        );
    }

    #[test]
    fn bound_variable_equated_to_function_call_is_a_filter() {
        let (_, rewritten) = hoist("f(X, Y) <- w(X, Y), Y = upper(X)", &["X", "Y"]);
        assert_eq!(
            rewritten,
            expect("f(X, Y) <- w(X, Y), #cmp0 = upper(X), Y = #cmp0")
        );
    }

    #[test]
    fn function_call_against_literal_is_hoisted() {
        let (_, rewritten) = hoist("f(X) <- w(X), len(X) < 2", &["X"]);
        assert_eq!(rewritten, expect("f(X) <- w(X), #cmp0 = len(X), #cmp0 < 2"));
    }

    #[test]
    fn arithmetic_against_arithmetic_is_hoisted() {
        let (_, rewritten) = hoist("f(N) <- n(N), N + 1 != N * 2", &["N"]);
        assert_eq!(hoisted_variables(&rewritten).len(), 2);
    }

    #[test]
    fn assignments_and_native_filters_are_unchanged() {
        for (rule_str, bound) in [
            (
                r#"a(S, C) <- e(S, O, P), C = concat(O, "|", P)"#,
                &["S", "O", "P"][..],
            ),
            ("a(X, Y) <- n(X), Y = X * 2, Y > 4", &["X"][..]),
            ("a(X) <- n(X, Y), X + 1 = Y", &["X", "Y"][..]),
            ("a(X) <- n(X), X * 2 > 4", &["X"][..]),
            ("a(X, Y) <- n(X, Y), X < Y", &["X", "Y"][..]),
        ] {
            let rule = parse_rule(rule_str).unwrap();
            let bound: Vec<String> = bound.iter().map(|v| v.to_string()).collect();
            assert!(
                matches!(hoist_comparison_operands(&rule, &bound), Cow::Borrowed(_)),
                "{rule_str}"
            );
        }
    }

    #[test]
    fn assignment_target_follows_binding_order() {
        let x = Term::Variable("X".to_string());
        let y = Term::Variable("Y".to_string());
        let call = parse_rule("a(Y) <- n(X), Y = upper(X)").unwrap().body[1].clone();
        let BodyPredicate::Comparison(_, _, call) = call else {
            unreachable!()
        };
        let eq = ComparisonOp::Equal;
        let bound_x = ["X".to_string()];
        let bound_xy = ["X".to_string(), "Y".to_string()];
        assert_eq!(assignment_target(&y, &eq, &call, &bound_x), Some("Y"));
        assert_eq!(assignment_target(&call, &eq, &y, &bound_x), Some("Y"));
        assert_eq!(assignment_target(&y, &eq, &call, &bound_xy), None);
        assert_eq!(assignment_target(&y, &eq, &x, &bound_x), Some("Y"));
        assert_eq!(assignment_target(&y, &eq, &x, &bound_xy), None);
        assert_eq!(
            assignment_target(&y, &ComparisonOp::NotEqual, &call, &bound_x),
            None
        );
    }
}
