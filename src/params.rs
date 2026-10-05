//! Parameterised programs: values bound to `$name` references, sent beside
//! the program text instead of inside it.
//!
//! A client sends `+eta($shipment, $date)` with the [`Params`]
//! `{"shipment": "S-77", "date": "2026-10-10"}`. The parser reads each
//! `$name` as [`Term::Param`] (or [`ArithExpr::Param`] inside arithmetic);
//! [`bind_statement`] then replaces it with the constant term its value
//! denotes, on the parsed statements, before they are authorized or run. A
//! value is never lexed, parsed or written into IQL text: what runs is the
//! bound syntax tree, so no value can change a program's structure, whatever
//! characters it holds.
//!
//! A parameter stands where a literal may stand (an atom argument, a
//! comparison or arithmetic operand, a function argument, an `hnsw_nearest`
//! query vector) and binds exactly the term its literal would: an int binds
//! `Term::Constant`, a float `Term::FloatConstant`, and so on. It cannot name
//! a relation or variable, set an aggregate's or `limit`'s arguments, or
//! appear in a meta command. Inside a string literal, `"$name"` is text.
//!
//! Nothing evaluates an unbound parameter: the IR builder refuses a rule that
//! still holds one ([`refuse_unbound`]), and so does every consumer of a
//! statement's constants.

use std::collections::BTreeSet;

use crate::ast::{ArithExpr, Atom, BodyPredicate, Rule, Term};
use crate::statement::{DeletePattern, MetaCommand, Statement};
pub use inputlayer_ws_protocol::{ParamValue, Params};

/// Replace every parameter in `statement` with its value from `params`,
/// recording the names it used in `used`. Fails, leaving `statement` partly
/// bound, on the first parameter without a value or whose value cannot
/// stand where it is referenced.
pub fn bind_statement(
    statement: &mut Statement,
    params: &Params,
    used: &mut BTreeSet<String>,
) -> Result<(), String> {
    let mut binder = Binder { params, used };
    match statement {
        Statement::Insert(op) => op
            .tuples
            .iter_mut()
            .flatten()
            .try_for_each(|term| binder.term(term)),
        Statement::Delete(op) => match &mut op.pattern {
            DeletePattern::SingleTuple(terms) => binder.terms(terms),
            DeletePattern::BulkTuples(rows) => {
                rows.iter_mut().try_for_each(|terms| binder.terms(terms))
            }
            DeletePattern::Conditional { head_args, body } => {
                binder.terms(head_args)?;
                binder.body(body)
            }
        },
        Statement::Update(op) => {
            for target in &mut op.deletes {
                binder.terms(&mut target.args)?;
            }
            for target in &mut op.inserts {
                binder.terms(&mut target.args)?;
            }
            binder.body(&mut op.body)
        }
        Statement::SessionRule(rule) | Statement::Fact(rule) | Statement::PersistentRule(rule) => {
            binder.rule(rule)
        }
        Statement::Query(goal) => {
            if let Some(atom) = &mut goal.goal {
                binder.atom(atom)?;
            }
            binder.body(&mut goal.body)
        }
        Statement::Meta(command) => match embedded_iql(command) {
            Some(iql) if references_params(iql) => Err(META_PARAMS.to_string()),
            _ => Ok(()),
        },
        Statement::TypeDecl(_) | Statement::SchemaDecl(_) | Statement::DeleteRelationOrRule(_) => {
            Ok(())
        }
    }
}

/// Why a meta command with a `$name` in its IQL is refused.
pub const META_PARAMS: &str = "Meta commands take no parameters: \
     `$name` references are bound only in IQL statements (facts, rules, queries, \
     inserts, deletes and updates), not in the IQL of `.why`, `.debug`, `.subscribe` \
     or `.rule edit`";

/// The IQL text a meta command carries, parsed only when it runs.
pub fn embedded_iql(command: &MetaCommand) -> Option<&str> {
    match command {
        MetaCommand::Debug(iql)
        | MetaCommand::Why(iql)
        | MetaCommand::WhyFull(iql)
        | MetaCommand::WhyNot(iql)
        | MetaCommand::Subscribe { query: iql, .. }
        | MetaCommand::RuleEdit { rule_text: iql, .. } => Some(iql),
        _ => None,
    }
}

/// Whether IQL text holds a `$` outside string literals: a parameter
/// reference, or a syntax error either way.
pub fn references_params(iql: &str) -> bool {
    iql.contains('$') && crate::parser::lexer::code_chars(iql).any(|(_, c)| c == '$')
}

/// The parameters in `params` that `used` does not name, in name order.
pub fn unused<'a>(params: &'a Params, used: &BTreeSet<String>) -> Vec<&'a str> {
    params
        .iter()
        .map(|(name, _)| name)
        .filter(|name| !used.contains(*name))
        .collect()
}

/// Refuse a rule that still references a parameter: nothing evaluates one
/// without its value.
pub fn refuse_unbound(rule: &Rule) -> Result<(), String> {
    match first_param_in_rule(rule) {
        Some(name) => Err(unbound(name)),
        None => Ok(()),
    }
}

/// The message for parameter `name` reached without a value.
pub fn unbound(name: &str) -> String {
    format!("Parameter ${name} has no value: send it in the request's params")
}

/// The first parameter `rule` references, if any.
pub fn first_param_in_rule(rule: &Rule) -> Option<&str> {
    rule.head
        .args
        .iter()
        .find_map(first_param)
        .or_else(|| rule.body.iter().find_map(first_param_in_predicate))
}

fn first_param_in_predicate(predicate: &BodyPredicate) -> Option<&str> {
    match predicate {
        BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => {
            atom.args.iter().find_map(first_param)
        }
        BodyPredicate::Comparison(left, _, right) => {
            first_param(left).or_else(|| first_param(right))
        }
        BodyPredicate::HnswNearest { query, .. } => first_param(query),
    }
}

/// The first parameter `term` references, if any.
pub fn first_param(term: &Term) -> Option<&str> {
    match term {
        Term::Param(name) => Some(name),
        Term::Arithmetic(expr) => first_param_in_arith(expr),
        Term::FunctionCall(_, args) => args.iter().find_map(first_param),
        Term::FieldAccess(base, _) => first_param(base),
        Term::RecordPattern(fields) => fields.iter().find_map(|(_, term)| first_param(term)),
        Term::Variable(_)
        | Term::Constant(_)
        | Term::Placeholder
        | Term::Aggregate(_, _)
        | Term::VectorLiteral(_)
        | Term::FloatConstant(_)
        | Term::StringConstant(_)
        | Term::BoolConstant(_) => None,
    }
}

fn first_param_in_arith(expr: &ArithExpr) -> Option<&str> {
    match expr {
        ArithExpr::Param(name) => Some(name),
        ArithExpr::Binary { left, right, .. } => {
            first_param_in_arith(left).or_else(|| first_param_in_arith(right))
        }
        ArithExpr::Variable(_) | ArithExpr::Constant(_) | ArithExpr::FloatConstant(_) => None,
    }
}

/// Replaces parameters with their values, noting which were used.
struct Binder<'a> {
    params: &'a Params,
    used: &'a mut BTreeSet<String>,
}

impl Binder<'_> {
    fn value(&mut self, name: &str) -> Result<&ParamValue, String> {
        let value = self.params.get(name).ok_or_else(|| unbound(name))?;
        if let Some(why) = value.invalid() {
            return Err(format!("Parameter ${name}: {why}"));
        }
        self.used.insert(name.to_string());
        Ok(value)
    }

    fn rule(&mut self, rule: &mut Rule) -> Result<(), String> {
        self.atom(&mut rule.head)?;
        self.body(&mut rule.body)
    }

    fn atom(&mut self, atom: &mut Atom) -> Result<(), String> {
        self.terms(&mut atom.args)
    }

    fn body(&mut self, body: &mut [BodyPredicate]) -> Result<(), String> {
        body.iter_mut().try_for_each(|predicate| match predicate {
            BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => self.atom(atom),
            BodyPredicate::Comparison(left, _, right) => {
                self.term(left)?;
                self.term(right)
            }
            BodyPredicate::HnswNearest { query, .. } => self.term(query),
        })
    }

    fn terms(&mut self, terms: &mut [Term]) -> Result<(), String> {
        terms.iter_mut().try_for_each(|term| self.term(term))
    }

    fn term(&mut self, term: &mut Term) -> Result<(), String> {
        match term {
            Term::Param(name) => {
                *term = match self.value(name)? {
                    ParamValue::Int(n) => Term::Constant(*n),
                    ParamValue::Float(f) => Term::FloatConstant(*f),
                    ParamValue::String(s) => Term::StringConstant(s.clone()),
                    ParamValue::Bool(b) => Term::BoolConstant(*b),
                    ParamValue::Vector(v) => Term::VectorLiteral(v.clone()),
                };
                Ok(())
            }
            Term::Arithmetic(expr) => self.arith(expr),
            Term::FunctionCall(_, args) => self.terms(args),
            Term::FieldAccess(base, _) => self.term(base),
            Term::RecordPattern(fields) => fields
                .iter_mut()
                .try_for_each(|(_, field)| self.term(field)),
            Term::Variable(_)
            | Term::Constant(_)
            | Term::Placeholder
            | Term::Aggregate(_, _)
            | Term::VectorLiteral(_)
            | Term::FloatConstant(_)
            | Term::StringConstant(_)
            | Term::BoolConstant(_) => Ok(()),
        }
    }

    fn arith(&mut self, expr: &mut ArithExpr) -> Result<(), String> {
        match expr {
            ArithExpr::Param(name) => {
                *expr = match self.value(name)? {
                    ParamValue::Int(n) => ArithExpr::Constant(*n),
                    ParamValue::Float(f) => ArithExpr::from_float(*f),
                    other => {
                        return Err(format!(
                            "Parameter ${name} is a {}: arithmetic takes an int or a float",
                            other.type_name()
                        ))
                    }
                };
                Ok(())
            }
            ArithExpr::Binary { left, right, .. } => {
                self.arith(left)?;
                self.arith(right)
            }
            ArithExpr::Variable(_) | ArithExpr::Constant(_) | ArithExpr::FloatConstant(_) => Ok(()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::statement::parse_statement;

    fn params(json: &str) -> Params {
        serde_json::from_str(json).unwrap()
    }

    /// `statement` bound with `params`, displayed, and the names it used.
    fn bind(statement: &str, json: &str) -> Result<(Statement, Vec<String>), String> {
        let mut statement = parse_statement(statement).unwrap();
        let mut used = BTreeSet::new();
        bind_statement(&mut statement, &params(json), &mut used)?;
        Ok((statement, used.into_iter().collect()))
    }

    #[test]
    fn insert_binds_each_value_as_its_literal_term() {
        let (statement, used) = bind(
            "+r($i, $f, $s, $b, $v)",
            r#"{"i": 7, "f": 0.5, "s": "a\"b", "b": true, "v": [1, 2.5]}"#,
        )
        .unwrap();
        let Statement::Insert(op) = statement else {
            panic!("not an insert");
        };
        assert_eq!(
            op.tuples,
            vec![vec![
                Term::Constant(7),
                Term::FloatConstant(0.5),
                Term::StringConstant("a\"b".into()),
                Term::BoolConstant(true),
                Term::VectorLiteral(vec![1.0, 2.5]),
            ]]
        );
        assert_eq!(used, ["b", "f", "i", "s", "v"]);
    }

    #[test]
    fn a_value_is_never_syntax() {
        // Each would change the statement if it were spliced into the text.
        for hostile in [
            r#"x"), evil(X) <- r(X"#,
            "a\nb",
            "$other",
            "_",
            "X",
            "1, 2",
            r"\",
            "\"",
        ] {
            let json = serde_json::json!({ "x": hostile }).to_string();
            let (statement, _) = bind("-r($x)", &json).unwrap();
            let Statement::Delete(op) = statement else {
                panic!("not a delete");
            };
            assert!(
                matches!(&op.pattern, DeletePattern::SingleTuple(t)
                    if t == &[Term::StringConstant(hostile.into())]),
                "{hostile:?}: {:?}",
                op.pattern
            );
        }
    }

    #[test]
    fn rules_queries_and_updates_bind_everywhere_a_literal_stands() {
        let json = r#"{"a": 1, "b": "x", "c": 2.5, "v": [0.5]}"#;
        for (statement, bound) in [
            (
                "+r(X, $a) <- s(X, $b), X > $a, !t(X, $b)",
                r#"r(X, 1) <- s(X, "x"), X > 1, !t(X, "x")"#,
            ),
            (
                "q(X) <- s(X, Y), Y = $a + X * $c",
                "q(X) <- s(X, Y), Y = 1+X*2.5",
            ),
            (
                "q(X) <- s(X, Y), Y = abs($a)",
                "q(X) <- s(X, Y), Y = abs(1)",
            ),
            (
                r#"q(I, D) <- hnsw_nearest("idx", $v, 3, I, D)"#,
                r#"q(I, D) <- hnsw_nearest("idx", [0.5], 3, I, D)"#,
            ),
        ] {
            let (statement, _) = bind(statement, json).unwrap();
            let rule = match statement {
                Statement::PersistentRule(rule) | Statement::SessionRule(rule) => rule,
                other => panic!("not a rule: {other:?}"),
            };
            assert_eq!(rule.to_string(), bound);
            assert!(first_param_in_rule(&rule).is_none());
        }
        let (statement, _) = bind("?s($a, Y), Y != $b", json).unwrap();
        let Statement::Query(goal) = statement else {
            panic!("not a query");
        };
        assert_eq!(goal.goal.unwrap().args[0], Term::Constant(1));
        assert_eq!(goal.body[0].to_string(), r#"Y != "x""#);
        let (statement, _) = bind(r#"-r(X, $b), +r(X, $a) <- r(X, "old")"#, json).unwrap();
        let Statement::Update(op) = statement else {
            panic!("not an update");
        };
        assert_eq!(op.deletes[0].args[1], Term::StringConstant("x".into()));
        assert_eq!(op.inserts[0].args[1], Term::Constant(1));
        let (statement, _) = bind("-r(X, $a) <- r(X, $a), s(X, $b)", json).unwrap();
        let Statement::Delete(op) = statement else {
            panic!("not a delete");
        };
        let DeletePattern::Conditional { head_args, body } = op.pattern else {
            panic!("not conditional");
        };
        assert_eq!(head_args[1], Term::Constant(1));
        assert_eq!(body[1].to_string(), r#"s(X, "x")"#);
    }

    #[test]
    fn a_missing_or_misplaced_value_is_refused() {
        let error = bind("+r($a, $b)", r#"{"a": 1}"#).unwrap_err();
        assert!(error.contains("$b has no value"), "{error}");
        let error = bind("q(X) <- s(X, Y), Y = X + $s", r#"{"s": "1"}"#).unwrap_err();
        assert!(error.contains("$s is a string"), "{error}");
        let error = bind("q(X) <- s(X, Y), Y = X + $s", r#"{"s": [1]}"#).unwrap_err();
        assert!(error.contains("$s is a vector"), "{error}");
        let mut nan = Params::new();
        nan.insert("f", f64::NAN).unwrap();
        let mut statement = parse_statement("+r($f)").unwrap();
        let error = bind_statement(&mut statement, &nan, &mut BTreeSet::new()).unwrap_err();
        assert!(error.contains("not finite"), "{error}");
    }

    #[test]
    fn meta_commands_refuse_parameters_in_their_iql() {
        for statement in [
            ".why ?r($x)",
            ".why full ?r($x)",
            ".debug ?r($x)",
            ".subscribe s ?r($x)",
        ] {
            let error = bind(statement, r#"{"x": 1}"#).unwrap_err();
            assert_eq!(error, META_PARAMS, "{statement}");
        }
        // `$` in a string literal is text, and other meta commands are untouched.
        assert!(bind(r#".why ?r("$x")"#, "{}").is_ok());
        assert!(bind(".kg use other", "{}").is_ok());
    }

    #[test]
    fn unused_names_the_parameters_never_referenced() {
        let params = params(r#"{"a": 1, "b": 2, "c": 3}"#);
        let used = BTreeSet::from(["b".to_string()]);
        assert_eq!(unused(&params, &used), ["a", "c"]);
    }

    #[test]
    fn refuse_unbound_finds_nested_references() {
        for rule in [
            "q(X) <- s(X, $a)",
            "q($a) <- s(X)",
            "q(X) <- s(X, Y), Y = X * ($a + 1)",
            "q(X) <- s(X, Y), Y = concat(X, $a)",
            "q(X) <- s(X), !t(X, $a)",
        ] {
            let rule = crate::parser::parse_rule(rule).unwrap();
            assert_eq!(refuse_unbound(&rule), Err(unbound("a")), "{rule}");
        }
        let rule = crate::parser::parse_rule(r#"q(X) <- s(X, "$a")"#).unwrap();
        assert!(refuse_unbound(&rule).is_ok());
    }
}
