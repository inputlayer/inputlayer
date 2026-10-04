//! The transaction boundary of a program.
//!
//! A program that changes persistent state commits all of those changes as one
//! transaction on its knowledge graph: facts (`+`, `-`, update), schema
//! declarations, persistent rules and rule removals (`-name`, `.rule drop`,
//! `.rule drop prefix`, `.rule remove`, `.rule clear`). Such a program may also
//! hold statements whose effects stay inside the request (session facts and
//! rules, type declarations, queries), but nothing else: commands that switch,
//! create or drop knowledge graphs, drop or clear relations, manage indexes,
//! users, access, ontologies or subscriptions, compact storage or inspect state
//! act outside the transaction. A program that mixes them with writes is
//! rejected before any statement runs.
//!
//! A proof (`.why`, `.why full`, `.why_not`) may end a program that writes: it
//! is evaluated on the snapshot the program's writes committed against, so it
//! explains the state the program's guard passed on. It must be the program's
//! last statement, and the program cannot also hold a query.

use crate::statement::meta::MetaCommand;
use crate::statement::Statement;

/// How a statement relates to its program's transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Role {
    /// A change that commits with the program's transaction.
    Write,
    /// No effect beyond the request; joins any program.
    Local,
    /// Acts outside any transaction; cannot share a program with writes.
    Outside,
    /// Explains state; may end a program that writes.
    Proof,
}

/// The role of `statement` in its program.
pub(super) fn role(statement: &Statement) -> Role {
    match statement {
        Statement::Insert(_)
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::SchemaDecl(_)
        | Statement::PersistentRule(_)
        | Statement::DeleteRelationOrRule(_)
        | Statement::Meta(
            MetaCommand::RuleDrop(_)
            | MetaCommand::RuleDropPrefix(_)
            | MetaCommand::RuleRemove { .. }
            | MetaCommand::RuleClear(_),
        ) => Role::Write,
        Statement::Fact(_)
        | Statement::SessionRule(_)
        | Statement::Query(_)
        | Statement::TypeDecl(_)
        | Statement::Meta(MetaCommand::RuleQuery(_)) => Role::Local,
        Statement::Meta(MetaCommand::Why(_) | MetaCommand::WhyFull(_) | MetaCommand::WhyNot(_)) => {
            Role::Proof
        }
        Statement::Meta(_) => Role::Outside,
    }
}

/// Whether the program commits a transaction: it has at least one write.
pub(super) fn is_transactional(statements: &[Statement]) -> bool {
    statements.iter().any(|s| role(s) == Role::Write)
}

/// A program that mixes writes with a statement that cannot join their
/// transaction.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct BoundaryViolation {
    /// Index of the first statement that cannot join.
    pub index: usize,
    pub kind: ViolationKind,
}

/// Why a statement cannot join a program that writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ViolationKind {
    /// The statement acts outside any transaction.
    Outside,
    /// A proof that is not the program's last statement.
    ProofNotLast,
    /// A query in a program that ends with a proof.
    QueryWithProof,
}

impl BoundaryViolation {
    /// The message for statement `text`, the one at [`Self::index`].
    pub fn message(&self, text: &str) -> String {
        match self.kind {
            ViolationKind::Outside => format!(
                "'{text}' cannot run in a program that changes facts, rules or schemas: \
                 those changes commit together as one transaction, and this command \
                 cannot join it. Nothing was applied; send the command in a separate request."
            ),
            ViolationKind::ProofNotLast => format!(
                "'{text}' must be the last statement of a program that changes facts, \
                 rules or schemas: a proof there explains the state the program's writes \
                 committed against. Nothing was applied."
            ),
            ViolationKind::QueryWithProof => format!(
                "'{text}' cannot run in a program that changes facts, rules or schemas \
                 and ends with a proof: the program replies with its statements' messages \
                 and the proof. Nothing was applied; send the query in a separate request."
            ),
        }
    }
}

/// Check that every statement of a transactional program can join its
/// transaction.
pub(super) fn check(statements: &[Statement]) -> Result<(), BoundaryViolation> {
    if !is_transactional(statements) {
        return Ok(());
    }
    let violation = |index, kind| Err(BoundaryViolation { index, kind });
    let last = statements.len() - 1;
    for (index, statement) in statements.iter().enumerate() {
        match role(statement) {
            Role::Outside => return violation(index, ViolationKind::Outside),
            Role::Proof if index != last => return violation(index, ViolationKind::ProofNotLast),
            _ => {}
        }
    }
    if role(&statements[last]) == Role::Proof {
        if let Some(index) = statements
            .iter()
            .position(|s| {
                matches!(
                    s,
                    Statement::Query(_) | Statement::Meta(MetaCommand::RuleQuery(_))
                )
            })
        {
            return violation(index, ViolationKind::QueryWithProof);
        }
    }
    Ok(())
}

/// Whether `statements`, a program that passed [`check`], ends with a proof
/// evaluated on the snapshot its writes commit against.
pub(super) fn has_pinned_proof(statements: &[Statement]) -> bool {
    is_transactional(statements) && statements.last().is_some_and(|s| role(s) == Role::Proof)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn parse(program: &str) -> Vec<Statement> {
        program
            .lines()
            .map(|line| crate::statement::parse_statement(line).unwrap())
            .collect()
    }

    #[test]
    fn writes_join_with_request_local_statements() {
        let program = parse(
            "+r(a: int)\n\
             +r(1)\n\
             +v(X) <- r(X)\n\
             -old\n\
             .rule drop gone\n\
             .rule drop prefix tmp_\n\
             .rule remove v 1\n\
             .rule clear w\n\
             -r(X) <- r(X), X > 5\n\
             s(X) <- r(X)\n\
             f(1)\n\
             ?v(X)",
        );
        assert!(is_transactional(&program));
        assert_eq!(check(&program), Ok(()));
    }

    #[test]
    fn programs_without_writes_are_not_checked() {
        let program = parse("?a(X)\n.kg use other\n.rel\n?b(X)");
        assert!(!is_transactional(&program));
        assert_eq!(check(&program), Ok(()));
    }

    #[test]
    fn outside_commands_cannot_share_a_program_with_writes() {
        for outside in [
            ".kg use other",
            ".kg",
            ".rel",
            ".rel drop r",
            ".clear prefix r",
            ".index list",
            ".compact",
            ".rule list",
            ".status",
        ] {
            let program = parse(&format!("+r(1)\n{outside}\n+r(2)"));
            assert_eq!(
                check(&program),
                Err(BoundaryViolation {
                    index: 1,
                    kind: ViolationKind::Outside
                }),
                "{outside}"
            );
        }
    }

    #[test]
    fn a_proof_may_end_a_program_that_writes() {
        for proof in [".why ?v(X)", ".why full ?v(X)", ".why_not v(3)"] {
            let program = parse(&format!("+r(1)\ns(X) <- r(X)\n{proof}"));
            assert_eq!(check(&program), Ok(()), "{proof}");
            assert!(has_pinned_proof(&program), "{proof}");
        }
        assert!(!has_pinned_proof(&parse("+r(1)")));
        // Without writes a proof runs as it always has.
        assert!(!has_pinned_proof(&parse("?r(X)\n.why ?r(X)")));
    }

    #[test]
    fn a_proof_in_a_program_that_writes_must_end_it() {
        for (program, index) in [
            ("+r(1)\n.why ?r(X)\n+r(2)", 1),
            (".why ?r(X)\n+r(1)\n.why ?r(X)", 0),
        ] {
            assert_eq!(
                check(&parse(program)),
                Err(BoundaryViolation {
                    index,
                    kind: ViolationKind::ProofNotLast
                }),
                "{program}"
            );
        }
    }

    #[test]
    fn a_program_that_writes_holds_a_query_or_a_proof_not_both() {
        assert_eq!(
            check(&parse("+r(1)\n?r(X)\n.why ?r(X)")),
            Err(BoundaryViolation {
                index: 1,
                kind: ViolationKind::QueryWithProof
            })
        );
        assert_eq!(
            check(&parse("+r(1)\n.rule foo\n.why ?r(X)")),
            Err(BoundaryViolation {
                index: 1,
                kind: ViolationKind::QueryWithProof
            })
        );
    }
}
