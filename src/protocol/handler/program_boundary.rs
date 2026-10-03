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
}

impl BoundaryViolation {
    /// The message for statement `text`, the one at [`Self::index`].
    pub fn message(&self, text: &str) -> String {
        format!(
            "'{text}' cannot run in a program that changes facts, rules or schemas: \
             those changes commit together as one transaction, and this command \
             cannot join it. Nothing was applied; send the command in a separate request."
        )
    }
}

/// Check that every statement of a transactional program can join its
/// transaction.
pub(super) fn check(statements: &[Statement]) -> Result<(), BoundaryViolation> {
    if !is_transactional(statements) {
        return Ok(());
    }
    match statements.iter().position(|s| role(s) == Role::Outside) {
        Some(index) => Err(BoundaryViolation { index }),
        None => Ok(()),
    }
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
                Err(BoundaryViolation { index: 1 }),
                "{outside}"
            );
        }
    }
}
