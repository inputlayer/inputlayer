//! Statement Parser for Datalog-Native Syntax
//!
//! Parses meta commands (`.kg`, `.rel`, `.rule`, `.help`, etc.), data ops (`+`/`-`),
//! type/schema declarations, rules (persistent `+` and session), and queries (`?-`).

// Submodules
pub mod data;
pub mod meta;
pub mod parser;
pub mod schema;
pub mod serialize;
pub mod types;

// Re-exports
pub use data::{DeleteOp, DeletePattern, DeleteTarget, InsertOp, InsertTarget, UpdateOp};
pub use meta::{IndexCreateOptions, LoadMode, MetaCommand};
pub use parser::{parse_query, parse_transient_rule, QueryGoal};
pub use schema::{ColumnDef, SchemaDecl};
pub use serialize::{
    RuleDef, SerializableArithExpr, SerializableArithOp, SerializableBodyPred, SerializableRule,
    SerializableTerm,
};
pub use types::{BaseType, RecordField, Refinement, RefinementArg, TypeDecl, TypeExpr};

use crate::ast::Rule;

// Statement Types
/// Top-level statement parsed from user input
#[derive(Debug, Clone)]
pub enum Statement {
    /// Meta commands (dot-prefix): .kg, .rel, .rule, etc.
    Meta(MetaCommand),
    /// Insert operation: +relation(args). or +relation[(t1), (t2), ...].
    Insert(InsertOp),
    /// Delete operation: -relation(args). or -relation(X) :- condition.
    Delete(DeleteOp),
    /// Update operation: -old, +new :- condition. (atomic)
    Update(UpdateOp),
    /// Type declaration: type Name: `TypeExpr`.
    TypeDecl(TypeDecl),
    /// Session rule: head :- body. (query-only, not materialized)
    SessionRule(Rule),
    /// Fact: relation(args). (base data)
    Fact(Rule),
    /// Query: ?- goal.
    Query(QueryGoal),
    /// Schema declaration via typed arguments: +name(col: type, ...). or name(col: type, ...).
    SchemaDecl(SchemaDecl),
    /// Persistent rule: +name(...) :- body. (DD materialized view)
    PersistentRule(Rule),
    /// Delete relation or rule: -name.
    DeleteRelationOrRule(String),
}

// Statement Parser
use parser::{
    extract_args_content, has_typed_arguments, is_simple_name_deletion, parse_persistent_rule,
    strip_inline_comment, validate_relation_name,
};

/// Parse a statement from user input
pub fn parse_statement(input: &str) -> Result<Statement, String> {
    let input = input.trim();

    // Strip inline comments (// ...) while respecting strings
    let input = strip_inline_comment(input);

    if input.is_empty() {
        return Err("Empty input".to_string());
    }

    // Meta commands start with '.'
    if input.starts_with('.') {
        return meta::parse_meta_command(input).map(Statement::Meta);
    }

    // The := operator is not valid syntax
    if input.contains(":=") {
        return Err("Invalid syntax: ':=' is not a valid operator".to_string());
    }

    // Type declaration: type Name: TypeExpr.
    if input.starts_with("type ") {
        return types::parse_type_decl(input).map(Statement::TypeDecl);
    }

    // Check for update pattern: -rel(...), +rel(...) :- body.
    // This must be checked before simple +/- to handle atomic updates
    if input.starts_with('-') || input.starts_with('+') {
        // Check if this is an update pattern (has both - and + before :-)
        if let Some(update) = data::try_parse_update(input)? {
            return Ok(Statement::Update(update));
        }
    }

    // Handle + prefix: schema declaration, persistent rule, or fact insert
    if let Some(rest) = input.strip_prefix('+') {
        // Check for persistent rule: +name(...) :- body.
        if rest.contains(":-") {
            return parse_persistent_rule(rest).map(Statement::PersistentRule);
        }

        // Check if this has typed arguments (schema declaration) or value arguments (insert)
        if let Some(args_content) = extract_args_content(rest) {
            if has_typed_arguments(args_content) {
                // Schema declaration: +name(col: type, ...).
                return schema::parse_schema_decl(rest, true);
            }
        }

        // Value arguments: insert operation
        return data::parse_insert(rest).map(Statement::Insert);
    }

    // Handle - prefix: delete tuple, conditional delete, or delete relation/view
    if let Some(rest) = input.strip_prefix('-') {
        // Check for simple name deletion: -name.
        if is_simple_name_deletion(rest) {
            let name = rest.trim().trim_end_matches('.').trim().to_string();
            validate_relation_name(&name)?;
            return Ok(Statement::DeleteRelationOrRule(name));
        }

        // Delete with arguments or conditional delete
        return data::parse_delete(rest).map(Statement::Delete);
    }

    // Query: ?- goal.
    if input.starts_with("?-") {
        return parse_query(&input[2..]).map(Statement::Query);
    }

    // Session rule: head :- body. (query-only, not materialized)
    if input.contains(":-") {
        return parse_transient_rule(input).map(Statement::SessionRule);
    }

    // No prefix - could be transient schema or transient fact
    if input.ends_with('.') && input.contains('(') {
        // Check if this has typed arguments (transient schema) or value arguments (fact)
        if let Some(args_content) = extract_args_content(input) {
            if has_typed_arguments(args_content) {
                // Transient schema declaration: name(col: type, ...).
                return schema::parse_schema_decl(input, false);
            }
        }

        // Value arguments: transient fact
        return data::parse_fact(input).map(Statement::Fact);
    }

    Err(format!("Unrecognized statement: {input}"))
}

// Re-export parse_rule_definition for convenience
pub use parser::parse_rule_definition;

// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BodyPredicate, Term};

    // Insert tests
    #[test]
    fn test_parse_single_insert() {
        let stmt = parse_statement("+edge(1, 2).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert_eq!(op.tuples.len(), 1);
            assert_eq!(op.tuples[0].len(), 2);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_bulk_insert() {
        let stmt = parse_statement("+edge[(1,2), (3,4), (5,6)].").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert_eq!(op.tuples.len(), 3);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_insert_with_strings() {
        let stmt = parse_statement("+person(\"alice\", 30).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "person");
            assert_eq!(op.tuples.len(), 1);
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "alice"));
            assert!(matches!(&op.tuples[0][1], Term::Constant(30)));
        } else {
            panic!("Expected Insert");
        }
    }

    // Delete tests
    #[test]
    fn test_parse_single_delete() {
        let stmt = parse_statement("-edge(1, 2).").unwrap();
        if let Statement::Delete(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert!(matches!(op.pattern, DeletePattern::SingleTuple(_)));
        } else {
            panic!("Expected Delete");
        }
    }

    #[test]
    fn test_parse_conditional_delete() {
        let stmt = parse_statement("-edge(X, Y) :- source(X).").unwrap();
        if let Statement::Delete(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert!(matches!(op.pattern, DeletePattern::Conditional { .. }));
        } else {
            panic!("Expected Delete");
        }
    }

    // Query tests
    #[test]
    fn test_parse_simple_query() {
        let stmt = parse_statement("?- path(1, X).").unwrap();
        if let Statement::Query(q) = stmt {
            assert_eq!(q.goal.relation, "path");
        } else {
            panic!("Expected Query");
        }
    }

    // Session rule tests
    #[test]
    fn test_parse_session_rule() {
        let stmt = parse_statement("result(X, Y) :- edge(X, Y), node(X).").unwrap();
        if let Statement::SessionRule(rule) = stmt {
            assert_eq!(rule.head.relation, "result");
        } else {
            panic!("Expected SessionRule");
        }
    }

    // Update tests
    #[test]
    fn test_parse_update() {
        let stmt = parse_statement("-edge(1, Y), +edge(1, 100) :- edge(1, Y).").unwrap();
        if let Statement::Update(op) = stmt {
            assert_eq!(op.deletes.len(), 1);
            assert_eq!(op.inserts.len(), 1);
            assert_eq!(op.deletes[0].relation, "edge");
            assert_eq!(op.inserts[0].relation, "edge");
        } else {
            panic!("Expected Update, got {:?}", stmt);
        }
    }

    // Atom vs Variable Parsing Tests
