//! Statement Parser for Datalog-Native Syntax
//!
//! This module implements the unified Datalog-native syntax for InputLayer:
//! - Meta Commands: `.db`, `.rel`, `.rule`, `.save`, `.status`, `.help`, `.quit`
//! - Data Manipulation: `+`/`-` operators (DD-native diff model)
//! - Type Declarations: `type Name: TypeExpr.`
//! - Schema Declarations: `+name(col: type, ...).`
//! - Rules: `+name(...) :- body.` (persistent) or `name(...) :- body.` (session)
//! - Queries: `?-` operator

// Submodules
pub mod data;
pub mod meta;
pub mod parser;
pub mod schema;
pub mod serialize;
pub mod types;

// Re-exports
pub use data::{DeleteOp, DeletePattern, DeleteTarget, InsertOp, InsertTarget, UpdateOp};
pub use meta::{LoadMode, MetaCommand};
pub use parser::QueryGoal;
pub use schema::{ColumnDef, SchemaDecl};
pub use serialize::{
    SerializableArithExpr, SerializableArithOp, SerializableBodyPred, SerializableConstraint,
    SerializableRule, SerializableTerm, RuleDef,
};
pub use types::{BaseType, RecordField, Refinement, RefinementArg, TypeDecl, TypeExpr};

use crate::ast::Rule;

// ============================================================================
// Statement Types
// ============================================================================

/// Top-level statement parsed from user input
#[derive(Debug, Clone)]
pub enum Statement {
    /// Meta commands (dot-prefix): .db, .rel, .rule, etc.
    Meta(MetaCommand),
    /// Insert operation: +relation(args). or +relation[(t1), (t2), ...].
    Insert(InsertOp),
    /// Delete operation: -relation(args). or -relation(X) :- condition.
    Delete(DeleteOp),
    /// Update operation: -old, +new :- condition. (atomic)
    Update(UpdateOp),
    /// Type declaration: type Name: TypeExpr.
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

// ============================================================================
// Statement Parser
// ============================================================================

use parser::{
    extract_args_content, has_typed_arguments, is_simple_name_deletion, parse_persistent_rule,
    parse_query, parse_transient_rule, strip_inline_comment, validate_relation_name,
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
    if input.starts_with('+') {
        let rest = &input[1..];

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
    if input.starts_with('-') {
        let rest = &input[1..];

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

    Err(format!("Unrecognized statement: {}", input))
}

// Re-export parse_rule_definition for convenience
pub use parser::parse_rule_definition;

// ============================================================================
// Tests
// ============================================================================

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
        let stmt = parse_statement("-edge(X, Y) :- X > 5.").unwrap();
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
        let stmt = parse_statement("result(X, Y) :- edge(X, Y), X < Y.").unwrap();
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

    // =========================================================================
    // Atom vs Variable Parsing Tests
    // =========================================================================

    #[test]
    fn test_uppercase_is_variable() {
        let stmt = parse_statement("+edge(X, Y).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::Variable(v) if v == "X"));
            assert!(matches!(&op.tuples[0][1], Term::Variable(v) if v == "Y"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_lowercase_is_atom() {
        let stmt = parse_statement("+parent(tom, liz).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "tom"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "liz"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_mixed_atoms_and_variables() {
        let stmt = parse_statement("?- parent(tom, X).").unwrap();
        if let Statement::Query(q) = stmt {
            assert_eq!(q.goal.relation, "parent");
            assert!(matches!(&q.goal.args[0], Term::StringConstant(s) if s == "tom"));
            assert!(matches!(&q.goal.args[1], Term::Variable(v) if v == "X"));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_underscore_prefix_is_variable() {
        let stmt = parse_statement("result(X) :- edge(_from, X).").unwrap();
        if let Statement::SessionRule(rule) = stmt {
            let body_atom = match &rule.body[0] {
                BodyPredicate::Positive(atom) => atom,
                _ => panic!("Expected positive atom"),
            };
            assert!(matches!(&body_atom.args[0], Term::Variable(v) if v == "_from"));
            assert!(matches!(&body_atom.args[1], Term::Variable(v) if v == "X"));
        } else {
            panic!("Expected SessionRule");
        }
    }

    #[test]
    fn test_placeholder_underscore() {
        let stmt = parse_statement("?- edge(_, X).").unwrap();
        if let Statement::Query(q) = stmt {
            assert!(matches!(&q.goal.args[0], Term::Placeholder));
            assert!(matches!(&q.goal.args[1], Term::Variable(v) if v == "X"));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_multichar_variables() {
        let stmt = parse_statement("result(Foo, BarBaz) :- data(Foo, BarBaz).").unwrap();
        if let Statement::SessionRule(rule) = stmt {
            assert!(matches!(&rule.head.args[0], Term::Variable(v) if v == "Foo"));
            assert!(matches!(&rule.head.args[1], Term::Variable(v) if v == "BarBaz"));
        } else {
            panic!("Expected SessionRule");
        }
    }

    #[test]
    fn test_multichar_atoms() {
        let stmt = parse_statement("+likes(alice, bob).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "alice"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "bob"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_atoms_with_numbers() {
        let stmt = parse_statement("+data(item1, item2).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "item1"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "item2"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_variables_with_underscores() {
        let stmt = parse_statement("result(X_val, Y_val) :- data(X_val, Y_val).").unwrap();
        if let Statement::SessionRule(rule) = stmt {
            assert!(matches!(&rule.head.args[0], Term::Variable(v) if v == "X_val"));
            assert!(matches!(&rule.head.args[1], Term::Variable(v) if v == "Y_val"));
        } else {
            panic!("Expected SessionRule");
        }
    }

    #[test]
    fn test_atoms_with_underscores() {
        let stmt = parse_statement("+data(my_item, other_thing).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "my_item"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "other_thing"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_persistent_rule_with_atoms() {
        let stmt = parse_statement("+child(X) :- parent(mary, X).").unwrap();
        if let Statement::PersistentRule(rule) = stmt {
            assert_eq!(rule.head.relation, "child");
            let body_atom = match &rule.body[0] {
                BodyPredicate::Positive(atom) => atom,
                _ => panic!("Expected positive atom"),
            };
            assert!(matches!(&body_atom.args[0], Term::StringConstant(s) if s == "mary"));
        } else {
            panic!("Expected PersistentRule, got {:?}", stmt);
        }
    }

    #[test]
    fn test_query_all_atoms() {
        let stmt = parse_statement("?- parent(tom, liz).").unwrap();
        if let Statement::Query(q) = stmt {
            assert!(matches!(&q.goal.args[0], Term::StringConstant(s) if s == "tom"));
            assert!(matches!(&q.goal.args[1], Term::StringConstant(s) if s == "liz"));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_integers_still_work() {
        let stmt = parse_statement("+edge(1, 2).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::Constant(1)));
            assert!(matches!(&op.tuples[0][1], Term::Constant(2)));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_floats_still_work() {
        let stmt = parse_statement("+data(3.14, 2.71).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::FloatConstant(f) if (*f - 3.14).abs() < 0.001));
            assert!(matches!(&op.tuples[0][1], Term::FloatConstant(f) if (*f - 2.71).abs() < 0.001));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_quoted_strings_still_work() {
        let stmt = parse_statement("+data(\"hello world\", \"test\").").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "hello world"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "test"));
        } else {
            panic!("Expected Insert");
        }
    }

    // =========================================================================
    // Unified Prefix Syntax Tests
    // =========================================================================

    #[test]
    fn test_parse_persistent_schema() {
        let stmt = parse_statement("+person(id: int, name: string).").unwrap();
        if let Statement::SchemaDecl(decl) = stmt {
            assert_eq!(decl.name, "person");
            assert!(decl.persistent);
            assert_eq!(decl.columns.len(), 2);
            assert_eq!(decl.columns[0].name, "id");
            assert_eq!(decl.columns[1].name, "name");
        } else {
            panic!("Expected SchemaDecl, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_transient_schema() {
        let stmt = parse_statement("temp(x: int, y: int).").unwrap();
        if let Statement::SchemaDecl(decl) = stmt {
            assert_eq!(decl.name, "temp");
            assert!(!decl.persistent);
            assert_eq!(decl.columns.len(), 2);
        } else {
            panic!("Expected SchemaDecl, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_persistent_rule() {
        let stmt = parse_statement("+reachable(X, Y) :- edge(X, Y).").unwrap();
        if let Statement::PersistentRule(rule) = stmt {
            assert_eq!(rule.head.relation, "reachable");
            assert_eq!(rule.body.len(), 1);
        } else {
            panic!("Expected PersistentRule, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_persistent_recursive_rule() {
        let stmt = parse_statement("+reachable(X, Z) :- reachable(X, Y), edge(Y, Z).").unwrap();
        if let Statement::PersistentRule(rule) = stmt {
            assert_eq!(rule.head.relation, "reachable");
            assert_eq!(rule.body.len(), 2);
        } else {
            panic!("Expected PersistentRule, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_delete_relation_or_rule() {
        let stmt = parse_statement("-reachable.").unwrap();
        if let Statement::DeleteRelationOrRule(name) = stmt {
            assert_eq!(name, "reachable");
        } else {
            panic!("Expected DeleteRelationOrRule, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_schema_with_constraints() {
        let stmt = parse_statement("+user(id: int, email: string).").unwrap();
        if let Statement::SchemaDecl(decl) = stmt {
            assert_eq!(decl.name, "user");
            assert!(decl.persistent);
        } else {
            panic!("Expected SchemaDecl, got {:?}", stmt);
        }
    }

    #[test]
    fn test_insert_not_schema() {
        let stmt = parse_statement("+person(1, \"alice\").").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "person");
            assert!(matches!(&op.tuples[0][0], Term::Constant(1)));
        } else {
            panic!("Expected Insert, got {:?}", stmt);
        }
    }

    #[test]
    fn test_fact_not_schema() {
        let stmt = parse_statement("person(1, \"alice\").").unwrap();
        if let Statement::Fact(rule) = stmt {
            assert_eq!(rule.head.relation, "person");
        } else {
            panic!("Expected Fact, got {:?}", stmt);
        }
    }

    #[test]
    fn test_distinguish_schema_from_insert() {
        let schema = parse_statement("+person(id: int, name: string).").unwrap();
        let insert = parse_statement("+person(1, \"alice\").").unwrap();

        assert!(matches!(schema, Statement::SchemaDecl(_)));
        assert!(matches!(insert, Statement::Insert(_)));
    }

    #[test]
    fn test_distinguish_transient_schema_from_fact() {
        let schema = parse_statement("temp(x: int, y: int).").unwrap();
        let fact = parse_statement("temp(1, 2).").unwrap();

        assert!(matches!(schema, Statement::SchemaDecl(_)));
        assert!(matches!(fact, Statement::Fact(_)));
    }

    #[test]
    fn test_conditional_delete_not_view_delete() {
        let stmt = parse_statement("-person(X, Y) :- person(X, Y), X > 5.").unwrap();
        if let Statement::Delete(op) = stmt {
            assert_eq!(op.relation, "person");
            assert!(matches!(op.pattern, DeletePattern::Conditional { .. }));
        } else {
            panic!("Expected Delete, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_schema_with_key_annotation() {
        let stmt = parse_statement("+user(id: int @key, name: string).").unwrap();
        if let Statement::SchemaDecl(decl) = stmt {
            assert_eq!(decl.name, "user");
            assert!(decl.persistent);
            assert_eq!(decl.columns.len(), 2);
            assert_eq!(decl.columns[0].name, "id");
            assert_eq!(decl.columns[0].annotations.len(), 1);
            assert!(matches!(decl.columns[0].annotations[0], crate::schema::ColumnAnnotation::Primary));
        } else {
            panic!("Expected SchemaDecl, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_schema_with_multiple_annotations() {
        let stmt = parse_statement("+user(id: int @key, email: string @unique @not_empty).").unwrap();
        if let Statement::SchemaDecl(decl) = stmt {
            assert_eq!(decl.name, "user");
            assert_eq!(decl.columns.len(), 2);
            // First column has @key
            assert_eq!(decl.columns[0].annotations.len(), 1);
            // Second column has @unique and @not_empty
            assert_eq!(decl.columns[1].annotations.len(), 2);
        } else {
            panic!("Expected SchemaDecl, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_load_command() {
        let stmt = parse_statement(".load file.dl").unwrap();
        if let Statement::Meta(MetaCommand::Load { path, mode }) = stmt {
            assert_eq!(path, "file.dl");
            assert_eq!(mode, LoadMode::Default);
        } else {
            panic!("Expected Load, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_load_with_replace() {
        let stmt = parse_statement(".load views.dl --replace").unwrap();
        if let Statement::Meta(MetaCommand::Load { path, mode }) = stmt {
            assert_eq!(path, "views.dl");
            assert_eq!(mode, LoadMode::Replace);
        } else {
            panic!("Expected Load with Replace, got {:?}", stmt);
        }
    }

    #[test]
    fn test_parse_load_with_merge() {
        let stmt = parse_statement(".load data.dl --merge").unwrap();
        if let Statement::Meta(MetaCommand::Load { path, mode }) = stmt {
            assert_eq!(path, "data.dl");
            assert_eq!(mode, LoadMode::Merge);
        } else {
            panic!("Expected Load with Merge, got {:?}", stmt);
        }
    }
}
