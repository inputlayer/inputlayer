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
