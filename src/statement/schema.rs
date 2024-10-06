//! Schema declaration parsing for `InputLayer`.
//!
//! This module handles schema declarations:
//! - `+name(col: type, ...).` - persistent schema
//! - `name(col: type, ...).` - session schema

use crate::schema::{ColumnSchema, RelationSchema, SchemaType};
use serde::{Deserialize, Serialize};

use super::parser::validate_relation_name;
use super::types::{parse_type_expr, split_respecting_braces, TypeExpr};

/// Schema declaration via unified prefix syntax: +name(col: type, ...). or name(col: type, ...).
/// Use `+` prefix for persistent schema, no prefix for session schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDecl {
    /// Relation name (must be lowercase)
    pub name: String,
    /// Column definitions (with types)
    pub columns: Vec<ColumnDef>,
    /// Whether this is a persistent schema (+) or session (no prefix)
    pub persistent: bool,
}

/// A column definition in a relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column type
    pub col_type: TypeExpr,
}

// Schema Parsing
