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
use super::Statement;

/// Parse a schema declaration using unified prefix syntax.
/// +name(col: type, ...). = persistent schema
/// name(col: type, ...). = session schema
pub fn parse_schema_decl(input: &str, persistent: bool) -> Result<Statement, String> {
    let input = input.trim().trim_end_matches('.');

    // Extract relation name and column definitions
    let paren_pos = input
        .find('(')
        .ok_or("Schema declaration must have columns: name(col: type, ...)")?;

    let name = input[..paren_pos].trim().to_string();
    validate_relation_name(&name)?;

    // Extract content between parentheses
    let content = input[paren_pos + 1..]
        .trim()
        .strip_suffix(')')
        .ok_or("Missing closing parenthesis in schema declaration")?;

    let columns = parse_rel_columns(content)?;

    Ok(Statement::SchemaDecl(SchemaDecl {
        name,
        columns,
        persistent,
    }))
}

/// Validate a column name - may include aggregation syntax like count<X>
/// or arithmetic expressions like P*Q
fn validate_column_name(col_name: &str) -> Result<(), String> {
    if col_name.is_empty() {
        return Err("Column name cannot be empty".to_string());
    }

    // Check for aggregation syntax: agg<Var>
    if let (Some(open), Some(close)) = (col_name.find('<'), col_name.find('>')) {
        if close > open + 1 {
            // Extract the aggregation function name and variable
            let agg_func = col_name[..open].trim();
            let agg_var = col_name[open + 1..close].trim();

            // Validate aggregation function
            let valid_aggs = ["count", "sum", "min", "max", "avg", "top_k"];
            if !valid_aggs.contains(&agg_func) {
                return Err(format!("Unknown aggregation function: '{agg_func}'"));
            }

            // Validate variable name
            if agg_var.is_empty() {
                return Err("Aggregation variable cannot be empty".to_string());
            }
            if !agg_var.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(format!("Invalid aggregation variable: '{agg_var}'"));
            }

            return Ok(());
        }
    }

    // Check for arithmetic expression (contains +, -, *, /)
    // These are computed columns like P*Q, P+1, etc.
    let arith_ops = ['+', '-', '*', '/'];
    if col_name.chars().any(|c| arith_ops.contains(&c)) {
        // Arithmetic expression - validate each token is alphanumeric/numeric or operator
        let valid_chars = col_name.chars().all(|c| {
            c.is_alphanumeric() || c == '_' || arith_ops.contains(&c) || c == '(' || c == ')'
        });
        if !valid_chars {
            return Err(format!("Invalid computed column expression: '{col_name}'"));
        }
        return Ok(());
    }

    // Regular column name - must be alphanumeric with underscores
    if !col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid column name: '{col_name}'"));
    }

    Ok(())
}

/// Parse relation columns from inside a schema declaration: +name(col: type, ...)
fn parse_rel_columns(content: &str) -> Result<Vec<ColumnDef>, String> {
    let mut columns = Vec::new();
    let parts = split_respecting_braces(content);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Split on first ':' to get name and type
        let colon_pos = part
            .find(':')
            .ok_or_else(|| format!("Column definition '{part}' must have type: 'name: type'"))?;

        let col_name = part[..colon_pos].trim().to_string();
        let type_str = part[colon_pos + 1..].trim();

        // Validate column name (may include aggregation syntax)
        validate_column_name(&col_name)?;

        // Parse the type (ignore any @annotations that may be present - they're deprecated)
        let type_str = strip_annotations(type_str);
        let col_type = parse_type_expr(&type_str)?;

        columns.push(ColumnDef {
            name: col_name,
            col_type,
        });
    }

    if columns.is_empty() {
        return Err("Relation must have at least one column".to_string());
    }

    Ok(columns)
}

/// Strip any @annotations from a type string for backwards compatibility
fn strip_annotations(type_str: &str) -> String {
    // Find the first @ that isn't inside parentheses
    let mut paren_depth: i32 = 0;

    for (i, c) in type_str.char_indices() {
        match c {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '@' if paren_depth == 0 => {
                // Found annotation - return everything before it
                return type_str[..i].trim().to_string();
            }
            _ => {}
        }
    }

    type_str.trim().to_string()
}

// Schema Definition Parsing
/// Try to parse a schema definition: `Name = schema(col: type, ...)`
/// Returns None if input doesn't match schema definition pattern
#[allow(dead_code)]
