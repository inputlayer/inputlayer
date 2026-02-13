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
    let input = input.trim();

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

        let col_type = parse_type_expr(type_str)?;

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

// Schema Definition Parsing
/// Try to parse a schema definition: `Name = schema(col: type, ...)`
/// Returns None if input doesn't match schema definition pattern
#[allow(dead_code)]
pub fn try_parse_schema_definition(input: &str) -> Result<Option<RelationSchema>, String> {
    let input = input.trim();

    // Must contain '=' but not ':=' or '<-'
    if !input.contains('=') || input.contains(":=") || input.contains("<-") {
        return Ok(None);
    }

    // Split on first '='
    let eq_pos = match input.find('=') {
        Some(p) => p,
        None => return Ok(None),
    };

    let name = input[..eq_pos].trim();
    let definition = input[eq_pos + 1..].trim();

    // Check if definition starts with 'schema('
    if !definition.starts_with("schema(") {
        return Ok(None);
    }

    // Validate name (must be valid identifier, start with uppercase)
    let Some(first_char) = name.chars().next() else {
        return Err("Schema name cannot be empty".to_string());
    };
    if !first_char.is_uppercase() {
        return Err(format!(
            "Schema name '{name}' must start with uppercase letter"
        ));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid schema name: '{name}'"));
    }

    // Extract columns from schema(...)
    let content = definition
        .strip_prefix("schema(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or("Invalid schema syntax: expected schema(...)")?;

    let columns = parse_schema_columns(content)?;

    let mut schema = RelationSchema::new(name);
    for col in columns {
        schema = schema.with_column(col);
    }

    Ok(Some(schema))
}

/// Parse schema columns from the inside of schema(...)
/// Format: `col1: type1, col2: type2, ...`
fn parse_schema_columns(content: &str) -> Result<Vec<ColumnSchema>, String> {
    let mut columns = Vec::new();

    // Split by comma (but respect nested parentheses)
    let parts = split_schema_columns(content);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Split on ':' to get name and type
        let colon_pos = part
            .find(':')
            .ok_or_else(|| format!("Invalid column definition '{part}': expected 'name: type'"))?;

        let col_name = part[..colon_pos].trim();
        let type_str = part[colon_pos + 1..].trim();

        // Validate column name
        if col_name.is_empty() {
            return Err("Column name cannot be empty".to_string());
        }
        if !col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!("Invalid column name: '{col_name}'"));
        }

        let data_type = SchemaType::from_str(type_str)
            .ok_or_else(|| format!("Unknown type '{type_str}' for column '{col_name}'"))?;

        columns.push(ColumnSchema::new(col_name, data_type));
    }

    if columns.is_empty() {
        return Err("Schema must have at least one column".to_string());
    }

    Ok(columns)
}

/// Split schema column definitions, respecting nested parentheses
fn split_schema_columns(content: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth: i32 = 0;
    let mut in_string = false;

    for ch in content.chars() {
        match ch {
            '"' if !in_string => {
                in_string = true;
                current.push(ch);
            }
            '"' if in_string => {
                in_string = false;
                current.push(ch);
            }
            '(' if !in_string => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_string => {
                // Clamp to 0 to handle malformed input
                paren_depth = (paren_depth - 1).max(0);
                current.push(ch);
            }
            ',' if paren_depth == 0 && !in_string => {
                result.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        result.push(current);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_persistent_schema() {
        let result = parse_schema_decl("person(id: int, name: string)", true).unwrap();
        if let Statement::SchemaDecl(decl) = result {
            assert_eq!(decl.name, "person");
            assert!(decl.persistent);
            assert_eq!(decl.columns.len(), 2);
            assert_eq!(decl.columns[0].name, "id");
            assert_eq!(decl.columns[1].name, "name");
        } else {
            panic!("Expected SchemaDecl");
        }
    }

    #[test]
    fn test_parse_session_schema() {
        let result = parse_schema_decl("temp(x: int, y: int)", false).unwrap();
        if let Statement::SchemaDecl(decl) = result {
            assert_eq!(decl.name, "temp");
            assert!(!decl.persistent);
            assert_eq!(decl.columns.len(), 2);
        } else {
            panic!("Expected SchemaDecl");
        }
    }

    #[test]
    fn test_parse_schema_columns() {
        let cols = parse_schema_columns("id: int, name: string").unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, SchemaType::Int);
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].data_type, SchemaType::String);
    }
}
