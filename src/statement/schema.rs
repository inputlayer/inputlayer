//! Schema declaration parsing for InputLayer.
//!
//! This module handles schema declarations:
//! - `+name(col: type @constraint, ...).` - persistent schema
//! - `name(col: type, ...).` - transient schema

use serde::{Deserialize, Serialize};
use crate::schema::{
    RelationSchema, ColumnSchema, SchemaType, ColumnAnnotation, TypeAlias,
};

use super::types::{TypeExpr, parse_type_expr, split_respecting_braces};
use super::parser::validate_relation_name;

/// Schema declaration via unified prefix syntax: +name(col: type, ...). or name(col: type, ...).
/// Use `+` prefix for persistent schema, no prefix for transient (session-only) schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDecl {
    /// Relation name (must be lowercase)
    pub name: String,
    /// Column definitions (with types and optional constraints like @key, @unique)
    pub columns: Vec<ColumnDef>,
    /// Whether this is a persistent schema (+) or transient (no prefix)
    pub persistent: bool,
}

/// A column definition in a relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column type
    pub col_type: TypeExpr,
    /// Column annotations (@key, @unique, @not_empty, etc.)
    #[serde(default)]
    pub annotations: Vec<ColumnAnnotation>,
}

// ============================================================================
// Schema Parsing
// ============================================================================

use super::Statement;

/// Parse a schema declaration using unified prefix syntax.
/// +name(col: type @constraint, ...). = persistent schema
/// name(col: type, ...). = transient schema (session only)
pub fn parse_schema_decl(input: &str, persistent: bool) -> Result<Statement, String> {
    let input = input.trim().trim_end_matches('.');

    // Extract relation name and column definitions
    let paren_pos = input.find('(')
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

/// Check if a column name is an aggregation pattern like `count<Var>`, `sum<Var>`, etc.
/// Returns the variable name if it's an aggregation, or the original name otherwise.
#[allow(dead_code)]
fn extract_head_var_from_column(col_name: &str) -> String {
    // Aggregation patterns: count<X>, sum<X>, min<X>, max<X>, avg<X>, top_k<X>
    if let Some(open) = col_name.find('<') {
        if let Some(close) = col_name.find('>') {
            if close > open + 1 {
                return col_name[open + 1..close].trim().to_string();
            }
        }
    }
    // Not an aggregation - return the column name as-is
    col_name.to_string()
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
                return Err(format!("Unknown aggregation function: '{}'", agg_func));
            }

            // Validate variable name
            if agg_var.is_empty() {
                return Err("Aggregation variable cannot be empty".to_string());
            }
            if !agg_var.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(format!("Invalid aggregation variable: '{}'", agg_var));
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
            return Err(format!("Invalid computed column expression: '{}'", col_name));
        }
        return Ok(());
    }

    // Regular column name - must be alphanumeric with underscores
    if !col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid column name: '{}'", col_name));
    }

    Ok(())
}

/// Parse relation columns from the inside of rel name(...)
/// Supports annotations: `name: type @key @unique`
fn parse_rel_columns(content: &str) -> Result<Vec<ColumnDef>, String> {
    let mut columns = Vec::new();
    let parts = split_respecting_braces(content);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Split on first ':' to get name and type+annotations
        let colon_pos = part.find(':')
            .ok_or_else(|| format!("Column definition '{}' must have type: 'name: type'", part))?;

        let col_name = part[..colon_pos].trim().to_string();
        let type_and_annot = part[colon_pos + 1..].trim();

        // Validate column name (may include aggregation syntax)
        validate_column_name(&col_name)?;

        // Split type from annotations: "int @key @unique" -> ("int", ["@key", "@unique"])
        let (type_str, annotations) = parse_type_and_annotations(type_and_annot)?;

        let col_type = parse_type_expr(&type_str)?;
        columns.push(ColumnDef { name: col_name, col_type, annotations });
    }

    if columns.is_empty() {
        return Err("Relation must have at least one column".to_string());
    }

    Ok(columns)
}

/// Parse type and annotations from a string like "int @key @unique"
/// Returns the type string and parsed annotations
fn parse_type_and_annotations(input: &str) -> Result<(String, Vec<ColumnAnnotation>), String> {
    let input = input.trim();

    // Find the first @ that isn't inside parentheses
    let mut paren_depth: i32 = 0;
    let mut first_at = None;

    for (i, c) in input.char_indices() {
        match c {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '@' if paren_depth == 0 && first_at.is_none() => {
                first_at = Some(i);
            }
            _ => {}
        }
    }

    match first_at {
        None => {
            // No annotations
            Ok((input.to_string(), vec![]))
        }
        Some(pos) => {
            let type_str = input[..pos].trim().to_string();
            let annot_str = input[pos..].trim();

            // Split annotation string by @ and parse each
            let annotation_tokens: Vec<String> = annot_str
                .split('@')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
                .collect();

            let annotations = parse_annotations(&annotation_tokens)?;
            Ok((type_str, annotations))
        }
    }
}

/// Parse annotation tokens into ColumnAnnotation values
fn parse_annotations(tokens: &[String]) -> Result<Vec<ColumnAnnotation>, String> {
    let mut annotations = Vec::new();

    for token in tokens {
        let ann = parse_single_annotation(token)?;
        annotations.push(ann);
    }

    Ok(annotations)
}

/// Parse a single annotation token
fn parse_single_annotation(token: &str) -> Result<ColumnAnnotation, String> {
    let token = token.trim();

    // Simple annotations (no arguments)
    match token.to_lowercase().as_str() {
        "primary" | "key" => return Ok(ColumnAnnotation::Primary),
        "not_empty" | "notempty" | "required" => return Ok(ColumnAnnotation::NotEmpty),
        "unique" => return Ok(ColumnAnnotation::Unique),
        _ => {}
    }

    // Annotations with arguments: name(args)
    if let Some(paren_pos) = token.find('(') {
        let name = token[..paren_pos].to_lowercase();
        let args = token[paren_pos + 1..].trim_end_matches(')');

        match name.as_str() {
            "range" => {
                // range(min, max)
                let parts: Vec<&str> = args.split(',').collect();
                if parts.len() != 2 {
                    return Err(format!("range requires two arguments: range(min, max), got: {}", args));
                }
                let min: i64 = parts[0].trim().parse()
                    .map_err(|_| format!("Invalid range min value: {}", parts[0].trim()))?;
                let max: i64 = parts[1].trim().parse()
                    .map_err(|_| format!("Invalid range max value: {}", parts[1].trim()))?;
                return Ok(ColumnAnnotation::Range { min, max });
            }
            "pattern" => {
                // pattern("regex")
                let regex = args.trim().trim_matches('"').to_string();
                if regex.is_empty() {
                    return Err("pattern requires a non-empty regex".to_string());
                }
                return Ok(ColumnAnnotation::Pattern { regex });
            }
            "references" | "foreign_key" | "fk" => {
                // references(Relation.column) or references(Relation, column)
                let parts: Vec<&str> = if args.contains('.') {
                    args.split('.').collect()
                } else {
                    args.split(',').collect()
                };
                if parts.len() != 2 {
                    return Err(format!("references requires relation.column or (relation, column): {}", args));
                }
                return Ok(ColumnAnnotation::ForeignKey {
                    relation: parts[0].trim().to_string(),
                    column: parts[1].trim().to_string(),
                });
            }
            "default" => {
                // default(value)
                let value = parse_default_value(args)?;
                return Ok(ColumnAnnotation::Default { value });
            }
            _ => {}
        }
    }

    Err(format!("Unknown annotation: '{}'", token))
}

/// Parse a default value from string
fn parse_default_value(s: &str) -> Result<crate::value::Value, String> {
    let s = s.trim();

    // String value
    if s.starts_with('"') && s.ends_with('"') {
        return Ok(crate::value::Value::string(&s[1..s.len()-1]));
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return Ok(crate::value::Value::Int64(n));
    }

    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Ok(crate::value::Value::Float64(f));
    }

    // Boolean
    match s.to_lowercase().as_str() {
        "true" => return Ok(crate::value::Value::Bool(true)),
        "false" => return Ok(crate::value::Value::Bool(false)),
        _ => {}
    }

    // Atom (lowercase identifier)
    if s.chars().next().map_or(false, |c| c.is_lowercase())
       && s.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(crate::value::Value::string(s));
    }

    Err(format!("Cannot parse default value: '{}'", s))
}

// ============================================================================
// Type Alias Parsing
// ============================================================================

/// Tokenize a type definition into type and constraint tokens
/// E.g., "string pattern(\"^[^@]+@[^@]+$\") not_empty" -> ["string", "pattern(\"^[^@]+@[^@]+$\")", "not_empty"]
fn tokenize_type_def(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut in_string = false;

    for ch in input.chars() {
        match ch {
            '"' => {
                in_string = !in_string;
                current.push(ch);
            }
            '(' if !in_string => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_string => {
                paren_depth -= 1;
                current.push(ch);
            }
            ' ' | '\t' | '\n' if paren_depth == 0 && !in_string => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Parse a type alias: `type Email = string pattern("^[^@]+@[^@]+$")`
#[allow(dead_code)]
pub fn parse_type_alias(input: &str) -> Result<TypeAlias, String> {
    // Remove "type " prefix and trailing period
    let input = input.trim_start_matches("type ").trim().trim_end_matches('.');

    // Split on '=' to get name and definition
    let eq_pos = input.find('=')
        .ok_or("Type alias must contain '='")?;

    let name = input[..eq_pos].trim();
    let definition = input[eq_pos + 1..].trim();

    // Validate name (must start with uppercase)
    if name.is_empty() {
        return Err("Type alias name cannot be empty".to_string());
    }
    if !name.chars().next().unwrap().is_uppercase() {
        return Err(format!("Type alias name '{}' must start with uppercase letter", name));
    }

    // Parse the definition: base_type followed by optional constraints
    let tokens = tokenize_type_def(definition);
    if tokens.is_empty() {
        return Err("Type alias definition cannot be empty".to_string());
    }

    // First token is the base type
    let base_type = SchemaType::from_str(&tokens[0])
        .ok_or_else(|| format!("Unknown base type: '{}'", tokens[0]))?;

    // Remaining tokens are constraints
    let annotations = parse_annotations(&tokens[1..])?;

    let mut alias = TypeAlias::new(name, base_type);
    alias.annotations = annotations;

    Ok(alias)
}

/// Try to parse a schema definition: `Name = schema(col: type, ...)`
/// Returns None if input doesn't match schema definition pattern
#[allow(dead_code)]
pub fn try_parse_schema_definition(input: &str) -> Result<Option<RelationSchema>, String> {
    let input = input.trim().trim_end_matches('.');

    // Must contain '=' but not ':=' or ':-'
    if !input.contains('=') || input.contains(":=") || input.contains(":-") {
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
    if name.is_empty() {
        return Err("Schema name cannot be empty".to_string());
    }
    if !name.chars().next().unwrap().is_uppercase() {
        return Err(format!("Schema name '{}' must start with uppercase letter", name));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid schema name: '{}'", name));
    }

    // Extract columns from schema(...)
    let content = definition.strip_prefix("schema(")
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
/// Format: `col1: type1 constraint1, col2: type2 constraint2, ...`
fn parse_schema_columns(content: &str) -> Result<Vec<ColumnSchema>, String> {
    let mut columns = Vec::new();

    // Split by comma (but respect nested parentheses)
    let parts = split_schema_columns(content);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Split on ':' to get name and type+constraints
        let colon_pos = part.find(':')
            .ok_or_else(|| format!("Invalid column definition '{}': expected 'name: type'", part))?;

        let col_name = part[..colon_pos].trim();
        let type_and_constraints = part[colon_pos + 1..].trim();

        // Validate column name
        if col_name.is_empty() {
            return Err("Column name cannot be empty".to_string());
        }
        if !col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!("Invalid column name: '{}'", col_name));
        }

        // Tokenize type and constraints
        let tokens = tokenize_type_def(type_and_constraints);
        if tokens.is_empty() {
            return Err(format!("Column '{}' is missing type", col_name));
        }

        // First token is the type
        let data_type = SchemaType::from_str(&tokens[0])
            .ok_or_else(|| format!("Unknown type '{}' for column '{}'", tokens[0], col_name))?;

        // Remaining tokens are annotations
        let annotations = parse_annotations(&tokens[1..])?;

        let mut col = ColumnSchema::new(col_name, data_type);
        col.annotations = annotations;
        columns.push(col);
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
    let mut paren_depth = 0;
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
                paren_depth -= 1;
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
    fn test_parse_transient_schema() {
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
    fn test_parse_schema_with_key_annotation() {
        let result = parse_schema_decl("user(id: int @key, name: string)", true).unwrap();
        if let Statement::SchemaDecl(decl) = result {
            assert_eq!(decl.name, "user");
            assert!(decl.persistent);
            assert_eq!(decl.columns.len(), 2);
            assert_eq!(decl.columns[0].name, "id");
            assert_eq!(decl.columns[0].annotations.len(), 1);
            assert!(matches!(decl.columns[0].annotations[0], ColumnAnnotation::Primary));
        } else {
            panic!("Expected SchemaDecl");
        }
    }

    #[test]
    fn test_parse_schema_with_multiple_annotations() {
        let result = parse_schema_decl("user(id: int @key, email: string @unique @not_empty)", true).unwrap();
        if let Statement::SchemaDecl(decl) = result {
            assert_eq!(decl.name, "user");
            assert_eq!(decl.columns.len(), 2);
            // First column has @key
            assert_eq!(decl.columns[0].annotations.len(), 1);
            // Second column has @unique and @not_empty
            assert_eq!(decl.columns[1].annotations.len(), 2);
        } else {
            panic!("Expected SchemaDecl");
        }
    }
}
