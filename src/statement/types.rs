//! Type system declarations for InputLayer.
//!
//! This module defines the type declaration syntax:
//! - `type Name: TypeExpr.`
//! - Type expressions: base types, lists, records, refined types

use serde::{Deserialize, Serialize};

/// Type declaration: type Name: TypeExpr.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDecl {
    /// Type name (must be uppercase, e.g., Email, User)
    pub name: String,
    /// The type expression
    pub type_expr: TypeExpr,
}

/// Type expression (right-hand side of type declaration)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeExpr {
    /// Base type: int, string, bool
    Base(BaseType),
    /// Reference to another type by name
    TypeRef(String),
    /// List type: list[T]
    List(Box<TypeExpr>),
    /// Record type: { field: type, ... }
    Record(Vec<RecordField>),
    /// Refined type: base_type(constraint1, constraint2, ...)
    Refined {
        base: Box<TypeExpr>,
        refinements: Vec<Refinement>,
    },
}

/// Base types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BaseType {
    Int,
    String,
    Bool,
    Float,
}

/// A field in a record type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordField {
    pub name: String,
    pub field_type: TypeExpr,
}

/// A refinement constraint on a type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Refinement {
    /// Refinement name (e.g., "range", "pattern", "not_empty")
    pub name: String,
    /// Arguments to the refinement
    pub args: Vec<RefinementArg>,
}

/// Argument to a refinement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefinementArg {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

// ============================================================================
// Parsing
// ============================================================================

/// Parse a type declaration: `type Name: TypeExpr.`
pub fn parse_type_decl(input: &str) -> Result<TypeDecl, String> {
    // Remove "type " prefix and trailing period
    let input = input.trim_start_matches("type ").trim().trim_end_matches('.');

    // Split on first ':' to get name and type expression
    let colon_pos = input.find(':')
        .ok_or("Type declaration must contain ':' (e.g., 'type Email: string.')")?;

    let name = input[..colon_pos].trim().to_string();
    let type_expr_str = input[colon_pos + 1..].trim();

    // Validate name (must start with uppercase)
    if name.is_empty() {
        return Err("Type name cannot be empty".to_string());
    }
    if !name.chars().next().unwrap().is_uppercase() {
        return Err(format!("Type name '{}' must start with uppercase letter", name));
    }

    // Parse the type expression
    let type_expr = parse_type_expr(type_expr_str)?;

    Ok(TypeDecl { name, type_expr })
}

/// Parse a type expression
pub fn parse_type_expr(input: &str) -> Result<TypeExpr, String> {
    let input = input.trim();

    if input.is_empty() {
        return Err("Type expression cannot be empty".to_string());
    }

    // Record type: { field: type, ... }
    if input.starts_with('{') && input.ends_with('}') {
        return parse_record_type(input);
    }

    // List type: list[T]
    if input.starts_with("list[") && input.ends_with(']') {
        let inner = &input[5..input.len() - 1];
        let inner_type = parse_type_expr(inner)?;
        return Ok(TypeExpr::List(Box::new(inner_type)));
    }

    // Check for refinements: base_type(constraint1, constraint2, ...)
    if let Some(paren_pos) = input.find('(') {
        if input.ends_with(')') {
            let base_str = &input[..paren_pos];
            let refinements_str = &input[paren_pos + 1..input.len() - 1];

            let base = parse_type_expr(base_str)?;
            let refinements = parse_refinements(refinements_str)?;

            return Ok(TypeExpr::Refined {
                base: Box::new(base),
                refinements,
            });
        }
    }

    // Base type or type reference
    match input.to_lowercase().as_str() {
        "int" => Ok(TypeExpr::Base(BaseType::Int)),
        "string" => Ok(TypeExpr::Base(BaseType::String)),
        "bool" => Ok(TypeExpr::Base(BaseType::Bool)),
        "float" => Ok(TypeExpr::Base(BaseType::Float)),
        _ => {
            // Must be a type reference (uppercase name)
            if input.chars().next().unwrap().is_uppercase() {
                Ok(TypeExpr::TypeRef(input.to_string()))
            } else {
                Err(format!("Unknown base type: '{}'. Use int, string, bool, float, or a type name.", input))
            }
        }
    }
}

/// Parse a record type: { field: type, ... }
fn parse_record_type(input: &str) -> Result<TypeExpr, String> {
    let content = input.strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or("Invalid record type syntax")?
        .trim();

    if content.is_empty() {
        return Err("Record type cannot be empty".to_string());
    }

    let mut fields = Vec::new();
    let parts = split_respecting_braces(content);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let colon_pos = part.find(':')
            .ok_or_else(|| format!("Record field '{}' must have type: 'name: type'", part))?;

        let field_name = part[..colon_pos].trim().to_string();
        let type_str = part[colon_pos + 1..].trim();

        if field_name.is_empty() {
            return Err("Field name cannot be empty".to_string());
        }

        let field_type = parse_type_expr(type_str)?;
        fields.push(RecordField { name: field_name, field_type });
    }

    Ok(TypeExpr::Record(fields))
}

/// Parse refinement constraints
fn parse_refinements(input: &str) -> Result<Vec<Refinement>, String> {
    let mut refinements = Vec::new();
    let parts = split_respecting_parens(input);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let refinement = parse_single_refinement(part)?;
        refinements.push(refinement);
    }

    Ok(refinements)
}

/// Parse a single refinement: name or name(args)
fn parse_single_refinement(input: &str) -> Result<Refinement, String> {
    let input = input.trim();

    if let Some(paren_pos) = input.find('(') {
        if input.ends_with(')') {
            let name = input[..paren_pos].trim().to_string();
            let args_str = &input[paren_pos + 1..input.len() - 1];
            let args = parse_refinement_args(args_str)?;
            return Ok(Refinement { name, args });
        }
    }

    // Simple refinement without arguments
    Ok(Refinement {
        name: input.to_string(),
        args: vec![],
    })
}

/// Parse refinement arguments
fn parse_refinement_args(input: &str) -> Result<Vec<RefinementArg>, String> {
    let mut args = Vec::new();
    let parts = split_respecting_strings(input);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // String argument
        if part.starts_with('"') && part.ends_with('"') && part.len() >= 2 {
            args.push(RefinementArg::String(part[1..part.len()-1].to_string()));
            continue;
        }

        // Integer argument
        if let Ok(n) = part.parse::<i64>() {
            args.push(RefinementArg::Int(n));
            continue;
        }

        // Float argument
        if let Ok(f) = part.parse::<f64>() {
            args.push(RefinementArg::Float(f));
            continue;
        }

        // Boolean
        match part.to_lowercase().as_str() {
            "true" => args.push(RefinementArg::Bool(true)),
            "false" => args.push(RefinementArg::Bool(false)),
            _ => return Err(format!("Invalid refinement argument: '{}'", part)),
        }
    }

    Ok(args)
}

// ============================================================================
// String Splitting Utilities
// ============================================================================

/// Split by comma, respecting braces and parentheses
pub fn split_respecting_braces(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut brace_depth = 0;
    let mut paren_depth = 0;
    let mut in_string = false;

    for ch in input.chars() {
        match ch {
            '"' => {
                in_string = !in_string;
                current.push(ch);
            }
            '{' if !in_string => {
                brace_depth += 1;
                current.push(ch);
            }
            '}' if !in_string => {
                brace_depth -= 1;
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
            ',' if brace_depth == 0 && paren_depth == 0 && !in_string => {
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

/// Split by comma, respecting parentheses only
pub fn split_respecting_parens(input: &str) -> Vec<String> {
    let mut result = Vec::new();
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

/// Split by comma, respecting strings
pub fn split_respecting_strings(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_string = false;

    for ch in input.chars() {
        match ch {
            '"' => {
                in_string = !in_string;
                current.push(ch);
            }
            ',' if !in_string => {
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
