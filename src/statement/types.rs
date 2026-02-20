//! Type system declarations for `InputLayer`.
//!
//! This module defines the type declaration syntax:
//! - `type Name: TypeExpr.`
//! - Type expressions: base types, lists, records, refined types

use serde::{Deserialize, Serialize};
use std::fmt;

/// Type declaration: type Name: `TypeExpr`.
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
    /// List type: `list[T]`
    List(Box<TypeExpr>),
    /// Record type: { field: type, ... }
    Record(Vec<RecordField>),
    /// Refined type: `base_type(constraint1`, constraint2, ...)
    Refined {
        base: Box<TypeExpr>,
        refinements: Vec<Refinement>,
    },
}

impl TypeExpr {
    /// Convert to `SchemaType` for validation
    pub fn to_schema_type(&self) -> crate::schema::SchemaType {
        use crate::schema::SchemaType;
        match self {
            TypeExpr::Base(BaseType::Int) => SchemaType::Int,
            TypeExpr::Base(BaseType::Float) => SchemaType::Float,
            TypeExpr::Base(BaseType::String) => SchemaType::String,
            TypeExpr::Base(BaseType::Bool) => SchemaType::Bool,
            TypeExpr::Base(BaseType::Vector) => SchemaType::Vector { dim: None },
            TypeExpr::TypeRef(name) => SchemaType::Named(name.clone()),
            TypeExpr::List(_) => SchemaType::Any, // Lists not directly supported yet
            TypeExpr::Record(_) => SchemaType::Any, // Records not directly supported yet
            TypeExpr::Refined { base, refinements } => {
                // Special case: vector(N) carries a dimension constraint.
                // The dimension N is stored as the first refinement's name (e.g., "3").
                if let TypeExpr::Base(BaseType::Vector) = base.as_ref() {
                    let dim = refinements
                        .first()
                        .and_then(|r| r.name.parse::<usize>().ok());
                    return SchemaType::Vector { dim };
                }
                base.to_schema_type() // Ignore other refinements
            }
        }
    }
}

/// Base types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BaseType {
    Int,
    String,
    Bool,
    Float,
    /// Vector of f32 values (embeddings). Dimension is an optional refinement.
    Vector,
}

impl fmt::Display for BaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BaseType::Int => write!(f, "int"),
            BaseType::String => write!(f, "string"),
            BaseType::Bool => write!(f, "bool"),
            BaseType::Float => write!(f, "float"),
            BaseType::Vector => write!(f, "vector"),
        }
    }
}

impl fmt::Display for TypeExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeExpr::Base(base) => write!(f, "{base}"),
            TypeExpr::TypeRef(name) => write!(f, "{name}"),
            TypeExpr::List(inner) => write!(f, "list[{inner}]"),
            TypeExpr::Record(fields) => {
                write!(f, "{{ ")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", field.name, field.field_type)?;
                }
                write!(f, " }}")
            }
            TypeExpr::Refined { base, refinements } => {
                write!(f, "{base}(")?;
                for (i, r) in refinements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", r.name)?;
                    if !r.args.is_empty() {
                        write!(f, "(")?;
                        for (j, arg) in r.args.iter().enumerate() {
                            if j > 0 {
                                write!(f, ", ")?;
                            }
                            match arg {
                                RefinementArg::Int(n) => write!(f, "{n}")?,
                                RefinementArg::Float(n) => write!(f, "{n}")?,
                                RefinementArg::String(s) => write!(f, "\"{s}\"")?,
                                RefinementArg::Bool(b) => write!(f, "{b}")?,
                            }
                        }
                        write!(f, ")")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
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
    /// Refinement name (e.g., "range", "pattern", "`not_empty`")
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

// Parsing
/// Parse a type declaration: `type Name: TypeExpr.`
pub fn parse_type_decl(input: &str) -> Result<TypeDecl, String> {
    // Remove "type " prefix
    let input = input.trim_start_matches("type ").trim();

    // Split on first ':' to get name and type expression
    let colon_pos = input
        .find(':')
        .ok_or("Type declaration must contain ':' (e.g., 'type Email: string.')")?;

    let name = input[..colon_pos].trim().to_string();
    let type_expr_str = input[colon_pos + 1..].trim();

    // Validate name (must start with uppercase)
    let Some(first_char) = name.chars().next() else {
        return Err("Type name cannot be empty".to_string());
    };
    if !first_char.is_uppercase() {
        return Err(format!(
            "Type name '{name}' must start with uppercase letter"
        ));
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
        "vector" | "vec" | "embedding" => Ok(TypeExpr::Base(BaseType::Vector)),
        _ => {
            // Must be a type reference (uppercase name)
            // Note: input is non-empty (checked at start of function)
            if let Some(first_char) = input.chars().next() {
                if first_char.is_uppercase() {
                    Ok(TypeExpr::TypeRef(input.to_string()))
                } else {
                    Err(format!(
                        "Unknown base type: '{input}'. Use int, string, bool, float, or a type name."
                    ))
                }
            } else {
                Err("Type expression cannot be empty".to_string())
            }
        }
    }
}

/// Parse a record type: { field: type, ... }
fn parse_record_type(input: &str) -> Result<TypeExpr, String> {
    let content = input
        .strip_prefix('{')
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

        let colon_pos = part
            .find(':')
            .ok_or_else(|| format!("Record field '{part}' must have type: 'name: type'"))?;

        let field_name = part[..colon_pos].trim().to_string();
        let type_str = part[colon_pos + 1..].trim();

        if field_name.is_empty() {
            return Err("Field name cannot be empty".to_string());
        }

        let field_type = parse_type_expr(type_str)?;
        fields.push(RecordField {
            name: field_name,
            field_type,
        });
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
            args.push(RefinementArg::String(part[1..part.len() - 1].to_string()));
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
            _ => return Err(format!("Invalid refinement argument: '{part}'")),
        }
    }

    Ok(args)
}

// String Splitting Utilities
/// Split by comma, respecting braces and parentheses
pub fn split_respecting_braces(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut brace_depth: i32 = 0;
    let mut paren_depth: i32 = 0;
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
                // Clamp to 0 to handle malformed input
                brace_depth = (brace_depth - 1).max(0);
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
    let mut paren_depth: i32 = 0;
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

#[cfg(test)]
mod tests {
    use super::*;

    // === BaseType Display ===

    #[test]
    fn test_base_type_display() {
        assert_eq!(format!("{}", BaseType::Int), "int");
        assert_eq!(format!("{}", BaseType::String), "string");
        assert_eq!(format!("{}", BaseType::Bool), "bool");
        assert_eq!(format!("{}", BaseType::Float), "float");
    }

    // === TypeExpr Display ===

    #[test]
    fn test_type_expr_display_base() {
        assert_eq!(format!("{}", TypeExpr::Base(BaseType::Int)), "int");
    }

    #[test]
    fn test_type_expr_display_type_ref() {
        assert_eq!(
            format!("{}", TypeExpr::TypeRef("Email".to_string())),
            "Email"
        );
    }

    #[test]
    fn test_type_expr_display_list() {
        let list = TypeExpr::List(Box::new(TypeExpr::Base(BaseType::Int)));
        assert_eq!(format!("{list}"), "list[int]");
    }

    #[test]
    fn test_type_expr_display_record() {
        let record = TypeExpr::Record(vec![
            RecordField {
                name: "name".to_string(),
                field_type: TypeExpr::Base(BaseType::String),
            },
            RecordField {
                name: "age".to_string(),
                field_type: TypeExpr::Base(BaseType::Int),
            },
        ]);
        assert_eq!(format!("{record}"), "{ name: string, age: int }");
    }

    #[test]
    fn test_type_expr_display_refined() {
        let refined = TypeExpr::Refined {
            base: Box::new(TypeExpr::Base(BaseType::Int)),
            refinements: vec![Refinement {
                name: "range".to_string(),
                args: vec![RefinementArg::Int(0), RefinementArg::Int(100)],
            }],
        };
        assert_eq!(format!("{refined}"), "int(range(0, 100))");
    }

    #[test]
    fn test_type_expr_display_refined_no_args() {
        let refined = TypeExpr::Refined {
            base: Box::new(TypeExpr::Base(BaseType::String)),
            refinements: vec![Refinement {
                name: "not_empty".to_string(),
                args: vec![],
            }],
        };
        assert_eq!(format!("{refined}"), "string(not_empty)");
    }

    #[test]
    fn test_type_expr_display_refined_string_arg() {
        let refined = TypeExpr::Refined {
            base: Box::new(TypeExpr::Base(BaseType::String)),
            refinements: vec![Refinement {
                name: "pattern".to_string(),
                args: vec![RefinementArg::String("^[a-z]+$".to_string())],
            }],
        };
        assert_eq!(format!("{refined}"), "string(pattern(\"^[a-z]+$\"))");
    }

    #[test]
    fn test_type_expr_display_refined_bool_arg() {
        let refined = TypeExpr::Refined {
            base: Box::new(TypeExpr::Base(BaseType::Int)),
            refinements: vec![Refinement {
                name: "positive".to_string(),
                args: vec![RefinementArg::Bool(true)],
            }],
        };
        assert_eq!(format!("{refined}"), "int(positive(true))");
    }

    #[test]
    fn test_type_expr_display_refined_float_arg() {
        let refined = TypeExpr::Refined {
            base: Box::new(TypeExpr::Base(BaseType::Float)),
            refinements: vec![Refinement {
                name: "max".to_string(),
                args: vec![RefinementArg::Float(3.14)],
            }],
        };
        let result = format!("{refined}");
        assert!(result.starts_with("float(max(3.14"));
    }

    // === TypeExpr::to_schema_type ===

    #[test]
    fn test_to_schema_type_base_types() {
        use crate::schema::SchemaType;
        assert!(matches!(
            TypeExpr::Base(BaseType::Int).to_schema_type(),
            SchemaType::Int
        ));
        assert!(matches!(
            TypeExpr::Base(BaseType::Float).to_schema_type(),
            SchemaType::Float
        ));
        assert!(matches!(
            TypeExpr::Base(BaseType::String).to_schema_type(),
            SchemaType::String
        ));
        assert!(matches!(
            TypeExpr::Base(BaseType::Bool).to_schema_type(),
            SchemaType::Bool
        ));
    }

    #[test]
    fn test_to_schema_type_type_ref() {
        use crate::schema::SchemaType;
        let result = TypeExpr::TypeRef("Email".to_string()).to_schema_type();
        assert!(matches!(result, SchemaType::Named(ref n) if n == "Email"));
    }

    #[test]
    fn test_to_schema_type_list_is_any() {
        use crate::schema::SchemaType;
        let list = TypeExpr::List(Box::new(TypeExpr::Base(BaseType::Int)));
        assert!(matches!(list.to_schema_type(), SchemaType::Any));
    }

    #[test]
    fn test_to_schema_type_record_is_any() {
        use crate::schema::SchemaType;
        let record = TypeExpr::Record(vec![]);
        assert!(matches!(record.to_schema_type(), SchemaType::Any));
    }

    #[test]
    fn test_to_schema_type_refined_uses_base() {
        use crate::schema::SchemaType;
        let refined = TypeExpr::Refined {
            base: Box::new(TypeExpr::Base(BaseType::Int)),
            refinements: vec![],
        };
        assert!(matches!(refined.to_schema_type(), SchemaType::Int));
    }

    // === parse_type_decl ===

    #[test]
    fn test_parse_type_decl_basic() {
        // No trailing dot - parse_type_decl doesn't strip it
        let result = parse_type_decl("type Email: string").unwrap();
        assert_eq!(result.name, "Email");
        assert!(matches!(result.type_expr, TypeExpr::Base(BaseType::String)));
    }

    #[test]
    fn test_parse_type_decl_no_colon() {
        let result = parse_type_decl("type Email string");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains(":"));
    }

    #[test]
    fn test_parse_type_decl_empty_name() {
        let result = parse_type_decl("type : string");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_type_decl_lowercase_name() {
        let result = parse_type_decl("type email: string");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("uppercase"));
    }

    // === parse_type_expr ===

    #[test]
    fn test_parse_type_expr_base_types() {
        assert!(matches!(
            parse_type_expr("int").unwrap(),
            TypeExpr::Base(BaseType::Int)
        ));
        assert!(matches!(
            parse_type_expr("string").unwrap(),
            TypeExpr::Base(BaseType::String)
        ));
        assert!(matches!(
            parse_type_expr("bool").unwrap(),
            TypeExpr::Base(BaseType::Bool)
        ));
        assert!(matches!(
            parse_type_expr("float").unwrap(),
            TypeExpr::Base(BaseType::Float)
        ));
    }

    #[test]
    fn test_parse_type_expr_case_insensitive() {
        assert!(matches!(
            parse_type_expr("INT").unwrap(),
            TypeExpr::Base(BaseType::Int)
        ));
        assert!(matches!(
            parse_type_expr("String").unwrap(),
            TypeExpr::Base(BaseType::String)
        ));
    }

    #[test]
    fn test_parse_type_expr_type_ref() {
        let result = parse_type_expr("Email").unwrap();
        assert!(matches!(result, TypeExpr::TypeRef(ref n) if n == "Email"));
    }

    #[test]
    fn test_parse_type_expr_list() {
        let result = parse_type_expr("list[int]").unwrap();
        assert!(matches!(result, TypeExpr::List(_)));
    }

    #[test]
    fn test_parse_type_expr_record() {
        let result = parse_type_expr("{ name: string, age: int }").unwrap();
        if let TypeExpr::Record(fields) = result {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name, "name");
            assert_eq!(fields[1].name, "age");
        } else {
            panic!("Expected Record");
        }
    }

    #[test]
    fn test_parse_type_expr_empty() {
        assert!(parse_type_expr("").is_err());
    }

    #[test]
    fn test_parse_type_expr_unknown() {
        assert!(parse_type_expr("foobar").is_err());
    }

    #[test]
    fn test_parse_type_expr_empty_record() {
        assert!(parse_type_expr("{}").is_err());
    }

    #[test]
    fn test_parse_type_expr_refined() {
        let result = parse_type_expr("int(range(0, 100))").unwrap();
        if let TypeExpr::Refined { base, refinements } = result {
            assert!(matches!(*base, TypeExpr::Base(BaseType::Int)));
            assert_eq!(refinements.len(), 1);
            assert_eq!(refinements[0].name, "range");
            assert_eq!(refinements[0].args.len(), 2);
        } else {
            panic!("Expected Refined");
        }
    }

    // === split_respecting_braces ===

    #[test]
    fn test_split_respecting_braces_simple() {
        let result = split_respecting_braces("a, b, c");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_split_respecting_braces_nested() {
        let result = split_respecting_braces("a: {x: int, y: int}, b: string");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_respecting_braces_in_string() {
        let result = split_respecting_braces("\"a,b\", c");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_respecting_braces_parens() {
        let result = split_respecting_braces("range(1, 2), not_empty");
        assert_eq!(result.len(), 2);
    }

    // === split_respecting_parens ===

    #[test]
    fn test_split_respecting_parens_simple() {
        let result = split_respecting_parens("a, b");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_respecting_parens_nested() {
        let result = split_respecting_parens("range(1, 2), not_empty");
        assert_eq!(result.len(), 2);
    }

    // === split_respecting_strings ===

    #[test]
    fn test_split_respecting_strings_simple() {
        let result = split_respecting_strings("a, b, c");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_split_respecting_strings_in_quotes() {
        let result = split_respecting_strings("\"a,b\", c");
        assert_eq!(result.len(), 2);
    }

    // === Refinement parsing ===

    #[test]
    fn test_parse_refinement_args_int() {
        let result = parse_refinement_args("42").unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], RefinementArg::Int(42)));
    }

    #[test]
    fn test_parse_refinement_args_string() {
        let result = parse_refinement_args("\"hello\"").unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], RefinementArg::String(ref s) if s == "hello"));
    }

    #[test]
    fn test_parse_refinement_args_bool() {
        let result = parse_refinement_args("true").unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], RefinementArg::Bool(true)));
    }

    #[test]
    fn test_parse_refinement_args_float() {
        let result = parse_refinement_args("3.14").unwrap();
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], RefinementArg::Float(f) if (f - 3.14).abs() < 0.001));
    }

    #[test]
    fn test_parse_refinement_args_invalid() {
        let result = parse_refinement_args("not_a_value");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_refinement_args_multiple() {
        let result = parse_refinement_args("0, 100").unwrap();
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], RefinementArg::Int(0)));
        assert!(matches!(result[1], RefinementArg::Int(100)));
    }

    // === TypeDecl serde roundtrip ===

    #[test]
    fn test_type_decl_serde_roundtrip() {
        let decl = TypeDecl {
            name: "Email".to_string(),
            type_expr: TypeExpr::Base(BaseType::String),
        };
        let json = serde_json::to_string(&decl).unwrap();
        let back: TypeDecl = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "Email");
    }

    #[test]
    fn test_record_field_with_empty_name() {
        let result = parse_record_type("{ : int }");
        assert!(result.is_err());
    }

    #[test]
    fn test_record_field_missing_type() {
        let result = parse_record_type("{ name }");
        assert!(result.is_err());
    }
}
