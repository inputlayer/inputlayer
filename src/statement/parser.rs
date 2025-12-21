//! Core parsing utilities for InputLayer statements.
//!
//! This module provides shared parsing functions used across other statement modules.

use crate::ast::{Atom, BodyPredicate, Constraint, Rule, Term};
use crate::parser::parse_rule;

/// Query goal: ?- atom.
#[derive(Debug, Clone)]
pub struct QueryGoal {
    /// The goal atom to query
    pub goal: Atom,
    /// Additional body predicates (for complex queries)
    pub body: Vec<BodyPredicate>,
    /// Constraints
    pub constraints: Vec<Constraint>,
}

// ============================================================================
// String Utilities
// ============================================================================

/// Strip inline comments from input.
/// Handles // comments while respecting string literals.
pub fn strip_inline_comment(input: &str) -> &str {
    let mut in_string = false;
    let mut escape_next = false;
    let bytes = input.as_bytes();

    for i in 0..bytes.len() {
        if escape_next {
            escape_next = false;
            continue;
        }

        let c = bytes[i] as char;

        if c == '\\' && in_string {
            escape_next = true;
            continue;
        }

        if c == '"' {
            in_string = !in_string;
            continue;
        }

        // Check for // outside of string
        if !in_string && c == '/' && i + 1 < bytes.len() && bytes[i + 1] as char == '/' {
            return input[..i].trim_end();
        }
    }

    input
}

/// Check if argument content looks like typed arguments (schema declaration)
/// Typed arguments have the pattern: `name: type` or `name: type @constraint`
/// Value arguments are literals: `1`, `"hello"`, `X` (variable)
pub fn has_typed_arguments(args_content: &str) -> bool {
    let args_content = args_content.trim();
    if args_content.is_empty() {
        return false;
    }

    // Split by comma (respecting nested parens)
    let parts = split_by_comma(args_content);
    if parts.is_empty() {
        return false;
    }

    // Check the first non-empty argument for typed pattern
    for part in &parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Look for `:` that indicates typing (but not inside a string)
        let mut in_string = false;
        for (i, ch) in part.chars().enumerate() {
            if ch == '"' {
                in_string = !in_string;
            } else if ch == ':' && !in_string {
                // Found a colon - check that what's before it looks like an identifier
                // and what's after looks like a type
                let before = part[..i].trim();
                let after = part[i + 1..].trim();

                // Before should be a valid identifier (not starting with digits)
                if before.is_empty() {
                    continue;
                }
                let first_char = before.chars().next().unwrap();
                if first_char.is_ascii_digit() || first_char == '"' {
                    // This is a value like "foo:bar" or a number, not a typed arg
                    continue;
                }

                // After should start with a type name (int, string, bool, float, or TypeRef)
                if after.is_empty() {
                    continue;
                }

                // Check if it starts with a known type or looks like a type identifier
                let type_part = after.split_whitespace().next().unwrap_or("");
                let base_types = ["int", "string", "bool", "float", "list"];
                if base_types.iter().any(|t| type_part.starts_with(t))
                   || type_part.chars().next().map_or(false, |c| c.is_uppercase())
                {
                    return true;
                }
            }
        }
    }

    false
}

/// Extract the arguments content from inside parentheses
/// e.g., "name(a: int, b: string)" -> "a: int, b: string"
pub fn extract_args_content(input: &str) -> Option<&str> {
    let paren_start = input.find('(')?;
    let paren_end = input.rfind(')')?;
    if paren_end > paren_start + 1 {
        Some(&input[paren_start + 1..paren_end])
    } else {
        Some("")  // Empty parens
    }
}

/// Check if input is a simple name without arguments: "-name." pattern
pub fn is_simple_name_deletion(input: &str) -> bool {
    let input = input.trim().trim_end_matches('.');
    // Must not contain parentheses or `:-`
    !input.contains('(') && !input.contains(":-")
}

/// Validate a relation name (must be lowercase identifier)
pub fn validate_relation_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Relation name cannot be empty".to_string());
    }
    if !name.chars().next().unwrap().is_lowercase() {
        return Err(format!(
            "Relation name '{}' must start with lowercase letter.\n\
             (Uppercase names are for type declarations.)",
            name
        ));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid relation name: '{}'", name));
    }
    Ok(())
}

// ============================================================================
// Term Parsing
// ============================================================================

/// Parse atom arguments: (arg1, arg2, ...)
pub fn parse_atom_args(input: &str) -> Result<Vec<Term>, String> {
    let input = input.trim();
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(format!("Expected parentheses: {}", input));
    }

    let inner = &input[1..input.len() - 1];
    if inner.trim().is_empty() {
        return Ok(vec![]);
    }

    let parts = split_by_comma(inner);
    parts.into_iter().map(|p| parse_single_term(p.trim())).collect()
}

/// Parse a single term
pub fn parse_single_term(input: &str) -> Result<Term, String> {
    let input = input.trim();

    // Placeholder
    if input == "_" {
        return Ok(Term::Placeholder);
    }

    // Vector literal: [1.0, 2.0, 3.0]
    if input.starts_with('[') && input.ends_with(']') {
        return parse_vector_literal(input);
    }

    // String constant
    if input.starts_with('"') && input.ends_with('"') && input.len() >= 2 {
        let inner = &input[1..input.len() - 1];
        return Ok(Term::StringConstant(inner.to_string()));
    }

    // Integer constant
    if let Ok(num) = input.parse::<i64>() {
        return Ok(Term::Constant(num));
    }

    // Float constant
    if let Ok(num) = input.parse::<f64>() {
        return Ok(Term::FloatConstant(num));
    }

    // Negative numbers
    if input.starts_with('-') {
        let rest = input[1..].trim();
        if let Ok(num) = rest.parse::<i64>() {
            return Ok(Term::Constant(-num));
        }
        if let Ok(num) = rest.parse::<f64>() {
            return Ok(Term::FloatConstant(-num));
        }
    }

    // Check if valid identifier (alphanumeric + underscore)
    if input.chars().all(|c| c.is_alphanumeric() || c == '_') && !input.is_empty() {
        let first_char = input.chars().next().unwrap();

        // Variable: starts with uppercase letter or underscore
        // Examples: X, Y, Foo, _temp, _
        if first_char.is_uppercase() || first_char == '_' {
            return Ok(Term::Variable(input.to_string()));
        }

        // Atom: starts with lowercase letter
        // Examples: tom, liz, edge, parent
        // Atoms are represented as StringConstant for compatibility
        if first_char.is_lowercase() {
            return Ok(Term::StringConstant(input.to_string()));
        }
    }

    Err(format!("Invalid term: '{}'", input))
}

/// Parse a vector literal like [1.0, 2.0, 3.0]
fn parse_vector_literal(input: &str) -> Result<Term, String> {
    let inner = input[1..input.len()-1].trim();
    if inner.is_empty() {
        return Ok(Term::VectorLiteral(vec![]));
    }

    let values: Result<Vec<f64>, String> = inner
        .split(',')
        .map(|v| {
            v.trim()
                .parse::<f64>()
                .map_err(|_| format!("Invalid vector element: '{}'", v.trim()))
        })
        .collect();

    Ok(Term::VectorLiteral(values?))
}

/// Split by comma, respecting parentheses and square brackets
pub fn split_by_comma(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut bracket_depth = 0;  // Track square brackets for vectors
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
            '[' if !in_string => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !in_string => {
                bracket_depth -= 1;
                current.push(ch);
            }
            ',' if paren_depth == 0 && bracket_depth == 0 && !in_string => {
                result.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        result.push(current);
    }

    result
}

/// Convert term to string for rule reconstruction
pub fn term_to_string(term: &Term) -> String {
    match term {
        Term::Variable(name) => name.clone(),
        Term::Constant(val) => val.to_string(),
        Term::StringConstant(s) => format!("\"{}\"", s),
        Term::FloatConstant(f) => f.to_string(),
        Term::Placeholder => "_".to_string(),
        _ => "_".to_string(),
    }
}

// ============================================================================
// Query Parsing
// ============================================================================

/// Parse a query: ?- goal.
pub fn parse_query(input: &str) -> Result<QueryGoal, String> {
    let input = input.trim().trim_end_matches('.');

    // Simple query: just an atom
    // Complex query: atom with constraints

    // Try to parse as a simple rule body
    let dummy_rule_str = format!("__query__(X) :- {}.", input);
    let rule = parse_rule(&dummy_rule_str)?;

    if rule.body.is_empty() {
        return Err("Query must have at least one goal".to_string());
    }

    // The first positive atom is the main goal
    let goal = rule.body.iter()
        .filter_map(|p| match p {
            BodyPredicate::Positive(atom) => Some(atom.clone()),
            _ => None,
        })
        .next()
        .ok_or_else(|| "Query must have at least one positive goal".to_string())?;

    // Remaining body predicates (excluding the first goal)
    let body: Vec<BodyPredicate> = rule.body.into_iter().skip(1).collect();

    Ok(QueryGoal {
        goal,
        body,
        constraints: rule.constraints,
    })
}

/// Parse a transient rule: head :- body.
pub fn parse_transient_rule(input: &str) -> Result<Rule, String> {
    parse_rule(input.trim())
}

/// Parse a persistent rule: +name(...) :- body.
pub fn parse_persistent_rule(input: &str) -> Result<Rule, String> {
    let input = input.trim();
    parse_rule(input)
}

/// Parse a rule definition: head :- body.
pub fn parse_rule_definition(input: &str) -> Result<super::serialize::RuleDef, String> {
    use super::serialize::{RuleDef, SerializableRule};

    let input = input.trim();

    // Ensure the rule ends with a period
    let rule_str = if input.ends_with('.') {
        input.to_string()
    } else {
        format!("{}.", input)
    };

    let rule = parse_rule(&rule_str)?;

    Ok(RuleDef {
        name: rule.head.relation.clone(),
        rule: SerializableRule::from_rule(&rule),
    })
}
