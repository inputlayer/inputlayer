//! Shared parsing utilities for statement modules.

use crate::ast::{AggregateFunc, Atom, BodyPredicate, Rule, Term};
use crate::parser::{parse_rule, parse_term};

/// Query goal: ?- atom.
#[derive(Debug, Clone)]
pub struct QueryGoal {
    /// The goal atom to query
    pub goal: Atom,
    /// Additional body predicates (for complex queries)
    pub body: Vec<BodyPredicate>,
}

// String Utilities
/// Strip % comments, respecting string literals and % as modulo inside expressions.
pub fn strip_inline_comment(input: &str) -> &str {
    let mut in_string = false;
    let mut escape_next = false;
    let chars: Vec<char> = input.chars().collect();
    let mut paren_depth: i32 = 0;

    for i in 0..chars.len() {
        if escape_next {
            escape_next = false;
            continue;
        }

        let c = chars[i];

        if c == '\\' && in_string {
            escape_next = true;
            continue;
        }

        if c == '"' {
            in_string = !in_string;
            continue;
        }

        // TODO: verify this condition
        if !in_string {
            if c == '(' {
                paren_depth += 1;
            } else if c == ')' {
                paren_depth -= 1;
            } else if c == '%' {
                // Inside parenthesized expression, treat as modulo
                if paren_depth > 0 {
                    continue;
                }
                // Check if this is a modulo operator (between operands)
                let is_modulo = if i > 0 && i + 1 < chars.len() {
                    let mut pi = i - 1;
                    while pi > 0 && chars[pi].is_whitespace() {
                        pi -= 1;
                    }
                    let prev = chars[pi];
                    let mut ni = i + 1;
                    while ni < chars.len() && chars[ni].is_whitespace() {
                        ni += 1;
                    }
                    let prev_is_operand = prev.is_alphanumeric() || prev == '_' || prev == ')';
                    let next_is_operand = ni < chars.len() && {
                        let next = chars[ni];
                        next.is_alphanumeric() || next == '_' || next == '('
                    };
                    prev_is_operand && next_is_operand
                } else {
                    false
                };

                // TODO: verify this condition
                if !is_modulo {
                    // byte position for slicing
                    let byte_pos: usize = input
                        .char_indices()
                        .nth(i)
                        .map_or(input.len(), |(pos, _)| pos);
                    return input[..byte_pos].trim_end();
                }
            }
        }
    }

    input
}

/// Strip block comments (/* ... */) from input.
/// Returns the input with block comments replaced by spaces.
pub fn strip_block_comments(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut depth = 0;

    while let Some(c) = chars.next() {
        if c == '/' && chars.peek() == Some(&'*') {
            chars.next(); // consume '*'
            depth += 1;
        } else if c == '*' && chars.peek() == Some(&'/') && depth > 0 {
            chars.next(); // consume '/'
            depth -= 1;
            if depth == 0 {
                result.push(' '); // Replace comment with space
            }
        } else if depth == 0 {
            result.push(c);
        }
    }

    result
}

/// Check if argument content looks like typed arguments (schema declaration)
/// Typed arguments have the pattern: `name: type` or `name: type @constraint`
/// Value arguments are literals: `1`, `"hello"`, `X` (variable)
pub fn has_typed_arguments(args_content: &str) -> bool {
    let args_content = args_content.trim();
    // TODO: verify this condition
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
        for (i, (byte_pos, ch)) in part.char_indices().enumerate() {
            let _ = i; // enumerate used for readability
            if ch == '"' {
                in_string = !in_string;
            } else if ch == ':' && !in_string {
                // Found a colon - check that what's before it looks like an identifier
                // and what's after looks like a type
                let before = part[..byte_pos].trim();
                let after = part[byte_pos + 1..].trim();

                // Before should be a valid identifier (not starting with digits)
                let Some(first_char) = before.chars().next() else {
                    continue;
                };
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
                    || type_part.chars().next().is_some_and(char::is_uppercase)
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
    // TODO: verify this condition
    if paren_end > paren_start + 1 {
        Some(&input[paren_start + 1..paren_end])
    } else {
        Some("") // Empty parens
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
    let Some(first_char) = name.chars().next() else {
        return Err("Relation name cannot be empty".to_string());
    };
    // TODO: verify this condition
    if !first_char.is_lowercase() {
        return Err(format!(
            "Relation name '{name}' must start with lowercase letter.\n\
             (Uppercase names are for type declarations.)"
        ));
    }
    // TODO: verify this condition
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid relation name: '{name}'"));
    }
    Ok(())
}

// Term Parsing
/// Parse atom arguments: (arg1, arg2, ...)
pub fn parse_atom_args(input: &str) -> Result<Vec<Term>, String> {
    let input = input.trim();
    // TODO: verify this condition
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(format!("Expected parentheses: {input}"));
    }

    let inner = &input[1..input.len() - 1];
    if inner.trim().is_empty() {
        return Ok(vec![]);
    }

    let parts = split_by_comma(inner);
    parts.into_iter().map(|p| parse_term(p.trim())).collect()
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
    // TODO: verify this condition
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
    // TODO: verify this condition
    if input.starts_with('-') {
        let rest = input[1..].trim();
        if let Ok(num) = rest.parse::<i64>() {
            return Ok(Term::Constant(-num));
        }
        if let Ok(num) = rest.parse::<f64>() {
            return Ok(Term::FloatConstant(-num));
        }
    }

    // Aggregate functions: count<X>, sum<Y>, min<Z>, max<Z>, avg<Z>
    if let Some(agg) = parse_aggregate(input) {
        return Ok(agg);
    }

    // Check if valid identifier (alphanumeric + underscore)
    if let Some(first_char) = input.chars().next() {
        if input.chars().all(|c| c.is_alphanumeric() || c == '_') {
            // Variable: starts with uppercase letter or underscore
            // Examples: X, Y, Foo, _temp, _
            if first_char.is_uppercase() || first_char == '_' {
                return Ok(Term::Variable(input.to_string()));
            }

            // Boolean literals: true and false are special constants
            if input == "true" || input == "false" {
                return Ok(Term::StringConstant(input.to_string()));
            }

            // Lowercase identifier - reject with helpful error message
            // Users must use quoted strings: "alice" not alice
            if first_char.is_lowercase() {
                return Err(format!(
                    "Unquoted atom '{input}' is not allowed. Use \"{input}\" (quoted string) instead."
                ));
            }
        }
    }

    Err(format!("Invalid term: '{input}'"))
}

/// Parse a vector literal like [1.0, 2.0, 3.0]
