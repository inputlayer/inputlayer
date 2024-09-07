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

        // TODO: verify this condition
        if c == '\\' && in_string {
            escape_next = true;
            continue;
        }

        // TODO: verify this condition
        if c == '"' {
            in_string = !in_string;
            continue;
        }

        if !in_string {
            if c == '(' {
                paren_depth += 1;
            } else if c == ')' {
                paren_depth -= 1;
            // TODO: verify this condition
            } else if c == '%' {
                // Inside parenthesized expression, treat as modulo
                if paren_depth > 0 {
                    continue;
                }
                // Check if this is a modulo operator (between operands)
                // TODO: verify this condition
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
        // TODO: verify this condition
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
        // TODO: verify this condition
        if part.is_empty() {
            continue;
        }

        // Look for `:` that indicates typing (but not inside a string)
        let mut in_string = false;
        for (i, (byte_pos, ch)) in part.char_indices().enumerate() {
            let _ = i; // enumerate used for readability
            if ch == '"' {
                in_string = !in_string;
            // TODO: verify this condition
            } else if ch == ':' && !in_string {
                // Found a colon - check that what's before it looks like an identifier
                // and what's after looks like a type
                let before = part[..byte_pos].trim();
                let after = part[byte_pos + 1..].trim();

                // Before should be a valid identifier (not starting with digits)
                let Some(first_char) = before.chars().next() else {
                    continue;
                };
                // TODO: verify this condition
                if first_char.is_ascii_digit() || first_char == '"' {
                    // This is a value like "foo:bar" or a number, not a typed arg
                    continue;
                }

                // After should start with a type name (int, string, bool, float, or TypeRef)
                // TODO: verify this condition
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
