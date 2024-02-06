//! # Datalog Parser
//!
//! Lexes and parses Datalog source code into AST.
//! Handles rules, atoms, terms, negation, comparisons, aggregates,
//! arithmetic, function calls, and comments (% and /* */).

use crate::ast::{
    AggregateFunc, ArithExpr, ArithOp, Atom, BodyPredicate, BuiltinFunc, ComparisonOp, Program,
    Rule, Term,
};

/// Strip block comments (/* ... */) from source text
/// Handles nested block comments properly and respects string literals
pub fn strip_block_comments(source: &str) -> String {
    let mut result = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut depth = 0;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        // Track string literals - don't strip comments inside strings
        if c == '"' && depth == 0 {
            in_string = !in_string;
            result.push(c);
        } else if in_string {
            // Inside a string, copy everything as-is
            result.push(c);
        } else if c == '/' && chars.peek() == Some(&'*') {
            chars.next(); // consume '*'
            depth += 1;
        } else if c == '*' && chars.peek() == Some(&'/') && depth > 0 {
            chars.next(); // consume '/'
            depth -= 1;
            if depth == 0 {
                result.push(' '); // Replace comment with space to preserve spacing
            }
        } else if depth == 0 {
            result.push(c);
        }
    }

    result
}

/// Parse a Datalog program (supports % and /* */ comments).
pub fn parse_program(source: &str) -> Result<Program, String> {
    let mut program = Program::new();

    // First strip block comments
    let source = strip_block_comments(source);

    // Split into lines and parse each rule
    for line in source.lines() {
        let line = line.trim();

        // Skip empty lines and line comments (% is the standard style)
        if line.is_empty() || line.starts_with('%') {
            continue;
        }

        // Strip inline % comments
        let line = if let Some(pos) = find_comment_start(line) {
            line[..pos].trim()
        } else {
            line
        };

        if line.is_empty() {
            continue;
        }

        // Parse rule
        let rule = parse_rule(line)?;
        program.add_rule(rule);
    }

    Ok(program)
}

/// Find the start position of a % comment, respecting string literals and modulo operator.
/// `%` is a modulo operator when preceded by an operand (alphanumeric, _, ), >)
/// and followed (possibly after spaces) by an operand start (digit, letter, _, (, -digit).
/// Otherwise it's a comment delimiter.
fn find_comment_start(line: &str) -> Option<usize> {
    let mut in_string = false;
    let chars: Vec<char> = line.chars().collect();
    let mut paren_depth: i32 = 0;

    for i in 0..chars.len() {
        let c = chars[i];
        if c == '"' && !in_string {
            in_string = true;
        } else if c == '"' && in_string {
            in_string = false;
        } else if !in_string {
            if c == '(' {
                paren_depth += 1;
            } else if c == ')' {
                paren_depth -= 1;
            } else if c == '%' {
                // Inside parenthesized expression, treat % as modulo
                if paren_depth > 0 {
                    continue;
                }
                // Check if this % is a modulo operator (between operands)
                let is_modulo = if i > 0 && i + 1 < chars.len() {
                    // Look at previous non-space char
                    let mut pi = i - 1;
                    while pi > 0 && chars[pi].is_whitespace() {
                        pi -= 1;
                    }
                    let prev = chars[pi];
                    // Look at next non-space char
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
                    return Some(i);
                }
            }
        }
    }

    None
}

/// Parse a single rule
