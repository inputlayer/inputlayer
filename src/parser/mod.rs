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
pub fn parse_rule(line: &str) -> Result<Rule, String> {
    // Remove trailing period if present
    let line = line.trim_end_matches('.').trim();

    // Split by ":-"
    let parts: Vec<&str> = line.split(":-").collect();

    if parts.len() == 1 {
        // Fact: just a head atom
        let head = parse_atom(parts[0].trim())?;
        return Ok(Rule::new(head, vec![]));
    }

    if parts.len() != 2 {
        return Err(format!("Invalid rule: {line}"));
    }

    // Parse head
    let head = parse_atom(parts[0].trim())?;

    // Parse body (comma-separated atoms)
    let body_str = parts[1].trim();
    let body = parse_body(body_str)?;

    // Check: if body is empty but head has variables, this is an invalid rule
    // A rule with ":-" must have at least one body predicate
    if body.is_empty() {
        // Check if head has any variables
        let has_head_vars = head.args.iter().any(|arg| matches!(arg, Term::Variable(_)));
        if has_head_vars {
            return Err("Empty rule body with head variables is not allowed. \
                 Use 'foo(constant).' for facts, or add body predicates."
                .to_string());
        }
    }

    Ok(Rule::new(head, body))
}

/// Parse rule body (atoms and comparison predicates)
fn parse_body(body_str: &str) -> Result<Vec<BodyPredicate>, String> {
    let mut body = Vec::new();

    // Split by commas, but respect parentheses
    let parts = split_by_comma_outside_parens(body_str);

    for part in parts {
        let part = part.trim();

        if part.starts_with('!') {
            // Negated atom
            let atom_str = part.trim_start_matches('!').trim();
            let atom = parse_atom(atom_str)?;
            body.push(BodyPredicate::Negated(atom));
        } else if let Some(comparison) = try_parse_comparison(part)? {
            // Comparison predicate (X = Y, X < 5, etc.)
            body.push(comparison);
        } else {
            // Positive atom
            let atom = parse_atom(part)?;
            body.push(BodyPredicate::Positive(atom));
        }
    }

    Ok(body)
}

/// Try to parse a comparison predicate (X = Y, X != 5, X < Y, etc.)
/// Returns None if this is not a comparison, Ok(Some(...)) if it is
fn try_parse_comparison(s: &str) -> Result<Option<BodyPredicate>, String> {
    // Check for comparison operators (order matters: check multi-char ops before single-char)
    // Important: == must come before = to prevent partial matching
    let operators = [
        ("!=", ComparisonOp::NotEqual),
        ("<=", ComparisonOp::LessOrEqual),
        (">=", ComparisonOp::GreaterOrEqual),
        ("==", ComparisonOp::Equal), // Must come before "="
        ("<", ComparisonOp::LessThan),
        (">", ComparisonOp::GreaterThan),
        ("=", ComparisonOp::Equal),
    ];

    for (op_str, op) in operators {
        // Find the operator, but not inside parentheses
        if let Some(pos) = find_operator_outside_parens(s, op_str) {
            let left_str = s[..pos].trim();
            let right_str = s[pos + op_str.len()..].trim();

            // Parse left and right as terms
            let left = parse_comparison_term(left_str)?;
            let right = parse_comparison_term(right_str)?;

            return Ok(Some(BodyPredicate::Comparison(left, op, right)));
        }
    }

    Ok(None)
}

/// Find an operator outside parentheses
fn find_operator_outside_parens(s: &str, op: &str) -> Option<usize> {
    let mut paren_depth: i32 = 0;
    let chars: Vec<char> = s.chars().collect();
    let op_chars: Vec<char> = op.chars().collect();

    for i in 0..chars.len() {
        match chars[i] {
            '(' => paren_depth += 1,
            // Clamp to 0 to handle malformed input with extra closing parens
            ')' => paren_depth = (paren_depth - 1).max(0),
            _ => {}
        }

        if paren_depth == 0 {
            // Check if operator matches at this position
            let mut matches = true;
            for (j, &op_char) in op_chars.iter().enumerate() {
                if i + j >= chars.len() || chars[i + j] != op_char {
                    matches = false;
                    break;
                }
            }
            if matches {
                return Some(i);
            }
        }
    }

    None
}

/// Parse a term for comparison - uses full `parse_term` for complete support
/// This allows function calls, arithmetic, vectors, etc. on either side
