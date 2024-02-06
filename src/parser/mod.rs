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
fn parse_comparison_term(s: &str) -> Result<Term, String> {
    // Delegate to the full term parser which handles all term types
    parse_term(s)
}

/// Split a string by commas, but only those outside parentheses and angle brackets
///
/// Note: Angle brackets in aggregates (count<x>) are tracked specially.
/// We only track angle depth for potential aggregates: when < immediately follows
/// a word character (no space).
fn split_by_comma_outside_parens(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth: i32 = 0;
    let mut angle_depth: i32 = 0;
    let chars = s.chars().peekable();

    for ch in chars {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                // Clamp to 0 to handle malformed input with extra closing parens
                paren_depth = (paren_depth - 1).max(0);
                current.push(ch);
            }
            '<' => {
                // Only track angle depth if this looks like aggregate syntax:
                // previous char was alphanumeric (word char), no space before <
                let prev_is_word = current
                    .chars()
                    .last()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_');
                if prev_is_word {
                    angle_depth += 1;
                }
                current.push(ch);
            }
            '>' => {
                current.push(ch);
                // Only decrement angle depth if we're in an aggregate
                if angle_depth > 0 {
                    angle_depth -= 1;
                }
            }
            ',' if paren_depth == 0 && angle_depth == 0 => {
                result.push(current.clone());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        result.push(current);
    }

    result
}

/// Parse an atom like "edge(x, y)" or "result(x, count<y>)"
fn parse_atom(s: &str) -> Result<Atom, String> {
    let s = s.trim();

    // Find opening parenthesis
    let paren_pos = s.find('(').ok_or_else(|| format!("Invalid atom: {s}"))?;

    let relation = s[..paren_pos].trim().to_string();

    // Extract arguments - find matching closing parenthesis
    let args_str = s[paren_pos + 1..].trim_end_matches(')').trim();

    let args = if args_str.is_empty() {
        vec![]
    } else {
        // Use smart split to handle aggregates like count<x>
        split_args_respecting_angles(args_str)
            .into_iter()
            .map(|arg| parse_term(arg.trim()))
            .collect::<Result<Vec<_>, _>>()?
    };

    Ok(Atom::new(relation, args))
}

/// Split atom arguments, respecting angle brackets, parentheses, and square brackets
fn split_args_respecting_angles(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut angle_depth: i32 = 0;
    let mut paren_depth: i32 = 0;
    let mut bracket_depth: i32 = 0;

    for ch in s.chars() {
        match ch {
            '<' => {
                angle_depth += 1;
                current.push(ch);
            }
            '>' => {
                // Clamp to 0 to handle malformed input
                angle_depth = (angle_depth - 1).max(0);
                current.push(ch);
            }
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                // Clamp to 0 to handle malformed input
                paren_depth = (paren_depth - 1).max(0);
                current.push(ch);
            }
            '[' => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' => {
                // Clamp to 0 to handle malformed input
                bracket_depth = (bracket_depth - 1).max(0);
                current.push(ch);
            }
            ',' if angle_depth == 0 && paren_depth == 0 && bracket_depth == 0 => {
                result.push(current.clone());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        result.push(current);
    }

    result
}

/// Parse a term (variable, constant, aggregate, function call, vector literal, or arithmetic expression)
///
/// Supports:
/// - Variables: x, y, foo
/// - Integer constants: 42, -10
/// - Float constants: 3.14, -0.5
/// - Placeholder: _
/// - Standard aggregates: `count<x>`, `sum<y>`, `min<z>`, `max<z>`, `avg<z>`
/// - Ranking aggregates: `top_k`<10, score>, `top_k_threshold`<10, score, 0.5>, `within_radius`<dist, 0.5>
/// - Arithmetic expressions: d+1, x*y, (a+b)*c
/// - Function calls: euclidean(v1, v2), normalize(v)
/// - Vector literals: [1.0, 2.0, 3.0]
/// - String constants: "hello"
/// Parse a single term from a string
/// This handles variables, constants, strings, aggregates, function calls,
/// and arithmetic expressions.
