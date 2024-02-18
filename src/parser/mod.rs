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
        result.push(current.clone());
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
                current.push(ch.clone());
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
pub fn parse_term(s: &str) -> Result<Term, String> {
    let s = s.trim();

    // Placeholder is "_"
    if s == "_" {
        return Ok(Term::Placeholder);
    }

    // Check for vector literal: [1.0, 2.0, 3.0]
    if s.starts_with('[') && s.ends_with(']') {
        return parse_vector_literal(s);
    }

    // Check for string literal: "hello"
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return Ok(Term::StringConstant(inner.to_string()));
    }

    // Check for aggregate syntax: func<params> or <func:var>
    if let Some(angle_pos) = s.find('<') {
        if s.ends_with('>') {
            let func_name = s[..angle_pos].trim();
            let params = &s[angle_pos + 1..s.len() - 1];

            // Handle <FUNC:VAR> syntax (function name inside angle brackets with colon)
            if func_name.is_empty() && params.contains(':') {
                if let Some(colon_pos) = params.find(':') {
                    let inner_func = params[..colon_pos].trim();
                    let inner_var = params[colon_pos + 1..].trim();
                    if let Some(func) = AggregateFunc::parse(inner_func) {
                        return Ok(Term::Aggregate(func, inner_var.to_string()));
                    }
                    return Err(format!("Unknown aggregate function: {inner_func}"));
                }
            }

            // Try standard aggregates first: func<params>
            if let Some(func) = AggregateFunc::parse(func_name) {
                return Ok(Term::Aggregate(func, params.trim().to_string()));
            }

            // Try new ranking aggregates
            let func_lower = func_name.to_lowercase();
            match func_lower.as_str() {
                "top_k" => {
                    if let Some(func) = AggregateFunc::parse_top_k(params) {
                        // For ranking aggregates, the "var" field is used to identify the group
                        // We'll use an empty string and rely on the aggregate's internal fields
                        return Ok(Term::Aggregate(func, String::new()));
                    }
                    return Err(format!("Invalid top_k parameters: {params}"));
                }
                "top_k_threshold" => {
                    if let Some(func) = AggregateFunc::parse_top_k_threshold(params) {
                        return Ok(Term::Aggregate(func, String::new()));
                    }
                    return Err(format!("Invalid top_k_threshold parameters: {params}"));
                }
                "within_radius" => {
                    if let Some(func) = AggregateFunc::parse_within_radius(params) {
                        return Ok(Term::Aggregate(func, String::new()));
                    }
                    return Err(format!("Invalid within_radius parameters: {params}"));
                }
                _ => {
                    return Err(format!("Unknown aggregate function: {func_name}"));
                }
            }
        }
    }

    // Check for function call: func(args)
    // Must check before arithmetic to avoid confusing func(x) with multiplication
    if let Some(paren_pos) = s.find('(') {
        if s.ends_with(')') {
            let func_name = s[..paren_pos].trim();
            // Check if this is a known built-in function
            if let Some(builtin) = BuiltinFunc::parse(func_name) {
                let args_str = &s[paren_pos + 1..s.len() - 1];
                let args = parse_function_args(args_str)?;
                return Ok(Term::FunctionCall(builtin, args));
            }
            // If not a known function, fall through to check if it could be something else
            // (like a parenthesized arithmetic expression)
        }
    }

    // Try to parse as integer first (before arithmetic check)
    if let Ok(num) = s.parse::<i64>() {
        return Ok(Term::Constant(num));
    }

    // Try to parse as float (before arithmetic check, to handle scientific notation like 1.0e-3)
    if let Ok(num) = s.parse::<f64>() {
        if num.is_finite() {
            return Ok(Term::FloatConstant(num));
        }
    }

    // Check for arithmetic expression (contains +, -, *, /, %)
    if contains_arithmetic_operator(s) {
        let expr = parse_arithmetic_expr(s)?;
        return Ok(Term::Arithmetic(expr));
    }

    // Handle negative numbers with spaces
    if s.starts_with('-') {
        let rest = s[1..].trim();
        if let Ok(num) = rest.parse::<i64>() {
            return Ok(Term::Constant(-num));
        }
        if let Ok(num) = rest.parse::<f64>() {
            return Ok(Term::FloatConstant(-num));
        }
    }

    // Check for identifier (variable or atom)
    if let Some(first_char) = s.chars().next() {
        if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
            // Variable: starts with uppercase letter or underscore
            // Examples: X, Y, Foo, _temp
            if first_char.is_uppercase() || first_char == '_' {
                return Ok(Term::Variable(s.to_string()));
            }

            // Boolean literals: true and false are special constants
            if s == "true" || s == "false" {
                return Ok(Term::StringConstant(s.to_string()));
            }

            // Lowercase identifier - reject with helpful error message
            // Users must use quoted strings: "alice" not alice
            if first_char.is_lowercase() {
                return Err(format!(
                    "Unquoted atom '{s}' is not allowed. Use \"{s}\" (quoted string) instead."
                ));
            }
        }
    }

    Err(format!("Invalid term: '{s}'"))
}

/// Parse a vector literal like [1.0, 2.0, 3.0]
fn parse_vector_literal(s: &str) -> Result<Term, String> {
    let inner = s[1..s.len() - 1].trim();
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

/// Parse function arguments (comma-separated terms)
fn parse_function_args(s: &str) -> Result<Vec<Term>, String> {
    let s = s.trim();
    if s.is_empty() {
        return Ok(vec![]);
    }

    // Split by commas, respecting nested structures
    split_args_respecting_angles(s)
        .into_iter()
        .map(|arg| parse_term(arg.trim()))
        .collect()
}

/// Check if string contains arithmetic operators (but not inside angle brackets).
/// Handles scientific notation: `e-` or `E-` in numbers is NOT a binary minus.
fn contains_arithmetic_operator(s: &str) -> bool {
    let mut angle_depth = 0;
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '<' => angle_depth += 1,
            '>' => angle_depth -= 1,
            '+' if angle_depth == 0 => {
                // Check for scientific notation: digit/dot followed by e/E then +
                if i >= 2
                    && (chars[i - 1] == 'e' || chars[i - 1] == 'E')
                    && (chars[i - 2].is_ascii_digit() || chars[i - 2] == '.')
                {
                    continue;
                }
                return true;
            }
            '*' | '/' | '%' if angle_depth == 0 => return true,
            '-' if angle_depth == 0 => {
                // Distinguish unary minus at start vs binary minus
                // Binary minus has an alphanumeric/paren/underscore before it (possibly with spaces)
                if i > 0 {
                    // Check for scientific notation: digit/dot followed by e/E then -
                    if i >= 2
                        && (chars[i - 1] == 'e' || chars[i - 1] == 'E')
                        && (chars[i - 2].is_ascii_digit() || chars[i - 2] == '.')
                    {
                        continue;
                    }
                    // Look backwards skipping whitespace to find the previous significant char
                    let mut j = i - 1;
                    while j > 0 && chars[j].is_whitespace() {
                        j -= 1;
                    }
                    let prev = chars[j];
                    if prev.is_alphanumeric() || prev == ')' || prev == '_' {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

/// Parse an arithmetic expression with proper precedence
///
/// Precedence (lowest to highest):
/// 1. + and - (left associative)
/// 2. * and / and % (left associative)
/// 3. Parentheses
fn parse_arithmetic_expr(s: &str) -> Result<ArithExpr, String> {
    let s = s.trim();
    parse_add_sub(s)
}

/// Parse addition and subtraction (lowest precedence)
fn parse_add_sub(s: &str) -> Result<ArithExpr, String> {
    let s = s.trim();

    // Find the rightmost + or - at the top level (outside parentheses)
    // We go right-to-left to ensure left-associativity
    let mut paren_depth: i32 = 0;
    let chars: Vec<char> = s.chars().collect();

    for i in (0..chars.len()).rev() {
        let ch = chars[i];
        match ch {
            ')' => paren_depth += 1,
            // Clamp to 0 to handle malformed input (iterating backwards)
            '(' => paren_depth = (paren_depth - 1).max(0),
            '+' if paren_depth == 0 => {
                // Skip scientific notation: e+ or E+
                if i >= 2
                    && (chars[i - 1] == 'e' || chars[i - 1] == 'E')
                    && (chars[i - 2].is_ascii_digit() || chars[i - 2] == '.')
                {
                    continue;
                }
                let left = &s[..i];
                let right = &s[i + 1..];
                if !left.is_empty() && !right.is_empty() {
                    return Ok(ArithExpr::Binary {
                        op: ArithOp::Add,
                        left: Box::new(parse_add_sub(left)?),
                        right: Box::new(parse_mul_div(right)?),
                    });
                }
            }
            '-' if paren_depth == 0 && i > 0 => {
                // Skip scientific notation: e- or E-
                if i >= 2
                    && (chars[i - 1] == 'e' || chars[i - 1] == 'E')
                    && (chars[i - 2].is_ascii_digit() || chars[i - 2] == '.')
                {
                    continue;
                }
                // Check it's binary minus (not unary) by looking for alphanumeric before it
                // Skip whitespace to find the previous significant character
                let mut j = i - 1;
                while j > 0 && chars[j].is_whitespace() {
                    j -= 1;
                }
                let prev = chars[j];
                if prev.is_alphanumeric() || prev == ')' || prev == '_' {
                    let left = &s[..i];
                    let right = &s[i + 1..];
                    if !left.is_empty() && !right.is_empty() {
                        return Ok(ArithExpr::Binary {
                            op: ArithOp::Sub,
                            left: Box::new(parse_add_sub(left)?),
                            right: Box::new(parse_mul_div(right)?),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    // No + or - at top level, try multiplication/division
    parse_mul_div(s)
}

/// Parse multiplication, division, modulo (higher precedence)
fn parse_mul_div(s: &str) -> Result<ArithExpr, String> {
    let s = s.trim();

    let mut paren_depth: i32 = 0;
    let chars: Vec<char> = s.chars().collect();

    for i in (0..chars.len()).rev() {
        let ch = chars[i];
        match ch {
            ')' => paren_depth += 1,
            // Clamp to 0 to handle malformed input (iterating backwards)
            '(' => paren_depth = (paren_depth - 1).max(0),
            '*' if paren_depth == 0 => {
                let left = &s[..i];
                let right = &s[i + 1..];
                if !left.is_empty() && !right.is_empty() {
                    return Ok(ArithExpr::Binary {
                        op: ArithOp::Mul,
                        left: Box::new(parse_mul_div(left)?),
                        right: Box::new(parse_primary(right)?),
                    });
                }
            }
            '/' if paren_depth == 0 => {
                let left = &s[..i];
                let right = &s[i + 1..];
                if !left.is_empty() && !right.is_empty() {
                    return Ok(ArithExpr::Binary {
                        op: ArithOp::Div,
                        left: Box::new(parse_mul_div(left)?),
                        right: Box::new(parse_primary(right)?),
                    });
                }
            }
            '%' if paren_depth == 0 => {
                let left = &s[..i];
                let right = &s[i + 1..];
                if !left.is_empty() && !right.is_empty() {
                    return Ok(ArithExpr::Binary {
                        op: ArithOp::Mod,
                        left: Box::new(parse_mul_div(left)?),
                        right: Box::new(parse_primary(right)?),
                    });
                }
            }
            _ => {}
        }
    }

    // No * or / or % at top level, parse primary
    parse_primary(s)
}

/// Parse primary expressions (variables, constants, parenthesized expressions)
fn parse_primary(s: &str) -> Result<ArithExpr, String> {
    let s = s.trim();

    // Handle parenthesized expression
    if s.starts_with('(') && s.ends_with(')') {
        // Check if the parens are matching (not something like "(a+b)*(c+d)")
        let mut depth = 0;
        let chars: Vec<char> = s.chars().collect();
        let mut matched_at_end = true;

        for (i, &ch) in chars.iter().enumerate() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 && i < chars.len() - 1 {
                        matched_at_end = false;
                        break;
                    }
                }
                _ => {}
            }
        }

        if matched_at_end && depth == 0 {
            return parse_arithmetic_expr(&s[1..s.len() - 1]);
        }
    }

    // Try to parse as constant
    if let Ok(num) = s.parse::<i64>() {
        return Ok(ArithExpr::Constant(num));
    }

    // Try to parse as float constant
    if let Ok(num) = s.parse::<f64>() {
        return Ok(ArithExpr::from_float(num));
    }

    // Handle negative numbers
    if s.starts_with('-') {
        if let Ok(num) = s[1..].trim().parse::<i64>() {
            return Ok(ArithExpr::Constant(-num));
        }
        if let Ok(num) = s[1..].trim().parse::<f64>() {
            return Ok(ArithExpr::from_float(-num));
        }
    }

    // Must be a variable
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') && !s.is_empty() {
        return Ok(ArithExpr::Variable(s.to_string()));
    }

    Err(format!("Invalid arithmetic expression: '{s}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_atom() {
        let atom = parse_atom("edge(X, Y)").unwrap();
        assert_eq!(atom.relation, "edge");
        assert_eq!(atom.args.len(), 2);
        assert!(matches!(atom.args[0], Term::Variable(_)));
    }

    #[test]
    fn test_parse_atom_with_constants() {
        let atom = parse_atom("edge(1, 2)").unwrap();
        assert_eq!(atom.relation, "edge");
        assert_eq!(atom.args.len(), 2);
        assert!(matches!(atom.args[0], Term::Constant(1)));
        assert!(matches!(atom.args[1], Term::Constant(2)));
    }

    #[test]
    fn test_parse_simple_rule() {
        let rule = parse_rule("path(X, Y) :- edge(X, Y)").unwrap();
        assert_eq!(rule.head.relation, "path");
        assert_eq!(rule.body.len(), 1);
        assert!(rule.body[0].is_positive());
    }

    #[test]
    fn test_parse_rule_with_multiple_body_atoms() {
        let rule = parse_rule("path(X, Z) :- edge(X, Y), edge(Y, Z)").unwrap();
        assert_eq!(rule.head.relation, "path");
        assert_eq!(rule.body.len(), 2);
    }

    #[test]
    fn test_parse_program() {
        let source = "
            path(X, Y) :- edge(X, Y).
            path(X, Z) :- path(X, Y), edge(Y, Z).
        ";

        let program = parse_program(source).unwrap();
        assert_eq!(program.rules.len(), 2);
    }

    #[test]
    fn test_parse_with_negation() {
        let rule = parse_rule("unreachable(X) :- node(X), !reach(X)").unwrap();
        assert_eq!(rule.body.len(), 2);
        assert!(rule.body[0].is_positive());
        assert!(rule.body[1].is_negated());
    }

    #[test]
    fn test_parse_fact() {
        let rule = parse_rule("edge(1, 2)").unwrap();
        assert_eq!(rule.head.relation, "edge");
        assert_eq!(rule.body.len(), 0); // Facts have empty body
    }

    // Aggregation Tests
    #[test]
    fn test_parse_aggregate_term_count() {
        let term = parse_term("count<X>").unwrap();
        assert!(matches!(term, Term::Aggregate(AggregateFunc::Count, ref v) if v == "X"));
    }

    #[test]
    fn test_parse_aggregate_term_sum() {
        let term = parse_term("sum<Amount>").unwrap();
        assert!(matches!(term, Term::Aggregate(AggregateFunc::Sum, ref v) if v == "Amount"));
    }

    #[test]
    fn test_parse_aggregate_term_min() {
        let term = parse_term("min<Score>").unwrap();
        assert!(matches!(term, Term::Aggregate(AggregateFunc::Min, ref v) if v == "Score"));
    }

    #[test]
    fn test_parse_aggregate_term_max() {
        let term = parse_term("max<Score>").unwrap();
        assert!(matches!(term, Term::Aggregate(AggregateFunc::Max, ref v) if v == "Score"));
    }

    #[test]
    fn test_parse_aggregate_term_avg() {
        let term = parse_term("avg<Value>").unwrap();
        assert!(matches!(term, Term::Aggregate(AggregateFunc::Avg, ref v) if v == "Value"));
    }

    #[test]
    fn test_parse_atom_with_aggregate() {
        let atom = parse_atom("result(X, count<Y>)").unwrap();
        assert_eq!(atom.relation, "result");
        assert_eq!(atom.args.len(), 2);
        assert!(matches!(atom.args[0], Term::Variable(ref v) if v == "X"));
        assert!(matches!(atom.args[1], Term::Aggregate(AggregateFunc::Count, ref v) if v == "Y"));
    }

    #[test]
    fn test_parse_atom_with_multiple_aggregates() {
        let atom = parse_atom("stats(Category, min<Price>, max<Price>, sum<Quantity>)").unwrap();
        assert_eq!(atom.relation, "stats");
        assert_eq!(atom.args.len(), 4);
        assert!(matches!(atom.args[0], Term::Variable(ref v) if v == "Category"));
        assert!(matches!(atom.args[1], Term::Aggregate(AggregateFunc::Min, ref v) if v == "Price"));
        assert!(matches!(atom.args[2], Term::Aggregate(AggregateFunc::Max, ref v) if v == "Price"));
        assert!(
            matches!(atom.args[3], Term::Aggregate(AggregateFunc::Sum, ref v) if v == "Quantity")
        );
    }

    #[test]
    fn test_parse_aggregation_rule() {
        let rule =
            parse_rule("total_sales(Category, sum<Amount>) :- sales(Category, Amount).").unwrap();
        assert_eq!(rule.head.relation, "total_sales");
        assert_eq!(rule.head.args.len(), 2);
        assert!(matches!(rule.head.args[0], Term::Variable(ref v) if v == "Category"));
        assert!(
            matches!(rule.head.args[1], Term::Aggregate(AggregateFunc::Sum, ref v) if v == "Amount")
        );
        assert_eq!(rule.body.len(), 1);
    }

    #[test]
    fn test_parse_count_rule() {
        let rule =
            parse_rule("item_count(Category, count<Item>) :- inventory(Category, Item).").unwrap();
        assert_eq!(rule.head.relation, "item_count");
        assert!(rule.head.has_aggregates());
        assert_eq!(rule.head.aggregates().len(), 1);
    }

    // Arithmetic Expression Tests
    #[test]
    fn test_parse_arithmetic_simple_add() {
        let term = parse_term("D+1").unwrap();
        if let Term::Arithmetic(expr) = term {
            assert!(matches!(
                expr,
                ArithExpr::Binary {
                    op: ArithOp::Add,
                    ..
                }
            ));
        } else {
            panic!("Expected arithmetic term, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_arithmetic_simple_sub() {
        let term = parse_term("X-Y").unwrap();
        if let Term::Arithmetic(expr) = term {
            assert!(matches!(
                expr,
                ArithExpr::Binary {
                    op: ArithOp::Sub,
                    ..
                }
            ));
        } else {
            panic!("Expected arithmetic term, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_arithmetic_mul() {
        let term = parse_term("A*B").unwrap();
        if let Term::Arithmetic(expr) = term {
            assert!(matches!(
                expr,
                ArithExpr::Binary {
                    op: ArithOp::Mul,
                    ..
                }
            ));
        } else {
            panic!("Expected arithmetic term, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_arithmetic_div() {
        let term = parse_term("X/2").unwrap();
        if let Term::Arithmetic(expr) = term {
            assert!(matches!(
                expr,
                ArithExpr::Binary {
                    op: ArithOp::Div,
                    ..
                }
            ));
        } else {
            panic!("Expected arithmetic term, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_arithmetic_mod() {
        let term = parse_term("N%2").unwrap();
        if let Term::Arithmetic(expr) = term {
            assert!(matches!(
                expr,
                ArithExpr::Binary {
                    op: ArithOp::Mod,
                    ..
                }
            ));
        } else {
            panic!("Expected arithmetic term, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_arithmetic_precedence() {
        // A + B * C should parse as A + (B * C)
        let term = parse_term("A+B*C").unwrap();
        if let Term::Arithmetic(ArithExpr::Binary { op, left, right }) = term {
            assert_eq!(op, ArithOp::Add);
            assert!(matches!(*left, ArithExpr::Variable(ref v) if v == "A"));
            assert!(matches!(
                *right,
                ArithExpr::Binary {
                    op: ArithOp::Mul,
                    ..
                }
            ));
        } else {
            panic!("Expected arithmetic term, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_arithmetic_with_parens() {
        // (A + B) * C
        let term = parse_term("(A+B)*C").unwrap();
        if let Term::Arithmetic(ArithExpr::Binary { op, left, right }) = term {
            assert_eq!(op, ArithOp::Mul);
            // Left should be Add(A, B)
            if let ArithExpr::Binary { op: inner_op, .. } = *left {
                assert_eq!(inner_op, ArithOp::Add, "Inner op should be Add");
            } else {
                panic!("Expected Add as left of Mul, got {:?}", left);
            }
            // Right should be Variable C
            assert!(
                matches!(*right, ArithExpr::Variable(ref v) if v == "C"),
                "Right should be C"
            );
        } else {
            panic!("Expected multiplication, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_rule_with_parens_in_body() {
        // result_paren(X, R) :- nums(X), R = (X + 5) * 2.
        let rule = parse_rule("result_paren(X, R) :- nums(X), R = (X + 5) * 2.").unwrap();
        assert_eq!(rule.head.relation, "result_paren");
        assert_eq!(rule.body.len(), 2);
        // Second body predicate should be a comparison R = Arithmetic(Mul(Add(X, 5), 2))
        if let BodyPredicate::Comparison(ref left, ComparisonOp::Equal, ref right) = rule.body[1] {
            assert!(
                matches!(left, Term::Variable(ref v) if v == "R"),
                "Left should be R, got {:?}",
                left
            );
            if let Term::Arithmetic(ArithExpr::Binary {
                op: ArithOp::Mul,
                ref left,
                ref right,
            }) = right
            {
                // Left of Mul should be Add(X, 5)
                if let ArithExpr::Binary {
                    op: ArithOp::Add,
                    left: ref add_left,
                    right: ref add_right,
                } = **left
                {
                    assert!(
                        matches!(**add_left, ArithExpr::Variable(ref v) if v == "X"),
                        "Should be X"
                    );
                    assert!(matches!(**add_right, ArithExpr::Constant(5)), "Should be 5");
                } else {
                    panic!("Expected Add(X, 5) as left of Mul, got {:?}", left);
                }
                // Right of Mul should be Constant(2)
                assert!(
                    matches!(**right, ArithExpr::Constant(2)),
                    "Should be 2, got {:?}",
                    right
                );
            } else {
                panic!("Expected Arithmetic Mul, got {:?}", right);
            }
        } else {
            panic!("Expected Comparison, got {:?}", rule.body[1]);
        }
    }

    #[test]
    fn test_parse_arithmetic_rule() {
        // dist(Y, D+1) :- dist(X, D), edge(X, Y).
        let rule = parse_rule("next_dist(Y, D+1) :- dist(X, D), edge(X, Y).").unwrap();
        assert_eq!(rule.head.relation, "next_dist");
        assert_eq!(rule.head.args.len(), 2);
        assert!(matches!(rule.head.args[0], Term::Variable(ref v) if v == "Y"));
        assert!(matches!(rule.head.args[1], Term::Arithmetic(_)));
    }

    #[test]
    fn test_arith_display_roundtrip() {
        use crate::ast::{ArithExpr, ArithOp};
        // (X + 5) * 2 should display with parens and roundtrip correctly
        let expr = ArithExpr::Binary {
            op: ArithOp::Mul,
            left: Box::new(ArithExpr::Binary {
                op: ArithOp::Add,
                left: Box::new(ArithExpr::Variable("X".into())),
                right: Box::new(ArithExpr::Constant(5)),
            }),
            right: Box::new(ArithExpr::Constant(2)),
        };
        assert_eq!(expr.to_string(), "(X+5)*2");

        // Total + Total/10 should NOT add unnecessary parens
        let expr2 = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("Total".into())),
            right: Box::new(ArithExpr::Binary {
                op: ArithOp::Div,
                left: Box::new(ArithExpr::Variable("Total".into())),
                right: Box::new(ArithExpr::Constant(10)),
            }),
        };
        assert_eq!(expr2.to_string(), "Total+Total/10");

        // ((X + 1) * 2 + 3) * 4 should preserve all needed parens
        let expr3 = ArithExpr::Binary {
            op: ArithOp::Mul,
            left: Box::new(ArithExpr::Binary {
                op: ArithOp::Add,
                left: Box::new(ArithExpr::Binary {
                    op: ArithOp::Mul,
                    left: Box::new(ArithExpr::Binary {
                        op: ArithOp::Add,
                        left: Box::new(ArithExpr::Variable("X".into())),
                        right: Box::new(ArithExpr::Constant(1)),
                    }),
                    right: Box::new(ArithExpr::Constant(2)),
                }),
                right: Box::new(ArithExpr::Constant(3)),
            }),
            right: Box::new(ArithExpr::Constant(4)),
        };
        assert_eq!(expr3.to_string(), "((X+1)*2+3)*4");

        // X * 5 + 2 should NOT parenthesize (mul has higher prec)
        let expr4 = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Binary {
                op: ArithOp::Mul,
                left: Box::new(ArithExpr::Variable("X".into())),
                right: Box::new(ArithExpr::Constant(5)),
            }),
            right: Box::new(ArithExpr::Constant(2)),
        };
        assert_eq!(expr4.to_string(), "X*5+2");

        // a / (b * c) should parenthesize right child (same precedence)
        let expr5 = ArithExpr::Binary {
            op: ArithOp::Div,
            left: Box::new(ArithExpr::Variable("A".into())),
            right: Box::new(ArithExpr::Binary {
                op: ArithOp::Mul,
                left: Box::new(ArithExpr::Variable("B".into())),
                right: Box::new(ArithExpr::Variable("C".into())),
            }),
        };
        assert_eq!(expr5.to_string(), "A/(B*C)");
    }

