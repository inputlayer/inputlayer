//! # Datalog Parser (Module 04)
//!
//! **Course Context**: Students implement this module in **Module 04: Parsing & AST Construction**.
//!
//! This module teaches:
//! - Lexing and tokenization of Datalog source code
//! - Recursive descent parsing techniques
//! - Abstract Syntax Tree (AST) construction
//! - Handling of Datalog syntax: rules, atoms, terms, constraints
//! - Error handling and reporting during parsing
//!
//! ## Learning Objectives (Module 04)
//!
//! Students learn to:
//! 1. Parse Datalog rules with head and body atoms
//! 2. Handle variables, constants, and placeholders
//! 3. Parse constraints (comparison operators: =, !=, <, >, <=, >=)
//! 4. Support negation (!atom)
//! 5. Handle comments and whitespace
//! 6. Build correct AST representations using shared types
//!
//! ## Key Concepts
//!
//! - **Lexical Analysis**: Breaking source text into tokens
//! - **Syntax Analysis**: Building structured representation (AST)
//! - **Operator Precedence**: Handling multi-character operators correctly
//! - **Parenthesis Matching**: Respecting structure in comma-separated lists
//!
//! ---
//!
//! # Implementation
//!
//! A minimal but complete parser for Datalog programs.
//! Supports: rules, atoms, variables, constants, basic constraints.

use crate::ast::{
    AggregateFunc, ArithExpr, ArithOp, Atom, BodyPredicate, BuiltinFunc, Constraint, Program, Rule,
    Term,
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

/// Parse a Datalog program from source text
///
/// Supports comments:
/// - `%` - Line comment (Prolog style, preferred)
/// - `/* ... */` - Block comment (C style, can span multiple lines)
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

/// Find the start position of a % comment, respecting string literals
fn find_comment_start(line: &str) -> Option<usize> {
    let mut in_string = false;
    let mut chars = line.char_indices();

    while let Some((i, c)) = chars.next() {
        if c == '"' && !in_string {
            in_string = true;
        } else if c == '"' && in_string {
            in_string = false;
        } else if c == '%' && !in_string {
            return Some(i);
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
        return Ok(Rule::new(head, vec![], vec![]));
    }

    if parts.len() != 2 {
        return Err(format!("Invalid rule: {}", line));
    }

    // Parse head
    let head = parse_atom(parts[0].trim())?;

    // Parse body (comma-separated atoms and constraints)
    let body_str = parts[1].trim();
    let (body, constraints) = parse_body(body_str)?;

    Ok(Rule::new(head, body, constraints))
}

/// Parse rule body (atoms and constraints)
fn parse_body(body_str: &str) -> Result<(Vec<BodyPredicate>, Vec<Constraint>), String> {
    let mut body = Vec::new();
    let mut constraints = Vec::new();

    // Split by commas, but respect parentheses
    let parts = split_by_comma_outside_parens(body_str);

    for part in parts {
        let part = part.trim();

        // Check if it's a constraint (contains comparison operators)
        // Be careful: aggregate syntax like count<x> uses < and > but isn't a constraint
        // A constraint is: var op var/const, where op is !=, <=, >=, <, >, =
        // An aggregate looks like: func<var> where func is count/sum/min/max/avg
        let is_constraint = if part.contains("!=") || part.contains("<=") || part.contains(">=") {
            true
        } else if part.contains('<') || part.contains('>') {
            // Check if it's an aggregate (func<var> pattern) vs constraint
            // Aggregates have format: word<word> with no spaces around <
            // Constraints have spaces: x < y or x > 5
            !is_aggregate_pattern(part)
        } else if part.contains('=') {
            // It's a constraint if it has '=' - this includes function calls like:
            // dist = euclidean(v, q)
            // x = y + 1
            true
        } else {
            false
        };

        if is_constraint {
            // It's a constraint
            constraints.push(parse_constraint(part)?);
        } else if part.starts_with('!') {
            // Negated atom
            let atom_str = part.trim_start_matches('!').trim();
            let atom = parse_atom(atom_str)?;
            body.push(BodyPredicate::Negated(atom));
        } else {
            // Positive atom
            let atom = parse_atom(part)?;
            body.push(BodyPredicate::Positive(atom));
        }
    }

    Ok((body, constraints))
}

/// Check if a string contains an aggregate pattern (e.g., count<x>)
/// vs a comparison constraint (e.g., x < 5)
fn is_aggregate_pattern(s: &str) -> bool {
    // Aggregate patterns look like: func<var> or func<params> where func is a known aggregate name
    // They don't have spaces around < and end with >
    if let Some(angle_pos) = s.find('<') {
        if s.ends_with('>') {
            let func_name = s[..angle_pos].trim().to_lowercase();
            // Check standard aggregates and new ranking aggregates
            return AggregateFunc::parse(&func_name).is_some()
                || func_name == "top_k"
                || func_name == "top_k_threshold"
                || func_name == "within_radius";
        }
    }
    false
}

/// Split a string by commas, but only those outside parentheses and angle brackets
///
/// Note: We need to distinguish between angle brackets in aggregates (count<x>)
/// and comparison operators in constraints (x >= 2, x < 5). The key insight is:
/// - Aggregate angle brackets: `word<word>` - no spaces around < or >
/// - Constraint operators: `x >= 2` or `x < 5` - typically have spaces
///
/// We only track angle depth for potential aggregates: when < immediately follows
/// a word character (no space).
fn split_by_comma_outside_parens(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut angle_depth = 0;
    let mut chars = s.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            '<' => {
                // Only track angle depth if this looks like aggregate syntax:
                // previous char was alphanumeric (word char), no space before <
                let prev_is_word = current
                    .chars()
                    .last()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false);
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
    let paren_pos = s.find('(').ok_or_else(|| format!("Invalid atom: {}", s))?;

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
    let mut angle_depth = 0;
    let mut paren_depth = 0;
    let mut bracket_depth = 0;

    for ch in s.chars() {
        match ch {
            '<' => {
                angle_depth += 1;
                current.push(ch);
            }
            '>' => {
                angle_depth -= 1;
                current.push(ch);
            }
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            '[' => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' => {
                bracket_depth -= 1;
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
/// - Standard aggregates: count<x>, sum<y>, min<z>, max<z>, avg<z>
/// - Ranking aggregates: top_k<10, score>, top_k_threshold<10, score, 0.5>, within_radius<dist, 0.5>
/// - Arithmetic expressions: d+1, x*y, (a+b)*c
/// - Function calls: euclidean(v1, v2), normalize(v)
/// - Vector literals: [1.0, 2.0, 3.0]
/// - String constants: "hello"
fn parse_term(s: &str) -> Result<Term, String> {
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

    // Check for aggregate syntax: func<params>
    if let Some(angle_pos) = s.find('<') {
        if s.ends_with('>') {
            let func_name = s[..angle_pos].trim();
            let params = &s[angle_pos + 1..s.len() - 1];

            // Try standard aggregates first
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
                    } else {
                        return Err(format!("Invalid top_k parameters: {}", params));
                    }
                }
                "top_k_threshold" => {
                    if let Some(func) = AggregateFunc::parse_top_k_threshold(params) {
                        return Ok(Term::Aggregate(func, String::new()));
                    } else {
                        return Err(format!("Invalid top_k_threshold parameters: {}", params));
                    }
                }
                "within_radius" => {
                    if let Some(func) = AggregateFunc::parse_within_radius(params) {
                        return Ok(Term::Aggregate(func, String::new()));
                    } else {
                        return Err(format!("Invalid within_radius parameters: {}", params));
                    }
                }
                _ => {
                    return Err(format!("Unknown aggregate function: {}", func_name));
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

    // Check for arithmetic expression (contains +, -, *, /, %)
    if contains_arithmetic_operator(s) {
        let expr = parse_arithmetic_expr(s)?;
        return Ok(Term::Arithmetic(expr));
    }

    // Try to parse as integer first
    if let Ok(num) = s.parse::<i64>() {
        return Ok(Term::Constant(num));
    }

    // Try to parse as float
    if let Ok(num) = s.parse::<f64>() {
        return Ok(Term::FloatConstant(num));
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
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') && !s.is_empty() {
        let first_char = s.chars().next().unwrap();

        // Variable: starts with uppercase letter or underscore
        // Examples: X, Y, Foo, _temp
        if first_char.is_uppercase() || first_char == '_' {
            return Ok(Term::Variable(s.to_string()));
        }

        // Atom: starts with lowercase letter
        // Examples: tom, liz, edge, parent
        // In standard Prolog/Datalog, lowercase identifiers are atoms (constants)
        // We represent atoms as StringConstant for compatibility
        if first_char.is_lowercase() {
            return Ok(Term::StringConstant(s.to_string()));
        }
    }

    Err(format!("Invalid term: '{}'", s))
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

/// Check if string contains arithmetic operators (but not inside angle brackets)
fn contains_arithmetic_operator(s: &str) -> bool {
    let mut angle_depth = 0;
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        match ch {
            '<' => angle_depth += 1,
            '>' => angle_depth -= 1,
            '+' | '*' | '/' | '%' if angle_depth == 0 => return true,
            '-' if angle_depth == 0 => {
                // Distinguish unary minus at start vs binary minus
                // Binary minus has a non-space char before it
                if i > 0 {
                    let prev = chars[i - 1];
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
    let mut paren_depth = 0;
    let chars: Vec<char> = s.chars().collect();

    for i in (0..chars.len()).rev() {
        let ch = chars[i];
        match ch {
            ')' => paren_depth += 1,
            '(' => paren_depth -= 1,
            '+' if paren_depth == 0 => {
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
                // Check it's binary minus (not unary)
                let prev = chars[i - 1];
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

    let mut paren_depth = 0;
    let chars: Vec<char> = s.chars().collect();

    for i in (0..chars.len()).rev() {
        let ch = chars[i];
        match ch {
            ')' => paren_depth += 1,
            '(' => paren_depth -= 1,
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

    // Handle negative numbers
    if s.starts_with('-') {
        if let Ok(num) = s[1..].trim().parse::<i64>() {
            return Ok(ArithExpr::Constant(-num));
        }
    }

    // Must be a variable
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') && !s.is_empty() {
        return Ok(ArithExpr::Variable(s.to_string()));
    }

    Err(format!("Invalid arithmetic expression: '{}'", s))
}

/// Parse a constraint like "x != y" or "x > 5"
fn parse_constraint(s: &str) -> Result<Constraint, String> {
    let s = s.trim();

    if let Some(pos) = s.find("!=") {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 2..].trim())?;
        return Ok(Constraint::NotEqual(left, right));
    }

    if let Some(pos) = s.find("<=") {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 2..].trim())?;
        return Ok(Constraint::LessOrEqual(left, right));
    }

    if let Some(pos) = s.find(">=") {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 2..].trim())?;
        return Ok(Constraint::GreaterOrEqual(left, right));
    }

    // Handle == (equality, same as =)
    if let Some(pos) = s.find("==") {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 2..].trim())?;
        return Ok(Constraint::Equal(left, right));
    }

    if let Some(pos) = s.find('<') {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 1..].trim())?;
        return Ok(Constraint::LessThan(left, right));
    }

    if let Some(pos) = s.find('>') {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 1..].trim())?;
        return Ok(Constraint::GreaterThan(left, right));
    }

    // Check for = (equality) last, after all other operators
    if let Some(pos) = s.find('=') {
        let left = parse_term(s[..pos].trim())?;
        let right = parse_term(s[pos + 1..].trim())?;
        return Ok(Constraint::Equal(left, right));
    }

    Err(format!("Invalid constraint: {}", s))
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
    fn test_parse_rule_with_constraint() {
        let rule = parse_rule("result(X, Y) :- edge(X, Y), X > 5").unwrap();
        assert_eq!(rule.body.len(), 1);
        assert_eq!(rule.constraints.len(), 1);
    }

    #[test]
    fn test_parse_rule_with_ge_constraint() {
        let rule = parse_rule("result(X, Y) :- data(X, Y), X >= 2").unwrap();
        assert_eq!(rule.body.len(), 1);
        assert_eq!(rule.constraints.len(), 1);
        // Verify it's a GreaterOrEqual constraint
        match &rule.constraints[0] {
            Constraint::GreaterOrEqual(left, right) => {
                assert!(matches!(left, Term::Variable(v) if v == "X"));
                assert!(matches!(right, Term::Constant(2)));
            }
            other => panic!("Expected GreaterOrEqual, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_rule_with_multiple_constraints() {
        let rule =
            parse_rule("result(X, Y) :- data(X, Y), X >= 2, X <= 4, Y >= 20, Y <= 40").unwrap();
        println!("Body: {:?}", rule.body);
        println!("Constraints: {:?}", rule.constraints);
        assert_eq!(rule.body.len(), 1, "Expected 1 body predicate");
        assert_eq!(rule.constraints.len(), 4, "Expected 4 constraints");
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

    // =========================================================================
    // Aggregation Tests
    // =========================================================================

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

    #[test]
    fn test_aggregation_vs_constraint() {
        // Make sure X < 5 is parsed as constraint, not aggregate
        let rule = parse_rule("result(X) :- data(X), X < 5.").unwrap();
        assert_eq!(rule.body.len(), 1);
        assert_eq!(rule.constraints.len(), 1);
    }

    // =========================================================================
    // Arithmetic Expression Tests
    // =========================================================================

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
        if let Term::Arithmetic(ArithExpr::Binary { op, .. }) = term {
            assert_eq!(op, ArithOp::Mul);
        } else {
            panic!("Expected multiplication, got {:?}", term);
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
    fn test_parse_arithmetic_constant_eval() {
        // Expression with only constants should be evaluable
        let term = parse_term("2+3*4").unwrap();
        if let Term::Arithmetic(expr) = term {
            // 2 + 3*4 = 2 + 12 = 14
            assert_eq!(expr.try_eval_constant(), Some(14));
        } else {
            panic!("Expected arithmetic term");
        }
    }

    #[test]
    fn test_parse_arithmetic_with_spaces() {
        let term = parse_term("D + 1").unwrap();
        if let Term::Arithmetic(ArithExpr::Binary {
            op: ArithOp::Add, ..
        }) = term
        {
            // Good
        } else {
            panic!("Expected Add expression");
        }
    }

    #[test]
    fn test_parse_arithmetic_complex() {
        // A * B + C * D
        let term = parse_term("A*B+C*D").unwrap();
        if let Term::Arithmetic(ArithExpr::Binary { op, left, right }) = term {
            assert_eq!(op, ArithOp::Add);
            assert!(matches!(
                *left,
                ArithExpr::Binary {
                    op: ArithOp::Mul,
                    ..
                }
            ));
            assert!(matches!(
                *right,
                ArithExpr::Binary {
                    op: ArithOp::Mul,
                    ..
                }
            ));
        } else {
            panic!("Expected Add of two Mul expressions");
        }
    }

    // =========================================================================
    // Vector Literal Tests
    // =========================================================================

    #[test]
    fn test_parse_vector_literal_simple() {
        let term = parse_term("[1.0, 2.0, 3.0]").unwrap();
        if let Term::VectorLiteral(values) = term {
            assert_eq!(values.len(), 3);
            assert!((values[0] - 1.0).abs() < f64::EPSILON);
            assert!((values[1] - 2.0).abs() < f64::EPSILON);
            assert!((values[2] - 3.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected VectorLiteral, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_vector_literal_empty() {
        let term = parse_term("[]").unwrap();
        if let Term::VectorLiteral(values) = term {
            assert!(values.is_empty());
        } else {
            panic!("Expected empty VectorLiteral");
        }
    }

    #[test]
    fn test_parse_vector_literal_single() {
        let term = parse_term("[42.5]").unwrap();
        if let Term::VectorLiteral(values) = term {
            assert_eq!(values.len(), 1);
            assert!((values[0] - 42.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected VectorLiteral");
        }
    }

    #[test]
    fn test_parse_vector_in_atom() {
        let atom = parse_atom("query([1.0, 2.0, 3.0])").unwrap();
        assert_eq!(atom.relation, "query");
        assert_eq!(atom.args.len(), 1);
        assert!(atom.args[0].is_vector_literal());
    }

    // =========================================================================
    // Function Call Tests
    // =========================================================================

    #[test]
    fn test_parse_function_call_euclidean() {
        let term = parse_term("euclidean(V1, V2)").unwrap();
        if let Term::FunctionCall(func, args) = term {
            assert_eq!(func, BuiltinFunc::Euclidean);
            assert_eq!(args.len(), 2);
            assert!(matches!(args[0], Term::Variable(ref v) if v == "V1"));
            assert!(matches!(args[1], Term::Variable(ref v) if v == "V2"));
        } else {
            panic!("Expected FunctionCall, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_function_call_cosine() {
        let term = parse_term("cosine(A, B)").unwrap();
        if let Term::FunctionCall(func, _) = term {
            assert_eq!(func, BuiltinFunc::Cosine);
        } else {
            panic!("Expected FunctionCall");
        }
    }

    #[test]
    fn test_parse_function_call_normalize() {
        let term = parse_term("normalize(V)").unwrap();
        if let Term::FunctionCall(func, args) = term {
            assert_eq!(func, BuiltinFunc::VecNormalize);
            assert_eq!(args.len(), 1);
        } else {
            panic!("Expected FunctionCall");
        }
    }

    #[test]
    fn test_parse_function_call_lsh_bucket() {
        let term = parse_term("lsh_bucket(V, 0, 8)").unwrap();
        if let Term::FunctionCall(func, args) = term {
            assert_eq!(func, BuiltinFunc::LshBucket);
            assert_eq!(args.len(), 3);
        } else {
            panic!("Expected FunctionCall");
        }
    }

    #[test]
    fn test_parse_function_call_with_vector_literal() {
        let term = parse_term("euclidean(V, [1.0, 2.0, 3.0])").unwrap();
        if let Term::FunctionCall(func, args) = term {
            assert_eq!(func, BuiltinFunc::Euclidean);
            assert_eq!(args.len(), 2);
            assert!(matches!(args[0], Term::Variable(_)));
            assert!(matches!(args[1], Term::VectorLiteral(_)));
        } else {
            panic!("Expected FunctionCall");
        }
    }

    #[test]
    fn test_parse_function_call_nested() {
        let term = parse_term("euclidean(normalize(V1), normalize(V2))").unwrap();
        if let Term::FunctionCall(func, args) = term {
            assert_eq!(func, BuiltinFunc::Euclidean);
            assert_eq!(args.len(), 2);
            assert!(matches!(
                args[0],
                Term::FunctionCall(BuiltinFunc::VecNormalize, _)
            ));
            assert!(matches!(
                args[1],
                Term::FunctionCall(BuiltinFunc::VecNormalize, _)
            ));
        } else {
            panic!("Expected nested FunctionCall");
        }
    }

    // =========================================================================
    // Float Constant Tests
    // =========================================================================

    #[test]
    fn test_parse_float_constant() {
        let term = parse_term("3.14").unwrap();
        if let Term::FloatConstant(v) = term {
            assert!((v - 3.14).abs() < f64::EPSILON);
        } else {
            panic!("Expected FloatConstant, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_negative_float() {
        let term = parse_term("-0.5").unwrap();
        if let Term::FloatConstant(v) = term {
            assert!((v - (-0.5)).abs() < f64::EPSILON);
        } else {
            panic!("Expected FloatConstant, got {:?}", term);
        }
    }

    // =========================================================================
    // New Aggregate Tests (TopK, TopKThreshold, WithinRadius)
    // =========================================================================

    #[test]
    fn test_parse_top_k_aggregate() {
        let term = parse_term("top_k<10, Score>").unwrap();
        if let Term::Aggregate(
            AggregateFunc::TopK {
                k,
                order_var,
                descending,
            },
            _,
        ) = term
        {
            assert_eq!(k, 10);
            assert_eq!(order_var, "Score");
            assert!(!descending);
        } else {
            panic!("Expected TopK aggregate, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_top_k_descending() {
        let term = parse_term("top_k<5, Dist, desc>").unwrap();
        if let Term::Aggregate(
            AggregateFunc::TopK {
                k,
                order_var,
                descending,
            },
            _,
        ) = term
        {
            assert_eq!(k, 5);
            assert_eq!(order_var, "Dist");
            assert!(descending);
        } else {
            panic!("Expected TopK aggregate");
        }
    }

    #[test]
    fn test_parse_top_k_threshold() {
        let term = parse_term("top_k_threshold<10, Score, 0.8>").unwrap();
        if let Term::Aggregate(
            AggregateFunc::TopKThreshold {
                k,
                order_var,
                threshold,
                descending,
            },
            _,
        ) = term
        {
            assert_eq!(k, 10);
            assert_eq!(order_var, "Score");
            assert!((threshold - 0.8).abs() < f64::EPSILON);
            assert!(!descending);
        } else {
            panic!("Expected TopKThreshold aggregate, got {:?}", term);
        }
    }

    #[test]
    fn test_parse_within_radius() {
        let term = parse_term("within_radius<Dist, 0.5>").unwrap();
        if let Term::Aggregate(
            AggregateFunc::WithinRadius {
                distance_var,
                max_distance,
            },
            _,
        ) = term
        {
            assert_eq!(distance_var, "Dist");
            assert!((max_distance - 0.5).abs() < f64::EPSILON);
        } else {
            panic!("Expected WithinRadius aggregate, got {:?}", term);
        }
    }

    // =========================================================================
    // Integration Tests - Complete Rules with Vector Operations
    // =========================================================================

    #[test]
    fn test_parse_vector_search_rule() {
        // Example: nearest(Id, Dist) :- vectors(Id, V), query(Q), Dist = euclidean(V, Q).
        // Note: The parser may count the constraint as a body item internally
        let rule =
            parse_rule("nearest(Id, Dist) :- vectors(Id, V), query(Q), Dist = euclidean(V, Q).")
                .unwrap();
        assert_eq!(rule.head.relation, "nearest");
        // Body contains vectors(Id, V) and query(Q) - constraint may or may not be counted
        assert!(rule.body.len() >= 2, "Expected at least 2 body predicates");

        // Check that euclidean function call is captured in constraints
        let euclidean_constraint = rule.constraints.iter().find(|c| {
            matches!(
                c,
                Constraint::Equal(
                    Term::Variable(_),
                    Term::FunctionCall(BuiltinFunc::Euclidean, _)
                )
            )
        });
        assert!(
            euclidean_constraint.is_some(),
            "Expected euclidean function call constraint, got {:?}",
            rule.constraints
        );

        if let Some(Constraint::Equal(Term::Variable(v), Term::FunctionCall(func, args))) =
            euclidean_constraint
        {
            assert_eq!(v, "Dist");
            assert_eq!(*func, BuiltinFunc::Euclidean);
            assert_eq!(args.len(), 2);
        }
    }

    #[test]
    fn test_parse_lsh_rule() {
        // Example: hash_t0(Id, Bucket) :- vectors(Id, V), Bucket = lsh_bucket(V, 0, 8).
        let rule =
            parse_rule("hash_t0(Id, Bucket) :- vectors(Id, V), Bucket = lsh_bucket(V, 0, 8).")
                .unwrap();
        assert_eq!(rule.head.relation, "hash_t0");
        assert_eq!(rule.constraints.len(), 1);

        if let Constraint::Equal(_, Term::FunctionCall(func, args)) = &rule.constraints[0] {
            assert_eq!(*func, BuiltinFunc::LshBucket);
            assert_eq!(args.len(), 3);
        } else {
            panic!("Expected lsh_bucket function call");
        }
    }

    #[test]
    fn test_parse_top_k_rule() {
        // Example: top_results(Id, Dist, top_k<10, Dist>) :- distances(Id, Dist).
        let rule =
            parse_rule("top_results(Id, Dist, top_k<10, Dist>) :- distances(Id, Dist).").unwrap();
        assert_eq!(rule.head.relation, "top_results");
        assert_eq!(rule.head.args.len(), 3);
        assert!(rule.head.has_aggregates());
    }

    // =========================================================================
    // Comment Parsing Tests
    // =========================================================================

    #[test]
    fn test_strip_block_comments_simple() {
        let source = "edge(1, 2). /* comment */ edge(3, 4).";
        let result = strip_block_comments(source);
        assert_eq!(result, "edge(1, 2).   edge(3, 4).");
    }

    #[test]
    fn test_strip_block_comments_multiline() {
        let source = "edge(1, 2).\n/* multi\nline\ncomment */\nedge(3, 4).";
        let result = strip_block_comments(source);
        assert_eq!(result, "edge(1, 2).\n \nedge(3, 4).");
    }

    #[test]
    fn test_strip_block_comments_nested() {
        let source = "edge(1, 2). /* outer /* inner */ outer */ edge(3, 4).";
        let result = strip_block_comments(source);
        assert_eq!(result, "edge(1, 2).   edge(3, 4).");
    }

    #[test]
    fn test_strip_block_comments_empty() {
        let source = "/* just a comment */";
        let result = strip_block_comments(source);
        assert_eq!(result, " ");
    }

    #[test]
    fn test_strip_block_comments_no_comments() {
        let source = "edge(1, 2). path(X, Y).";
        let result = strip_block_comments(source);
        assert_eq!(result, source);
    }

    #[test]
    fn test_strip_block_comments_in_string_ignored() {
        // Block comments inside strings should NOT be stripped
        let source = r#"+message("test /* not a comment */ test")."#;
        let result = strip_block_comments(source);
        assert_eq!(result, source);
    }

    #[test]
    fn test_strip_block_comments_mixed_string_and_comment() {
        // String followed by actual comment
        let source = r#"+message("hello"). /* real comment */ +edge(1, 2)."#;
        let result = strip_block_comments(source);
        assert_eq!(result, r#"+message("hello").   +edge(1, 2)."#);
    }

    #[test]
    fn test_find_comment_start_simple() {
        let line = "edge(1, 2). % comment";
        let pos = find_comment_start(line);
        assert_eq!(pos, Some(12));
    }

    #[test]
    fn test_find_comment_start_no_comment() {
        let line = "edge(1, 2).";
        let pos = find_comment_start(line);
        assert_eq!(pos, None);
    }

    #[test]
    fn test_find_comment_start_in_string_ignored() {
        let line = r#"message("hello % world")."#;
        let pos = find_comment_start(line);
        assert_eq!(pos, None);
    }

    #[test]
    fn test_find_comment_start_after_string() {
        let line = r#"message("hello"). % comment"#;
        let pos = find_comment_start(line);
        assert_eq!(pos, Some(18));
    }

    #[test]
    fn test_parse_program_with_line_comments() {
        let source = "
            % This is a comment
            path(X, Y) :- edge(X, Y).
            % Another comment
            path(X, Z) :- path(X, Y), edge(Y, Z).
        ";
        let program = parse_program(source).unwrap();
        assert_eq!(program.rules.len(), 2);
    }

    #[test]
    fn test_parse_program_with_inline_comments() {
        let source = "
            path(X, Y) :- edge(X, Y). % base case
            path(X, Z) :- path(X, Y), edge(Y, Z). % recursive case
        ";
        let program = parse_program(source).unwrap();
        assert_eq!(program.rules.len(), 2);
    }

    #[test]
    fn test_parse_program_with_block_comments() {
        let source = "
            /* This rule defines direct paths */
            path(X, Y) :- edge(X, Y).
            /*
             * This rule defines transitive paths
             * through recursive evaluation
             */
            path(X, Z) :- path(X, Y), edge(Y, Z).
        ";
        let program = parse_program(source).unwrap();
        assert_eq!(program.rules.len(), 2);
    }

    #[test]
    fn test_parse_program_with_mixed_comments() {
        let source = "
            % Line comment
            path(X, Y) :- edge(X, Y). % inline comment
            /* Block comment */
            path(X, Z) :- path(X, Y), edge(Y, Z).
        ";
        let program = parse_program(source).unwrap();
        assert_eq!(program.rules.len(), 2);
    }

    #[test]
    fn test_parse_program_comment_only() {
        let source = "
            % Just comments
            /* No rules here */
        ";
        let program = parse_program(source).unwrap();
        assert_eq!(program.rules.len(), 0);
    }
}
