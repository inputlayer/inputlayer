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
    if !first_char.is_lowercase() {
        return Err(format!(
            "Relation name '{name}' must start with lowercase letter.\n\
             (Uppercase names are for type declarations.)"
        ));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid relation name: '{name}'"));
    }
    Ok(())
}

// Term Parsing
/// Parse atom arguments: (arg1, arg2, ...)
pub fn parse_atom_args(input: &str) -> Result<Vec<Term>, String> {
    let input = input.trim();
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
fn parse_vector_literal(input: &str) -> Result<Term, String> {
    let inner = input[1..input.len() - 1].trim();
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

/// Parse an aggregate function like count<X>, sum<Y>, min<Z>, max<Z>, avg<Z>, or <FUNC:VAR>
fn parse_aggregate(input: &str) -> Option<Term> {
    // Check for pattern: func<params> or <FUNC:VAR> where func is an aggregate
    if let Some(lt_pos) = input.find('<') {
        if let Some(gt_pos) = input.rfind('>') {
            if gt_pos > lt_pos && gt_pos == input.len() - 1 {
                let func_name = &input[..lt_pos];
                let params = &input[lt_pos + 1..gt_pos].trim();

                // Handle <FUNC:VAR> syntax (function name inside angle brackets with colon)
                if func_name.is_empty() && params.contains(':') {
                    if let Some(colon_pos) = params.find(':') {
                        let inner_func = params[..colon_pos].trim().to_lowercase();
                        let inner_var = params[colon_pos + 1..].trim();
                        let agg_func = match inner_func.as_str() {
                            "count" => Some(AggregateFunc::Count),
                            "count_distinct" | "countdistinct" => {
                                Some(AggregateFunc::CountDistinct)
                            }
                            "sum" => Some(AggregateFunc::Sum),
                            "min" => Some(AggregateFunc::Min),
                            "max" => Some(AggregateFunc::Max),
                            "avg" => Some(AggregateFunc::Avg),
                            _ => None,
                        };
                        if let Some(func) = agg_func {
                            return Some(Term::Aggregate(func, inner_var.to_string()));
                        }
                    }
                }

                let func_lower = func_name.to_lowercase();

                // Check for ranking aggregates: top_k, top_k_threshold, within_radius
                match func_lower.as_str() {
                    "top_k" => {
                        if let Some(func) = AggregateFunc::parse_top_k(params) {
                            return Some(Term::Aggregate(func, String::new()));
                        }
                    }
                    "top_k_threshold" => {
                        if let Some(func) = AggregateFunc::parse_top_k_threshold(params) {
                            return Some(Term::Aggregate(func, String::new()));
                        }
                    }
                    "within_radius" => {
                        if let Some(func) = AggregateFunc::parse_within_radius(params) {
                            return Some(Term::Aggregate(func, String::new()));
                        }
                    }
                    _ => {}
                }

                // Standard aggregates with single variable parameter
                if let Some(first_char) = params.chars().next() {
                    if first_char.is_uppercase() || first_char == '_' {
                        let agg_func = match func_lower.as_str() {
                            "count" => Some(AggregateFunc::Count),
                            "count_distinct" | "countdistinct" => {
                                Some(AggregateFunc::CountDistinct)
                            }
                            "sum" => Some(AggregateFunc::Sum),
                            "min" => Some(AggregateFunc::Min),
                            "max" => Some(AggregateFunc::Max),
                            "avg" => Some(AggregateFunc::Avg),
                            _ => None,
                        };

                        if let Some(func) = agg_func {
                            return Some(Term::Aggregate(func, (*params).to_string()));
                        }
                    }
                }
            }
        }
    }
    None
}

/// Split by comma, respecting parentheses, square brackets, and angle brackets
pub fn split_by_comma(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth: i32 = 0;
    let mut bracket_depth: i32 = 0; // Track square brackets for vectors
    let mut angle_depth: i32 = 0; // Track angle brackets for aggregates like top_k<3, Points, desc>
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
            '[' if !in_string => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !in_string => {
                // Clamp to 0 to handle malformed input
                bracket_depth = (bracket_depth - 1).max(0);
                current.push(ch);
            }
            '<' if !in_string => {
                angle_depth += 1;
                current.push(ch);
            }
            '>' if !in_string => {
                // Clamp to 0 to handle malformed input
                angle_depth = (angle_depth - 1).max(0);
                current.push(ch);
            }
            ',' if paren_depth == 0 && bracket_depth == 0 && angle_depth == 0 && !in_string => {
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
        Term::StringConstant(s) => format!("\"{s}\""),
        Term::FloatConstant(f) => f.to_string(),
        Term::Placeholder => "_".to_string(),
        _ => "_".to_string(),
    }
}

// Query Parsing
/// Parse a query: ?- goal.
pub fn parse_query(input: &str) -> Result<QueryGoal, String> {
    let input = input.trim().trim_end_matches('.');

    // Try to parse as a simple rule body
    let dummy_rule_str = format!("__query__(X) :- {input}.");
    let rule = parse_rule(&dummy_rule_str)?;

    if rule.body.is_empty() {
        return Err("Query must have at least one goal".to_string());
    }

    // The first positive atom is the main goal
    let goal = rule
        .body
        .iter()
        .find_map(|p| match p {
            BodyPredicate::Positive(atom) => Some(atom.clone()),
            _ => None,
        })
        .ok_or_else(|| "Query must have at least one positive goal".to_string())?;

    // Remaining body predicates (excluding the first goal)
    let body: Vec<BodyPredicate> = rule.body.into_iter().skip(1).collect();

    Ok(QueryGoal { goal, body })
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
        format!("{input}.")
    };

    let rule = parse_rule(&rule_str)?;

    Ok(RuleDef {
        name: rule.head.relation.clone(),
        rule: SerializableRule::from_rule(&rule),
    })
}
