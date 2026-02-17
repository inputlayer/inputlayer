//! Shared parsing utilities for statement modules.

use crate::ast::{AggregateFunc, Atom, BodyPredicate, Rule, Term};
use crate::parser::{parse_rule, parse_term};

/// Sort direction for query result ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Query goal: ?- atom.
#[derive(Debug, Clone)]
pub struct QueryGoal {
    /// The goal atom to query
    pub goal: Atom,
    /// Additional body predicates (for complex queries)
    pub body: Vec<BodyPredicate>,
    /// Ordering annotations: (variable_name, direction)
    pub order_by: Vec<(String, SortDirection)>,
    /// Maximum number of rows to return
    pub limit: Option<usize>,
    /// Number of rows to skip before returning
    pub offset: Option<usize>,
}

// String Utilities
/// Strip `//` comments, respecting string literals.
pub fn strip_inline_comment(input: &str) -> &str {
    let mut in_string = false;
    let bytes = input.as_bytes();

    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'"' {
            in_string = !in_string;
        } else if !in_string && bytes[i] == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            return input[..i].trim_end();
        }
        i += 1;
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
    let input = input.trim();
    // Must not contain parentheses or `<-`
    !input.contains('(') && !input.contains("<-")
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
                return Ok(Term::BoolConstant(input == "true"));
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

/// Parse an aggregate function like count<X>, sum<Y>, min<Z>, max<Z>, avg<Z>
fn parse_aggregate(input: &str) -> Option<Term> {
    // Check for pattern: func<params> where func is an aggregate
    if let Some(lt_pos) = input.find('<') {
        if let Some(gt_pos) = input.rfind('>') {
            if gt_pos > lt_pos && gt_pos == input.len() - 1 {
                let func_name = &input[..lt_pos];
                let params = &input[lt_pos + 1..gt_pos].trim();

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
///
/// Supports `:asc`/`:desc` annotations on variables in the goal atom, e.g.
/// `?relation(X, Score:desc)`. Annotations are stripped before parsing and
/// recorded as `order_by` on the returned `QueryGoal`.
pub fn parse_query(input: &str) -> Result<QueryGoal, String> {
    let input = input.trim();

    // Extract :asc/:desc annotations from the goal atom arguments
    // before passing to the parser (which doesn't understand them).
    let (cleaned_input, order_by) = strip_sort_annotations(input);

    // Try to parse as a simple rule body
    let dummy_rule_str = format!("__query__(X) <- {cleaned_input}");
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

    // Remaining body predicates (excluding the first goal).
    // Extract limit(...) as a special pseudo-predicate.
    let mut body = Vec::new();
    let mut limit = None;
    let mut offset = None;

    for pred in rule.body.into_iter().skip(1) {
        if let BodyPredicate::Positive(ref atom) = pred {
            if atom.relation == "limit" {
                // limit(N) or limit(N, Offset)
                match atom.args.as_slice() {
                    [Term::Constant(n)] => {
                        if *n < 0 {
                            return Err("limit must be non-negative".to_string());
                        }
                        limit = Some(*n as usize);
                    }
                    [Term::Constant(n), Term::Constant(o)] => {
                        if *n < 0 {
                            return Err("limit must be non-negative".to_string());
                        }
                        if *o < 0 {
                            return Err("offset must be non-negative".to_string());
                        }
                        limit = Some(*n as usize);
                        offset = Some(*o as usize);
                    }
                    _ => {
                        return Err(
                            "limit() expects 1 or 2 integer arguments: limit(N) or limit(N, Offset)"
                                .to_string(),
                        );
                    }
                }
                continue;
            }
        }
        body.push(pred);
    }

    Ok(QueryGoal {
        goal,
        body,
        order_by,
        limit,
        offset,
    })
}

/// Strip `:asc`/`:desc` annotations from the first atom's arguments.
///
/// Given `rel(X, Score:desc, Name:asc), cond(X)`, returns
/// `("rel(X, Score, Name), cond(X)", [(Score, Desc), (Name, Asc)])`.
fn strip_sort_annotations(input: &str) -> (String, Vec<(String, SortDirection)>) {
    // Find the first '(' to locate the goal atom's arguments
    let Some(open_paren) = input.find('(') else {
        return (input.to_string(), vec![]);
    };

    // Find the matching ')' for this opening paren (respecting nesting)
    let mut depth = 0;
    let mut close_paren = None;
    for (i, ch) in input[open_paren..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_paren = Some(open_paren + i);
                    break;
                }
            }
            _ => {}
        }
    }

    let Some(close_paren) = close_paren else {
        return (input.to_string(), vec![]);
    };

    let prefix = &input[..=open_paren]; // "rel("
    let args_str = &input[open_paren + 1..close_paren]; // "X, Score:desc, Name:asc"
    let suffix = &input[close_paren..]; // "), cond(X)"

    let mut order_by = Vec::new();
    let mut cleaned_args = Vec::new();

    for arg in split_top_level(args_str, ',') {
        let arg = arg.trim();
        if let Some(base) = arg.strip_suffix(":desc") {
            let var = base.trim().to_string();
            order_by.push((var.clone(), SortDirection::Desc));
            cleaned_args.push(var);
        } else if let Some(base) = arg.strip_suffix(":asc") {
            let var = base.trim().to_string();
            order_by.push((var.clone(), SortDirection::Asc));
            cleaned_args.push(var);
        } else {
            cleaned_args.push(arg.to_string());
        }
    }

    let result = format!("{prefix}{}{suffix}", cleaned_args.join(", "));
    (result, order_by)
}

/// Split a string by a delimiter at the top level (respecting parentheses).
fn split_top_level(s: &str, delim: char) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, ch) in s.char_indices() {
        match ch {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            c if c == delim && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Parse a transient rule: head <- body.
pub fn parse_transient_rule(input: &str) -> Result<Rule, String> {
    parse_rule(input.trim())
}

/// Parse a persistent rule: +name(...) <- body.
pub fn parse_persistent_rule(input: &str) -> Result<Rule, String> {
    let input = input.trim();
    parse_rule(input)
}

/// Parse a rule definition: head <- body.
pub fn parse_rule_definition(input: &str) -> Result<super::serialize::RuleDef, String> {
    use super::serialize::{RuleDef, SerializableRule};

    let input = input.trim();

    let rule = parse_rule(input)?;

    Ok(RuleDef {
        name: rule.head.relation.clone(),
        rule: SerializableRule::from_rule(&rule),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // === strip_inline_comment ===

    #[test]
    fn test_strip_inline_comment_no_comment() {
        assert_eq!(strip_inline_comment("hello world"), "hello world");
    }

    #[test]
    fn test_strip_inline_comment_with_comment() {
        assert_eq!(strip_inline_comment("hello // world"), "hello");
    }

    #[test]
    fn test_strip_inline_comment_in_string() {
        assert_eq!(
            strip_inline_comment("\"hello // world\""),
            "\"hello // world\""
        );
    }

    #[test]
    fn test_strip_inline_comment_after_string() {
        assert_eq!(strip_inline_comment("\"hello\" // comment"), "\"hello\"");
    }

    #[test]
    fn test_strip_inline_comment_empty() {
        assert_eq!(strip_inline_comment(""), "");
    }

    // === strip_block_comments ===

    #[test]
    fn test_strip_block_comments_none() {
        assert_eq!(strip_block_comments("hello world"), "hello world");
    }

    #[test]
    fn test_strip_block_comments_simple() {
        // "hello " + space (replacement) + " world" = 3 spaces total
        assert_eq!(
            strip_block_comments("hello /* comment */ world"),
            "hello   world"
        );
    }

    #[test]
    fn test_strip_block_comments_nested() {
        // "a " + space (replacement) + " b" = 3 spaces total
        assert_eq!(
            strip_block_comments("a /* outer /* inner */ still outer */ b"),
            "a   b"
        );
    }

    #[test]
    fn test_strip_block_comments_unclosed() {
        // Unclosed block comment: everything after /* is consumed
        let result = strip_block_comments("hello /* unclosed");
        assert_eq!(result, "hello ");
    }

    // === has_typed_arguments ===

    #[test]
    fn test_has_typed_arguments_true() {
        assert!(has_typed_arguments("name: string, age: int"));
    }

    #[test]
    fn test_has_typed_arguments_false_values() {
        assert!(!has_typed_arguments("1, 2, 3"));
    }

    #[test]
    fn test_has_typed_arguments_false_variables() {
        assert!(!has_typed_arguments("X, Y, Z"));
    }

    #[test]
    fn test_has_typed_arguments_empty() {
        assert!(!has_typed_arguments(""));
    }

    #[test]
    fn test_has_typed_arguments_type_ref() {
        assert!(has_typed_arguments("email: Email"));
    }

    // === extract_args_content ===

    #[test]
    fn test_extract_args_content_simple() {
        assert_eq!(extract_args_content("name(a, b)"), Some("a, b"));
    }

    #[test]
    fn test_extract_args_content_empty_parens() {
        assert_eq!(extract_args_content("name()"), Some(""));
    }

    #[test]
    fn test_extract_args_content_no_parens() {
        assert_eq!(extract_args_content("name"), None);
    }

    // === is_simple_name_deletion ===

    #[test]
    fn test_is_simple_name_deletion_true() {
        assert!(is_simple_name_deletion("relation"));
    }

    #[test]
    fn test_is_simple_name_deletion_false_with_parens() {
        assert!(!is_simple_name_deletion("relation(X)"));
    }

    #[test]
    fn test_is_simple_name_deletion_false_with_arrow() {
        assert!(!is_simple_name_deletion("foo(X) <- bar(X)"));
    }

    // === validate_relation_name ===

    #[test]
    fn test_validate_relation_name_valid() {
        assert!(validate_relation_name("edge").is_ok());
        assert!(validate_relation_name("my_relation").is_ok());
        assert!(validate_relation_name("r2d2").is_ok());
    }

    #[test]
    fn test_validate_relation_name_uppercase() {
        let result = validate_relation_name("Edge");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("lowercase"));
    }

    #[test]
    fn test_validate_relation_name_empty() {
        assert!(validate_relation_name("").is_err());
    }

    #[test]
    fn test_validate_relation_name_invalid_chars() {
        assert!(validate_relation_name("my-relation").is_err());
    }

    // === parse_single_term ===

    #[test]
    fn test_parse_single_term_placeholder() {
        let result = parse_single_term("_").unwrap();
        assert!(matches!(result, Term::Placeholder));
    }

    #[test]
    fn test_parse_single_term_string() {
        let result = parse_single_term("\"hello\"").unwrap();
        assert!(matches!(result, Term::StringConstant(ref s) if s == "hello"));
    }

    #[test]
    fn test_parse_single_term_integer() {
        let result = parse_single_term("42").unwrap();
        assert!(matches!(result, Term::Constant(42)));
    }

    #[test]
    fn test_parse_single_term_float() {
        let result = parse_single_term("3.14").unwrap();
        assert!(matches!(result, Term::FloatConstant(f) if (f - 3.14).abs() < 0.001));
    }

    #[test]
    fn test_parse_single_term_variable() {
        let result = parse_single_term("X").unwrap();
        assert!(matches!(result, Term::Variable(ref n) if n == "X"));
    }

    #[test]
    fn test_parse_single_term_underscore_var() {
        // Single underscore is placeholder, but _temp is a variable
        let result = parse_single_term("_temp").unwrap();
        assert!(matches!(result, Term::Variable(ref n) if n == "_temp"));
    }

    #[test]
    fn test_parse_single_term_bool_true() {
        let result = parse_single_term("true").unwrap();
        assert!(matches!(result, Term::BoolConstant(true)));
    }

    #[test]
    fn test_parse_single_term_bool_false() {
        let result = parse_single_term("false").unwrap();
        assert!(matches!(result, Term::BoolConstant(false)));
    }

    #[test]
    fn test_parse_single_term_unquoted_atom_error() {
        let result = parse_single_term("alice");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("quoted string"));
    }

    #[test]
    fn test_parse_single_term_vector_literal() {
        let result = parse_single_term("[1.0, 2.0, 3.0]").unwrap();
        if let Term::VectorLiteral(v) = result {
            assert_eq!(v.len(), 3);
        } else {
            panic!("Expected VectorLiteral");
        }
    }

    #[test]
    fn test_parse_single_term_empty_vector() {
        let result = parse_single_term("[]").unwrap();
        if let Term::VectorLiteral(v) = result {
            assert!(v.is_empty());
        } else {
            panic!("Expected VectorLiteral");
        }
    }

    #[test]
    fn test_parse_single_term_negative_int() {
        let result = parse_single_term("-5").unwrap();
        assert!(matches!(result, Term::Constant(-5)));
    }

    #[test]
    fn test_parse_single_term_negative_float() {
        let result = parse_single_term("-2.5").unwrap();
        assert!(matches!(result, Term::FloatConstant(f) if (f - (-2.5)).abs() < 0.001));
    }

    // === parse_atom_args ===

    #[test]
    fn test_parse_atom_args_simple() {
        let result = parse_atom_args("(X, Y)").unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_parse_atom_args_empty() {
        let result = parse_atom_args("()").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_atom_args_no_parens() {
        assert!(parse_atom_args("X, Y").is_err());
    }

    // === split_by_comma ===

    #[test]
    fn test_split_by_comma_simple() {
        let result = split_by_comma("a, b, c");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_split_by_comma_nested_parens() {
        let result = split_by_comma("f(a, b), c");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_by_comma_nested_brackets() {
        let result = split_by_comma("[1, 2], c");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_by_comma_nested_angles() {
        let result = split_by_comma("top_k<3, X, Y:desc>, Z");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_by_comma_in_string() {
        let result = split_by_comma("\"a,b\", c");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_split_by_comma_empty() {
        let result = split_by_comma("");
        assert!(result.is_empty());
    }

    // === term_to_string ===

    #[test]
    fn test_term_to_string_variable() {
        assert_eq!(term_to_string(&Term::Variable("X".to_string())), "X");
    }

    #[test]
    fn test_term_to_string_constant() {
        assert_eq!(term_to_string(&Term::Constant(42)), "42");
    }

    #[test]
    fn test_term_to_string_string_constant() {
        assert_eq!(
            term_to_string(&Term::StringConstant("hello".to_string())),
            "\"hello\""
        );
    }

    #[test]
    fn test_term_to_string_float() {
        let result = term_to_string(&Term::FloatConstant(3.14));
        assert!(result.starts_with("3.14"));
    }

    #[test]
    fn test_term_to_string_placeholder() {
        assert_eq!(term_to_string(&Term::Placeholder), "_");
    }

    // === parse_query ===

    #[test]
    fn test_parse_query_simple() {
        let result = parse_query("edge(X, Y)").unwrap();
        assert_eq!(result.goal.relation, "edge");
        assert_eq!(result.goal.args.len(), 2);
    }

    #[test]
    fn test_parse_query_with_extra_body() {
        let result = parse_query("edge(X, Y), X > 1").unwrap();
        assert_eq!(result.goal.relation, "edge");
        assert!(!result.body.is_empty());
    }

    // === parse_transient_rule / parse_persistent_rule ===

    #[test]
    fn test_parse_transient_rule() {
        let result = parse_transient_rule("path(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(result.head.relation, "path");
    }

    #[test]
    fn test_parse_persistent_rule() {
        let result = parse_persistent_rule("path(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(result.head.relation, "path");
    }

    // === parse_rule_definition ===

    #[test]
    fn test_parse_rule_definition() {
        let result = parse_rule_definition("path(X, Y) <- edge(X, Y)").unwrap();
        assert_eq!(result.name, "path");
    }

    // === parse_aggregate ===

    #[test]
    fn test_parse_aggregate_count() {
        let result = parse_aggregate("count<X>").unwrap();
        assert!(matches!(result, Term::Aggregate(AggregateFunc::Count, _)));
    }

    #[test]
    fn test_parse_aggregate_sum() {
        let result = parse_aggregate("sum<Y>").unwrap();
        assert!(matches!(result, Term::Aggregate(AggregateFunc::Sum, _)));
    }

    #[test]
    fn test_parse_aggregate_min() {
        let result = parse_aggregate("min<Z>").unwrap();
        assert!(matches!(result, Term::Aggregate(AggregateFunc::Min, _)));
    }

    #[test]
    fn test_parse_aggregate_max() {
        let result = parse_aggregate("max<Z>").unwrap();
        assert!(matches!(result, Term::Aggregate(AggregateFunc::Max, _)));
    }

    #[test]
    fn test_parse_aggregate_avg() {
        let result = parse_aggregate("avg<X>").unwrap();
        assert!(matches!(result, Term::Aggregate(AggregateFunc::Avg, _)));
    }

    #[test]
    fn test_parse_aggregate_count_distinct() {
        let result = parse_aggregate("count_distinct<X>").unwrap();
        assert!(matches!(
            result,
            Term::Aggregate(AggregateFunc::CountDistinct, _)
        ));
    }

    #[test]
    fn test_parse_aggregate_not_aggregate() {
        assert!(parse_aggregate("hello").is_none());
    }

    #[test]
    fn test_parse_aggregate_lowercase_var() {
        // Lowercase first char = not a valid variable, so not a valid aggregate
        assert!(parse_aggregate("count<x>").is_none());
    }

    // === parse_query limit/offset ===

    #[test]
    fn test_parse_query_with_limit() {
        let result = parse_query("data(X), limit(3)").unwrap();
        assert_eq!(result.goal.relation, "data");
        assert_eq!(result.limit, Some(3));
        assert_eq!(result.offset, None);
        // limit pseudo-predicate should NOT appear in body
        assert!(result.body.is_empty());
    }

    #[test]
    fn test_parse_query_with_limit_and_offset() {
        let result = parse_query("data(X), limit(5, 2)").unwrap();
        assert_eq!(result.goal.relation, "data");
        assert_eq!(result.limit, Some(5));
        assert_eq!(result.offset, Some(2));
        assert!(result.body.is_empty());
    }

    #[test]
    fn test_parse_query_limit_zero() {
        let result = parse_query("data(X), limit(0)").unwrap();
        assert_eq!(result.limit, Some(0));
        assert_eq!(result.offset, None);
    }

    #[test]
    fn test_parse_query_negative_limit_rejected() {
        let result = parse_query("data(X), limit(-1)");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("non-negative"));
    }

    #[test]
    fn test_parse_query_negative_offset_rejected() {
        let result = parse_query("data(X), limit(5, -1)");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("non-negative"));
    }

    #[test]
    fn test_parse_query_no_limit() {
        let result = parse_query("data(X)").unwrap();
        assert_eq!(result.limit, None);
        assert_eq!(result.offset, None);
    }
}
