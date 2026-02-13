//! Syntax highlighting for InputLayer Datalog.
//!
//! Uses a PEG grammar (`datalog.pest`) to tokenize input into classified spans,
//! then maps each span to ANSI terminal colors for REPL highlighting.
//!
//! The `.pest` grammar file is a reusable artifact that can also drive
//! web-based syntax highlighting (e.g., for documentation sites).

pub mod highlight;

use pest::Parser;
use pest_derive::Parser;
use std::ops::Range;

#[derive(Parser)]
#[grammar = "syntax/datalog.pest"]
struct DatalogTokenizer;

/// Token classification for syntax highlighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    Comment,
    StringLiteral,
    MetaCommand,
    QueryMarker,
    RuleArrow,
    OperatorPrefix,
    NegationPrefix,
    ComparisonOp,
    Number,
    Aggregate,
    BuiltinFn,
    Keyword,
    Variable,
    Identifier,
    BodyIdentifier,
    SchemaColumn,
    SortOrder,
    ArithOp,
    Punctuation,
    Whitespace,
    Unknown,
}

impl TokenKind {
    /// ANSI escape code for this token kind (dark terminal background).
    pub fn ansi_code(self) -> &'static str {
        match self {
            Self::Comment => "\x1b[90m",          // dark gray
            Self::StringLiteral => "\x1b[32m",    // green
            Self::MetaCommand => "\x1b[1;35m",    // bold magenta
            Self::QueryMarker => "\x1b[1;36m",    // bold cyan
            Self::RuleArrow => "\x1b[1;35m",      // bold magenta
            Self::OperatorPrefix => "\x1b[1;36m", // bold cyan
            Self::NegationPrefix => "\x1b[1;31m", // bold red
            Self::ComparisonOp => "\x1b[31m",     // red
            Self::Number => "\x1b[36m",           // cyan
            Self::Aggregate => "\x1b[1;33m",      // bold yellow
            Self::BuiltinFn => "\x1b[33m",        // yellow
            Self::Keyword => "\x1b[1;34m",        // bold blue
            Self::Variable => "\x1b[94m",         // bright blue
            Self::Identifier => "\x1b[97m",       // bright white (relations)
            Self::BodyIdentifier => "\x1b[36m",   // cyan (body relations)
            Self::SchemaColumn => "\x1b[3;94m",   // italic bright blue
            Self::SortOrder => "\x1b[1;34m",      // bold blue
            Self::ArithOp => "\x1b[31m",          // red
            Self::Punctuation => "\x1b[90m",      // dark gray
            Self::Whitespace => "",               // no color
            Self::Unknown => "",                  // no color
        }
    }
}

/// A classified token span.
#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Range<usize>,
}

/// Tokenize a line of Datalog input into classified spans.
///
/// On parse failure (partial/malformed input), returns the entire input
/// as a single `Unknown` token so highlighting degrades gracefully.
pub fn tokenize(input: &str) -> Vec<Token> {
    let pairs = match DatalogTokenizer::parse(Rule::line, input) {
        Ok(pairs) => pairs,
        Err(_) => {
            return vec![Token {
                kind: TokenKind::Unknown,
                span: 0..input.len(),
            }];
        }
    };

    let mut tokens = Vec::new();

    for pair in pairs.flatten() {
        let kind = match pair.as_rule() {
            Rule::COMMENT => TokenKind::Comment,
            Rule::string_literal => TokenKind::StringLiteral,
            Rule::meta_command => TokenKind::MetaCommand,
            Rule::query_marker => TokenKind::QueryMarker,
            Rule::rule_arrow => TokenKind::RuleArrow,
            Rule::operator_prefix => TokenKind::OperatorPrefix,
            Rule::negation_prefix => TokenKind::NegationPrefix,
            Rule::comparison_op => TokenKind::ComparisonOp,
            Rule::number => TokenKind::Number,
            Rule::aggregate => TokenKind::Aggregate,
            Rule::builtin_fn => TokenKind::BuiltinFn,
            Rule::keyword => TokenKind::Keyword,
            Rule::variable => TokenKind::Variable,
            Rule::identifier => TokenKind::Identifier,
            Rule::arith_op => TokenKind::ArithOp,
            Rule::punctuation => TokenKind::Punctuation,
            Rule::whitespace => TokenKind::Whitespace,
            Rule::any_char => TokenKind::Unknown,
            // line, token, and hidden rules are structural — skip
            _ => continue,
        };

        let span = pair.as_span();
        tokens.push(Token {
            kind,
            span: span.start()..span.end(),
        });
    }

    tokens
}

/// Schema type names recognized for `name: type` column detection.
const SCHEMA_TYPES: &[&str] = &[
    "int",
    "integer",
    "i32",
    "i64",
    "float",
    "double",
    "f64",
    "number",
    "symbol",
    "string",
    "str",
    "text",
    "bool",
    "boolean",
    "timestamp",
    "time",
    "datetime",
    "vector",
    "embedding",
    "vec",
    "any",
    "list",
];

/// Promote flat tokens to semantic variants based on structural context.
///
/// Applies three passes over the token stream:
/// 1. **Head/body split** — identifiers after `<-` become `BodyIdentifier`
/// 2. **Schema columns** — `name: type` patterns promote name to `SchemaColumn`
/// 3. **Sort order** — `:desc`/`:asc` annotations become `SortOrder`
pub fn semanticize(tokens: &mut [Token], input: &str) {
    // Pass 1: Head/body split — find RuleArrow and promote identifiers after it
    if let Some(arrow_idx) = tokens.iter().position(|t| t.kind == TokenKind::RuleArrow) {
        for token in &mut tokens[arrow_idx + 1..] {
            if token.kind == TokenKind::Identifier {
                token.kind = TokenKind::BodyIdentifier;
            }
        }
    }

    // Pass 2: Schema columns — look for `Identifier + Punctuation(":") + type`
    // Scan windows of 3 non-whitespace tokens
    let non_ws: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, t)| t.kind != TokenKind::Whitespace)
        .map(|(i, _)| i)
        .collect();

    for window in non_ws.windows(3) {
        let (i_name, i_colon, i_type) = (window[0], window[1], window[2]);
        let name_tok = &tokens[i_name];
        let colon_tok = &tokens[i_colon];
        let type_tok = &tokens[i_type];

        // Name must be an identifier (head or body)
        if !matches!(
            name_tok.kind,
            TokenKind::Identifier | TokenKind::BodyIdentifier
        ) {
            continue;
        }

        // Colon punctuation
        if colon_tok.kind != TokenKind::Punctuation || &input[colon_tok.span.clone()] != ":" {
            continue;
        }

        // Type must be a keyword or an identifier matching a schema type
        let type_text = &input[type_tok.span.clone()];
        let is_schema_type = match type_tok.kind {
            TokenKind::Keyword => true,
            TokenKind::Identifier | TokenKind::BodyIdentifier => SCHEMA_TYPES.contains(&type_text),
            _ => false,
        };

        if is_schema_type {
            tokens[i_name].kind = TokenKind::SchemaColumn;
            tokens[i_type].kind = TokenKind::Keyword;
        }
    }

    // Pass 3: Sort order — look for `Punctuation(":") + Identifier("desc"|"asc")`
    for window in non_ws.windows(2) {
        let (i_colon, i_sort) = (window[0], window[1]);
        let colon_tok = &tokens[i_colon];
        let sort_tok = &tokens[i_sort];

        if colon_tok.kind != TokenKind::Punctuation || &input[colon_tok.span.clone()] != ":" {
            continue;
        }

        if matches!(
            sort_tok.kind,
            TokenKind::Identifier | TokenKind::BodyIdentifier
        ) {
            let text = &input[sort_tok.span.clone()];
            if text == "desc" || text == "asc" {
                tokens[i_sort].kind = TokenKind::SortOrder;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token_kinds(input: &str) -> Vec<(TokenKind, &str)> {
        tokenize(input)
            .into_iter()
            .filter(|t| t.kind != TokenKind::Whitespace)
            .map(|t| (t.kind, &input[t.span]))
            .collect()
    }

    #[test]
    fn test_query_with_variables() {
        let tokens = token_kinds("?edge(X, Y)");
        assert_eq!(tokens[0], (TokenKind::QueryMarker, "?"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "edge"));
        assert_eq!(tokens[2], (TokenKind::Punctuation, "("));
        assert_eq!(tokens[3], (TokenKind::Variable, "X"));
        assert_eq!(tokens[4], (TokenKind::Punctuation, ","));
        assert_eq!(tokens[5], (TokenKind::Variable, "Y"));
        assert_eq!(tokens[6], (TokenKind::Punctuation, ")"));
    }

    #[test]
    fn test_insert_with_string_and_number() {
        let tokens = token_kinds("+person(\"alice\", 30)");
        assert_eq!(tokens[0], (TokenKind::OperatorPrefix, "+"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "person"));
        assert_eq!(tokens[2], (TokenKind::Punctuation, "("));
        assert_eq!(tokens[3], (TokenKind::StringLiteral, "\"alice\""));
        assert_eq!(tokens[4], (TokenKind::Punctuation, ","));
        assert_eq!(tokens[5], (TokenKind::Number, "30"));
        assert_eq!(tokens[6], (TokenKind::Punctuation, ")"));
    }

    #[test]
    fn test_meta_command_kg_create() {
        let tokens = token_kinds(".kg create test");
        assert_eq!(tokens[0], (TokenKind::MetaCommand, ".kg create"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "test"));
    }

    #[test]
    fn test_line_comment() {
        let tokens = token_kinds("// this is a comment");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].0, TokenKind::Comment);
    }

    #[test]
    fn test_rule_with_arrow() {
        let tokens = token_kinds("path(X, Z) <- edge(X, Y), edge(Y, Z)");
        let kinds: Vec<TokenKind> = tokens.iter().map(|t| t.0).collect();
        assert!(kinds.contains(&TokenKind::RuleArrow));
        assert!(kinds.contains(&TokenKind::Variable));
        assert!(kinds.contains(&TokenKind::Identifier));
    }

    #[test]
    fn test_string_literal() {
        let tokens = token_kinds("\"hello world\"");
        assert_eq!(tokens[0].0, TokenKind::StringLiteral);
        assert_eq!(tokens[0].1, "\"hello world\"");
    }

    #[test]
    fn test_number_integer() {
        let tokens = token_kinds("42");
        assert_eq!(tokens[0], (TokenKind::Number, "42"));
    }

    #[test]
    fn test_number_float() {
        let tokens = token_kinds("3.14");
        assert_eq!(tokens[0], (TokenKind::Number, "3.14"));
    }

    #[test]
    fn test_number_scientific() {
        let tokens = token_kinds("1e5");
        // "1e5" - the integer rule matches "1", then "e5" is identifier
        // This is acceptable for highlighting purposes
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_comparison_operators() {
        for op in &[">=", "<=", "!=", "<", ">", "="] {
            let tokens = token_kinds(op);
            assert_eq!(tokens[0].0, TokenKind::ComparisonOp, "Failed for {op}");
        }
    }

    #[test]
    fn test_aggregate_functions() {
        for agg in &[
            "count",
            "sum",
            "min",
            "max",
            "avg",
            "top_k",
            "count_distinct",
        ] {
            let tokens = token_kinds(agg);
            assert_eq!(tokens[0].0, TokenKind::Aggregate, "Failed for {agg}");
        }
    }

    #[test]
    fn test_builtin_functions() {
        for func in &[
            "euclidean",
            "cosine",
            "len",
            "upper",
            "abs",
            "sqrt",
            "time_now",
        ] {
            let tokens = token_kinds(func);
            assert_eq!(tokens[0].0, TokenKind::BuiltinFn, "Failed for {func}");
        }
    }

    #[test]
    fn test_keywords() {
        for kw in &["type", "true", "false", "int", "string", "bool", "float"] {
            let tokens = token_kinds(kw);
            assert_eq!(tokens[0].0, TokenKind::Keyword, "Failed for {kw}");
        }
    }

    #[test]
    fn test_negation_prefix() {
        let tokens = token_kinds("!edge(X, Y)");
        assert_eq!(tokens[0].0, TokenKind::NegationPrefix);
    }

    #[test]
    fn test_variable_with_underscore() {
        let tokens = token_kinds("_temp");
        assert_eq!(tokens[0], (TokenKind::Variable, "_temp"));
    }

    #[test]
    fn test_empty_input() {
        let tokens = tokenize("");
        assert!(tokens.is_empty() || tokens.iter().all(|t| t.kind == TokenKind::Whitespace));
    }

    #[test]
    fn test_graceful_degradation() {
        // Partial/malformed input should not panic
        let tokens = tokenize("?edge(X,");
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_meta_command_help() {
        let tokens = token_kinds(".help");
        assert_eq!(tokens[0], (TokenKind::MetaCommand, ".help"));
    }

    #[test]
    fn test_meta_command_quit() {
        let tokens = token_kinds(".quit");
        assert_eq!(tokens[0], (TokenKind::MetaCommand, ".quit"));
    }

    #[test]
    fn test_persistent_rule() {
        let tokens = token_kinds("+path(X, Y) <- edge(X, Y)");
        // + is separate from path
        assert_eq!(tokens[0], (TokenKind::OperatorPrefix, "+"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "path"));
        // Should contain rule arrow
        let has_arrow = tokens.iter().any(|t| t.0 == TokenKind::RuleArrow);
        assert!(has_arrow);
    }

    #[test]
    fn test_delete_prefix() {
        let tokens = token_kinds("-edge(1, 2)");
        assert_eq!(tokens[0], (TokenKind::OperatorPrefix, "-"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "edge"));
    }

    #[test]
    fn test_bulk_insert() {
        let tokens = token_kinds("+sales[(\"North\", 100)]");
        assert_eq!(tokens[0], (TokenKind::OperatorPrefix, "+"));
        assert_eq!(tokens[1], (TokenKind::Identifier, "sales"));
        assert_eq!(tokens[2], (TokenKind::Punctuation, "["));
    }

    #[test]
    fn test_block_comment() {
        let tokens = token_kinds("/* block comment */");
        assert_eq!(tokens[0].0, TokenKind::Comment);
    }

    #[test]
    fn test_mixed_statement() {
        // Complex real-world statement
        let input = "?person(X, Name), X > 5, len(Name) >= 3";
        let tokens = token_kinds(input);
        let kinds: Vec<TokenKind> = tokens.iter().map(|t| t.0).collect();
        assert!(kinds.contains(&TokenKind::QueryMarker));
        assert!(kinds.contains(&TokenKind::Variable));
        assert!(kinds.contains(&TokenKind::Identifier));
        assert!(kinds.contains(&TokenKind::Number));
        assert!(kinds.contains(&TokenKind::ComparisonOp));
        assert!(kinds.contains(&TokenKind::BuiltinFn));
    }

    // --- Semantic highlighting tests ---

    fn semantic_token_kinds(input: &str) -> Vec<(TokenKind, &str)> {
        let mut tokens = tokenize(input);
        semanticize(&mut tokens, input);
        tokens
            .into_iter()
            .filter(|t| t.kind != TokenKind::Whitespace)
            .map(|t| (t.kind, &input[t.span]))
            .collect()
    }

    #[test]
    fn test_semanticize_body_identifiers() {
        let input = "+path(X, Z) <- edge(X, Y), edge(Y, Z)";
        let tokens = semantic_token_kinds(input);
        // Head: "path" stays Identifier
        assert_eq!(tokens[1], (TokenKind::Identifier, "path"));
        // Body: "edge" tokens become BodyIdentifier
        let body_idents: Vec<_> = tokens
            .iter()
            .filter(|t| t.0 == TokenKind::BodyIdentifier)
            .collect();
        assert_eq!(body_idents.len(), 2);
        assert_eq!(body_idents[0].1, "edge");
        assert_eq!(body_idents[1].1, "edge");
    }

    #[test]
    fn test_semanticize_schema_columns() {
        let input = "+employee(emp_id: int, name: string, embedding: vector)";
        let tokens = semantic_token_kinds(input);
        // Schema columns
        let schema_cols: Vec<_> = tokens
            .iter()
            .filter(|t| t.0 == TokenKind::SchemaColumn)
            .collect();
        assert_eq!(schema_cols.len(), 3);
        assert_eq!(schema_cols[0].1, "emp_id");
        assert_eq!(schema_cols[1].1, "name");
        assert_eq!(schema_cols[2].1, "embedding");
        // Types should be Keyword
        let keywords: Vec<_> = tokens
            .iter()
            .filter(|t| t.0 == TokenKind::Keyword)
            .collect();
        assert!(keywords.iter().any(|t| t.1 == "int"));
        assert!(keywords.iter().any(|t| t.1 == "string"));
        assert!(keywords.iter().any(|t| t.1 == "vector"));
    }

    #[test]
    fn test_semanticize_sort_order() {
        let input = "top_k<3, Name, Score:desc>";
        let tokens = semantic_token_kinds(input);
        let sort: Vec<_> = tokens
            .iter()
            .filter(|t| t.0 == TokenKind::SortOrder)
            .collect();
        assert_eq!(sort.len(), 1);
        assert_eq!(sort[0].1, "desc");
    }

    #[test]
    fn test_semanticize_no_arrow() {
        // Fact assertions have no body — all identifiers stay as Identifier
        let input = "+edge(1, 2)";
        let tokens = semantic_token_kinds(input);
        assert!(!tokens.iter().any(|t| t.0 == TokenKind::BodyIdentifier));
        assert_eq!(tokens[1], (TokenKind::Identifier, "edge"));
    }

    #[test]
    fn test_semanticize_preserves_head() {
        // Query marker — identifiers in head position stay as Identifier
        let input = "?relation(X, Y)";
        let tokens = semantic_token_kinds(input);
        assert_eq!(tokens[1], (TokenKind::Identifier, "relation"));
        assert!(!tokens.iter().any(|t| t.0 == TokenKind::BodyIdentifier));
    }
}
