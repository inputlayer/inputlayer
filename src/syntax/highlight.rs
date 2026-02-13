//! Rustyline `Highlighter` integration for Datalog syntax highlighting.
//!
//! Provides `DatalogHelper` which plugs into rustyline's `Editor` to
//! color-code REPL input as the user types.

use std::borrow::Cow;

use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper, Result};

use super::{tokenize, TokenKind};

const RESET: &str = "\x1b[0m";
const PROMPT_COLOR: &str = "\x1b[1;32m"; // bold green

/// Rustyline helper that provides syntax highlighting for Datalog input.
pub struct DatalogHelper;

impl Default for DatalogHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl DatalogHelper {
    pub fn new() -> Self {
        Self
    }
}

impl Helper for DatalogHelper {}

impl Completer for DatalogHelper {
    type Candidate = String;

    fn complete(
        &self,
        _line: &str,
        _pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Self::Candidate>)> {
        Ok((0, Vec::new()))
    }
}

impl Hinter for DatalogHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        None
    }
}

impl Validator for DatalogHelper {
    fn validate(&self, _ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Highlighter for DatalogHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if line.is_empty() {
            return Cow::Borrowed(line);
        }

        let tokens = tokenize(line);

        // If there's only one Unknown token spanning everything, skip highlighting
        if tokens.len() == 1 && tokens[0].kind == TokenKind::Unknown {
            return Cow::Borrowed(line);
        }

        let mut result = String::with_capacity(line.len() * 2);
        let mut last_end = 0;

        for token in &tokens {
            // Emit any gap between tokens as plain text
            if token.span.start > last_end {
                result.push_str(&line[last_end..token.span.start]);
            }

            let code = token.kind.ansi_code();
            let text = &line[token.span.clone()];

            if code.is_empty() {
                result.push_str(text);
            } else {
                result.push_str(code);
                result.push_str(text);
                result.push_str(RESET);
            }

            last_end = token.span.end;
        }

        // Emit any trailing text
        if last_end < line.len() {
            result.push_str(&line[last_end..]);
        }

        Cow::Owned(result)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        // Color the KG name portion of the prompt (everything before "> ")
        if let Some(pos) = prompt.rfind("> ") {
            let kg_name = &prompt[..pos];
            Cow::Owned(format!("{PROMPT_COLOR}{kg_name}{RESET}> "))
        } else {
            Cow::Borrowed(prompt)
        }
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        // Always re-highlight — the grammar is fast enough for interactive use
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highlight_empty_line() {
        let h = DatalogHelper::new();
        let result = h.highlight("", 0);
        assert_eq!(result.as_ref(), "");
    }

    #[test]
    fn test_highlight_produces_ansi() {
        let h = DatalogHelper::new();
        let result = h.highlight("?- edge(X, Y).", 0);
        // Should contain ANSI escape codes
        assert!(result.contains("\x1b["));
        // Should contain reset codes
        assert!(result.contains(RESET));
    }

    #[test]
    fn test_highlight_prompt_with_kg() {
        let h = DatalogHelper::new();
        let result = h.highlight_prompt("mydb> ", false);
        assert!(result.contains(PROMPT_COLOR));
        assert!(result.contains(RESET));
        assert!(result.contains("mydb"));
    }

    #[test]
    fn test_highlight_prompt_without_kg() {
        let h = DatalogHelper::new();
        let result = h.highlight_prompt("inputlayer> ", false);
        assert!(result.contains(PROMPT_COLOR));
    }

    #[test]
    fn test_highlight_char_returns_true() {
        let h = DatalogHelper::new();
        assert!(h.highlight_char("test", 0, false));
    }

    #[test]
    fn test_highlight_preserves_text_content() {
        let h = DatalogHelper::new();
        let input = "?- edge(X, Y).";
        let result = h.highlight(input, 0);
        // Strip ANSI codes and verify text is preserved
        let stripped = strip_ansi(&result);
        assert_eq!(stripped, input);
    }

    fn strip_ansi(s: &str) -> String {
        let mut result = String::new();
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\x1b' {
                // Skip until 'm'
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next == 'm' {
                        break;
                    }
                }
            } else {
                result.push(c);
            }
        }
        result
    }
}
