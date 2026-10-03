//! Quote-aware scanning shared by the IQL parsers.
//!
//! String literals are `"..."` with backslash escapes. Every splitter and
//! operator search goes through here so that text inside a literal is never
//! mistaken for syntax.

/// Byte offset just past the literal whose opening quote is at `start`,
/// or `None` if it is unterminated.
pub fn string_end(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// True when `s` is exactly one string literal.
pub fn is_string_literal(s: &str) -> bool {
    s.starts_with('"') && string_end(s, 0) == Some(s.len())
}

/// Iterator over `(byte_offset, char)` outside string literals.
/// Literals, quotes included, are skipped. An unterminated literal runs to the end.
pub struct CodeChars<'a> {
    s: &'a str,
    pos: usize,
}

impl Iterator for CodeChars<'_> {
    type Item = (usize, char);

    fn next(&mut self) -> Option<(usize, char)> {
        loop {
            let ch = self.s[self.pos..].chars().next()?;
            let at = self.pos;
            if ch == '"' {
                self.pos = string_end(self.s, at).unwrap_or(self.s.len());
                continue;
            }
            self.pos += ch.len_utf8();
            return Some((at, ch));
        }
    }
}

/// Characters of `s` outside string literals.
pub fn code_chars(s: &str) -> CodeChars<'_> {
    CodeChars { s, pos: 0 }
}

/// First byte offset of `pat` outside string literals.
pub fn find_outside_strings(s: &str, pat: &str) -> Option<usize> {
    code_chars(s)
        .map(|(i, _)| i)
        .find(|&i| s[i..].starts_with(pat))
}

/// True when `pat` occurs outside string literals.
pub fn contains_outside_strings(s: &str, pat: &str) -> bool {
    find_outside_strings(s, pat).is_some()
}

/// First byte offset of `pat` outside string literals and parentheses.
pub fn find_top_level(s: &str, pat: &str) -> Option<usize> {
    let mut depth: i32 = 0;
    for (i, ch) in code_chars(s) {
        match ch {
            '(' => depth += 1,
            ')' => depth = (depth - 1).max(0),
            _ => {}
        }
        if depth == 0 && s[i..].starts_with(pat) {
            return Some(i);
        }
    }
    None
}

/// How `split_top_level` treats `<` and `>`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Angles {
    /// Not grouping characters.
    Ignore,
    /// Every `<` opens a group.
    All,
    /// `<` opens a group only right after a word character (`count<X>`),
    /// so `X < 5` stays a comparison.
    AfterWord,
}

/// Split `s` at `delim` outside string literals, `()`, `[]` and, per `angles`, `<>`.
/// A trailing empty part is dropped.
pub fn split_top_level(s: &str, delim: char, angles: Angles) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let (mut paren, mut bracket, mut angle) = (0i32, 0i32, 0i32);
    for (i, ch) in code_chars(s) {
        match ch {
            '(' => paren += 1,
            ')' => paren = (paren - 1).max(0),
            '[' => bracket += 1,
            ']' => bracket = (bracket - 1).max(0),
            '<' if angles == Angles::All => angle += 1,
            '<' if angles == Angles::AfterWord
                && s[..i]
                    .chars()
                    .next_back()
                    .is_some_and(|c| c.is_alphanumeric() || c == '_') =>
            {
                angle += 1;
            }
            '>' if angles != Angles::Ignore => angle = (angle - 1).max(0),
            c if c == delim && paren == 0 && bracket == 0 && angle == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    if start < s.len() {
        parts.push(&s[start..]);
    }
    parts
}

/// Decode the body of a string literal. Handles `\n`, `\t`, `\r`, `\\` and `\"`;
/// other escapes are kept verbatim.
pub fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('\\') => out.push('\\'),
            Some('"') => out.push('"'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Encode `s` as the body of a string literal; inverse of `unescape`.
pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out
}

/// Remove `/* ... */` comments (nested) outside string literals, replacing each with a space.
pub fn strip_block_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut depth = 0;
    let mut i = 0;
    while let Some(ch) = source[i..].chars().next() {
        let rest = &source[i..];
        if depth == 0 && ch == '"' {
            let end = string_end(source, i).unwrap_or(source.len());
            out.push_str(&source[i..end]);
            i = end;
        } else if rest.starts_with("/*") {
            depth += 1;
            i += 2;
        } else if depth > 0 && rest.starts_with("*/") {
            depth -= 1;
            i += 2;
            if depth == 0 {
                out.push(' ');
            }
        } else {
            if depth == 0 {
                out.push(ch);
            }
            i += ch.len_utf8();
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn string_end_skips_escaped_quotes() {
        assert_eq!(string_end(r#""a\"b" x"#, 0), Some(6));
        assert_eq!(string_end(r#""a\\" x"#, 0), Some(5));
        assert_eq!(string_end(r#""open"#, 0), None);
    }

    #[test]
    fn literal_detection() {
        assert!(is_string_literal(r#""say \"hi\"""#));
        assert!(!is_string_literal(r#""a" + "b""#));
        assert!(!is_string_literal(r#""a\""#));
    }

    #[test]
    fn split_ignores_text_in_literals() {
        assert_eq!(
            split_top_level(r#"U, "hi, there""#, ',', Angles::All),
            vec!["U", r#" "hi, there""#]
        );
        assert_eq!(
            split_top_level(r#"("a \"x, y\" b"), (2)"#, ',', Angles::Ignore),
            vec![r#"("a \"x, y\" b")"#, " (2)"]
        );
        assert_eq!(
            split_top_level(r#""a<b", X"#, ',', Angles::All),
            vec![r#""a<b""#, " X"]
        );
        assert_eq!(
            split_top_level("a(X), count<X, Y>, X < 5, b(Y)", ',', Angles::AfterWord),
            vec!["a(X)", " count<X, Y>", " X < 5", " b(Y)"]
        );
    }

    #[test]
    fn find_skips_literals() {
        assert_eq!(find_outside_strings(r#"M != "x<-y" <- z"#, "<-"), Some(12));
        assert_eq!(find_top_level(r#"f(X < 1), "<" < Y"#, "<"), Some(14));
    }

    #[test]
    fn escape_roundtrip() {
        for s in [
            "",
            "plain",
            r"C:\temp",
            "say \"hi\"",
            "a\nb\tc\r",
            r"\n literal",
        ] {
            assert_eq!(unescape(&escape(s)), s);
        }
        assert_eq!(unescape(r"\d+"), r"\d+");
    }

    #[test]
    fn block_comments_respect_escaped_quotes() {
        assert_eq!(
            strip_block_comments(r#"a "x \" /* y" /* z */ b"#),
            r#"a "x \" /* y"   b"#
        );
    }

    mod prop {
        use super::super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn unescape_inverts_escape(s in "\\PC{0,40}|[\\\\\"\n\t\r a]{0,20}") {
                prop_assert_eq!(unescape(&escape(&s)), s.clone());
                let lit = format!("\"{}\"", escape(&s));
                prop_assert!(is_string_literal(&lit));
            }
        }
    }
}
