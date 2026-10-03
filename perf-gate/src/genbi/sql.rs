//! The small SQL subset genbi-trust uses for its checks and mutations.
//!
//! Checks are `SELECT col, ... FROM rel [WHERE col = lit AND ...] [ORDER BY
//! ...]`; mutations are `INSERT [OR IGNORE] INTO rel VALUES (...)`, `DELETE
//! FROM rel [WHERE ...]` and `UPDATE rel SET col = lit, ... [WHERE ...]`.
//! Anything else (aggregates, `EXISTS`, `IN`, `INSERT ... SELECT`, `NULL`) is
//! reported as unsupported with the reason, never approximated.

use std::fmt;

use anyhow::{bail, Result};

/// A SQL literal.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Text(String),
}

impl fmt::Display for Literal {
    /// IQL spelling of the literal.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::Int(v) => write!(f, "{v}"),
            Literal::Float(v) => write!(f, "{v:?}"),
            Literal::Text(v) => write!(f, "{}", serde_json::Value::String(v.clone())),
        }
    }
}

/// `column = literal`.
#[derive(Debug, Clone, PartialEq)]
pub struct Eq {
    pub column: String,
    pub value: Literal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select {
        columns: Vec<String>,
        relation: String,
        filter: Vec<Eq>,
    },
    Insert {
        relation: String,
        values: Vec<Literal>,
    },
    Delete {
        relation: String,
        filter: Vec<Eq>,
    },
    Update {
        relation: String,
        set: Vec<Eq>,
        filter: Vec<Eq>,
    },
}

/// Parse a `;`-separated script; empty statements are skipped.
pub fn parse_script(sql: &str) -> Result<Vec<Statement>> {
    let tokens = tokenize(sql)?;
    tokens
        .split(|t| *t == Token::Punct(';'))
        .filter(|s| !s.is_empty())
        .map(|s| Parser { tokens: s, pos: 0 }.statement())
        .collect()
}

/// Parse exactly one statement.
pub fn parse(sql: &str) -> Result<Statement> {
    let mut statements = parse_script(sql)?;
    if statements.len() != 1 {
        bail!("expected one statement, found {}", statements.len());
    }
    Ok(statements.remove(0))
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Lit(Literal),
    Punct(char),
}

fn tokenize(sql: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = sql.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '\'' {
            let mut text = String::new();
            i += 1;
            loop {
                match chars.get(i) {
                    None => bail!("unterminated string literal"),
                    Some('\'') if chars.get(i + 1) == Some(&'\'') => {
                        text.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(&ch) => {
                        text.push(ch);
                        i += 1;
                    }
                }
            }
            tokens.push(Token::Lit(Literal::Text(text)));
        } else if c.is_ascii_digit()
            || (c == '-' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let start = i;
            i += 1;
            while chars
                .get(i)
                .is_some_and(|ch| ch.is_ascii_digit() || *ch == '.')
            {
                i += 1;
            }
            let text: String = chars[start..i].iter().collect();
            tokens.push(Token::Lit(if text.contains('.') {
                Literal::Float(text.parse()?)
            } else {
                Literal::Int(text.parse()?)
            }));
        } else if c.is_alphabetic() || c == '_' {
            let start = i;
            while chars
                .get(i)
                .is_some_and(|ch| ch.is_alphanumeric() || *ch == '_')
            {
                i += 1;
            }
            tokens.push(Token::Word(chars[start..i].iter().collect()));
        } else if "(),=*;<>!".contains(c) {
            tokens.push(Token::Punct(c));
            i += 1;
        } else if c == '.' {
            bail!("unsupported qualified name (subquery or join)");
        } else {
            bail!("unsupported character '{c}'");
        }
    }
    Ok(tokens)
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl Parser<'_> {
    fn statement(&mut self) -> Result<Statement> {
        let statement = match self.word()?.to_ascii_uppercase().as_str() {
            "SELECT" => self.select()?,
            "INSERT" => self.insert()?,
            "DELETE" => {
                self.keyword("FROM")?;
                let relation = self.word()?;
                let filter = self.filter()?;
                Statement::Delete { relation, filter }
            }
            "UPDATE" => {
                let relation = self.word()?;
                self.keyword("SET")?;
                let set = self.assignments("WHERE")?;
                let filter = self.filter()?;
                Statement::Update {
                    relation,
                    set,
                    filter,
                }
            }
            other => bail!("unsupported statement {other}"),
        };
        if let Some(token) = self.tokens.get(self.pos) {
            bail!("unsupported clause at {token:?}");
        }
        Ok(statement)
    }

    fn select(&mut self) -> Result<Statement> {
        let mut columns = vec![self.column()?];
        while self.eat(&Token::Punct(',')) {
            columns.push(self.column()?);
        }
        self.keyword("FROM")?;
        let relation = self.word()?;
        let filter = self.filter()?;
        if self.eat_keyword("ORDER") {
            // Result order is not part of the check (multiset comparison).
            self.pos = self.tokens.len();
        }
        Ok(Statement::Select {
            columns,
            relation,
            filter,
        })
    }

    fn insert(&mut self) -> Result<Statement> {
        if self.eat_keyword("OR") {
            // `OR IGNORE`: inserting an existing fact is a no-op in IQL too.
            self.keyword("IGNORE")?;
        }
        self.keyword("INTO")?;
        let relation = self.word()?;
        if !self.eat_keyword("VALUES") {
            bail!("unsupported INSERT form (only VALUES)");
        }
        self.expect(&Token::Punct('('))?;
        let mut values = vec![self.literal()?];
        while self.eat(&Token::Punct(',')) {
            values.push(self.literal()?);
        }
        self.expect(&Token::Punct(')'))?;
        Ok(Statement::Insert { relation, values })
    }

    /// A plain column name; expressions and `*` are unsupported.
    fn column(&mut self) -> Result<String> {
        let name = self.word()?;
        if self.tokens.get(self.pos) == Some(&Token::Punct('(')) {
            bail!("unsupported expression {name}(...)");
        }
        if ["EXISTS", "DISTINCT", "CASE"].contains(&name.to_ascii_uppercase().as_str()) {
            bail!("unsupported select item {name}");
        }
        Ok(name)
    }

    /// `[WHERE eq AND eq ...]`.
    fn filter(&mut self) -> Result<Vec<Eq>> {
        if !self.eat_keyword("WHERE") {
            return Ok(Vec::new());
        }
        let mut filter = vec![self.eq()?];
        while self.eat_keyword("AND") {
            filter.push(self.eq()?);
        }
        Ok(filter)
    }

    /// `col = lit, ...` up to `stop` or the end.
    fn assignments(&mut self, stop: &str) -> Result<Vec<Eq>> {
        let mut set = vec![self.eq()?];
        while self.eat(&Token::Punct(',')) {
            set.push(self.eq()?);
        }
        if let Some(Token::Word(w)) = self.tokens.get(self.pos) {
            if !w.eq_ignore_ascii_case(stop) {
                bail!("unsupported clause {w}");
            }
        }
        Ok(set)
    }

    fn eq(&mut self) -> Result<Eq> {
        let column = self.word()?;
        if !self.eat(&Token::Punct('=')) {
            bail!("unsupported condition on {column} (only `column = literal`)");
        }
        Ok(Eq {
            column,
            value: self.literal()?,
        })
    }

    fn literal(&mut self) -> Result<Literal> {
        match self.tokens.get(self.pos) {
            Some(Token::Lit(value)) => {
                self.pos += 1;
                Ok(value.clone())
            }
            Some(Token::Word(w)) if w.eq_ignore_ascii_case("NULL") => {
                bail!("unsupported NULL value (IQL has no NULL)")
            }
            other => bail!("expected a literal, found {other:?}"),
        }
    }

    fn word(&mut self) -> Result<String> {
        match self.tokens.get(self.pos) {
            Some(Token::Word(w)) => {
                self.pos += 1;
                Ok(w.clone())
            }
            other => bail!("expected a name, found {other:?}"),
        }
    }

    fn keyword(&mut self, keyword: &str) -> Result<()> {
        if self.eat_keyword(keyword) {
            Ok(())
        } else {
            bail!("expected {keyword}, found {:?}", self.tokens.get(self.pos))
        }
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        match self.tokens.get(self.pos) {
            Some(Token::Word(w)) if w.eq_ignore_ascii_case(keyword) => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    fn eat(&mut self, token: &Token) -> bool {
        if self.tokens.get(self.pos) == Some(token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: &Token) -> Result<()> {
        if self.eat(token) {
            Ok(())
        } else {
            bail!("expected {token:?}, found {:?}", self.tokens.get(self.pos))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(v: &str) -> Literal {
        Literal::Text(v.to_string())
    }

    #[test]
    fn parses_a_filtered_select() {
        let s = parse("SELECT order_id,net_cents FROM model_visible_orders WHERE tenant='t1' AND reader='analyst' ORDER BY order_id").unwrap();
        assert_eq!(
            s,
            Statement::Select {
                columns: vec!["order_id".into(), "net_cents".into()],
                relation: "model_visible_orders".into(),
                filter: vec![
                    Eq {
                        column: "tenant".into(),
                        value: text("t1")
                    },
                    Eq {
                        column: "reader".into(),
                        value: text("analyst")
                    },
                ],
            }
        );
    }

    #[test]
    fn parses_mutation_scripts() {
        let s = parse_script(
            "INSERT OR IGNORE INTO edges VALUES('t1','A','O''Brien');\
             DELETE FROM review_seed;\
             UPDATE source_health SET watermark=12,status='ok' WHERE source_id='erp';",
        )
        .unwrap();
        assert_eq!(s.len(), 3);
        assert_eq!(
            s[0],
            Statement::Insert {
                relation: "edges".into(),
                values: vec![text("t1"), text("A"), text("O'Brien")],
            }
        );
        assert!(matches!(&s[1], Statement::Delete { filter, .. } if filter.is_empty()));
        assert!(
            matches!(&s[2], Statement::Update { set, filter, .. } if set.len() == 2 && filter.len() == 1)
        );
    }

    #[test]
    fn numbers_keep_their_type() {
        let s = parse("INSERT INTO t VALUES(-3, 2.5)").unwrap();
        assert_eq!(
            s,
            Statement::Insert {
                relation: "t".into(),
                values: vec![Literal::Int(-3), Literal::Float(2.5)],
            }
        );
        assert_eq!(Literal::Float(1.0).to_string(), "1.0");
        assert_eq!(text("a\"b").to_string(), "\"a\\\"b\"");
    }

    #[test]
    fn rejects_what_it_cannot_translate() {
        for sql in [
            "SELECT COUNT(*) FROM items WHERE tenant='t1'",
            "SELECT account FROM m WHERE account IN ('a','b')",
            "SELECT 'x',EXISTS(SELECT 1 FROM m) FROM m",
            "SELECT parent_revision FROM r GROUP BY parent_revision",
            "INSERT INTO job_state VALUES('a',NULL)",
            "INSERT INTO t SELECT a FROM u",
            "SELECT a FROM t WHERE b > 1",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
    }
}
