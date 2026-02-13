//! Data manipulation statements for `InputLayer`.
//!
//! This module handles insert, delete, and update operations:
//! - `+relation(args).` - single insert
//! - `+relation[(t1), (t2), ...]` - bulk insert
//! - `-relation(args).` - single delete
//! - `-relation(X, Y) <- condition.` - conditional delete
//! - `-old, +new <- condition.` - atomic update

use crate::ast::{Atom, BodyPredicate, Rule, Term};
use crate::parser::parse_rule;

/// Insert operation: +relation(args).
#[derive(Debug, Clone)]
pub struct InsertOp {
    /// Relation name
    pub relation: String,
    /// Tuples to insert (each inner Vec is one tuple's arguments)
    pub tuples: Vec<Vec<Term>>,
}

/// Delete operation: -relation(args). or -relation(X) <- body.
#[derive(Debug, Clone)]
pub struct DeleteOp {
    /// Relation name
    pub relation: String,
    /// Delete pattern
    pub pattern: DeletePattern,
}

/// Pattern for delete operations
#[derive(Debug, Clone)]
pub enum DeletePattern {
    /// Single tuple: -edge(1, 2).
    SingleTuple(Vec<Term>),
    /// Bulk tuples: -edge[(1, 2), (3, 4)].
    BulkTuples(Vec<Vec<Term>>),
    /// Conditional delete: -edge(X, Y) <- condition.
    Conditional {
        /// Variables in the head
        head_args: Vec<Term>,
        /// Body predicates (conditions)
        body: Vec<BodyPredicate>,
    },
}

/// Update operation: -old, +new <- condition. (atomic)
#[derive(Debug, Clone)]
pub struct UpdateOp {
    /// Deletions to perform
    pub deletes: Vec<DeleteTarget>,
    /// Insertions to perform
    pub inserts: Vec<InsertTarget>,
    /// Condition body (what to match)
    pub body: Vec<BodyPredicate>,
}

/// A single delete target in an update
#[derive(Debug, Clone)]
pub struct DeleteTarget {
    pub relation: String,
    pub args: Vec<Term>,
}

/// A single insert target in an update
#[derive(Debug, Clone)]
pub struct InsertTarget {
    pub relation: String,
    pub args: Vec<Term>,
}

// Parsing
use super::parser::{parse_atom_args, parse_single_term, split_by_comma, term_to_string};

/// Parse an insert operation: +relation(args). or +relation[(t1), (t2), ...].
pub fn parse_insert(input: &str) -> Result<InsertOp, String> {
    let input = input.trim();

    // Check for bulk insert: relation[(t1), (t2), ...]
    // Only if [ appears before ( (otherwise [ is inside args like vectors)
    if let Some(bracket_pos) = input.find('[') {
        let paren_before = input.find('(').is_none_or(|p| bracket_pos < p);
        if paren_before {
            let relation = input[..bracket_pos].trim().to_string();
            let tuples_str = &input[bracket_pos..];
            let tuples = parse_bulk_tuples(tuples_str)?;
            return Ok(InsertOp { relation, tuples });
        }
    }

    // Single insert: relation(args)
    if let Some(paren_pos) = input.find('(') {
        let relation = input[..paren_pos].trim().to_string();
        let args_str = input[paren_pos..].trim();
        let args = parse_atom_args(args_str)?;
        return Ok(InsertOp {
            relation,
            tuples: vec![args],
        });
    }

    Err(format!("Invalid insert syntax: +{input}"))
}

/// Parse bulk tuples: [(1,2), (3,4), (5,6)]
fn parse_bulk_tuples(input: &str) -> Result<Vec<Vec<Term>>, String> {
    let input = input.trim();
    if !input.starts_with('[') || !input.ends_with(']') {
        return Err("Bulk insert must be in format: relation[(t1), (t2), ...]".to_string());
    }

    let inner = &input[1..input.len() - 1];
    let mut tuples = Vec::new();
    let mut current = String::new();
    let mut paren_depth: i32 = 0;

    for ch in inner.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                // Clamp to 0 to handle malformed input
                paren_depth = (paren_depth - 1).max(0);
                current.push(ch);
            }
            ',' if paren_depth == 0 => {
                let tuple = parse_tuple(current.trim())?;
                tuples.push(tuple);
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        let tuple = parse_tuple(current.trim())?;
        tuples.push(tuple);
    }

    Ok(tuples)
}

/// Parse a single tuple: (1, 2) or (1, "hello")
fn parse_tuple(input: &str) -> Result<Vec<Term>, String> {
    let input = input.trim();
    if input.starts_with('(') && input.ends_with(')') {
        parse_atom_args(input)
    } else {
        // Single value tuple
        let term = parse_single_term(input)?;
        Ok(vec![term])
    }
}

/// Parse a delete operation
pub fn parse_delete(input: &str) -> Result<DeleteOp, String> {
    let input = input.trim();

    // Check for conditional delete: relation(X, Y) <- condition.
    if input.contains("<-") {
        let parts: Vec<&str> = input.splitn(2, "<-").collect();
        if parts.len() != 2 {
            return Err("Invalid conditional delete syntax".to_string());
        }

        let head_str = parts[0].trim();
        let body_str = parts[1].trim();

        // Parse the head
        let (relation, head_args) = parse_head_atom(head_str)?;

        // Parse the body using the existing parser
        let dummy_rule_str = format!(
            "__dummy__({}) <- {}",
            head_args
                .iter()
                .map(term_to_string)
                .collect::<Vec<_>>()
                .join(", "),
            body_str
        );
        let rule = parse_rule(&dummy_rule_str)?;

        return Ok(DeleteOp {
            relation,
            pattern: DeletePattern::Conditional {
                head_args,
                body: rule.body,
            },
        });
    }

    // Check for bulk delete: relation[(t1), (t2), ...]
    // Only if [ appears before ( (otherwise [ is inside args like vectors)
    if let Some(bracket_pos) = input.find('[') {
        let paren_before = input.find('(').is_none_or(|p| bracket_pos < p);
        if paren_before {
            let relation = input[..bracket_pos].trim().to_string();
            let tuples_str = &input[bracket_pos..];
            let tuples = parse_bulk_tuples(tuples_str)?;
            return Ok(DeleteOp {
                relation,
                pattern: DeletePattern::BulkTuples(tuples),
            });
        }
    }

    // Simple delete: relation(args)
    let (relation, args) = parse_head_atom(input)?;
    Ok(DeleteOp {
        relation,
        pattern: DeletePattern::SingleTuple(args),
    })
}

/// Try to parse an update pattern: -rel(...), +rel(...) <- body.
pub fn try_parse_update(input: &str) -> Result<Option<UpdateOp>, String> {
    // An update has the pattern: -rel1(...), +rel2(...) <- body.
    // It must have both - and + before <-

    if !input.contains("<-") {
        return Ok(None);
    }

    let parts: Vec<&str> = input.splitn(2, "<-").collect();
    if parts.len() != 2 {
        return Ok(None);
    }

    let head_part = parts[0].trim();
    let body_part = parts[1].trim();

    // Split head by comma (outside parentheses)
    let head_items = split_by_comma(head_part);

    let mut deletes = Vec::new();
    let mut inserts = Vec::new();

    for item in head_items {
        let item = item.trim();
        if item.starts_with('-') {
            let (relation, args) = parse_head_atom(&item[1..])?;
            deletes.push(DeleteTarget { relation, args });
        } else if item.starts_with('+') {
            let (relation, args) = parse_head_atom(&item[1..])?;
            inserts.push(InsertTarget { relation, args });
        } else {
            // Not an update pattern
            return Ok(None);
        }
    }

    // Must have at least one delete and one insert
    if deletes.is_empty() || inserts.is_empty() {
        return Ok(None);
    }

    // Parse the body using existing parser
    let dummy_rule_str = format!("__dummy__(X) <- {body_part}");
    let rule = parse_rule(&dummy_rule_str)?;

    Ok(Some(UpdateOp {
        deletes,
        inserts,
        body: rule.body,
    }))
}

/// Parse a head atom and return (relation, args)
fn parse_head_atom(input: &str) -> Result<(String, Vec<Term>), String> {
    let input = input.trim();
    if let Some(paren_pos) = input.find('(') {
        let relation = input[..paren_pos].trim().to_string();
        let args = parse_atom_args(&input[paren_pos..])?;
        Ok((relation, args))
    } else {
        Err(format!("Invalid atom syntax: {input}"))
    }
}

/// Parse a fact: atom. (rule with empty body)
pub fn parse_fact(input: &str) -> Result<Rule, String> {
    let input = input.trim();

    // Parse as an atom and create a rule with empty body
    let head = parse_atom_for_fact(input)?;
    Ok(Rule::new(head, vec![]))
}

/// Parse an atom for a fact (similar to `parse_head_atom` but returns Atom)
fn parse_atom_for_fact(input: &str) -> Result<Atom, String> {
    let input = input.trim();
    if let Some(paren_pos) = input.find('(') {
        let relation = input[..paren_pos].trim().to_string();
        let args = parse_atom_args(&input[paren_pos..])?;
        Ok(Atom::new(relation, args))
    } else {
        Err(format!("Invalid fact syntax: {input}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_insert() {
        let op = parse_insert("edge(1, 2)").unwrap();
        assert_eq!(op.relation, "edge");
        assert_eq!(op.tuples.len(), 1);
        assert_eq!(op.tuples[0].len(), 2);
    }

    #[test]
    fn test_parse_bulk_insert() {
        let op = parse_insert("edge[(1,2), (3,4), (5,6)]").unwrap();
        assert_eq!(op.relation, "edge");
        assert_eq!(op.tuples.len(), 3);
    }

    #[test]
    fn test_parse_insert_with_strings() {
        let op = parse_insert("person(\"alice\", 30)").unwrap();
        assert_eq!(op.relation, "person");
        assert_eq!(op.tuples.len(), 1);
        assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "alice"));
        assert!(matches!(&op.tuples[0][1], Term::Constant(30)));
    }

    #[test]
    fn test_parse_single_delete() {
        let op = parse_delete("edge(1, 2)").unwrap();
        assert_eq!(op.relation, "edge");
        assert!(matches!(op.pattern, DeletePattern::SingleTuple(_)));
    }

    #[test]
    fn test_parse_conditional_delete() {
        let op = parse_delete("edge(X, Y) <- source(X)").unwrap();
        assert_eq!(op.relation, "edge");
        assert!(matches!(op.pattern, DeletePattern::Conditional { .. }));
    }

    #[test]
    fn test_parse_update() {
        let update = try_parse_update("-edge(1, Y), +edge(1, 100) <- edge(1, Y)").unwrap();
        assert!(update.is_some());
        let op = update.unwrap();
        assert_eq!(op.deletes.len(), 1);
        assert_eq!(op.inserts.len(), 1);
        assert_eq!(op.deletes[0].relation, "edge");
        assert_eq!(op.inserts[0].relation, "edge");
    }
}
