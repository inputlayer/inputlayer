//! Statement Parser for Datalog-Native Syntax
//!
//! This module implements the unified Datalog-native syntax for InputLayer:
//! - Meta Commands: `.db`, `.rel`, `.view`, `.save`, `.status`, `.help`, `.quit`
//! - Data Manipulation: `+`/`-` operators (DD-native diff model)
//! - Persistent Views: `:=` operator
//! - Transient Rules: `:-` operator
//! - Queries: `?-` operator

use datalog_ast::{Atom, BodyPredicate, Constraint, Rule, Term};
use serde::{Deserialize, Serialize};

// ============================================================================
// Statement Types
// ============================================================================

/// Top-level statement parsed from user input
#[derive(Debug, Clone)]
pub enum Statement {
    /// Meta commands (dot-prefix): .db, .rel, .view, etc.
    Meta(MetaCommand),
    /// Insert operation: +relation(args). or +relation[(t1), (t2), ...].
    Insert(InsertOp),
    /// Delete operation: -relation(args). or -relation(X) :- condition.
    Delete(DeleteOp),
    /// Update operation: -old, +new :- condition. (atomic)
    Update(UpdateOp),
    /// View definition: head := body. (persistent)
    View(ViewDef),
    /// Transient rule: head :- body. (not persistent)
    TransientRule(Rule),
    /// Query: ?- goal.
    Query(QueryGoal),
    /// Schema definition: Name = schema(col: type constraints, ...)
    SchemaDef(crate::schema::RelationSchema),
    /// Type alias definition: type Name = base_type constraints
    TypeAliasDef(crate::schema::TypeAlias),
}

/// Meta commands for database/relation/view management
#[derive(Debug, Clone, PartialEq)]
pub enum MetaCommand {
    // Database commands
    DbShow,
    DbList,
    DbCreate(String),
    DbUse(String),
    DbDrop(String),

    // Relation commands
    RelList,
    RelDescribe(String),

    // View commands
    ViewList,
    ViewQuery(String),      // .view <name> - query the view and show results
    ViewDef(String),        // .view def <name> - show view definition
    ViewDrop(String),
    ViewEdit {              // .view edit <name> <index> <rule> - edit specific rule
        name: String,
        index: usize,
        rule_text: String,
    },
    ViewClear(String),      // .view clear <name> - clear all rules for re-registration

    // Session commands (transient rules)
    SessionList,            // .session - list session rules
    SessionClear,           // .session clear - clear all session rules
    SessionDrop(usize),     // .session drop <n> - remove rule #n (0-based internally)

    // System commands
    Compact,
    Status,
    Help,
    Quit,
}

/// Insert operation: +relation(args).
#[derive(Debug, Clone)]
pub struct InsertOp {
    /// Relation name
    pub relation: String,
    /// Tuples to insert (each inner Vec is one tuple's arguments)
    pub tuples: Vec<Vec<Term>>,
}

/// Delete operation: -relation(args). or -relation(X) :- body.
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
    /// Conditional delete: -edge(X, Y) :- X > 5.
    Conditional {
        /// Variables in the head
        head_args: Vec<Term>,
        /// Body predicates (conditions)
        body: Vec<BodyPredicate>,
        /// Constraints
        constraints: Vec<Constraint>,
    },
}

/// Update operation: -old, +new :- condition. (atomic)
#[derive(Debug, Clone)]
pub struct UpdateOp {
    /// Deletions to perform
    pub deletes: Vec<DeleteTarget>,
    /// Insertions to perform
    pub inserts: Vec<InsertTarget>,
    /// Condition body (what to match)
    pub body: Vec<BodyPredicate>,
    /// Constraints
    pub constraints: Vec<Constraint>,
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

/// View definition: head := body. (persistent rule)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDef {
    /// View name (head relation)
    pub name: String,
    /// The rule defining this view
    pub rule: SerializableRule,
}

/// A serializable representation of a Rule for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableRule {
    pub head_relation: String,
    pub head_args: Vec<SerializableTerm>,
    pub body: Vec<SerializableBodyPred>,
    pub constraints: Vec<SerializableConstraint>,
}

/// Serializable term for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableTerm {
    Variable(String),
    Constant(i64),
    StringConstant(String),
    FloatConstant(f64),
    Placeholder,
    /// Aggregate function with variable name (e.g., count<X>, sum<Amount>)
    Aggregate(SerializableAggregateFunc, String),
    /// Arithmetic expression (e.g., D+1, X*Y)
    Arithmetic(SerializableArithExpr),
}

/// Serializable arithmetic expression for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableArithExpr {
    Variable(String),
    Constant(i64),
    Binary {
        op: SerializableArithOp,
        left: Box<SerializableArithExpr>,
        right: Box<SerializableArithExpr>,
    },
}

/// Serializable arithmetic operator for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Serializable body predicate for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableBodyPred {
    pub relation: String,
    pub args: Vec<SerializableTerm>,
    pub negated: bool,
}

/// Serializable constraint for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableConstraint {
    Equal(SerializableTerm, SerializableTerm),
    NotEqual(SerializableTerm, SerializableTerm),
    LessThan(SerializableTerm, SerializableTerm),
    LessOrEqual(SerializableTerm, SerializableTerm),
    GreaterThan(SerializableTerm, SerializableTerm),
    GreaterOrEqual(SerializableTerm, SerializableTerm),
}

/// Serializable aggregate function for JSON storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SerializableAggregateFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    TopK { k: usize, order_var: String, descending: bool },
    TopKThreshold { k: usize, order_var: String, threshold: f64, descending: bool },
    WithinRadius { distance_var: String, max_distance: f64 },
}

impl SerializableAggregateFunc {
    /// Convert from datalog_ast::AggregateFunc
    pub fn from_aggregate_func(func: &datalog_ast::AggregateFunc) -> Self {
        use datalog_ast::AggregateFunc;
        match func {
            AggregateFunc::Count => SerializableAggregateFunc::Count,
            AggregateFunc::Sum => SerializableAggregateFunc::Sum,
            AggregateFunc::Min => SerializableAggregateFunc::Min,
            AggregateFunc::Max => SerializableAggregateFunc::Max,
            AggregateFunc::Avg => SerializableAggregateFunc::Avg,
            AggregateFunc::TopK { k, order_var, descending } =>
                SerializableAggregateFunc::TopK { k: *k, order_var: order_var.clone(), descending: *descending },
            AggregateFunc::TopKThreshold { k, order_var, threshold, descending } =>
                SerializableAggregateFunc::TopKThreshold { k: *k, order_var: order_var.clone(), threshold: *threshold, descending: *descending },
            AggregateFunc::WithinRadius { distance_var, max_distance } =>
                SerializableAggregateFunc::WithinRadius { distance_var: distance_var.clone(), max_distance: *max_distance },
        }
    }

    /// Convert to datalog_ast::AggregateFunc
    pub fn to_aggregate_func(&self) -> datalog_ast::AggregateFunc {
        use datalog_ast::AggregateFunc;
        match self {
            SerializableAggregateFunc::Count => AggregateFunc::Count,
            SerializableAggregateFunc::Sum => AggregateFunc::Sum,
            SerializableAggregateFunc::Min => AggregateFunc::Min,
            SerializableAggregateFunc::Max => AggregateFunc::Max,
            SerializableAggregateFunc::Avg => AggregateFunc::Avg,
            SerializableAggregateFunc::TopK { k, order_var, descending } =>
                AggregateFunc::TopK { k: *k, order_var: order_var.clone(), descending: *descending },
            SerializableAggregateFunc::TopKThreshold { k, order_var, threshold, descending } =>
                AggregateFunc::TopKThreshold { k: *k, order_var: order_var.clone(), threshold: *threshold, descending: *descending },
            SerializableAggregateFunc::WithinRadius { distance_var, max_distance } =>
                AggregateFunc::WithinRadius { distance_var: distance_var.clone(), max_distance: *max_distance },
        }
    }
}

/// Query goal: ?- atom.
#[derive(Debug, Clone)]
pub struct QueryGoal {
    /// The goal atom to query
    pub goal: Atom,
    /// Additional body predicates (for complex queries)
    pub body: Vec<BodyPredicate>,
    /// Constraints
    pub constraints: Vec<Constraint>,
}

// ============================================================================
// Conversion Helpers
// ============================================================================

impl SerializableRule {
    /// Convert from datalog_ast::Rule
    pub fn from_rule(rule: &Rule) -> Self {
        SerializableRule {
            head_relation: rule.head.relation.clone(),
            head_args: rule.head.args.iter().map(SerializableTerm::from_term).collect(),
            body: rule.body.iter().map(SerializableBodyPred::from_body_pred).collect(),
            constraints: rule.constraints.iter().map(SerializableConstraint::from_constraint).collect(),
        }
    }

    /// Convert to datalog_ast::Rule
    pub fn to_rule(&self) -> Rule {
        let head = Atom::new(
            self.head_relation.clone(),
            self.head_args.iter().map(|t| t.to_term()).collect(),
        );
        let body = self.body.iter().map(|b| b.to_body_pred()).collect();
        let constraints = self.constraints.iter().map(|c| c.to_constraint()).collect();
        Rule::new(head, body, constraints)
    }
}

impl SerializableTerm {
    pub fn from_term(term: &Term) -> Self {
        match term {
            Term::Variable(name) => SerializableTerm::Variable(name.clone()),
            Term::Constant(val) => SerializableTerm::Constant(*val),
            Term::StringConstant(s) => SerializableTerm::StringConstant(s.clone()),
            Term::FloatConstant(f) => SerializableTerm::FloatConstant(*f),
            Term::Placeholder => SerializableTerm::Placeholder,
            // Handle aggregate terms (e.g., count<X>, sum<Amount>)
            Term::Aggregate(func, var) => SerializableTerm::Aggregate(
                SerializableAggregateFunc::from_aggregate_func(func),
                var.clone(),
            ),
            // Handle arithmetic expressions (e.g., D+1, X*Y)
            Term::Arithmetic(expr) => SerializableTerm::Arithmetic(
                SerializableArithExpr::from_arith_expr(expr),
            ),
            // For other complex terms (FunctionCall, VectorLiteral),
            // we simplify to placeholder as they're not typically used in view definitions
            _ => SerializableTerm::Placeholder,
        }
    }

    pub fn to_term(&self) -> Term {
        match self {
            SerializableTerm::Variable(name) => Term::Variable(name.clone()),
            SerializableTerm::Constant(val) => Term::Constant(*val),
            SerializableTerm::StringConstant(s) => Term::StringConstant(s.clone()),
            SerializableTerm::FloatConstant(f) => Term::FloatConstant(*f),
            SerializableTerm::Placeholder => Term::Placeholder,
            SerializableTerm::Aggregate(func, var) => Term::Aggregate(
                func.to_aggregate_func(),
                var.clone(),
            ),
            SerializableTerm::Arithmetic(expr) => Term::Arithmetic(
                expr.to_arith_expr(),
            ),
        }
    }
}

impl SerializableArithExpr {
    /// Convert from datalog_ast::ArithExpr
    pub fn from_arith_expr(expr: &datalog_ast::ArithExpr) -> Self {
        use datalog_ast::ArithExpr;
        match expr {
            ArithExpr::Variable(name) => SerializableArithExpr::Variable(name.clone()),
            ArithExpr::Constant(val) => SerializableArithExpr::Constant(*val),
            ArithExpr::Binary { op, left, right } => SerializableArithExpr::Binary {
                op: SerializableArithOp::from_arith_op(op),
                left: Box::new(Self::from_arith_expr(left)),
                right: Box::new(Self::from_arith_expr(right)),
            },
        }
    }

    /// Convert to datalog_ast::ArithExpr
    pub fn to_arith_expr(&self) -> datalog_ast::ArithExpr {
        use datalog_ast::ArithExpr;
        match self {
            SerializableArithExpr::Variable(name) => ArithExpr::Variable(name.clone()),
            SerializableArithExpr::Constant(val) => ArithExpr::Constant(*val),
            SerializableArithExpr::Binary { op, left, right } => ArithExpr::Binary {
                op: op.to_arith_op(),
                left: Box::new(left.to_arith_expr()),
                right: Box::new(right.to_arith_expr()),
            },
        }
    }
}

impl SerializableArithOp {
    /// Convert from datalog_ast::ArithOp
    pub fn from_arith_op(op: &datalog_ast::ArithOp) -> Self {
        use datalog_ast::ArithOp;
        match op {
            ArithOp::Add => SerializableArithOp::Add,
            ArithOp::Sub => SerializableArithOp::Sub,
            ArithOp::Mul => SerializableArithOp::Mul,
            ArithOp::Div => SerializableArithOp::Div,
            ArithOp::Mod => SerializableArithOp::Mod,
        }
    }

    /// Convert to datalog_ast::ArithOp
    pub fn to_arith_op(&self) -> datalog_ast::ArithOp {
        use datalog_ast::ArithOp;
        match self {
            SerializableArithOp::Add => ArithOp::Add,
            SerializableArithOp::Sub => ArithOp::Sub,
            SerializableArithOp::Mul => ArithOp::Mul,
            SerializableArithOp::Div => ArithOp::Div,
            SerializableArithOp::Mod => ArithOp::Mod,
        }
    }
}

impl SerializableBodyPred {
    pub fn from_body_pred(pred: &BodyPredicate) -> Self {
        let (atom, negated) = match pred {
            BodyPredicate::Positive(atom) => (atom, false),
            BodyPredicate::Negated(atom) => (atom, true),
        };
        SerializableBodyPred {
            relation: atom.relation.clone(),
            args: atom.args.iter().map(SerializableTerm::from_term).collect(),
            negated,
        }
    }

    pub fn to_body_pred(&self) -> BodyPredicate {
        let atom = Atom::new(
            self.relation.clone(),
            self.args.iter().map(|t| t.to_term()).collect(),
        );
        if self.negated {
            BodyPredicate::Negated(atom)
        } else {
            BodyPredicate::Positive(atom)
        }
    }
}

impl SerializableConstraint {
    pub fn from_constraint(constraint: &Constraint) -> Self {
        match constraint {
            Constraint::Equal(l, r) => SerializableConstraint::Equal(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::NotEqual(l, r) => SerializableConstraint::NotEqual(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::LessThan(l, r) => SerializableConstraint::LessThan(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::LessOrEqual(l, r) => SerializableConstraint::LessOrEqual(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::GreaterThan(l, r) => SerializableConstraint::GreaterThan(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
            Constraint::GreaterOrEqual(l, r) => SerializableConstraint::GreaterOrEqual(
                SerializableTerm::from_term(l),
                SerializableTerm::from_term(r),
            ),
        }
    }

    pub fn to_constraint(&self) -> Constraint {
        match self {
            SerializableConstraint::Equal(l, r) => Constraint::Equal(l.to_term(), r.to_term()),
            SerializableConstraint::NotEqual(l, r) => Constraint::NotEqual(l.to_term(), r.to_term()),
            SerializableConstraint::LessThan(l, r) => Constraint::LessThan(l.to_term(), r.to_term()),
            SerializableConstraint::LessOrEqual(l, r) => Constraint::LessOrEqual(l.to_term(), r.to_term()),
            SerializableConstraint::GreaterThan(l, r) => Constraint::GreaterThan(l.to_term(), r.to_term()),
            SerializableConstraint::GreaterOrEqual(l, r) => Constraint::GreaterOrEqual(l.to_term(), r.to_term()),
        }
    }
}

// ============================================================================
// Statement Parser
// ============================================================================

use crate::parser::parse_rule;

/// Parse a statement from user input
pub fn parse_statement(input: &str) -> Result<Statement, String> {
    let input = input.trim();

    if input.is_empty() {
        return Err("Empty input".to_string());
    }

    // Meta commands start with '.'
    if input.starts_with('.') {
        return parse_meta_command(input).map(Statement::Meta);
    }

    // Type alias: type Name = base_type constraints
    if input.starts_with("type ") {
        return parse_type_alias(input).map(Statement::TypeAliasDef);
    }

    // Check for update pattern: -rel(...), +rel(...) :- body.
    // This must be checked before simple +/- to handle atomic updates
    if (input.starts_with('-') || input.starts_with('+')) && input.contains(":=").not() {
        // Check if this is an update pattern (has both - and + before :-)
        if let Some(update) = try_parse_update(input)? {
            return Ok(Statement::Update(update));
        }
    }

    // Insert: +relation(args). or +relation[(t1), (t2), ...].
    if input.starts_with('+') {
        return parse_insert(&input[1..]).map(Statement::Insert);
    }

    // Delete: -relation(args). or -relation(X) :- condition.
    if input.starts_with('-') {
        return parse_delete(&input[1..]).map(Statement::Delete);
    }

    // Query: ?- goal.
    if input.starts_with("?-") {
        return parse_query(&input[2..]).map(Statement::Query);
    }

    // View definition: head := body.
    if input.contains(":=") {
        return parse_view_definition(input).map(Statement::View);
    }

    // Schema definition: Name = schema(col: type, ...)
    // Must be checked before transient rule since both use identifiers
    if let Some(schema_result) = try_parse_schema_definition(input)? {
        return Ok(Statement::SchemaDef(schema_result));
    }

    // Transient rule: head :- body.
    if input.contains(":-") {
        return parse_transient_rule(input).map(Statement::TransientRule);
    }

    Err(format!("Unrecognized statement: {}", input))
}

/// Parse a meta command
fn parse_meta_command(input: &str) -> Result<MetaCommand, String> {
    let input = input.trim_start_matches('.');
    let parts: Vec<&str> = input.split_whitespace().collect();

    if parts.is_empty() {
        return Err("Empty meta command".to_string());
    }

    match parts[0].to_lowercase().as_str() {
        "db" => {
            if parts.len() == 1 {
                Ok(MetaCommand::DbShow)
            } else {
                match parts[1].to_lowercase().as_str() {
                    "list" => Ok(MetaCommand::DbList),
                    "create" => {
                        if parts.len() < 3 {
                            Err("Usage: .db create <name>".to_string())
                        } else {
                            Ok(MetaCommand::DbCreate(parts[2].to_string()))
                        }
                    }
                    "use" => {
                        if parts.len() < 3 {
                            Err("Usage: .db use <name>".to_string())
                        } else {
                            Ok(MetaCommand::DbUse(parts[2].to_string()))
                        }
                    }
                    "drop" => {
                        if parts.len() < 3 {
                            Err("Usage: .db drop <name>".to_string())
                        } else {
                            Ok(MetaCommand::DbDrop(parts[2].to_string()))
                        }
                    }
                    _ => Err(format!("Unknown db subcommand: {}", parts[1])),
                }
            }
        }
        "rel" | "relation" => {
            if parts.len() == 1 {
                Ok(MetaCommand::RelList)
            } else {
                Ok(MetaCommand::RelDescribe(parts[1].to_string()))
            }
        }
        "view" => {
            if parts.len() == 1 {
                Ok(MetaCommand::ViewList)
            } else if parts[1].to_lowercase() == "drop" {
                if parts.len() < 3 {
                    Err("Usage: .view drop <name>".to_string())
                } else {
                    Ok(MetaCommand::ViewDrop(parts[2].to_string()))
                }
            } else if parts[1].to_lowercase() == "def" {
                if parts.len() < 3 {
                    Err("Usage: .view def <name>".to_string())
                } else {
                    Ok(MetaCommand::ViewDef(parts[2].to_string()))
                }
            } else if parts[1].to_lowercase() == "edit" {
                // .view edit <name> <index> <rule>
                if parts.len() < 5 {
                    Err("Usage: .view edit <name> <index> <rule>\nExample: .view edit connected 2 connected(X, Z) := edge(X, Y), connected(Y, Z).".to_string())
                } else {
                    let name = parts[2].to_string();
                    let index: usize = parts[3].parse()
                        .map_err(|_| format!("Invalid index '{}': must be a number (1-based)", parts[3]))?;
                    if index == 0 {
                        return Err("Index must be 1 or greater (1-based indexing)".to_string());
                    }
                    // The rule is everything after the index
                    let rule_start = input.find(parts[3]).unwrap() + parts[3].len();
                    let rule_text = input[rule_start..].trim().to_string();
                    if rule_text.is_empty() {
                        return Err("Missing rule definition".to_string());
                    }
                    Ok(MetaCommand::ViewEdit { name, index: index - 1, rule_text }) // Convert to 0-based
                }
            } else if parts[1].to_lowercase() == "clear" {
                // .view clear <name> - clear all rules
                if parts.len() < 3 {
                    Err("Usage: .view clear <name>".to_string())
                } else {
                    Ok(MetaCommand::ViewClear(parts[2].to_string()))
                }
            } else {
                // .view <name> - query the view and show computed results
                Ok(MetaCommand::ViewQuery(parts[1].to_string()))
            }
        }
        "session" | "rules" => {
            if parts.len() == 1 {
                Ok(MetaCommand::SessionList)
            } else {
                match parts[1].to_lowercase().as_str() {
                    "clear" => Ok(MetaCommand::SessionClear),
                    "drop" => {
                        if parts.len() < 3 {
                            Err("Usage: .session drop <n>".to_string())
                        } else {
                            let index: usize = parts[2].parse()
                                .map_err(|_| format!("Invalid index '{}': must be a number (1-based)", parts[2]))?;
                            if index == 0 {
                                return Err("Index must be 1 or greater (1-based indexing)".to_string());
                            }
                            Ok(MetaCommand::SessionDrop(index - 1))  // Convert to 0-based
                        }
                    }
                    _ => Err(format!("Unknown session subcommand: {}. Use: clear, drop <n>", parts[1]))
                }
            }
        }
        "compact" => Ok(MetaCommand::Compact),
        "status" => Ok(MetaCommand::Status),
        "help" | "?" => Ok(MetaCommand::Help),
        "quit" | "exit" | "q" => Ok(MetaCommand::Quit),
        _ => Err(format!("Unknown meta command: .{}", parts[0])),
    }
}

/// Parse an insert operation: +relation(args). or +relation[(t1), (t2), ...].
fn parse_insert(input: &str) -> Result<InsertOp, String> {
    let input = input.trim().trim_end_matches('.');

    // Check for bulk insert: relation[(t1), (t2), ...]
    if let Some(bracket_pos) = input.find('[') {
        let relation = input[..bracket_pos].trim().to_string();
        let tuples_str = &input[bracket_pos..];
        let tuples = parse_bulk_tuples(tuples_str)?;
        return Ok(InsertOp { relation, tuples });
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

    Err(format!("Invalid insert syntax: +{}", input))
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
    let mut paren_depth = 0;

    for ch in inner.chars() {
        match ch {
            '(' => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' => {
                paren_depth -= 1;
                current.push(ch);
            }
            ',' if paren_depth == 0 => {
                let tuple = parse_tuple(&current.trim())?;
                tuples.push(tuple);
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        let tuple = parse_tuple(&current.trim())?;
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

/// Parse atom arguments: (arg1, arg2, ...)
fn parse_atom_args(input: &str) -> Result<Vec<Term>, String> {
    let input = input.trim();
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(format!("Expected parentheses: {}", input));
    }

    let inner = &input[1..input.len() - 1];
    if inner.trim().is_empty() {
        return Ok(vec![]);
    }

    let parts = split_by_comma(inner);
    parts.into_iter().map(|p| parse_single_term(p.trim())).collect()
}

/// Parse a single term
fn parse_single_term(input: &str) -> Result<Term, String> {
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

    // Check if valid identifier (alphanumeric + underscore)
    if input.chars().all(|c| c.is_alphanumeric() || c == '_') && !input.is_empty() {
        let first_char = input.chars().next().unwrap();

        // Variable: starts with uppercase letter or underscore
        // Examples: X, Y, Foo, _temp, _
        if first_char.is_uppercase() || first_char == '_' {
            return Ok(Term::Variable(input.to_string()));
        }

        // Atom: starts with lowercase letter
        // Examples: tom, liz, edge, parent
        // Atoms are represented as StringConstant for compatibility
        if first_char.is_lowercase() {
            return Ok(Term::StringConstant(input.to_string()));
        }
    }

    Err(format!("Invalid term: '{}'", input))
}

/// Parse a vector literal like [1.0, 2.0, 3.0]
fn parse_vector_literal(input: &str) -> Result<Term, String> {
    let inner = input[1..input.len()-1].trim();
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

/// Split by comma, respecting parentheses and square brackets
fn split_by_comma(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut bracket_depth = 0;  // Track square brackets for vectors
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
                paren_depth -= 1;
                current.push(ch);
            }
            '[' if !in_string => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !in_string => {
                bracket_depth -= 1;
                current.push(ch);
            }
            ',' if paren_depth == 0 && bracket_depth == 0 && !in_string => {
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

/// Parse a delete operation
fn parse_delete(input: &str) -> Result<DeleteOp, String> {
    let input = input.trim().trim_end_matches('.');

    // Check for conditional delete: relation(X, Y) :- condition.
    if input.contains(":-") {
        let parts: Vec<&str> = input.splitn(2, ":-").collect();
        if parts.len() != 2 {
            return Err("Invalid conditional delete syntax".to_string());
        }

        let head_str = parts[0].trim();
        let body_str = parts[1].trim();

        // Parse the head
        let (relation, head_args) = parse_head_atom(head_str)?;

        // Parse the body using the existing parser
        let dummy_rule_str = format!("__dummy__({}) :- {}",
            head_args.iter().map(term_to_string).collect::<Vec<_>>().join(", "),
            body_str
        );
        let rule = parse_rule(&dummy_rule_str)?;

        return Ok(DeleteOp {
            relation,
            pattern: DeletePattern::Conditional {
                head_args,
                body: rule.body,
                constraints: rule.constraints,
            },
        });
    }

    // Simple delete: relation(args)
    let (relation, args) = parse_head_atom(input)?;
    Ok(DeleteOp {
        relation,
        pattern: DeletePattern::SingleTuple(args),
    })
}

/// Parse a head atom and return (relation, args)
fn parse_head_atom(input: &str) -> Result<(String, Vec<Term>), String> {
    let input = input.trim();
    if let Some(paren_pos) = input.find('(') {
        let relation = input[..paren_pos].trim().to_string();
        let args = parse_atom_args(&input[paren_pos..])?;
        Ok((relation, args))
    } else {
        Err(format!("Invalid atom syntax: {}", input))
    }
}

/// Convert term to string for rule reconstruction
fn term_to_string(term: &Term) -> String {
    match term {
        Term::Variable(name) => name.clone(),
        Term::Constant(val) => val.to_string(),
        Term::StringConstant(s) => format!("\"{}\"", s),
        Term::FloatConstant(f) => f.to_string(),
        Term::Placeholder => "_".to_string(),
        _ => "_".to_string(),
    }
}

/// Try to parse an update pattern: -rel(...), +rel(...) :- body.
fn try_parse_update(input: &str) -> Result<Option<UpdateOp>, String> {
    // An update has the pattern: -rel1(...), +rel2(...) :- body.
    // It must have both - and + before :-

    if !input.contains(":-") {
        return Ok(None);
    }

    let parts: Vec<&str> = input.splitn(2, ":-").collect();
    if parts.len() != 2 {
        return Ok(None);
    }

    let head_part = parts[0].trim();
    let body_part = parts[1].trim().trim_end_matches('.');

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
    let dummy_rule_str = format!("__dummy__(X) :- {}", body_part);
    let rule = parse_rule(&dummy_rule_str)?;

    Ok(Some(UpdateOp {
        deletes,
        inserts,
        body: rule.body,
        constraints: rule.constraints,
    }))
}

/// Parse a view definition: head := body.
pub fn parse_view_definition(input: &str) -> Result<ViewDef, String> {
    let input = input.trim().trim_end_matches('.');

    let parts: Vec<&str> = input.splitn(2, ":=").collect();
    if parts.len() != 2 {
        return Err("Invalid view definition syntax".to_string());
    }

    let head_str = parts[0].trim();
    let body_str = parts[1].trim();

    // Convert := to :- for parsing as a rule
    let rule_str = format!("{} :- {}.", head_str, body_str);
    let rule = parse_rule(&rule_str)?;

    Ok(ViewDef {
        name: rule.head.relation.clone(),
        rule: SerializableRule::from_rule(&rule),
    })
}

/// Parse a transient rule: head :- body.
fn parse_transient_rule(input: &str) -> Result<Rule, String> {
    parse_rule(input.trim())
}

/// Parse a query: ?- goal.
fn parse_query(input: &str) -> Result<QueryGoal, String> {
    let input = input.trim().trim_end_matches('.');

    // Simple query: just an atom
    // Complex query: atom with constraints

    // Try to parse as a simple rule body
    let dummy_rule_str = format!("__query__(X) :- {}.", input);
    let rule = parse_rule(&dummy_rule_str)?;

    if rule.body.is_empty() {
        return Err("Query must have at least one goal".to_string());
    }

    // The first positive atom is the main goal
    let goal = rule.body.iter()
        .filter_map(|p| match p {
            BodyPredicate::Positive(atom) => Some(atom.clone()),
            _ => None,
        })
        .next()
        .ok_or_else(|| "Query must have at least one positive goal".to_string())?;

    // Remaining body predicates (excluding the first goal)
    let body: Vec<BodyPredicate> = rule.body.into_iter().skip(1).collect();

    Ok(QueryGoal {
        goal,
        body,
        constraints: rule.constraints,
    })
}

// ============================================================================
// Schema and Type Alias Parsing
// ============================================================================

use crate::schema::{
    RelationSchema, ColumnSchema, SchemaType, ColumnAnnotation, TypeAlias,
};

/// Parse a type alias: `type Email = string pattern("^[^@]+@[^@]+$")`
fn parse_type_alias(input: &str) -> Result<TypeAlias, String> {
    // Remove "type " prefix and trailing period
    let input = input.trim_start_matches("type ").trim().trim_end_matches('.');

    // Split on '=' to get name and definition
    let eq_pos = input.find('=')
        .ok_or("Type alias must contain '='")?;

    let name = input[..eq_pos].trim();
    let definition = input[eq_pos + 1..].trim();

    // Validate name (must start with uppercase)
    if name.is_empty() {
        return Err("Type alias name cannot be empty".to_string());
    }
    if !name.chars().next().unwrap().is_uppercase() {
        return Err(format!("Type alias name '{}' must start with uppercase letter", name));
    }

    // Parse the definition: base_type followed by optional constraints
    let tokens = tokenize_type_def(definition);
    if tokens.is_empty() {
        return Err("Type alias definition cannot be empty".to_string());
    }

    // First token is the base type
    let base_type = SchemaType::from_str(&tokens[0])
        .ok_or_else(|| format!("Unknown base type: '{}'", tokens[0]))?;

    // Remaining tokens are constraints
    let annotations = parse_annotations(&tokens[1..])?;

    let mut alias = TypeAlias::new(name, base_type);
    alias.annotations = annotations;

    Ok(alias)
}

/// Try to parse a schema definition: `Name = schema(col: type, ...)`
/// Returns None if input doesn't match schema definition pattern
fn try_parse_schema_definition(input: &str) -> Result<Option<RelationSchema>, String> {
    let input = input.trim().trim_end_matches('.');

    // Must contain '=' but not ':=' or ':-'
    if !input.contains('=') || input.contains(":=") || input.contains(":-") {
        return Ok(None);
    }

    // Split on first '='
    let eq_pos = match input.find('=') {
        Some(p) => p,
        None => return Ok(None),
    };

    let name = input[..eq_pos].trim();
    let definition = input[eq_pos + 1..].trim();

    // Check if definition starts with 'schema('
    if !definition.starts_with("schema(") {
        return Ok(None);
    }

    // Validate name (must be valid identifier, start with uppercase)
    if name.is_empty() {
        return Err("Schema name cannot be empty".to_string());
    }
    if !name.chars().next().unwrap().is_uppercase() {
        return Err(format!("Schema name '{}' must start with uppercase letter", name));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(format!("Invalid schema name: '{}'", name));
    }

    // Extract columns from schema(...)
    let content = definition.strip_prefix("schema(")
        .and_then(|s| s.strip_suffix(')'))
        .ok_or("Invalid schema syntax: expected schema(...)")?;

    let columns = parse_schema_columns(content)?;

    let mut schema = RelationSchema::new(name);
    for col in columns {
        schema = schema.with_column(col);
    }

    Ok(Some(schema))
}

/// Parse schema columns from the inside of schema(...)
/// Format: `col1: type1 constraint1, col2: type2 constraint2, ...`
fn parse_schema_columns(content: &str) -> Result<Vec<ColumnSchema>, String> {
    let mut columns = Vec::new();

    // Split by comma (but respect nested parentheses)
    let parts = split_schema_columns(content);

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Split on ':' to get name and type+constraints
        let colon_pos = part.find(':')
            .ok_or_else(|| format!("Invalid column definition '{}': expected 'name: type'", part))?;

        let col_name = part[..colon_pos].trim();
        let type_and_constraints = part[colon_pos + 1..].trim();

        // Validate column name
        if col_name.is_empty() {
            return Err("Column name cannot be empty".to_string());
        }
        if !col_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!("Invalid column name: '{}'", col_name));
        }

        // Tokenize type and constraints
        let tokens = tokenize_type_def(type_and_constraints);
        if tokens.is_empty() {
            return Err(format!("Column '{}' is missing type", col_name));
        }

        // First token is the type
        let data_type = SchemaType::from_str(&tokens[0])
            .ok_or_else(|| format!("Unknown type '{}' for column '{}'", tokens[0], col_name))?;

        // Remaining tokens are annotations
        let annotations = parse_annotations(&tokens[1..])?;

        let mut col = ColumnSchema::new(col_name, data_type);
        col.annotations = annotations;
        columns.push(col);
    }

    if columns.is_empty() {
        return Err("Schema must have at least one column".to_string());
    }

    Ok(columns)
}

/// Split schema column definitions, respecting nested parentheses
fn split_schema_columns(content: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut in_string = false;

    for ch in content.chars() {
        match ch {
            '"' if !in_string => {
                in_string = true;
                current.push(ch);
            }
            '"' if in_string => {
                in_string = false;
                current.push(ch);
            }
            '(' if !in_string => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_string => {
                paren_depth -= 1;
                current.push(ch);
            }
            ',' if paren_depth == 0 && !in_string => {
                result.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        result.push(current);
    }

    result
}

/// Tokenize a type definition into type and constraint tokens
/// E.g., "string pattern(\"^[^@]+@[^@]+$\") not_empty" -> ["string", "pattern(\"^[^@]+@[^@]+$\")", "not_empty"]
fn tokenize_type_def(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
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
                paren_depth -= 1;
                current.push(ch);
            }
            ' ' | '\t' | '\n' if paren_depth == 0 && !in_string => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

/// Parse annotation tokens into ColumnAnnotation values
fn parse_annotations(tokens: &[String]) -> Result<Vec<ColumnAnnotation>, String> {
    let mut annotations = Vec::new();

    for token in tokens {
        let ann = parse_single_annotation(token)?;
        annotations.push(ann);
    }

    Ok(annotations)
}

/// Parse a single annotation token
fn parse_single_annotation(token: &str) -> Result<ColumnAnnotation, String> {
    let token = token.trim();

    // Simple annotations (no arguments)
    match token.to_lowercase().as_str() {
        "primary" => return Ok(ColumnAnnotation::Primary),
        "not_empty" | "notempty" | "required" => return Ok(ColumnAnnotation::NotEmpty),
        "unique" => return Ok(ColumnAnnotation::Unique),
        _ => {}
    }

    // Annotations with arguments: name(args)
    if let Some(paren_pos) = token.find('(') {
        let name = token[..paren_pos].to_lowercase();
        let args = token[paren_pos + 1..].trim_end_matches(')');

        match name.as_str() {
            "range" => {
                // range(min, max)
                let parts: Vec<&str> = args.split(',').collect();
                if parts.len() != 2 {
                    return Err(format!("range requires two arguments: range(min, max), got: {}", args));
                }
                let min: i64 = parts[0].trim().parse()
                    .map_err(|_| format!("Invalid range min value: {}", parts[0].trim()))?;
                let max: i64 = parts[1].trim().parse()
                    .map_err(|_| format!("Invalid range max value: {}", parts[1].trim()))?;
                return Ok(ColumnAnnotation::Range { min, max });
            }
            "pattern" => {
                // pattern("regex")
                let regex = args.trim().trim_matches('"').to_string();
                if regex.is_empty() {
                    return Err("pattern requires a non-empty regex".to_string());
                }
                return Ok(ColumnAnnotation::Pattern { regex });
            }
            "references" | "foreign_key" | "fk" => {
                // references(Relation.column) or references(Relation, column)
                let parts: Vec<&str> = if args.contains('.') {
                    args.split('.').collect()
                } else {
                    args.split(',').collect()
                };
                if parts.len() != 2 {
                    return Err(format!("references requires relation.column or (relation, column): {}", args));
                }
                return Ok(ColumnAnnotation::ForeignKey {
                    relation: parts[0].trim().to_string(),
                    column: parts[1].trim().to_string(),
                });
            }
            "default" => {
                // default(value)
                let value = parse_default_value(args)?;
                return Ok(ColumnAnnotation::Default { value });
            }
            _ => {}
        }
    }

    Err(format!("Unknown annotation: '{}'", token))
}

/// Parse a default value from string
fn parse_default_value(s: &str) -> Result<crate::value::Value, String> {
    let s = s.trim();

    // String value
    if s.starts_with('"') && s.ends_with('"') {
        return Ok(crate::value::Value::string(&s[1..s.len()-1]));
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return Ok(crate::value::Value::Int64(n));
    }

    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Ok(crate::value::Value::Float64(f));
    }

    // Boolean
    match s.to_lowercase().as_str() {
        "true" => return Ok(crate::value::Value::Bool(true)),
        "false" => return Ok(crate::value::Value::Bool(false)),
        _ => {}
    }

    // Atom (lowercase identifier)
    if s.chars().next().map_or(false, |c| c.is_lowercase())
       && s.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Ok(crate::value::Value::string(s));
    }

    Err(format!("Cannot parse default value: '{}'", s))
}

// Trait for bool::not() in older Rust
trait BoolNot {
    fn not(&self) -> bool;
}

impl BoolNot for bool {
    fn not(&self) -> bool {
        !*self
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Meta command tests
    #[test]
    fn test_parse_db_show() {
        let stmt = parse_statement(".db").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::DbShow)));
    }

    #[test]
    fn test_parse_db_list() {
        let stmt = parse_statement(".db list").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::DbList)));
    }

    #[test]
    fn test_parse_db_create() {
        let stmt = parse_statement(".db create test").unwrap();
        if let Statement::Meta(MetaCommand::DbCreate(name)) = stmt {
            assert_eq!(name, "test");
        } else {
            panic!("Expected DbCreate");
        }
    }

    #[test]
    fn test_parse_db_use() {
        let stmt = parse_statement(".db use mydb").unwrap();
        if let Statement::Meta(MetaCommand::DbUse(name)) = stmt {
            assert_eq!(name, "mydb");
        } else {
            panic!("Expected DbUse");
        }
    }

    #[test]
    fn test_parse_rel_list() {
        let stmt = parse_statement(".rel").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::RelList)));
    }

    #[test]
    fn test_parse_rel_describe() {
        let stmt = parse_statement(".rel edge").unwrap();
        if let Statement::Meta(MetaCommand::RelDescribe(name)) = stmt {
            assert_eq!(name, "edge");
        } else {
            panic!("Expected RelDescribe");
        }
    }

    #[test]
    fn test_parse_view_list() {
        let stmt = parse_statement(".view").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::ViewList)));
    }

    #[test]
    fn test_parse_view_query() {
        let stmt = parse_statement(".view path").unwrap();
        if let Statement::Meta(MetaCommand::ViewQuery(name)) = stmt {
            assert_eq!(name, "path");
        } else {
            panic!("Expected ViewQuery");
        }
    }

    #[test]
    fn test_parse_view_def() {
        let stmt = parse_statement(".view def path").unwrap();
        if let Statement::Meta(MetaCommand::ViewDef(name)) = stmt {
            assert_eq!(name, "path");
        } else {
            panic!("Expected ViewDef");
        }
    }

    #[test]
    fn test_parse_view_drop() {
        let stmt = parse_statement(".view drop path").unwrap();
        if let Statement::Meta(MetaCommand::ViewDrop(name)) = stmt {
            assert_eq!(name, "path");
        } else {
            panic!("Expected ViewDrop");
        }
    }

    #[test]
    fn test_parse_compact() {
        let stmt = parse_statement(".compact").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::Compact)));
    }

    #[test]
    fn test_parse_status() {
        let stmt = parse_statement(".status").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::Status)));
    }

    #[test]
    fn test_parse_help() {
        let stmt = parse_statement(".help").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::Help)));
    }

    #[test]
    fn test_parse_quit() {
        let stmt = parse_statement(".quit").unwrap();
        assert!(matches!(stmt, Statement::Meta(MetaCommand::Quit)));

        let stmt2 = parse_statement(".exit").unwrap();
        assert!(matches!(stmt2, Statement::Meta(MetaCommand::Quit)));
    }

    // Insert tests
    #[test]
    fn test_parse_single_insert() {
        let stmt = parse_statement("+edge(1, 2).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert_eq!(op.tuples.len(), 1);
            assert_eq!(op.tuples[0].len(), 2);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_bulk_insert() {
        let stmt = parse_statement("+edge[(1,2), (3,4), (5,6)].").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert_eq!(op.tuples.len(), 3);
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_parse_insert_with_strings() {
        let stmt = parse_statement("+person(\"alice\", 30).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert_eq!(op.relation, "person");
            assert_eq!(op.tuples.len(), 1);
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "alice"));
            assert!(matches!(&op.tuples[0][1], Term::Constant(30)));
        } else {
            panic!("Expected Insert");
        }
    }

    // Delete tests
    #[test]
    fn test_parse_single_delete() {
        let stmt = parse_statement("-edge(1, 2).").unwrap();
        if let Statement::Delete(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert!(matches!(op.pattern, DeletePattern::SingleTuple(_)));
        } else {
            panic!("Expected Delete");
        }
    }

    #[test]
    fn test_parse_conditional_delete() {
        let stmt = parse_statement("-edge(X, Y) :- X > 5.").unwrap();
        if let Statement::Delete(op) = stmt {
            assert_eq!(op.relation, "edge");
            assert!(matches!(op.pattern, DeletePattern::Conditional { .. }));
        } else {
            panic!("Expected Delete");
        }
    }

    // View tests
    #[test]
    fn test_parse_view_definition() {
        let stmt = parse_statement("path(X, Y) := edge(X, Y).").unwrap();
        if let Statement::View(def) = stmt {
            assert_eq!(def.name, "path");
        } else {
            panic!("Expected View");
        }
    }

    #[test]
    fn test_parse_recursive_view() {
        let stmt = parse_statement("path(X, Z) := edge(X, Y), path(Y, Z).").unwrap();
        if let Statement::View(def) = stmt {
            assert_eq!(def.name, "path");
            assert_eq!(def.rule.body.len(), 2);
        } else {
            panic!("Expected View");
        }
    }

    // Query tests
    #[test]
    fn test_parse_simple_query() {
        let stmt = parse_statement("?- path(1, X).").unwrap();
        if let Statement::Query(q) = stmt {
            assert_eq!(q.goal.relation, "path");
        } else {
            panic!("Expected Query");
        }
    }

    // Transient rule tests
    #[test]
    fn test_parse_transient_rule() {
        let stmt = parse_statement("result(X, Y) :- edge(X, Y), X < Y.").unwrap();
        if let Statement::TransientRule(rule) = stmt {
            assert_eq!(rule.head.relation, "result");
        } else {
            panic!("Expected TransientRule");
        }
    }

    // Update tests
    #[test]
    fn test_parse_update() {
        let stmt = parse_statement("-edge(1, Y), +edge(1, 100) :- edge(1, Y).").unwrap();
        if let Statement::Update(op) = stmt {
            assert_eq!(op.deletes.len(), 1);
            assert_eq!(op.inserts.len(), 1);
            assert_eq!(op.deletes[0].relation, "edge");
            assert_eq!(op.inserts[0].relation, "edge");
        } else {
            panic!("Expected Update, got {:?}", stmt);
        }
    }

    // Serialization tests
    #[test]
    fn test_serializable_rule_roundtrip() {
        let rule_str = "path(X, Y) :- edge(X, Y).";
        let rule = parse_rule(rule_str).unwrap();
        let serializable = SerializableRule::from_rule(&rule);
        let json = serde_json::to_string(&serializable).unwrap();
        let deserialized: SerializableRule = serde_json::from_str(&json).unwrap();
        let restored = deserialized.to_rule();
        assert_eq!(restored.head.relation, "path");
    }

    // =========================================================================
    // Atom vs Variable Parsing Tests (Phase 3)
    // =========================================================================

    #[test]
    fn test_uppercase_is_variable() {
        // Uppercase identifiers are variables
        let stmt = parse_statement("+edge(X, Y).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::Variable(v) if v == "X"));
            assert!(matches!(&op.tuples[0][1], Term::Variable(v) if v == "Y"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_lowercase_is_atom() {
        // Lowercase identifiers are atoms (StringConstant)
        let stmt = parse_statement("+parent(tom, liz).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "tom"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "liz"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_mixed_atoms_and_variables() {
        // Mix of atoms and variables: parent(tom, X)
        let stmt = parse_statement("?- parent(tom, X).").unwrap();
        if let Statement::Query(q) = stmt {
            assert_eq!(q.goal.relation, "parent");
            assert!(matches!(&q.goal.args[0], Term::StringConstant(s) if s == "tom"));
            assert!(matches!(&q.goal.args[1], Term::Variable(v) if v == "X"));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_underscore_prefix_is_variable() {
        // Underscore-prefixed identifiers are variables
        let stmt = parse_statement("result(X) :- edge(_from, X).").unwrap();
        if let Statement::TransientRule(rule) = stmt {
            let body_atom = match &rule.body[0] {
                BodyPredicate::Positive(atom) => atom,
                _ => panic!("Expected positive atom"),
            };
            assert!(matches!(&body_atom.args[0], Term::Variable(v) if v == "_from"));
            assert!(matches!(&body_atom.args[1], Term::Variable(v) if v == "X"));
        } else {
            panic!("Expected TransientRule");
        }
    }

    #[test]
    fn test_placeholder_underscore() {
        // Single underscore is placeholder
        let stmt = parse_statement("?- edge(_, X).").unwrap();
        if let Statement::Query(q) = stmt {
            assert!(matches!(&q.goal.args[0], Term::Placeholder));
            assert!(matches!(&q.goal.args[1], Term::Variable(v) if v == "X"));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_multichar_variables() {
        // Multi-character uppercase-starting identifiers are variables
        let stmt = parse_statement("result(Foo, BarBaz) :- data(Foo, BarBaz).").unwrap();
        if let Statement::TransientRule(rule) = stmt {
            assert!(matches!(&rule.head.args[0], Term::Variable(v) if v == "Foo"));
            assert!(matches!(&rule.head.args[1], Term::Variable(v) if v == "BarBaz"));
        } else {
            panic!("Expected TransientRule");
        }
    }

    #[test]
    fn test_multichar_atoms() {
        // Multi-character lowercase-starting identifiers are atoms
        let stmt = parse_statement("+likes(alice, bob).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "alice"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "bob"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_atoms_with_numbers() {
        // Atoms can contain numbers (but must start with lowercase)
        let stmt = parse_statement("+data(item1, item2).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "item1"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "item2"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_variables_with_underscores() {
        // Variables can contain underscores
        let stmt = parse_statement("result(X_val, Y_val) :- data(X_val, Y_val).").unwrap();
        if let Statement::TransientRule(rule) = stmt {
            assert!(matches!(&rule.head.args[0], Term::Variable(v) if v == "X_val"));
            assert!(matches!(&rule.head.args[1], Term::Variable(v) if v == "Y_val"));
        } else {
            panic!("Expected TransientRule");
        }
    }

    #[test]
    fn test_atoms_with_underscores() {
        // Atoms can contain underscores (but must start with lowercase)
        let stmt = parse_statement("+data(my_item, other_thing).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "my_item"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "other_thing"));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_view_with_atoms() {
        // Views can reference atoms in body
        let stmt = parse_statement("child(X) := parent(mary, X).").unwrap();
        if let Statement::View(def) = stmt {
            assert_eq!(def.name, "child");
            // The body should have 'mary' as an atom
            let rule = def.rule.to_rule();
            let body_atom = match &rule.body[0] {
                BodyPredicate::Positive(atom) => atom,
                _ => panic!("Expected positive atom"),
            };
            assert!(matches!(&body_atom.args[0], Term::StringConstant(s) if s == "mary"));
        } else {
            panic!("Expected View");
        }
    }

    #[test]
    fn test_query_all_atoms() {
        // Query with all atoms (boolean query)
        let stmt = parse_statement("?- parent(tom, liz).").unwrap();
        if let Statement::Query(q) = stmt {
            assert!(matches!(&q.goal.args[0], Term::StringConstant(s) if s == "tom"));
            assert!(matches!(&q.goal.args[1], Term::StringConstant(s) if s == "liz"));
        } else {
            panic!("Expected Query");
        }
    }

    #[test]
    fn test_integers_still_work() {
        // Integers should still be parsed as constants
        let stmt = parse_statement("+edge(1, 2).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::Constant(1)));
            assert!(matches!(&op.tuples[0][1], Term::Constant(2)));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_floats_still_work() {
        // Floats should still be parsed as float constants
        let stmt = parse_statement("+data(3.14, 2.71).").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::FloatConstant(f) if (*f - 3.14).abs() < 0.001));
            assert!(matches!(&op.tuples[0][1], Term::FloatConstant(f) if (*f - 2.71).abs() < 0.001));
        } else {
            panic!("Expected Insert");
        }
    }

    #[test]
    fn test_quoted_strings_still_work() {
        // Quoted strings should still work
        let stmt = parse_statement("+data(\"hello world\", \"test\").").unwrap();
        if let Statement::Insert(op) = stmt {
            assert!(matches!(&op.tuples[0][0], Term::StringConstant(s) if s == "hello world"));
            assert!(matches!(&op.tuples[0][1], Term::StringConstant(s) if s == "test"));
        } else {
            panic!("Expected Insert");
        }
    }
}
