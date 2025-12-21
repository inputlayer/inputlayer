//! # Datalog AST - Abstract Syntax Tree Types
//!
//! Abstract Syntax Tree types for Datalog programs.
//! Used across multiple modules for parsing and semantic analysis.
//!
//! ## Builders
//!
//! For programmatic construction of AST nodes, see the [`builders`] module
//! which provides fluent APIs like `AtomBuilder` and `RuleBuilder`.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub mod builders;

// ============================================================================
// Core AST Types
// ============================================================================

/// Aggregation function types for Datalog
///
/// Note: Does not implement Hash or Eq because TopKThreshold and WithinRadius
/// contain f64 fields which don't implement these traits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// Top-K aggregate: select top k results ordered by a variable
    /// Syntax: top_k<10, score> or top_k<10, score, desc>
    TopK {
        k: usize,
        order_var: String,
        descending: bool,
    },
    /// Top-K with threshold: only return results if score meets threshold
    /// Syntax: top_k_threshold<10, score, 0.5> or top_k_threshold<10, score, 0.5, desc>
    TopKThreshold {
        k: usize,
        order_var: String,
        threshold: f64,
        descending: bool,
    },
    /// Within radius: all results within a distance threshold (range query)
    /// Syntax: within_radius<dist, 0.5>
    WithinRadius {
        distance_var: String,
        max_distance: f64,
    },
}

/// Built-in function for vector/scalar operations
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BuiltinFunc {
    // Distance functions
    /// Euclidean distance: euclidean(v1, v2) -> Float64
    Euclidean,
    /// Cosine distance: cosine(v1, v2) -> Float64 (1 - similarity)
    Cosine,
    /// Dot product: dot(v1, v2) -> Float64
    DotProduct,
    /// Manhattan distance: manhattan(v1, v2) -> Float64
    Manhattan,

    // LSH functions
    /// LSH bucket: lsh_bucket(v, table_idx, num_hyperplanes) -> Int64
    /// num_hyperplanes controls precision vs recall tradeoff
    LshBucket,

    // Vector operations
    /// Normalize vector: normalize(v) -> Vector
    VecNormalize,
    /// Get vector dimension: vec_dim(v) -> Int64
    VecDim,
    /// Add vectors: vec_add(v1, v2) -> Vector
    VecAdd,
    /// Scale vector: vec_scale(v, scalar) -> Vector
    VecScale,

    // Temporal functions
    /// Get current time: time_now() -> Timestamp (Unix milliseconds)
    TimeNow,
    /// Time difference: time_diff(t1, t2) -> Int64 (milliseconds)
    TimeDiff,
    /// Add duration to timestamp: time_add(ts, duration_ms) -> Timestamp
    TimeAdd,
    /// Subtract duration from timestamp: time_sub(ts, duration_ms) -> Timestamp
    TimeSub,
    /// Exponential time decay: time_decay(ts, now, half_life_ms) -> Float64 [0,1]
    TimeDecay,
    /// Linear time decay: time_decay_linear(ts, now, max_age_ms) -> Float64 [0,1]
    TimeDecayLinear,
    /// Check if t1 < t2: time_before(t1, t2) -> Bool
    TimeBefore,
    /// Check if t1 > t2: time_after(t1, t2) -> Bool
    TimeAfter,
    /// Check if ts in [start, end]: time_between(ts, start, end) -> Bool
    TimeBetween,
    /// Check if ts is within duration of now: within_last(ts, now, duration_ms) -> Bool
    WithinLast,
    /// Check if intervals overlap: intervals_overlap(s1, e1, s2, e2) -> Bool
    IntervalsOverlap,
    /// Check if interval 1 contains interval 2: interval_contains(s1, e1, s2, e2) -> Bool
    IntervalContains,
    /// Get interval duration: interval_duration(start, end) -> Int64
    IntervalDuration,
    /// Check if point is in interval: point_in_interval(ts, start, end) -> Bool
    PointInInterval,
}

impl BuiltinFunc {
    /// Parse a built-in function name
    pub fn parse(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "euclidean" => Some(BuiltinFunc::Euclidean),
            "cosine" => Some(BuiltinFunc::Cosine),
            "dot" => Some(BuiltinFunc::DotProduct),
            "manhattan" => Some(BuiltinFunc::Manhattan),
            "lsh_bucket" => Some(BuiltinFunc::LshBucket),
            "normalize" => Some(BuiltinFunc::VecNormalize),
            "vec_dim" => Some(BuiltinFunc::VecDim),
            "vec_add" => Some(BuiltinFunc::VecAdd),
            "vec_scale" => Some(BuiltinFunc::VecScale),
            // Temporal functions
            "time_now" => Some(BuiltinFunc::TimeNow),
            "time_diff" => Some(BuiltinFunc::TimeDiff),
            "time_add" => Some(BuiltinFunc::TimeAdd),
            "time_sub" => Some(BuiltinFunc::TimeSub),
            "time_decay" => Some(BuiltinFunc::TimeDecay),
            "time_decay_linear" => Some(BuiltinFunc::TimeDecayLinear),
            "time_before" => Some(BuiltinFunc::TimeBefore),
            "time_after" => Some(BuiltinFunc::TimeAfter),
            "time_between" => Some(BuiltinFunc::TimeBetween),
            "within_last" => Some(BuiltinFunc::WithinLast),
            "intervals_overlap" => Some(BuiltinFunc::IntervalsOverlap),
            "interval_contains" => Some(BuiltinFunc::IntervalContains),
            "interval_duration" => Some(BuiltinFunc::IntervalDuration),
            "point_in_interval" => Some(BuiltinFunc::PointInInterval),
            _ => None,
        }
    }

    /// Get the expected number of arguments
    pub fn arity(&self) -> usize {
        match self {
            BuiltinFunc::Euclidean
            | BuiltinFunc::Cosine
            | BuiltinFunc::DotProduct
            | BuiltinFunc::Manhattan
            | BuiltinFunc::VecAdd
            | BuiltinFunc::VecScale => 2,
            BuiltinFunc::LshBucket => 3, // (vector, table_idx, num_hyperplanes)
            BuiltinFunc::VecNormalize | BuiltinFunc::VecDim => 1,
            // Temporal functions
            BuiltinFunc::TimeNow => 0,
            BuiltinFunc::TimeDiff
            | BuiltinFunc::TimeAdd
            | BuiltinFunc::TimeSub
            | BuiltinFunc::TimeBefore
            | BuiltinFunc::TimeAfter
            | BuiltinFunc::IntervalDuration => 2,
            BuiltinFunc::TimeDecay
            | BuiltinFunc::TimeDecayLinear
            | BuiltinFunc::TimeBetween
            | BuiltinFunc::WithinLast
            | BuiltinFunc::PointInInterval => 3,
            BuiltinFunc::IntervalsOverlap | BuiltinFunc::IntervalContains => 4,
        }
    }

    /// Get the string representation of the function name
    pub fn as_str(&self) -> &'static str {
        match self {
            BuiltinFunc::Euclidean => "euclidean",
            BuiltinFunc::Cosine => "cosine",
            BuiltinFunc::DotProduct => "dot",
            BuiltinFunc::Manhattan => "manhattan",
            BuiltinFunc::LshBucket => "lsh_bucket",
            BuiltinFunc::VecNormalize => "normalize",
            BuiltinFunc::VecDim => "vec_dim",
            BuiltinFunc::VecAdd => "vec_add",
            BuiltinFunc::VecScale => "vec_scale",
            // Temporal functions
            BuiltinFunc::TimeNow => "time_now",
            BuiltinFunc::TimeDiff => "time_diff",
            BuiltinFunc::TimeAdd => "time_add",
            BuiltinFunc::TimeSub => "time_sub",
            BuiltinFunc::TimeDecay => "time_decay",
            BuiltinFunc::TimeDecayLinear => "time_decay_linear",
            BuiltinFunc::TimeBefore => "time_before",
            BuiltinFunc::TimeAfter => "time_after",
            BuiltinFunc::TimeBetween => "time_between",
            BuiltinFunc::WithinLast => "within_last",
            BuiltinFunc::IntervalsOverlap => "intervals_overlap",
            BuiltinFunc::IntervalContains => "interval_contains",
            BuiltinFunc::IntervalDuration => "interval_duration",
            BuiltinFunc::PointInInterval => "point_in_interval",
        }
    }
}

/// Arithmetic operators for expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
    Div,
    /// Modulo (%)
    Mod,
}

impl ArithOp {
    /// Parse an arithmetic operator from a string
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "+" => Some(ArithOp::Add),
            "-" => Some(ArithOp::Sub),
            "*" => Some(ArithOp::Mul),
            "/" => Some(ArithOp::Div),
            "%" => Some(ArithOp::Mod),
            _ => None,
        }
    }

    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Mod => "%",
        }
    }
}

/// Arithmetic expression tree
///
/// Represents arithmetic expressions like `d + 1` or `x * y + z`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ArithExpr {
    /// A variable reference
    Variable(String),
    /// A constant value
    Constant(i64),
    /// Binary operation
    Binary {
        op: ArithOp,
        left: Box<ArithExpr>,
        right: Box<ArithExpr>,
    },
}

impl ArithExpr {
    /// Get all variables referenced in this expression
    pub fn variables(&self) -> std::collections::HashSet<String> {
        let mut vars = std::collections::HashSet::new();
        self.collect_variables(&mut vars);
        vars
    }

    fn collect_variables(&self, vars: &mut std::collections::HashSet<String>) {
        match self {
            ArithExpr::Variable(name) => {
                vars.insert(name.clone());
            }
            ArithExpr::Constant(_) => {}
            ArithExpr::Binary { left, right, .. } => {
                left.collect_variables(vars);
                right.collect_variables(vars);
            }
        }
    }

    /// Check if this is a simple variable or constant
    pub fn is_simple(&self) -> bool {
        matches!(self, ArithExpr::Variable(_) | ArithExpr::Constant(_))
    }

    /// Try to evaluate as a constant if all values are known
    pub fn try_eval_constant(&self) -> Option<i64> {
        match self {
            ArithExpr::Constant(v) => Some(*v),
            ArithExpr::Variable(_) => None,
            ArithExpr::Binary { op, left, right } => {
                let l = left.try_eval_constant()?;
                let r = right.try_eval_constant()?;
                Some(match op {
                    ArithOp::Add => l + r,
                    ArithOp::Sub => l - r,
                    ArithOp::Mul => l * r,
                    ArithOp::Div => {
                        if r == 0 {
                            return None;
                        }
                        l / r
                    }
                    ArithOp::Mod => {
                        if r == 0 {
                            return None;
                        }
                        l % r
                    }
                })
            }
        }
    }
}

impl AggregateFunc {
    /// Parse an aggregate function name (for simple aggregates like count, sum, etc.)
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "count" => Some(AggregateFunc::Count),
            "sum" => Some(AggregateFunc::Sum),
            "min" => Some(AggregateFunc::Min),
            "max" => Some(AggregateFunc::Max),
            "avg" => Some(AggregateFunc::Avg),
            _ => None,
        }
    }

    /// Parse top_k with parameters: top_k<10, score> or top_k<10, score, desc>
    pub fn parse_top_k(params: &str) -> Option<Self> {
        let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
        if parts.len() < 2 {
            return None;
        }

        let k: usize = parts[0].parse().ok()?;
        let order_var = parts[1].to_string();
        let descending = parts.get(2).map(|s| s.to_lowercase() == "desc").unwrap_or(false);

        Some(AggregateFunc::TopK { k, order_var, descending })
    }

    /// Parse top_k_threshold with parameters: top_k_threshold<10, score, 0.5> or top_k_threshold<10, score, 0.5, desc>
    pub fn parse_top_k_threshold(params: &str) -> Option<Self> {
        let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
        if parts.len() < 3 {
            return None;
        }

        let k: usize = parts[0].parse().ok()?;
        let order_var = parts[1].to_string();
        let threshold: f64 = parts[2].parse().ok()?;
        let descending = parts.get(3).map(|s| s.to_lowercase() == "desc").unwrap_or(false);

        Some(AggregateFunc::TopKThreshold { k, order_var, threshold, descending })
    }

    /// Parse within_radius with parameters: within_radius<dist, 0.5>
    pub fn parse_within_radius(params: &str) -> Option<Self> {
        let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
        if parts.len() < 2 {
            return None;
        }

        let distance_var = parts[0].to_string();
        let max_distance: f64 = parts[1].parse().ok()?;

        Some(AggregateFunc::WithinRadius { distance_var, max_distance })
    }

    /// Check if this is a ranking aggregate (affects output cardinality)
    pub fn is_ranking(&self) -> bool {
        matches!(
            self,
            AggregateFunc::TopK { .. }
                | AggregateFunc::TopKThreshold { .. }
                | AggregateFunc::WithinRadius { .. }
        )
    }
}

/// Represents a variable or constant in Datalog
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Variable(String), // e.g., "x", "y", "z"
    Constant(i64),    // e.g., 42, 100
    Placeholder,      // For parser - represents "_" in Datalog
    /// Aggregation term: `count<x>`, `sum<y>`, `min<z>`, `max<z>`, `avg<z>`
    Aggregate(AggregateFunc, String), // (function, variable_name)
    /// Arithmetic expression term: `d + 1`, `x * y`, etc.
    Arithmetic(ArithExpr),
    /// Function call term: `euclidean(v1, v2)`, `normalize(v)`, etc.
    FunctionCall(BuiltinFunc, Vec<Term>),
    /// Vector literal: `[1.0, 2.0, 3.0]`
    VectorLiteral(Vec<f64>),
    /// Float constant for function arguments
    FloatConstant(f64),
    /// String constant
    StringConstant(String),
    /// Field access on a record variable: `U.id`, `P.amount`
    FieldAccess(Box<Term>, String),
    /// Record pattern for destructuring in atom arguments: `{ id: x, name: y }`
    RecordPattern(Vec<(String, Term)>),
}

impl Term {
    /// Check if this term is a variable
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Check if this term is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Check if this term is an aggregate
    pub fn is_aggregate(&self) -> bool {
        matches!(self, Term::Aggregate(_, _))
    }

    /// Check if this term is an arithmetic expression
    pub fn is_arithmetic(&self) -> bool {
        matches!(self, Term::Arithmetic(_))
    }

    /// Check if this term is a function call
    pub fn is_function_call(&self) -> bool {
        matches!(self, Term::FunctionCall(_, _))
    }

    /// Check if this term is a vector literal
    pub fn is_vector_literal(&self) -> bool {
        matches!(self, Term::VectorLiteral(_))
    }

    /// Check if this term is a float constant
    pub fn is_float_constant(&self) -> bool {
        matches!(self, Term::FloatConstant(_))
    }

    /// Get variable name if this is a variable
    pub fn as_variable(&self) -> Option<&str> {
        if let Term::Variable(name) = self {
            Some(name)
        } else {
            None
        }
    }

    /// Get aggregate info if this is an aggregate term
    pub fn as_aggregate(&self) -> Option<(&AggregateFunc, &str)> {
        if let Term::Aggregate(func, var) = self {
            Some((func, var))
        } else {
            None
        }
    }

    /// Get arithmetic expression if this is an arithmetic term
    pub fn as_arithmetic(&self) -> Option<&ArithExpr> {
        if let Term::Arithmetic(expr) = self {
            Some(expr)
        } else {
            None
        }
    }

    /// Get function call info if this is a function call term
    pub fn as_function_call(&self) -> Option<(&BuiltinFunc, &[Term])> {
        if let Term::FunctionCall(func, args) = self {
            Some((func, args))
        } else {
            None
        }
    }

    /// Get vector literal if this is a vector literal term
    pub fn as_vector_literal(&self) -> Option<&[f64]> {
        if let Term::VectorLiteral(values) = self {
            Some(values)
        } else {
            None
        }
    }

    /// Get float constant if this is a float constant term
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Term::FloatConstant(v) => Some(*v),
            Term::Constant(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Get all variables referenced by this term
    pub fn variables(&self) -> std::collections::HashSet<String> {
        match self {
            Term::Variable(name) => {
                let mut set = std::collections::HashSet::new();
                set.insert(name.clone());
                set
            }
            Term::Aggregate(func, var) => {
                let mut set = std::collections::HashSet::new();
                // For standard aggregates, just the var
                // For TopK variants, also include the order_var
                match func {
                    AggregateFunc::TopK { order_var, .. }
                    | AggregateFunc::TopKThreshold { order_var, .. } => {
                        set.insert(order_var.clone());
                    }
                    AggregateFunc::WithinRadius { distance_var, .. } => {
                        set.insert(distance_var.clone());
                    }
                    _ => {}
                }
                if !var.is_empty() {
                    set.insert(var.clone());
                }
                set
            }
            Term::Arithmetic(expr) => expr.variables(),
            Term::FunctionCall(_, args) => {
                args.iter().flat_map(|t| t.variables()).collect()
            }
            _ => std::collections::HashSet::new(),
        }
    }
}

/// Represents an atom like edge(x, y) or reach(x)
#[derive(Debug, Clone, PartialEq)]
pub struct Atom {
    pub relation: String,
    pub args: Vec<Term>,
}

impl Atom {
    /// Create a new atom
    pub fn new(relation: String, args: Vec<Term>) -> Self {
        Atom { relation, args }
    }

    /// Get all variables in this atom (including variables inside aggregates and arithmetic)
    pub fn variables(&self) -> HashSet<String> {
        let mut vars = HashSet::new();
        for term in &self.args {
            vars.extend(term.variables());
        }
        vars
    }

    /// Check if this atom contains any aggregate terms
    pub fn has_aggregates(&self) -> bool {
        self.args.iter().any(Term::is_aggregate)
    }

    /// Check if this atom contains any arithmetic expressions
    pub fn has_arithmetic(&self) -> bool {
        self.args.iter().any(Term::is_arithmetic)
    }

    /// Get all aggregate terms in this atom
    pub fn aggregates(&self) -> Vec<(&AggregateFunc, &str)> {
        self.args.iter().filter_map(|t| t.as_aggregate()).collect()
    }

    /// Get all arithmetic expressions in this atom
    pub fn arithmetic_terms(&self) -> Vec<(usize, &ArithExpr)> {
        self.args
            .iter()
            .enumerate()
            .filter_map(|(i, t)| t.as_arithmetic().map(|e| (i, e)))
            .collect()
    }

    /// Check if this atom contains any function calls
    pub fn has_function_calls(&self) -> bool {
        self.args.iter().any(Term::is_function_call)
    }

    /// Get all function call terms in this atom
    pub fn function_calls(&self) -> Vec<(usize, &BuiltinFunc, &[Term])> {
        self.args
            .iter()
            .enumerate()
            .filter_map(|(i, t)| t.as_function_call().map(|(f, a)| (i, f, a)))
            .collect()
    }

    /// Check if this atom contains any vector literals
    pub fn has_vector_literals(&self) -> bool {
        self.args.iter().any(Term::is_vector_literal)
    }

    /// Get the arity (number of arguments) of this atom
    pub fn arity(&self) -> usize {
        self.args.len()
    }
}

/// Represents a comparison constraint (x != y, x < 10, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    NotEqual(Term, Term),
    LessThan(Term, Term),
    LessOrEqual(Term, Term),
    GreaterThan(Term, Term),
    GreaterOrEqual(Term, Term),
    Equal(Term, Term), // For completeness
}

impl Constraint {
    /// Get all variables in this constraint
    pub fn variables(&self) -> HashSet<String> {
        let (left, right) = match self {
            Constraint::NotEqual(l, r)
            | Constraint::LessThan(l, r)
            | Constraint::LessOrEqual(l, r)
            | Constraint::GreaterThan(l, r)
            | Constraint::GreaterOrEqual(l, r)
            | Constraint::Equal(l, r) => (l, r),
        };

        let mut vars = HashSet::new();
        if let Term::Variable(name) = left {
            vars.insert(name.clone());
        }
        if let Term::Variable(name) = right {
            vars.insert(name.clone());
        }
        vars
    }
}

/// Represents a body predicate (positive or negated atom)
/// Used in rule bodies to support stratified negation
#[derive(Debug, Clone, PartialEq)]
pub enum BodyPredicate {
    Positive(Atom),
    Negated(Atom),
}

impl BodyPredicate {
    /// Get the underlying atom
    pub fn atom(&self) -> &Atom {
        match self {
            BodyPredicate::Positive(atom) | BodyPredicate::Negated(atom) => atom,
        }
    }

    /// Check if this is a positive atom
    pub fn is_positive(&self) -> bool {
        matches!(self, BodyPredicate::Positive(_))
    }

    /// Check if this is a negated atom
    pub fn is_negated(&self) -> bool {
        matches!(self, BodyPredicate::Negated(_))
    }

    /// Get all variables in this predicate
    pub fn variables(&self) -> HashSet<String> {
        self.atom().variables()
    }
}

/// Represents a single Datalog rule
#[derive(Debug, Clone)]
pub struct Rule {
    pub head: Atom,
    pub body: Vec<BodyPredicate>,
    pub constraints: Vec<Constraint>,
}

impl Rule {
    /// Create a new rule
    pub fn new(head: Atom, body: Vec<BodyPredicate>, constraints: Vec<Constraint>) -> Self {
        Rule {
            head,
            body,
            constraints,
        }
    }

    /// Create a rule with only positive body atoms (no negation)
    pub fn new_simple(head: Atom, body: Vec<Atom>, constraints: Vec<Constraint>) -> Self {
        Rule {
            head,
            body: body.into_iter().map(BodyPredicate::Positive).collect(),
            constraints,
        }
    }

    /// Check if this rule is safe (all head variables appear in positive body atoms or are bound by function calls)
    pub fn is_safe(&self) -> bool {
        let head_vars = self.head.variables();
        let mut safe_vars = self.positive_body_variables();

        // Also include variables bound by function calls in constraints
        // e.g., Dist = euclidean(V, Q) binds Dist
        safe_vars.extend(self.function_bound_variables());

        head_vars.is_subset(&safe_vars)
    }

    /// Get variables that are bound by function calls in constraints
    /// e.g., Dist = euclidean(V, Q) binds the variable Dist
    pub fn function_bound_variables(&self) -> HashSet<String> {
        let mut bound_vars = HashSet::new();

        for constraint in &self.constraints {
            if let Constraint::Equal(Term::Variable(var), Term::FunctionCall(_, _)) = constraint {
                bound_vars.insert(var.clone());
            }
            // Also handle the reverse: FunctionCall = Variable
            if let Constraint::Equal(Term::FunctionCall(_, _), Term::Variable(var)) = constraint {
                bound_vars.insert(var.clone());
            }
        }

        bound_vars
    }

    /// Get all variables in positive body atoms
    pub fn positive_body_variables(&self) -> HashSet<String> {
        self.body
            .iter()
            .filter(|pred| pred.is_positive())
            .flat_map(BodyPredicate::variables)
            .collect()
    }

    /// Get all variables in this rule
    pub fn variables(&self) -> HashSet<String> {
        let mut vars = self.head.variables();

        for pred in &self.body {
            vars.extend(pred.variables());
        }

        for constraint in &self.constraints {
            vars.extend(constraint.variables());
        }

        vars
    }

    /// Check if this rule is recursive (head relation appears in body)
    pub fn is_recursive(&self) -> bool {
        self.body
            .iter()
            .any(|pred| pred.atom().relation == self.head.relation)
    }

    /// Get all positive body atoms
    pub fn positive_body_atoms(&self) -> Vec<&Atom> {
        self.body
            .iter()
            .filter_map(|pred| match pred {
                BodyPredicate::Positive(atom) => Some(atom),
                BodyPredicate::Negated(_) => None,
            })
            .collect()
    }

    /// Get all negated body atoms
    pub fn negated_body_atoms(&self) -> Vec<&Atom> {
        self.body
            .iter()
            .filter_map(|pred| match pred {
                BodyPredicate::Negated(atom) => Some(atom),
                BodyPredicate::Positive(_) => None,
            })
            .collect()
    }
}

/// Represents a complete Datalog program
#[derive(Debug, Clone)]
pub struct Program {
    pub rules: Vec<Rule>,
}

impl Program {
    /// Create a new empty program
    pub fn new() -> Self {
        Program { rules: Vec::new() }
    }

    /// Add a rule to the program
    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    /// Returns all IDB relations (those that appear as heads of rules)
    pub fn idbs(&self) -> Vec<String> {
        let mut idbs: Vec<String> = self
            .rules
            .iter()
            .map(|rule| rule.head.relation.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        idbs.sort();
        idbs
    }

    /// Returns all EDB relations (those that appear in bodies but never as heads)
    pub fn edbs(&self) -> Vec<String> {
        let idb_set: HashSet<String> = self.idbs().into_iter().collect();

        let mut body_relations: HashSet<String> = HashSet::new();
        for rule in &self.rules {
            for pred in &rule.body {
                body_relations.insert(pred.atom().relation.clone());
            }
        }

        let mut edbs: Vec<String> = body_relations.difference(&idb_set).cloned().collect();

        edbs.sort();
        edbs
    }

    /// Get all relation names (both EDB and IDB)
    pub fn all_relations(&self) -> Vec<String> {
        let mut all: HashSet<String> = HashSet::new();

        // Add IDBs
        for idb in self.idbs() {
            all.insert(idb);
        }

        // Add EDBs
        for edb in self.edbs() {
            all.insert(edb);
        }

        let mut result: Vec<String> = all.into_iter().collect();
        result.sort();
        result
    }

    /// Check if all rules in the program are safe
    pub fn is_safe(&self) -> bool {
        self.rules.iter().all(Rule::is_safe)
    }

    /// Get all recursive rules
    pub fn recursive_rules(&self) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|rule| rule.is_recursive())
            .collect()
    }

    /// Get all non-recursive rules
    pub fn non_recursive_rules(&self) -> Vec<&Rule> {
        self.rules
            .iter()
            .filter(|rule| !rule.is_recursive())
            .collect()
    }
}

impl Default for Program {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_func_parse() {
        assert_eq!(AggregateFunc::parse("count"), Some(AggregateFunc::Count));
        assert_eq!(AggregateFunc::parse("sum"), Some(AggregateFunc::Sum));
        assert_eq!(AggregateFunc::parse("min"), Some(AggregateFunc::Min));
        assert_eq!(AggregateFunc::parse("max"), Some(AggregateFunc::Max));
        assert_eq!(AggregateFunc::parse("avg"), Some(AggregateFunc::Avg));
    }

    #[test]
    fn test_term_is_variable() {
        assert!(Term::Variable("x".to_string()).is_variable());
        assert!(!Term::Constant(42).is_variable());
    }

    #[test]
    fn test_atom_creation() {
        let atom = Atom::new(
            "edge".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Variable("y".to_string()),
            ],
        );

        assert_eq!(atom.relation, "edge");
        assert_eq!(atom.arity(), 2);
    }

    #[test]
    fn test_rule_safety() {
        let head = Atom::new("reach".to_string(), vec![Term::Variable("y".to_string())]);
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "reach".to_string(),
                vec![Term::Variable("x".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )),
        ];

        let rule = Rule::new(head, body, vec![]);
        assert!(rule.is_safe());
        assert!(rule.is_recursive());
    }

    #[test]
    fn test_program_edbs_idbs() {
        let mut program = Program::new();

        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("y".to_string())]),
            vec![
                Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
            ],
            vec![],
        ));

        let idbs = program.idbs();
        let edbs = program.edbs();

        assert_eq!(idbs, vec!["reach"]);
        assert_eq!(edbs, vec!["edge", "source"]);
    }
}
