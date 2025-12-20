//! # Datalog AST - Shared Library
//!
//! Abstract Syntax Tree types for Datalog programs.
//! Used across multiple modules (M01, M04, M05) for consistency.

use std::collections::HashSet;

// ============================================================================
// Core AST Types
// ============================================================================

/// Aggregation function types for Datalog
///
/// Note: Does not implement Hash or Eq because TopKThreshold and WithinRadius
/// contain f64 fields which don't implement these traits.
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
///
/// ## Examples
///
/// ```
/// use datalog_ast::{ArithExpr, ArithOp};
///
/// // Simple: d + 1
/// let expr = ArithExpr::Binary {
///     op: ArithOp::Add,
///     left: Box::new(ArithExpr::Variable("d".to_string())),
///     right: Box::new(ArithExpr::Constant(1)),
/// };
/// ```
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
///
/// # Examples
/// ```
/// use datalog_ast::Term;
///
/// let var = Term::Variable("x".to_string());
/// let const_val = Term::Constant(42);
/// let placeholder = Term::Placeholder;  // For parser
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Variable(String), // e.g., "x", "y", "z"
    Constant(i64),    // e.g., 42, 100
    Placeholder,      // For parser - represents "_" in Datalog
    /// Aggregation term: `count<x>`, `sum<y>`, `min<z>`, `max<z>`, `avg<z>`
    Aggregate(AggregateFunc, String), // (function, variable_name)
    /// Arithmetic expression term: `d + 1`, `x * y`, etc.
    ///
    /// Used in head atoms for computed columns:
    /// ```text
    /// dist(y, d+1) :- dist(x, d), edge(x, y).
    /// ```
    Arithmetic(ArithExpr),
    /// Function call term: `euclidean(v1, v2)`, `normalize(v)`, etc.
    ///
    /// Used for vector operations in body and head:
    /// ```text
    /// nearest(id, dist) :- vectors(id, v), query(q), dist = euclidean(v, q).
    /// ```
    FunctionCall(BuiltinFunc, Vec<Term>),
    /// Vector literal: `[1.0, 2.0, 3.0]`
    ///
    /// Used for inline vector values:
    /// ```text
    /// query([1.0, 2.0, 3.0]).
    /// ```
    VectorLiteral(Vec<f64>),
    /// Float constant for function arguments
    FloatConstant(f64),
    /// String constant
    StringConstant(String),
    /// Field access on a record variable: `U.id`, `P.amount`
    ///
    /// Used for accessing fields of record-typed variables:
    /// ```text
    /// high_spender(id) :- user(U: User), U.id > 1000.
    /// ```
    FieldAccess(Box<Term>, String),
    /// Record pattern for destructuring in atom arguments: `{ id: x, name: y }`
    ///
    /// Used for pattern matching on record types:
    /// ```text
    /// user({ id: x, name: _, email: e }).
    /// ```
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
///
/// # Examples
/// ```text
/// reach(y) :- reach(x), edge(x, y).
/// ```
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // AggregateFunc Tests
    // ========================================================================

    #[test]
    fn test_aggregate_func_parse() {
        assert_eq!(AggregateFunc::parse("count"), Some(AggregateFunc::Count));
        assert_eq!(AggregateFunc::parse("sum"), Some(AggregateFunc::Sum));
        assert_eq!(AggregateFunc::parse("min"), Some(AggregateFunc::Min));
        assert_eq!(AggregateFunc::parse("max"), Some(AggregateFunc::Max));
        assert_eq!(AggregateFunc::parse("avg"), Some(AggregateFunc::Avg));
    }

    #[test]
    fn test_aggregate_func_case_insensitive() {
        assert_eq!(AggregateFunc::parse("COUNT"), Some(AggregateFunc::Count));
        assert_eq!(AggregateFunc::parse("SUM"), Some(AggregateFunc::Sum));
        assert_eq!(AggregateFunc::parse("Min"), Some(AggregateFunc::Min));
        assert_eq!(AggregateFunc::parse("MaX"), Some(AggregateFunc::Max));
        assert_eq!(AggregateFunc::parse("AVG"), Some(AggregateFunc::Avg));
    }

    #[test]
    fn test_aggregate_func_invalid() {
        assert_eq!(AggregateFunc::parse("invalid"), None);
        assert_eq!(AggregateFunc::parse(""), None);
        assert_eq!(AggregateFunc::parse("mean"), None);
    }

    #[test]
    fn test_aggregate_func_traits() {
        let func = AggregateFunc::Count;
        let cloned = func.clone();
        assert_eq!(func, cloned);

        // Note: AggregateFunc no longer implements Hash due to f64 fields
        // Test equality instead
        let func2 = AggregateFunc::Count;
        let func3 = AggregateFunc::Sum;
        assert_eq!(func, func2);
        assert_ne!(func, func3);
    }

    // ========================================================================
    // ArithOp Tests
    // ========================================================================

    #[test]
    fn test_arith_op_parse() {
        assert_eq!(ArithOp::parse("+"), Some(ArithOp::Add));
        assert_eq!(ArithOp::parse("-"), Some(ArithOp::Sub));
        assert_eq!(ArithOp::parse("*"), Some(ArithOp::Mul));
        assert_eq!(ArithOp::parse("/"), Some(ArithOp::Div));
        assert_eq!(ArithOp::parse("%"), Some(ArithOp::Mod));
    }

    #[test]
    fn test_arith_op_parse_invalid() {
        assert_eq!(ArithOp::parse("^"), None);
        assert_eq!(ArithOp::parse(""), None);
        assert_eq!(ArithOp::parse("++"), None);
        assert_eq!(ArithOp::parse("add"), None);
    }

    #[test]
    fn test_arith_op_as_str() {
        assert_eq!(ArithOp::Add.as_str(), "+");
        assert_eq!(ArithOp::Sub.as_str(), "-");
        assert_eq!(ArithOp::Mul.as_str(), "*");
        assert_eq!(ArithOp::Div.as_str(), "/");
        assert_eq!(ArithOp::Mod.as_str(), "%");
    }

    #[test]
    fn test_arith_op_roundtrip() {
        for op in [
            ArithOp::Add,
            ArithOp::Sub,
            ArithOp::Mul,
            ArithOp::Div,
            ArithOp::Mod,
        ] {
            let s = op.as_str();
            assert_eq!(ArithOp::parse(s), Some(op));
        }
    }

    #[test]
    fn test_arith_op_traits() {
        let op = ArithOp::Add;
        let copied = op;
        assert_eq!(op, copied);

        let mut set = HashSet::new();
        set.insert(ArithOp::Add);
        set.insert(ArithOp::Sub);
        assert_eq!(set.len(), 2);
    }

    // ========================================================================
    // ArithExpr Tests
    // ========================================================================

    #[test]
    fn test_arith_expr_variable() {
        let expr = ArithExpr::Variable("x".to_string());
        assert!(expr.is_simple());

        let vars = expr.variables();
        assert_eq!(vars.len(), 1);
        assert!(vars.contains("x"));
    }

    #[test]
    fn test_arith_expr_constant() {
        let expr = ArithExpr::Constant(42);
        assert!(expr.is_simple());

        let vars = expr.variables();
        assert!(vars.is_empty());
    }

    #[test]
    fn test_arith_expr_binary() {
        // d + 1
        let expr = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("d".to_string())),
            right: Box::new(ArithExpr::Constant(1)),
        };

        assert!(!expr.is_simple());

        let vars = expr.variables();
        assert_eq!(vars.len(), 1);
        assert!(vars.contains("d"));
    }

    #[test]
    fn test_arith_expr_nested_binary() {
        // (x + y) * z
        let expr = ArithExpr::Binary {
            op: ArithOp::Mul,
            left: Box::new(ArithExpr::Binary {
                op: ArithOp::Add,
                left: Box::new(ArithExpr::Variable("x".to_string())),
                right: Box::new(ArithExpr::Variable("y".to_string())),
            }),
            right: Box::new(ArithExpr::Variable("z".to_string())),
        };

        let vars = expr.variables();
        assert_eq!(vars.len(), 3);
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
        assert!(vars.contains("z"));
    }

    #[test]
    fn test_arith_expr_try_eval_constant() {
        // Constant evaluation
        let const_expr = ArithExpr::Constant(42);
        assert_eq!(const_expr.try_eval_constant(), Some(42));

        // Variable cannot be evaluated
        let var_expr = ArithExpr::Variable("x".to_string());
        assert_eq!(var_expr.try_eval_constant(), None);

        // 10 + 5 = 15
        let add_expr = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Constant(10)),
            right: Box::new(ArithExpr::Constant(5)),
        };
        assert_eq!(add_expr.try_eval_constant(), Some(15));

        // 10 - 3 = 7
        let sub_expr = ArithExpr::Binary {
            op: ArithOp::Sub,
            left: Box::new(ArithExpr::Constant(10)),
            right: Box::new(ArithExpr::Constant(3)),
        };
        assert_eq!(sub_expr.try_eval_constant(), Some(7));

        // 6 * 7 = 42
        let mul_expr = ArithExpr::Binary {
            op: ArithOp::Mul,
            left: Box::new(ArithExpr::Constant(6)),
            right: Box::new(ArithExpr::Constant(7)),
        };
        assert_eq!(mul_expr.try_eval_constant(), Some(42));

        // 20 / 4 = 5
        let div_expr = ArithExpr::Binary {
            op: ArithOp::Div,
            left: Box::new(ArithExpr::Constant(20)),
            right: Box::new(ArithExpr::Constant(4)),
        };
        assert_eq!(div_expr.try_eval_constant(), Some(5));

        // 17 % 5 = 2
        let mod_expr = ArithExpr::Binary {
            op: ArithOp::Mod,
            left: Box::new(ArithExpr::Constant(17)),
            right: Box::new(ArithExpr::Constant(5)),
        };
        assert_eq!(mod_expr.try_eval_constant(), Some(2));
    }

    #[test]
    fn test_arith_expr_division_by_zero() {
        let div_by_zero = ArithExpr::Binary {
            op: ArithOp::Div,
            left: Box::new(ArithExpr::Constant(10)),
            right: Box::new(ArithExpr::Constant(0)),
        };
        assert_eq!(div_by_zero.try_eval_constant(), None);

        let mod_by_zero = ArithExpr::Binary {
            op: ArithOp::Mod,
            left: Box::new(ArithExpr::Constant(10)),
            right: Box::new(ArithExpr::Constant(0)),
        };
        assert_eq!(mod_by_zero.try_eval_constant(), None);
    }

    #[test]
    fn test_arith_expr_mixed_eval() {
        // x + 5 cannot be evaluated (has variable)
        let mixed = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("x".to_string())),
            right: Box::new(ArithExpr::Constant(5)),
        };
        assert_eq!(mixed.try_eval_constant(), None);
    }

    #[test]
    fn test_arith_expr_traits() {
        let expr1 = ArithExpr::Constant(42);
        let expr2 = expr1.clone();
        assert_eq!(expr1, expr2);

        let mut set = HashSet::new();
        set.insert(ArithExpr::Constant(1));
        set.insert(ArithExpr::Constant(2));
        assert_eq!(set.len(), 2);
    }

    // ========================================================================
    // Term Tests
    // ========================================================================

    #[test]
    fn test_term_creation() {
        let var = Term::Variable("x".to_string());
        let const_val = Term::Constant(42);
        let placeholder = Term::Placeholder;

        assert!(var.is_variable());
        assert!(const_val.is_constant());
        assert!(!placeholder.is_variable());
    }

    #[test]
    fn test_term_is_variable() {
        assert!(Term::Variable("x".to_string()).is_variable());
        assert!(!Term::Constant(42).is_variable());
        assert!(!Term::Placeholder.is_variable());
        assert!(!Term::Aggregate(AggregateFunc::Count, "x".to_string()).is_variable());
        assert!(!Term::Arithmetic(ArithExpr::Constant(1)).is_variable());
    }

    #[test]
    fn test_term_is_constant() {
        assert!(!Term::Variable("x".to_string()).is_constant());
        assert!(Term::Constant(42).is_constant());
        assert!(!Term::Placeholder.is_constant());
        assert!(!Term::Aggregate(AggregateFunc::Count, "x".to_string()).is_constant());
        assert!(!Term::Arithmetic(ArithExpr::Constant(1)).is_constant());
    }

    #[test]
    fn test_term_is_aggregate() {
        assert!(!Term::Variable("x".to_string()).is_aggregate());
        assert!(!Term::Constant(42).is_aggregate());
        assert!(!Term::Placeholder.is_aggregate());
        assert!(Term::Aggregate(AggregateFunc::Count, "x".to_string()).is_aggregate());
        assert!(!Term::Arithmetic(ArithExpr::Constant(1)).is_aggregate());
    }

    #[test]
    fn test_term_is_arithmetic() {
        assert!(!Term::Variable("x".to_string()).is_arithmetic());
        assert!(!Term::Constant(42).is_arithmetic());
        assert!(!Term::Placeholder.is_arithmetic());
        assert!(!Term::Aggregate(AggregateFunc::Count, "x".to_string()).is_arithmetic());
        assert!(Term::Arithmetic(ArithExpr::Constant(1)).is_arithmetic());
    }

    #[test]
    fn test_term_as_variable() {
        assert_eq!(Term::Variable("x".to_string()).as_variable(), Some("x"));
        assert_eq!(Term::Constant(42).as_variable(), None);
        assert_eq!(Term::Placeholder.as_variable(), None);
        assert_eq!(
            Term::Aggregate(AggregateFunc::Count, "x".to_string()).as_variable(),
            None
        );
    }

    #[test]
    fn test_term_as_aggregate() {
        let agg = Term::Aggregate(AggregateFunc::Sum, "x".to_string());
        let (func, var) = agg.as_aggregate().unwrap();
        assert_eq!(*func, AggregateFunc::Sum);
        assert_eq!(var, "x");

        assert!(Term::Variable("x".to_string()).as_aggregate().is_none());
        assert!(Term::Constant(42).as_aggregate().is_none());
    }

    #[test]
    fn test_term_as_arithmetic() {
        let arith = Term::Arithmetic(ArithExpr::Constant(42));
        let expr = arith.as_arithmetic().unwrap();
        assert_eq!(*expr, ArithExpr::Constant(42));

        assert!(Term::Variable("x".to_string()).as_arithmetic().is_none());
        assert!(Term::Constant(42).as_arithmetic().is_none());
    }

    #[test]
    fn test_term_variables() {
        // Variable term
        let var_vars = Term::Variable("x".to_string()).variables();
        assert_eq!(var_vars.len(), 1);
        assert!(var_vars.contains("x"));

        // Constant term
        let const_vars = Term::Constant(42).variables();
        assert!(const_vars.is_empty());

        // Placeholder term
        let placeholder_vars = Term::Placeholder.variables();
        assert!(placeholder_vars.is_empty());

        // Aggregate term
        let agg_vars = Term::Aggregate(AggregateFunc::Sum, "y".to_string()).variables();
        assert_eq!(agg_vars.len(), 1);
        assert!(agg_vars.contains("y"));

        // Arithmetic term with variable
        let arith_vars = Term::Arithmetic(ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("a".to_string())),
            right: Box::new(ArithExpr::Variable("b".to_string())),
        })
        .variables();
        assert_eq!(arith_vars.len(), 2);
        assert!(arith_vars.contains("a"));
        assert!(arith_vars.contains("b"));
    }

    #[test]
    fn test_term_traits() {
        let term1 = Term::Variable("x".to_string());
        let term2 = term1.clone();
        assert_eq!(term1, term2);

        // Test that different terms are not equal
        let term3 = Term::Constant(42);
        assert_ne!(term1, term3);
    }

    // ========================================================================
    // Atom Tests
    // ========================================================================

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

        let vars = atom.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
    }

    #[test]
    fn test_atom_empty_args() {
        let atom = Atom::new("empty".to_string(), vec![]);
        assert_eq!(atom.arity(), 0);
        assert!(atom.variables().is_empty());
    }

    #[test]
    fn test_atom_has_aggregates() {
        let atom_without = Atom::new(
            "test".to_string(),
            vec![Term::Variable("x".to_string()), Term::Constant(42)],
        );
        assert!(!atom_without.has_aggregates());

        let atom_with = Atom::new(
            "test".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Aggregate(AggregateFunc::Count, "y".to_string()),
            ],
        );
        assert!(atom_with.has_aggregates());
    }

    #[test]
    fn test_atom_has_arithmetic() {
        let atom_without = Atom::new(
            "test".to_string(),
            vec![Term::Variable("x".to_string()), Term::Constant(42)],
        );
        assert!(!atom_without.has_arithmetic());

        let atom_with = Atom::new(
            "test".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Arithmetic(ArithExpr::Constant(1)),
            ],
        );
        assert!(atom_with.has_arithmetic());
    }

    #[test]
    fn test_atom_aggregates() {
        let atom = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Aggregate(AggregateFunc::Sum, "y".to_string()),
                Term::Aggregate(AggregateFunc::Count, "z".to_string()),
            ],
        );

        let aggs = atom.aggregates();
        assert_eq!(aggs.len(), 2);
        assert_eq!(*aggs[0].0, AggregateFunc::Sum);
        assert_eq!(aggs[0].1, "y");
        assert_eq!(*aggs[1].0, AggregateFunc::Count);
        assert_eq!(aggs[1].1, "z");
    }

    #[test]
    fn test_atom_arithmetic_terms() {
        let expr1 = ArithExpr::Binary {
            op: ArithOp::Add,
            left: Box::new(ArithExpr::Variable("d".to_string())),
            right: Box::new(ArithExpr::Constant(1)),
        };
        let expr2 = ArithExpr::Variable("x".to_string());

        let atom = Atom::new(
            "dist".to_string(),
            vec![
                Term::Variable("y".to_string()),
                Term::Arithmetic(expr1.clone()),
                Term::Constant(0),
                Term::Arithmetic(expr2.clone()),
            ],
        );

        let arith = atom.arithmetic_terms();
        assert_eq!(arith.len(), 2);
        assert_eq!(arith[0].0, 1); // index 1
        assert_eq!(*arith[0].1, expr1);
        assert_eq!(arith[1].0, 3); // index 3
        assert_eq!(*arith[1].1, expr2);
    }

    #[test]
    fn test_atom_variables_with_aggregates() {
        let atom = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Aggregate(AggregateFunc::Sum, "y".to_string()),
            ],
        );

        let vars = atom.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
    }

    #[test]
    fn test_atom_variables_with_arithmetic() {
        let atom = Atom::new(
            "dist".to_string(),
            vec![
                Term::Variable("y".to_string()),
                Term::Arithmetic(ArithExpr::Binary {
                    op: ArithOp::Add,
                    left: Box::new(ArithExpr::Variable("d".to_string())),
                    right: Box::new(ArithExpr::Constant(1)),
                }),
            ],
        );

        let vars = atom.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains("y"));
        assert!(vars.contains("d"));
    }

    #[test]
    fn test_atom_traits() {
        let atom1 = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        let atom2 = atom1.clone();
        assert_eq!(atom1, atom2);

        // Test that different atoms are not equal
        let atom3 = Atom::new("other".to_string(), vec![]);
        assert_ne!(atom1, atom3);
    }

    // ========================================================================
    // Constraint Tests
    // ========================================================================

    #[test]
    fn test_constraint_not_equal() {
        let constraint = Constraint::NotEqual(
            Term::Variable("x".to_string()),
            Term::Variable("y".to_string()),
        );

        let vars = constraint.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
    }

    #[test]
    fn test_constraint_less_than() {
        let constraint = Constraint::LessThan(Term::Variable("x".to_string()), Term::Constant(10));

        let vars = constraint.variables();
        assert_eq!(vars.len(), 1);
        assert!(vars.contains("x"));
    }

    #[test]
    fn test_constraint_less_or_equal() {
        let constraint = Constraint::LessOrEqual(
            Term::Variable("a".to_string()),
            Term::Variable("b".to_string()),
        );

        let vars = constraint.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains("a"));
        assert!(vars.contains("b"));
    }

    #[test]
    fn test_constraint_greater_than() {
        let constraint =
            Constraint::GreaterThan(Term::Constant(100), Term::Variable("x".to_string()));

        let vars = constraint.variables();
        assert_eq!(vars.len(), 1);
        assert!(vars.contains("x"));
    }

    #[test]
    fn test_constraint_greater_or_equal() {
        let constraint = Constraint::GreaterOrEqual(
            Term::Variable("x".to_string()),
            Term::Variable("y".to_string()),
        );

        let vars = constraint.variables();
        assert_eq!(vars.len(), 2);
    }

    #[test]
    fn test_constraint_equal() {
        let constraint = Constraint::Equal(Term::Variable("x".to_string()), Term::Constant(5));

        let vars = constraint.variables();
        assert_eq!(vars.len(), 1);
        assert!(vars.contains("x"));
    }

    #[test]
    fn test_constraint_constants_only() {
        let constraint = Constraint::Equal(Term::Constant(5), Term::Constant(5));

        let vars = constraint.variables();
        assert!(vars.is_empty());
    }

    #[test]
    fn test_constraint_traits() {
        let c1 = Constraint::Equal(Term::Constant(1), Term::Constant(1));
        let c2 = c1.clone();
        assert_eq!(c1, c2);

        // Test that different constraints are not equal
        let c3 = Constraint::NotEqual(Term::Constant(1), Term::Constant(2));
        assert_ne!(c1, c3);
    }

    // ========================================================================
    // BodyPredicate Tests
    // ========================================================================

    #[test]
    fn test_body_predicate_positive() {
        let atom = Atom::new("edge".to_string(), vec![Term::Variable("x".to_string())]);
        let pred = BodyPredicate::Positive(atom.clone());

        assert!(pred.is_positive());
        assert!(!pred.is_negated());
        assert_eq!(pred.atom(), &atom);
    }

    #[test]
    fn test_body_predicate_negated() {
        let atom = Atom::new("visited".to_string(), vec![Term::Variable("x".to_string())]);
        let pred = BodyPredicate::Negated(atom.clone());

        assert!(!pred.is_positive());
        assert!(pred.is_negated());
        assert_eq!(pred.atom(), &atom);
    }

    #[test]
    fn test_body_predicate_variables() {
        let atom = Atom::new(
            "edge".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Variable("y".to_string()),
            ],
        );

        let pos_vars = BodyPredicate::Positive(atom.clone()).variables();
        let neg_vars = BodyPredicate::Negated(atom).variables();

        assert_eq!(pos_vars.len(), 2);
        assert_eq!(neg_vars.len(), 2);
        assert!(pos_vars.contains("x"));
        assert!(pos_vars.contains("y"));
    }

    #[test]
    fn test_body_predicate_traits() {
        let atom = Atom::new("test".to_string(), vec![]);
        let pred1 = BodyPredicate::Positive(atom.clone());
        let pred2 = pred1.clone();
        assert_eq!(pred1, pred2);

        let neg = BodyPredicate::Negated(atom);
        assert_ne!(pred1, neg);
    }

    // ========================================================================
    // Rule Tests
    // ========================================================================

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

        assert!(rule.is_safe()); // y appears in edge(x, y)
        assert!(rule.is_recursive()); // reach appears in head and body
    }

    #[test]
    fn test_rule_unsafe() {
        // unsafe: z is in head but not in any positive body atom
        let head = Atom::new("test".to_string(), vec![Term::Variable("z".to_string())]);
        let body = vec![BodyPredicate::Positive(Atom::new(
            "source".to_string(),
            vec![Term::Variable("x".to_string())],
        ))];

        let rule = Rule::new(head, body, vec![]);
        assert!(!rule.is_safe());
    }

    #[test]
    fn test_rule_unsafe_negated_only() {
        // unsafe: x only appears in negated body
        let head = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        let body = vec![BodyPredicate::Negated(Atom::new(
            "source".to_string(),
            vec![Term::Variable("x".to_string())],
        ))];

        let rule = Rule::new(head, body, vec![]);
        assert!(!rule.is_safe());
    }

    #[test]
    fn test_rule_new_simple() {
        let head = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        let body = vec![Atom::new(
            "source".to_string(),
            vec![Term::Variable("x".to_string())],
        )];

        let rule = Rule::new_simple(head, body, vec![]);

        assert_eq!(rule.body.len(), 1);
        assert!(rule.body[0].is_positive());
    }

    #[test]
    fn test_rule_is_recursive() {
        // Recursive: reach(y) :- reach(x), edge(x, y).
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
        assert!(rule.is_recursive());
    }

    #[test]
    fn test_rule_not_recursive() {
        // Non-recursive: reach(x) :- source(x).
        let head = Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]);
        let body = vec![BodyPredicate::Positive(Atom::new(
            "source".to_string(),
            vec![Term::Variable("x".to_string())],
        ))];

        let rule = Rule::new(head, body, vec![]);
        assert!(!rule.is_recursive());
    }

    #[test]
    fn test_rule_positive_body_variables() {
        let head = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        let body = vec![
            BodyPredicate::Positive(Atom::new(
                "a".to_string(),
                vec![Term::Variable("x".to_string())],
            )),
            BodyPredicate::Negated(Atom::new(
                "b".to_string(),
                vec![Term::Variable("y".to_string())],
            )),
            BodyPredicate::Positive(Atom::new(
                "c".to_string(),
                vec![Term::Variable("z".to_string())],
            )),
        ];

        let rule = Rule::new(head, body, vec![]);
        let pos_vars = rule.positive_body_variables();

        assert_eq!(pos_vars.len(), 2);
        assert!(pos_vars.contains("x"));
        assert!(pos_vars.contains("z"));
        assert!(!pos_vars.contains("y")); // y is only in negated atom
    }

    #[test]
    fn test_rule_variables() {
        let head = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        let body = vec![BodyPredicate::Positive(Atom::new(
            "a".to_string(),
            vec![Term::Variable("y".to_string())],
        ))];
        let constraints = vec![Constraint::LessThan(
            Term::Variable("z".to_string()),
            Term::Constant(10),
        )];

        let rule = Rule::new(head, body, constraints);
        let vars = rule.variables();

        assert_eq!(vars.len(), 3);
        assert!(vars.contains("x"));
        assert!(vars.contains("y"));
        assert!(vars.contains("z"));
    }

    #[test]
    fn test_rule_positive_body_atoms() {
        let head = Atom::new("test".to_string(), vec![]);
        let pos1 = Atom::new("a".to_string(), vec![]);
        let neg = Atom::new("b".to_string(), vec![]);
        let pos2 = Atom::new("c".to_string(), vec![]);

        let body = vec![
            BodyPredicate::Positive(pos1.clone()),
            BodyPredicate::Negated(neg),
            BodyPredicate::Positive(pos2.clone()),
        ];

        let rule = Rule::new(head, body, vec![]);
        let pos_atoms = rule.positive_body_atoms();

        assert_eq!(pos_atoms.len(), 2);
        assert_eq!(pos_atoms[0], &pos1);
        assert_eq!(pos_atoms[1], &pos2);
    }

    #[test]
    fn test_rule_negated_body_atoms() {
        let head = Atom::new("test".to_string(), vec![]);
        let pos = Atom::new("a".to_string(), vec![]);
        let neg1 = Atom::new("b".to_string(), vec![]);
        let neg2 = Atom::new("c".to_string(), vec![]);

        let body = vec![
            BodyPredicate::Positive(pos),
            BodyPredicate::Negated(neg1.clone()),
            BodyPredicate::Negated(neg2.clone()),
        ];

        let rule = Rule::new(head, body, vec![]);
        let neg_atoms = rule.negated_body_atoms();

        assert_eq!(neg_atoms.len(), 2);
        assert_eq!(neg_atoms[0], &neg1);
        assert_eq!(neg_atoms[1], &neg2);
    }

    #[test]
    fn test_rule_traits() {
        let head = Atom::new("test".to_string(), vec![]);
        let rule = Rule::new(head, vec![], vec![]);
        let _cloned = rule.clone();
        // Rule doesn't implement PartialEq, so we just test clone works
    }

    // ========================================================================
    // Program Tests
    // ========================================================================

    #[test]
    fn test_program_new() {
        let program = Program::new();
        assert!(program.rules.is_empty());
    }

    #[test]
    fn test_program_default() {
        let program = Program::default();
        assert!(program.rules.is_empty());
    }

    #[test]
    fn test_program_add_rule() {
        let mut program = Program::new();
        assert_eq!(program.rules.len(), 0);

        program.add_rule(Rule::new_simple(
            Atom::new("test".to_string(), vec![]),
            vec![],
            vec![],
        ));
        assert_eq!(program.rules.len(), 1);
    }

    #[test]
    fn test_program_edbs_idbs() {
        let mut program = Program::new();

        // reach(x) :- source(x).
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // reach(y) :- reach(x), edge(x, y).
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

    #[test]
    fn test_program_all_relations() {
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

        let all = program.all_relations();
        assert_eq!(all, vec!["edge", "reach", "source"]);
    }

    #[test]
    fn test_program_is_safe() {
        let mut safe_program = Program::new();
        safe_program.add_rule(Rule::new_simple(
            Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));
        assert!(safe_program.is_safe());

        let mut unsafe_program = Program::new();
        unsafe_program.add_rule(Rule::new_simple(
            Atom::new("test".to_string(), vec![Term::Variable("z".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));
        assert!(!unsafe_program.is_safe());
    }

    #[test]
    fn test_program_recursive_rules() {
        let mut program = Program::new();

        // Non-recursive: reach(x) :- source(x).
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        // Recursive: reach(y) :- reach(x), edge(x, y).
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

        let recursive = program.recursive_rules();
        let non_recursive = program.non_recursive_rules();

        assert_eq!(recursive.len(), 1);
        assert_eq!(non_recursive.len(), 1);
        assert_eq!(recursive[0].head.relation, "reach");
        assert_eq!(non_recursive[0].head.relation, "reach");
    }

    #[test]
    fn test_program_multiple_idbs() {
        let mut program = Program::new();

        // path(x, y) :- edge(x, y).
        program.add_rule(Rule::new_simple(
            Atom::new(
                "path".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "edge".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![],
        ));

        // reach(x) :- source(x).
        program.add_rule(Rule::new_simple(
            Atom::new("reach".to_string(), vec![Term::Variable("x".to_string())]),
            vec![Atom::new(
                "source".to_string(),
                vec![Term::Variable("x".to_string())],
            )],
            vec![],
        ));

        let idbs = program.idbs();
        assert_eq!(idbs.len(), 2);
        assert!(idbs.contains(&"path".to_string()));
        assert!(idbs.contains(&"reach".to_string()));
    }

    #[test]
    fn test_program_empty() {
        let program = Program::new();

        assert!(program.idbs().is_empty());
        assert!(program.edbs().is_empty());
        assert!(program.all_relations().is_empty());
        assert!(program.is_safe()); // Empty program is vacuously safe
        assert!(program.recursive_rules().is_empty());
        assert!(program.non_recursive_rules().is_empty());
    }

    #[test]
    fn test_program_traits() {
        let program = Program::new();
        let _cloned = program.clone();
        // Program doesn't implement PartialEq, so we just test clone works
    }

    // ========================================================================
    // BuiltinFunc Tests
    // ========================================================================

    #[test]
    fn test_builtin_func_parse() {
        assert_eq!(BuiltinFunc::parse("euclidean"), Some(BuiltinFunc::Euclidean));
        assert_eq!(BuiltinFunc::parse("cosine"), Some(BuiltinFunc::Cosine));
        assert_eq!(BuiltinFunc::parse("dot"), Some(BuiltinFunc::DotProduct));
        assert_eq!(BuiltinFunc::parse("manhattan"), Some(BuiltinFunc::Manhattan));
        assert_eq!(BuiltinFunc::parse("lsh_bucket"), Some(BuiltinFunc::LshBucket));
        assert_eq!(BuiltinFunc::parse("normalize"), Some(BuiltinFunc::VecNormalize));
        assert_eq!(BuiltinFunc::parse("vec_dim"), Some(BuiltinFunc::VecDim));
        assert_eq!(BuiltinFunc::parse("vec_add"), Some(BuiltinFunc::VecAdd));
        assert_eq!(BuiltinFunc::parse("vec_scale"), Some(BuiltinFunc::VecScale));
    }

    #[test]
    fn test_builtin_func_parse_case_insensitive() {
        assert_eq!(BuiltinFunc::parse("EUCLIDEAN"), Some(BuiltinFunc::Euclidean));
        assert_eq!(BuiltinFunc::parse("Cosine"), Some(BuiltinFunc::Cosine));
    }

    #[test]
    fn test_builtin_func_parse_invalid() {
        assert_eq!(BuiltinFunc::parse("unknown"), None);
        assert_eq!(BuiltinFunc::parse(""), None);
    }

    #[test]
    fn test_builtin_func_arity() {
        assert_eq!(BuiltinFunc::Euclidean.arity(), 2);
        assert_eq!(BuiltinFunc::Cosine.arity(), 2);
        assert_eq!(BuiltinFunc::DotProduct.arity(), 2);
        assert_eq!(BuiltinFunc::Manhattan.arity(), 2);
        assert_eq!(BuiltinFunc::LshBucket.arity(), 3);
        assert_eq!(BuiltinFunc::VecNormalize.arity(), 1);
        assert_eq!(BuiltinFunc::VecDim.arity(), 1);
        assert_eq!(BuiltinFunc::VecAdd.arity(), 2);
        assert_eq!(BuiltinFunc::VecScale.arity(), 2);
    }

    #[test]
    fn test_builtin_func_as_str() {
        assert_eq!(BuiltinFunc::Euclidean.as_str(), "euclidean");
        assert_eq!(BuiltinFunc::Cosine.as_str(), "cosine");
        assert_eq!(BuiltinFunc::DotProduct.as_str(), "dot");
    }

    // ========================================================================
    // New Aggregate Tests
    // ========================================================================

    #[test]
    fn test_top_k_parse() {
        let func = AggregateFunc::parse_top_k("10, score").unwrap();
        match func {
            AggregateFunc::TopK { k, order_var, descending } => {
                assert_eq!(k, 10);
                assert_eq!(order_var, "score");
                assert!(!descending);
            }
            _ => panic!("Expected TopK"),
        }
    }

    #[test]
    fn test_top_k_parse_descending() {
        let func = AggregateFunc::parse_top_k("5, dist, desc").unwrap();
        match func {
            AggregateFunc::TopK { k, order_var, descending } => {
                assert_eq!(k, 5);
                assert_eq!(order_var, "dist");
                assert!(descending);
            }
            _ => panic!("Expected TopK"),
        }
    }

    #[test]
    fn test_top_k_threshold_parse() {
        let func = AggregateFunc::parse_top_k_threshold("10, score, 0.8").unwrap();
        match func {
            AggregateFunc::TopKThreshold { k, order_var, threshold, descending } => {
                assert_eq!(k, 10);
                assert_eq!(order_var, "score");
                assert!((threshold - 0.8).abs() < f64::EPSILON);
                assert!(!descending);
            }
            _ => panic!("Expected TopKThreshold"),
        }
    }

    #[test]
    fn test_within_radius_parse() {
        let func = AggregateFunc::parse_within_radius("dist, 0.5").unwrap();
        match func {
            AggregateFunc::WithinRadius { distance_var, max_distance } => {
                assert_eq!(distance_var, "dist");
                assert!((max_distance - 0.5).abs() < f64::EPSILON);
            }
            _ => panic!("Expected WithinRadius"),
        }
    }

    #[test]
    fn test_aggregate_is_ranking() {
        assert!(!AggregateFunc::Count.is_ranking());
        assert!(!AggregateFunc::Sum.is_ranking());
        assert!(AggregateFunc::TopK { k: 10, order_var: "x".to_string(), descending: false }.is_ranking());
        assert!(AggregateFunc::TopKThreshold { k: 10, order_var: "x".to_string(), threshold: 0.5, descending: false }.is_ranking());
        assert!(AggregateFunc::WithinRadius { distance_var: "d".to_string(), max_distance: 0.5 }.is_ranking());
    }

    // ========================================================================
    // FunctionCall Term Tests
    // ========================================================================

    #[test]
    fn test_term_function_call() {
        let func_call = Term::FunctionCall(
            BuiltinFunc::Euclidean,
            vec![Term::Variable("v1".to_string()), Term::Variable("v2".to_string())],
        );

        assert!(func_call.is_function_call());
        let (func, args) = func_call.as_function_call().unwrap();
        assert_eq!(*func, BuiltinFunc::Euclidean);
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn test_term_function_call_variables() {
        let func_call = Term::FunctionCall(
            BuiltinFunc::Euclidean,
            vec![Term::Variable("v1".to_string()), Term::Variable("v2".to_string())],
        );

        let vars = func_call.variables();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains("v1"));
        assert!(vars.contains("v2"));
    }

    #[test]
    fn test_term_vector_literal() {
        let vec_lit = Term::VectorLiteral(vec![1.0, 2.0, 3.0]);

        assert!(vec_lit.is_vector_literal());
        let values = vec_lit.as_vector_literal().unwrap();
        assert_eq!(values.len(), 3);
        assert!((values[0] - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_term_float_constant() {
        let float_const = Term::FloatConstant(3.14);

        assert!(float_const.is_float_constant());
        assert!((float_const.as_float().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_atom_has_function_calls() {
        let atom_without = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        assert!(!atom_without.has_function_calls());

        let atom_with = Atom::new(
            "test".to_string(),
            vec![Term::FunctionCall(BuiltinFunc::Euclidean, vec![
                Term::Variable("v1".to_string()),
                Term::Variable("v2".to_string()),
            ])],
        );
        assert!(atom_with.has_function_calls());
    }

    #[test]
    fn test_atom_function_calls() {
        let atom = Atom::new(
            "result".to_string(),
            vec![
                Term::Variable("id".to_string()),
                Term::FunctionCall(BuiltinFunc::Euclidean, vec![
                    Term::Variable("v1".to_string()),
                    Term::Variable("v2".to_string()),
                ]),
            ],
        );

        let calls = atom.function_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, 1); // index
        assert_eq!(*calls[0].1, BuiltinFunc::Euclidean);
    }

    #[test]
    fn test_atom_has_vector_literals() {
        let atom_without = Atom::new("test".to_string(), vec![Term::Variable("x".to_string())]);
        assert!(!atom_without.has_vector_literals());

        let atom_with = Atom::new(
            "query".to_string(),
            vec![Term::VectorLiteral(vec![1.0, 2.0, 3.0])],
        );
        assert!(atom_with.has_vector_literals());
    }
}
