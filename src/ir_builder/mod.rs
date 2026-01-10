//! # IR Builder (Module 05)
//!
//! **Course Context**: Students implement this module in **Module 05: IR Construction**.
//!
//! This module teaches:
//! - Converting Abstract Syntax Tree (AST) to Intermediate Representation (IR)
//! - Logical query plan generation
//! - Schema inference and tracking through pipeline stages
//! - Join key inference from shared variables
//! - Constraint translation to predicates
//!
//! ## Learning Objectives (Module 05)
//!
//! Students learn to:
//! 1. Build IRNode trees from Datalog rules
//! 2. Generate Scan nodes for base relations
//! 3. Construct Join nodes with proper key inference
//! 4. Build Filter nodes from constraints
//! 5. Generate Map nodes for projections
//! 6. Track schemas through all transformations
//! 7. Integrate with Catalog for schema management
//!
//! ## Key Concepts
//!
//! - **Logical Query Plan**: High-level representation of query operations
//! - **Schema Propagation**: Maintaining variable bindings through pipeline
//! - **Join Key Inference**: Determining join columns from shared variables
//! - **Predicate Translation**: Converting AST constraints to IR predicates
//!
//! ## Pipeline Position
//!
//! ```text
//! AST (Program/Rules) → [IR Builder] → IRNode tree → Optimizer
//! ```
//!
//! ---
//!
//! # Implementation
//!
//! Takes Datalog rules (AST) and converts them to intermediate representation (IR)
//! suitable for optimization and code generation.

use crate::ast::{Atom, BodyPredicate, BuiltinFunc, Constraint, Rule, Term};
use crate::ir::{BuiltinFunction, IRExpression, IRNode, Predicate};
use std::collections::HashSet;

use crate::catalog::Catalog;

/// IR Builder converts AST to IR
pub struct IRBuilder {
    catalog: Catalog,
}

impl IRBuilder {
    /// Create a new IR builder with a catalog
    pub fn new(catalog: Catalog) -> Self {
        IRBuilder { catalog }
    }

    /// Build IR from a rule
    pub fn build_ir(&self, rule: &Rule) -> Result<IRNode, String> {
        // 1. Build scans for all positive body atoms
        let mut scans = self.build_scans(rule)?;

        if scans.is_empty() {
            return Err("Rule has no positive body atoms".to_string());
        }

        // 2. Build join tree from positive atoms
        let mut current = scans.remove(0);
        if std::env::var("DATALOG_DEBUG").is_ok() {
            eprintln!("DEBUG IR build: first scan = {:?}", current.output_schema());
        }
        for scan in scans {
            if std::env::var("DATALOG_DEBUG").is_ok() {
                eprintln!(
                    "DEBUG IR build: joining with scan = {:?}",
                    scan.output_schema()
                );
            }
            current = self.build_join(current, scan)?;
            if std::env::var("DATALOG_DEBUG").is_ok() {
                eprintln!("DEBUG IR build: after join = {:?}", current.output_schema());
            }
        }

        // 3. Apply filters (constraints) - skips function call assignments
        current = self.build_filters(current, rule)?;

        // 4. Apply antijoins for negated predicates
        current = self.build_antijoins(current, rule)?;

        // 5. Apply computed columns (function call constraints like Dist = euclidean(V, Q))
        current = self.build_computed_columns(current, rule)?;

        // 6. Apply projection to match head schema
        current = self.build_projection(current, rule)?;

        Ok(current)
    }

    /// Build scan nodes for all positive body atoms
    fn build_scans(&self, rule: &Rule) -> Result<Vec<IRNode>, String> {
        let mut scans = Vec::new();

        for pred in &rule.body {
            if let BodyPredicate::Positive(atom) = pred {
                let mut scan = self.build_scan(atom)?;

                // Apply filters for any constants in positive body atoms
                // For example, color(X, 0) needs to filter color to only rows where col1 == 0
                for (i, term) in atom.args.iter().enumerate() {
                    if let Term::Constant(val) = term {
                        scan = IRNode::Filter {
                            input: Box::new(scan),
                            predicate: Predicate::ColumnEqConst(i, *val),
                        };
                    }
                }

                scans.push(scan);
            }
        }

        Ok(scans)
    }

    /// Build a single scan node
    fn build_scan(&self, atom: &Atom) -> Result<IRNode, String> {
        // Schema comes from the atom's arguments (variable bindings)
        // Each occurrence of the same relation can have different variable names
        let schema: Vec<String> = atom
            .args
            .iter()
            .enumerate()
            .map(|(i, term)| match term {
                Term::Variable(v) => v.clone(),
                Term::Constant(_) => format!("col{}", i),
                Term::Placeholder => format!("col{}", i),
                // Aggregates in body atoms refer to the variable they aggregate
                Term::Aggregate(_, v) => v.clone(),
                // Arithmetic expressions - use the variables they reference
                Term::Arithmetic(expr) => {
                    // Use the first variable referenced, or generate a name
                    let vars = expr.variables();
                    vars.into_iter()
                        .next()
                        .unwrap_or_else(|| format!("expr{}", i))
                }
                // Function calls - generate a name
                Term::FunctionCall(_, _) => format!("func{}", i),
                // Vector literals - generate a name
                Term::VectorLiteral(_) => format!("vec{}", i),
                // Float constants - generate a name
                Term::FloatConstant(_) => format!("float{}", i),
                // String constants - generate a name
                Term::StringConstant(_) => format!("str{}", i),
                // Field access - use the field name
                Term::FieldAccess(_, field) => field.clone(),
                // Record pattern - generate a name
                Term::RecordPattern(_) => format!("rec{}", i),
            })
            .collect();

        Ok(IRNode::Scan {
            relation: atom.relation.clone(),
            schema,
        })
    }

    /// Build a join between two IR nodes
    fn build_join(&self, left: IRNode, right: IRNode) -> Result<IRNode, String> {
        let left_schema = left.output_schema();
        let right_schema = right.output_schema();

        // Find shared variables
        let left_vars: HashSet<_> = left_schema.iter().collect();
        let right_vars: HashSet<_> = right_schema.iter().collect();
        let shared: Vec<_> = left_vars
            .intersection(&right_vars)
            .map(|s| s.to_string())
            .collect();

        let (left_keys, right_keys) =
            self.catalog
                .infer_join_keys(&left_schema, &right_schema, &shared);

        // Output schema: all columns from left, then non-key columns from right
        let mut output_schema = left_schema;
        for (i, col) in right_schema.iter().enumerate() {
            if !right_keys.contains(&i) {
                output_schema.push(col.clone());
            }
        }

        Ok(IRNode::Join {
            left: Box::new(left),
            right: Box::new(right),
            left_keys,
            right_keys,
            output_schema,
        })
    }

    /// Build filter nodes for constraints
    ///
    /// Skips function call assignment constraints (e.g., Dist = euclidean(V, Q))
    /// which are handled by build_computed_columns instead.
    fn build_filters(&self, mut input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        for constraint in &rule.constraints {
            // Skip function call assignments - they're handled in build_computed_columns
            if self.is_function_call_constraint(constraint) {
                continue;
            }

            let predicate = self.constraint_to_predicate(constraint, &input.output_schema())?;
            input = IRNode::Filter {
                input: Box::new(input),
                predicate,
            };
        }

        Ok(input)
    }

    /// Check if a constraint is a function call assignment (e.g., Dist = euclidean(V, Q))
    fn is_function_call_constraint(&self, constraint: &Constraint) -> bool {
        matches!(
            constraint,
            Constraint::Equal(Term::Variable(_), Term::FunctionCall(_, _))
                | Constraint::Equal(Term::FunctionCall(_, _), Term::Variable(_))
        )
    }

    /// Build antijoin nodes for negated predicates
    ///
    /// For each negated predicate `!relation(X, Y)`, we create an Antijoin node
    /// that removes tuples from the current result that have matches in the negated relation.
    fn build_antijoins(&self, mut input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        for pred in &rule.body {
            if let BodyPredicate::Negated(atom) = pred {
                input = self.build_antijoin(input, atom, rule)?;
            }
        }
        Ok(input)
    }

    /// Build a single antijoin node for a negated predicate
    fn build_antijoin(
        &self,
        left: IRNode,
        negated_atom: &Atom,
        _rule: &Rule,
    ) -> Result<IRNode, String> {
        // 1. Build scan for the negated relation
        let mut right = self.build_scan(negated_atom)?;

        // 2. Apply filters for any constants in the negated atom
        // For example, !reach(1, X) needs to filter reach to only rows where col0 == 1
        for (i, term) in negated_atom.args.iter().enumerate() {
            if let Term::Constant(val) = term {
                right = IRNode::Filter {
                    input: Box::new(right),
                    predicate: Predicate::ColumnEqConst(i, *val),
                };
            }
        }

        // 3. Get schemas
        let left_schema = left.output_schema();
        let right_schema = right.output_schema();

        // 4. Find join keys by matching variable names between left and negated atom
        let (left_keys, right_keys) = self.infer_antijoin_keys(&left_schema, &right_schema)?;

        if left_keys.is_empty() {
            return Err(format!(
                "Negated predicate !{}(...) shares no variables with positive predicates. \
                 Negation requires at least one shared variable.",
                negated_atom.relation
            ));
        }

        // 5. Build Antijoin node - output schema is same as left (we're filtering left)
        Ok(IRNode::Antijoin {
            left: Box::new(left),
            right: Box::new(right),
            left_keys,
            right_keys,
            output_schema: left_schema,
        })
    }

    /// Infer antijoin keys by finding shared variable names between schemas
    fn infer_antijoin_keys(
        &self,
        left_schema: &[String],
        right_schema: &[String],
    ) -> Result<(Vec<usize>, Vec<usize>), String> {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();

        // For each column in right schema, check if it matches a column in left schema
        for (right_idx, right_col) in right_schema.iter().enumerate() {
            // Skip generated column names (col0, col1, etc.)
            if right_col.starts_with("col")
                || right_col.starts_with("expr")
                || right_col.starts_with("func")
                || right_col.starts_with("vec")
                || right_col.starts_with("float")
                || right_col.starts_with("str")
            {
                continue;
            }

            // Find matching variable in left schema
            if let Some(left_idx) = left_schema.iter().position(|s| s == right_col) {
                left_keys.push(left_idx);
                right_keys.push(right_idx);
            }
        }

        Ok((left_keys, right_keys))
    }

    /// Build computed columns for function call constraints
    ///
    /// Handles constraints like `Dist = euclidean(V, Q)` by creating a Compute node
    /// that adds the computed column to the schema.
    fn build_computed_columns(&self, input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        let mut expressions = Vec::new();
        let schema = input.output_schema();

        for constraint in &rule.constraints {
            // Only process function call assignments
            let (var_name, func, args) = match constraint {
                Constraint::Equal(Term::Variable(v), Term::FunctionCall(f, a)) => (v, f, a),
                Constraint::Equal(Term::FunctionCall(f, a), Term::Variable(v)) => (v, f, a),
                _ => continue,
            };

            // Convert AST function to IR function
            let ir_func = self.ast_func_to_ir_func(func)?;

            // Convert AST arguments to IR expressions
            let ir_args: Vec<IRExpression> = args
                .iter()
                .map(|term| self.term_to_ir_expr(term, &schema))
                .collect::<Result<Vec<_>, _>>()?;

            expressions.push((
                var_name.clone(),
                IRExpression::FunctionCall(ir_func, ir_args),
            ));
        }

        if expressions.is_empty() {
            // No function call constraints, return input unchanged
            Ok(input)
        } else {
            Ok(IRNode::Compute {
                input: Box::new(input),
                expressions,
            })
        }
    }

    /// Convert AST BuiltinFunc to IR BuiltinFunction
    fn ast_func_to_ir_func(&self, func: &BuiltinFunc) -> Result<BuiltinFunction, String> {
        match func {
            BuiltinFunc::Euclidean => Ok(BuiltinFunction::Euclidean),
            BuiltinFunc::Cosine => Ok(BuiltinFunction::Cosine),
            BuiltinFunc::DotProduct => Ok(BuiltinFunction::DotProduct),
            BuiltinFunc::Manhattan => Ok(BuiltinFunction::Manhattan),
            BuiltinFunc::LshBucket => Ok(BuiltinFunction::LshBucket),
            BuiltinFunc::VecNormalize => Ok(BuiltinFunction::VecNormalize),
            BuiltinFunc::VecDim => Ok(BuiltinFunction::VecDim),
            BuiltinFunc::VecAdd => Ok(BuiltinFunction::VecAdd),
            BuiltinFunc::VecScale => Ok(BuiltinFunction::VecScale),
            BuiltinFunc::TimeNow => Err("TimeNow function not yet supported in IR".to_string()),
            BuiltinFunc::TimeDiff => Err("TimeDiff function not yet supported in IR".to_string()),
            BuiltinFunc::TimeAdd => Err("TimeAdd function not yet supported in IR".to_string()),
            BuiltinFunc::TimeSub => Err("TimeSub function not yet supported in IR".to_string()),
            BuiltinFunc::TimeDecay => Err("TimeDecay function not yet supported in IR".to_string()),
            BuiltinFunc::TimeDecayLinear => {
                Err("TimeDecayLinear function not yet supported in IR".to_string())
            }
            BuiltinFunc::TimeBefore => {
                Err("TimeBefore function not yet supported in IR".to_string())
            }
            BuiltinFunc::TimeAfter => Err("TimeAfter function not yet supported in IR".to_string()),
            BuiltinFunc::TimeBetween => {
                Err("TimeBetween function not yet supported in IR".to_string())
            }
            BuiltinFunc::WithinLast => {
                Err("WithinLast function not yet supported in IR".to_string())
            }
            BuiltinFunc::IntervalsOverlap => {
                Err("IntervalsOverlap function not yet supported in IR".to_string())
            }
            BuiltinFunc::IntervalContains => {
                Err("IntervalContains function not yet supported in IR".to_string())
            }
            BuiltinFunc::IntervalDuration => {
                Err("IntervalDuration function not yet supported in IR".to_string())
            }
            BuiltinFunc::PointInInterval => {
                Err("PointInInterval function not yet supported in IR".to_string())
            }
        }
    }

    /// Convert AST Term to IR Expression
    fn term_to_ir_expr(&self, term: &Term, schema: &[String]) -> Result<IRExpression, String> {
        match term {
            Term::Variable(name) => {
                // Find column index in schema
                let idx = schema.iter().position(|s| s == name).ok_or_else(|| {
                    format!("Variable '{}' not found in schema {:?}", name, schema)
                })?;
                Ok(IRExpression::Column(idx))
            }
            Term::Constant(val) => Ok(IRExpression::IntConstant(*val)),
            Term::FloatConstant(val) => Ok(IRExpression::FloatConstant(*val)),
            Term::VectorLiteral(vals) => {
                // Convert f64 to f32 (AST uses f64, IR uses f32)
                let f32_vals: Vec<f32> = vals.iter().map(|&v| v as f32).collect();
                Ok(IRExpression::VectorLiteral(f32_vals))
            }
            Term::FunctionCall(func, args) => {
                let ir_func = self.ast_func_to_ir_func(func)?;
                let ir_args: Vec<IRExpression> = args
                    .iter()
                    .map(|t| self.term_to_ir_expr(t, schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(IRExpression::FunctionCall(ir_func, ir_args))
            }
            Term::StringConstant(_) => {
                Err("String constants not supported in expressions".to_string())
            }
            Term::Placeholder => Err("Placeholders not supported in expressions".to_string()),
            Term::Arithmetic(_) => {
                Err("Arithmetic expressions should use build_projection_with_computed".to_string())
            }
            Term::Aggregate(_, _) => Err("Aggregates should use build_aggregation".to_string()),
            Term::FieldAccess(_, _) => {
                Err("Field access not yet supported in expressions".to_string())
            }
            Term::RecordPattern(_) => {
                Err("Record patterns not yet supported in expressions".to_string())
            }
        }
    }

    /// Convert AST constraint to IR predicate
    fn constraint_to_predicate(
        &self,
        constraint: &Constraint,
        schema: &[String],
    ) -> Result<Predicate, String> {
        match constraint {
            Constraint::NotEqual(left, right) => {
                self.build_equality_predicate(left, right, schema, false)
            }
            Constraint::LessThan(left, right) => self.build_ordering_predicate(
                left,
                right,
                schema,
                |col, val| Predicate::ColumnLtConst(col, val),
                |col, val| Predicate::ColumnLtFloat(col, val),
            ),
            Constraint::GreaterThan(left, right) => self.build_ordering_predicate(
                left,
                right,
                schema,
                |col, val| Predicate::ColumnGtConst(col, val),
                |col, val| Predicate::ColumnGtFloat(col, val),
            ),
            Constraint::LessOrEqual(left, right) => self.build_ordering_predicate(
                left,
                right,
                schema,
                |col, val| Predicate::ColumnLeConst(col, val),
                |col, val| Predicate::ColumnLeFloat(col, val),
            ),
            Constraint::GreaterOrEqual(left, right) => self.build_ordering_predicate(
                left,
                right,
                schema,
                |col, val| Predicate::ColumnGeConst(col, val),
                |col, val| Predicate::ColumnGeFloat(col, val),
            ),
            Constraint::Equal(left, right) => {
                self.build_equality_predicate(left, right, schema, true)
            }
        }
    }

    /// Build equality or inequality predicate (supports string, float, and int)
    fn build_equality_predicate(
        &self,
        left: &Term,
        right: &Term,
        schema: &[String],
        is_equal: bool,
    ) -> Result<Predicate, String> {
        // Extract column positions
        let left_col = match left {
            Term::Variable(v) => schema.iter().position(|s| s == v),
            _ => None,
        };
        let right_col = match right {
            Term::Variable(v) => schema.iter().position(|s| s == v),
            _ => None,
        };

        // Extract constants of various types
        let left_int = match left {
            Term::Constant(c) => Some(*c),
            _ => None,
        };
        let right_int = match right {
            Term::Constant(c) => Some(*c),
            _ => None,
        };
        let left_str = match left {
            Term::StringConstant(s) => Some(s.clone()),
            _ => None,
        };
        let right_str = match right {
            Term::StringConstant(s) => Some(s.clone()),
            _ => None,
        };
        let left_float = match left {
            Term::FloatConstant(f) => Some(*f),
            _ => None,
        };
        let right_float = match right {
            Term::FloatConstant(f) => Some(*f),
            _ => None,
        };

        // Try variable vs string constant
        if let (Some(col), Some(s)) = (left_col, right_str.clone()) {
            return Ok(if is_equal {
                Predicate::ColumnEqStr(col, s)
            } else {
                Predicate::ColumnNeStr(col, s)
            });
        }
        if let (Some(col), Some(s)) = (right_col, left_str.clone()) {
            return Ok(if is_equal {
                Predicate::ColumnEqStr(col, s)
            } else {
                Predicate::ColumnNeStr(col, s)
            });
        }

        // Try variable vs float constant
        if let (Some(col), Some(f)) = (left_col, right_float) {
            return Ok(if is_equal {
                Predicate::ColumnEqFloat(col, f)
            } else {
                Predicate::ColumnNeFloat(col, f)
            });
        }
        if let (Some(col), Some(f)) = (right_col, left_float) {
            return Ok(if is_equal {
                Predicate::ColumnEqFloat(col, f)
            } else {
                Predicate::ColumnNeFloat(col, f)
            });
        }

        // Try variable vs int constant
        if let (Some(col), Some(val)) = (left_col, right_int) {
            return Ok(if is_equal {
                Predicate::ColumnEqConst(col, val)
            } else {
                Predicate::ColumnNeConst(col, val)
            });
        }
        if let (Some(col), Some(val)) = (right_col, left_int) {
            return Ok(if is_equal {
                Predicate::ColumnEqConst(col, val)
            } else {
                Predicate::ColumnNeConst(col, val)
            });
        }

        // Try variable vs variable
        if let (Some(l_col), Some(r_col)) = (left_col, right_col) {
            return Ok(if is_equal {
                Predicate::ColumnsEq(l_col, r_col)
            } else {
                Predicate::ColumnsNe(l_col, r_col)
            });
        }

        Err("Unsupported equality constraint pattern".to_string())
    }

    /// Build ordering predicate (for <, >, <=, >=) - supports int and float
    fn build_ordering_predicate<FI, FF>(
        &self,
        left: &Term,
        right: &Term,
        schema: &[String],
        int_pred: FI,
        float_pred: FF,
    ) -> Result<Predicate, String>
    where
        FI: Fn(usize, i64) -> Predicate,
        FF: Fn(usize, f64) -> Predicate,
    {
        // Extract column positions
        let left_col = match left {
            Term::Variable(v) => schema.iter().position(|s| s == v),
            _ => None,
        };
        let right_col = match right {
            Term::Variable(v) => schema.iter().position(|s| s == v),
            _ => None,
        };

        // Extract constants
        let left_int = match left {
            Term::Constant(c) => Some(*c),
            _ => None,
        };
        let right_int = match right {
            Term::Constant(c) => Some(*c),
            _ => None,
        };
        let left_float = match left {
            Term::FloatConstant(f) => Some(*f),
            _ => None,
        };
        let right_float = match right {
            Term::FloatConstant(f) => Some(*f),
            _ => None,
        };

        // Try variable vs float constant
        if let (Some(col), Some(f)) = (left_col, right_float) {
            return Ok(float_pred(col, f));
        }
        if let (Some(col), Some(f)) = (right_col, left_float) {
            return Ok(float_pred(col, f));
        }

        // Try variable vs int constant
        if let (Some(col), Some(val)) = (left_col, right_int) {
            return Ok(int_pred(col, val));
        }
        if let (Some(col), Some(val)) = (right_col, left_int) {
            return Ok(int_pred(col, val));
        }

        // String ordering is not supported
        if matches!(left, Term::StringConstant(_)) || matches!(right, Term::StringConstant(_)) {
            return Err("String ordering comparisons (<, >, <=, >=) not supported".to_string());
        }

        Err("Unsupported ordering constraint pattern".to_string())
    }

    /// Convert an AST ArithExpr to an IR IRExpression
    fn arith_expr_to_ir_expression(
        expr: &crate::ast::ArithExpr,
        schema: &[String],
    ) -> Result<crate::ir::IRExpression, String> {
        use crate::ast::ArithExpr;
        use crate::ir::IRExpression;

        match expr {
            ArithExpr::Variable(name) => {
                let col_idx = schema.iter().position(|s| s == name).ok_or_else(|| {
                    format!("Variable '{}' not found in schema for arithmetic", name)
                })?;
                Ok(IRExpression::Column(col_idx))
            }
            ArithExpr::Constant(val) => Ok(IRExpression::IntConstant(*val)),
            ArithExpr::Binary { op, left, right } => {
                let left_ir = Self::arith_expr_to_ir_expression(left, schema)?;
                let right_ir = Self::arith_expr_to_ir_expression(right, schema)?;
                // Convert from AST ArithOp to IR ArithOp
                let ir_op = Self::convert_arith_op(op);
                Ok(IRExpression::Arithmetic {
                    op: ir_op,
                    left: Box::new(left_ir),
                    right: Box::new(right_ir),
                })
            }
        }
    }

    /// Convert AST ArithOp to IR ArithOp
    fn convert_arith_op(op: &crate::ast::ArithOp) -> crate::ir::ArithOp {
        use crate::ast::ArithOp as AstOp;
        use crate::ir::ArithOp as IrOp;
        match op {
            AstOp::Add => IrOp::Add,
            AstOp::Sub => IrOp::Sub,
            AstOp::Mul => IrOp::Mul,
            AstOp::Div => IrOp::Div,
            AstOp::Mod => IrOp::Mod,
        }
    }

    /// Build projection to match rule head
    /// If the head contains aggregates, builds an Aggregate IR node
    /// If the head contains arithmetic or constants, builds a Compute + Map pipeline
    fn build_projection(&self, input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        let input_schema = input.output_schema();
        let head = &rule.head;

        // Check if head contains any aggregates
        if head.has_aggregates() {
            return self.build_aggregation(input, rule);
        }

        // Check if head contains any arithmetic expressions or constants
        // Constants in the head need to be computed as new columns
        let has_constants = head.args.iter().any(|t| matches!(t, Term::Constant(_)));
        if head.has_arithmetic() || has_constants {
            return self.build_projection_with_computed(input, rule);
        }

        // Build output schema and projection mapping
        let mut projection = Vec::new();
        let mut output_schema = Vec::new();

        for (i, term) in head.args.iter().enumerate() {
            match term {
                Term::Variable(v) => {
                    // Find this variable in input schema
                    let pos = input_schema
                        .iter()
                        .position(|s| s == v)
                        .ok_or_else(|| format!("Variable {} not found in schema", v))?;

                    projection.push(pos);
                    output_schema.push(v.clone());
                }
                Term::Constant(_) => {
                    // Should not reach here - handled by has_constants check above
                    unreachable!("Constants should be handled by build_projection_with_computed");
                }
                Term::Placeholder => {
                    // Use positional name
                    projection.push(i);
                    output_schema.push(format!("col{}", i));
                }
                Term::Aggregate(_, _) => {
                    // Should not reach here - handled by has_aggregates() check above
                    unreachable!("Aggregates should be handled by build_aggregation");
                }
                Term::Arithmetic(_) => {
                    // Should not reach here - handled by has_arithmetic() check above
                    unreachable!("Arithmetic should be handled by build_projection_with_computed");
                }
                Term::FunctionCall(_, _) => {
                    // Function calls in head require computing a new column via Compute node
                    return Err("Function calls in rule head not yet fully supported. Use Compute node directly.".to_string());
                }
                Term::VectorLiteral(_) => {
                    // Vector literals in head - could be supported as constants
                    return Err("Vector literals in rule head not yet supported.".to_string());
                }
                Term::FloatConstant(_) => {
                    return Err("Float constants in rule head not yet supported.".to_string());
                }
                Term::StringConstant(_) => {
                    return Err("String constants in rule head not yet supported.".to_string());
                }
                Term::FieldAccess(_, _) => {
                    return Err("Field access in rule head not yet supported.".to_string());
                }
                Term::RecordPattern(_) => {
                    return Err("Record patterns in rule head not yet supported.".to_string());
                }
            }
        }

        // Check if projection is identity
        let is_identity = projection.iter().enumerate().all(|(i, &p)| i == p)
            && projection.len() == input_schema.len();

        if is_identity {
            // No projection needed
            Ok(input)
        } else {
            Ok(IRNode::Map {
                input: Box::new(input),
                projection,
                output_schema,
            })
        }
    }

    /// Build projection with computed expressions (arithmetic or constants) in the head
    /// Creates a Compute node to evaluate expressions, then a Map to project
    fn build_projection_with_computed(&self, input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        use crate::ir::IRExpression;

        let input_schema = input.output_schema();
        let head = &rule.head;

        if std::env::var("DATALOG_DEBUG").is_ok() {
            eprintln!("DEBUG build_projection_with_computed:");
            eprintln!("  input_schema = {:?}", input_schema);
            eprintln!("  head = {:?}", head.args);
        }

        // Collect: which head terms are variables (project) vs computed (arithmetic/constant)
        let mut compute_expressions: Vec<(String, IRExpression)> = Vec::new();
        let mut final_projection: Vec<usize> = Vec::new();
        let mut final_output_schema: Vec<String> = Vec::new();

        // Track the schema after computing new columns
        let mut extended_schema = input_schema.clone();

        for (head_idx, term) in head.args.iter().enumerate() {
            match term {
                Term::Variable(v) => {
                    // Find in input schema - project this column
                    let pos = input_schema.iter().position(|s| s == v).ok_or_else(|| {
                        format!("Variable '{}' not found in schema {:?}", v, input_schema)
                    })?;
                    final_projection.push(pos);
                    final_output_schema.push(v.clone());
                    if std::env::var("DATALOG_DEBUG").is_ok() {
                        eprintln!(
                            "  head[{}] Variable({}) -> project col {}",
                            head_idx, v, pos
                        );
                    }
                }
                Term::Arithmetic(expr) => {
                    // Convert AST expression to IR expression
                    let ir_expr = Self::arith_expr_to_ir_expression(expr, &input_schema)?;

                    // Generate a name for the computed column
                    let col_name = format!("_computed_{}", head_idx);
                    compute_expressions.push((col_name.clone(), ir_expr));

                    // The computed column will be appended at the end of extended schema
                    let computed_col_idx = extended_schema.len();
                    extended_schema.push(col_name.clone());
                    final_projection.push(computed_col_idx);
                    final_output_schema.push(col_name.clone());
                    if std::env::var("DATALOG_DEBUG").is_ok() {
                        eprintln!(
                            "  head[{}] Arithmetic -> compute col {} ({})",
                            head_idx, computed_col_idx, col_name
                        );
                    }
                }
                Term::Constant(val) => {
                    // Constants in head are computed as constant columns
                    let ir_expr = IRExpression::IntConstant(*val);

                    // Generate a name for the constant column
                    let col_name = format!("_const_{}", head_idx);
                    compute_expressions.push((col_name.clone(), ir_expr));

                    // The computed column will be appended at the end of extended schema
                    let computed_col_idx = extended_schema.len();
                    extended_schema.push(col_name.clone());
                    final_projection.push(computed_col_idx);
                    final_output_schema.push(col_name.clone());
                    if std::env::var("DATALOG_DEBUG").is_ok() {
                        eprintln!(
                            "  head[{}] Constant({}) -> compute col {} ({})",
                            head_idx, val, computed_col_idx, col_name
                        );
                    }
                }
                Term::Placeholder => {
                    // Placeholders in head don't make semantic sense - they indicate
                    // "don't care" but the head defines output columns. For now, skip them.
                    // This is a questionable Datalog construct.
                    if std::env::var("DATALOG_DEBUG").is_ok() {
                        eprintln!(
                            "  head[{}] Placeholder -> SKIPPED (invalid in head)",
                            head_idx
                        );
                    }
                    // Don't add anything to projection - placeholders in head are ignored
                    continue;
                }
                _ => {
                    return Err(format!(
                        "Unsupported term type in head with computed expressions: {:?}",
                        term
                    ));
                }
            }
        }

        if std::env::var("DATALOG_DEBUG").is_ok() {
            eprintln!("  extended_schema = {:?}", extended_schema);
            eprintln!("  final_projection = {:?}", final_projection);
            eprintln!("  final_output_schema = {:?}", final_output_schema);
        }

        // Build the Compute node if we have expressions
        let computed = if compute_expressions.is_empty() {
            input
        } else {
            IRNode::Compute {
                input: Box::new(input),
                expressions: compute_expressions,
            }
        };

        // Check if projection is identity on the extended schema
        let is_identity = final_projection.iter().enumerate().all(|(i, &p)| i == p)
            && final_projection.len() == extended_schema.len();

        if is_identity {
            Ok(computed)
        } else {
            Ok(IRNode::Map {
                input: Box::new(computed),
                projection: final_projection,
                output_schema: final_output_schema,
            })
        }
    }

    /// Build an Aggregate IR node for rules with aggregates in the head
    fn build_aggregation(&self, input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        use crate::ast::AggregateFunc;
        use crate::ir::AggregateFunction;

        let input_schema = input.output_schema();
        let head = &rule.head;

        // Separate group-by variables from aggregate terms
        let mut group_by = Vec::new();
        let mut aggregations = Vec::new();
        let mut output_schema = Vec::new();

        for term in &head.args {
            match term {
                Term::Variable(v) => {
                    // This is a group-by variable
                    let pos = input_schema
                        .iter()
                        .position(|s| s == v)
                        .ok_or_else(|| format!("Variable {} not found in schema", v))?;
                    group_by.push(pos);
                    output_schema.push(v.clone());
                }
                Term::Aggregate(func, var_name) => {
                    // This is an aggregate
                    let col_pos =
                        input_schema
                            .iter()
                            .position(|s| s == var_name)
                            .ok_or_else(|| {
                                format!("Variable {} not found in schema for aggregation", var_name)
                            })?;

                    let ir_func = match func {
                        AggregateFunc::Count => AggregateFunction::Count,
                        AggregateFunc::Sum => AggregateFunction::Sum,
                        AggregateFunc::Min => AggregateFunction::Min,
                        AggregateFunc::Max => AggregateFunction::Max,
                        AggregateFunc::Avg => AggregateFunction::Avg,
                        AggregateFunc::TopK { k, descending, .. } => AggregateFunction::TopK {
                            k: *k,
                            order_col: col_pos,
                            descending: *descending,
                        },
                        AggregateFunc::TopKThreshold {
                            k,
                            threshold,
                            descending,
                            ..
                        } => AggregateFunction::TopKThreshold {
                            k: *k,
                            order_col: col_pos,
                            threshold: *threshold,
                            descending: *descending,
                        },
                        AggregateFunc::WithinRadius { max_distance, .. } => {
                            AggregateFunction::WithinRadius {
                                distance_col: col_pos,
                                max_distance: *max_distance,
                            }
                        }
                    };

                    aggregations.push((ir_func, col_pos));
                    // Name the output column based on function
                    output_schema.push(format!("{}_{}", func_to_str(func), var_name));
                }
                Term::Constant(_) => {
                    return Err("Constants in aggregation head not supported".to_string());
                }
                Term::Placeholder => {
                    return Err("Placeholders in aggregation head not supported".to_string());
                }
                Term::Arithmetic(_) => {
                    return Err(
                        "Arithmetic expressions in aggregation head not yet supported".to_string(),
                    );
                }
                Term::FunctionCall(_, _) => {
                    return Err("Function calls in aggregation head not yet supported".to_string());
                }
                Term::VectorLiteral(_) => {
                    return Err("Vector literals in aggregation head not supported".to_string());
                }
                Term::FloatConstant(_) => {
                    return Err("Float constants in aggregation head not supported".to_string());
                }
                Term::StringConstant(_) => {
                    return Err("String constants in aggregation head not supported".to_string());
                }
                Term::FieldAccess(_, _) => {
                    return Err("Field access in aggregation head not supported".to_string());
                }
                Term::RecordPattern(_) => {
                    return Err("Record patterns in aggregation head not supported".to_string());
                }
            }
        }

        Ok(IRNode::Aggregate {
            input: Box::new(input),
            group_by,
            aggregations,
            output_schema,
        })
    }
}

/// Helper function to convert aggregate function to string
fn func_to_str(func: &crate::ast::AggregateFunc) -> &'static str {
    use crate::ast::AggregateFunc;
    match func {
        AggregateFunc::Count => "count",
        AggregateFunc::Sum => "sum",
        AggregateFunc::Min => "min",
        AggregateFunc::Max => "max",
        AggregateFunc::Avg => "avg",
        AggregateFunc::TopK { .. } => "top_k",
        AggregateFunc::TopKThreshold { .. } => "top_k_threshold",
        AggregateFunc::WithinRadius { .. } => "within_radius",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Atom;

    fn make_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.register_relation("edge".to_string(), vec!["x".to_string(), "y".to_string()]);
        catalog.register_relation("path".to_string(), vec!["x".to_string(), "y".to_string()]);
        catalog
    }

    #[test]
    fn test_build_scan() {
        let catalog = make_catalog();
        let builder = IRBuilder::new(catalog);

        let atom = Atom::new(
            "edge".to_string(),
            vec![
                Term::Variable("x".to_string()),
                Term::Variable("y".to_string()),
            ],
        );

        let ir = builder.build_scan(&atom).unwrap();
        match ir {
            IRNode::Scan { relation, schema } => {
                assert_eq!(relation, "edge");
                assert_eq!(schema, vec!["x", "y"]);
            }
            _ => panic!("Expected Scan node"),
        }
    }

    #[test]
    fn test_build_simple_rule() {
        let catalog = make_catalog();
        let builder = IRBuilder::new(catalog);

        // result(x, y) :- edge(x, y)
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
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
        );

        let ir = builder.build_ir(&rule).unwrap();

        // Should be just a scan (projection is identity)
        assert!(ir.is_scan());
    }

    #[test]
    fn test_build_join_rule() {
        let catalog = make_catalog();
        let builder = IRBuilder::new(catalog);

        // result(x, z) :- edge(x, y), edge(y, z)
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("z".to_string()),
                ],
            ),
            vec![
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("x".to_string()),
                        Term::Variable("y".to_string()),
                    ],
                ),
                Atom::new(
                    "edge".to_string(),
                    vec![
                        Term::Variable("y".to_string()),
                        Term::Variable("z".to_string()),
                    ],
                ),
            ],
            vec![],
        );

        let ir = builder.build_ir(&rule).unwrap();

        // Should contain a join (might be wrapped in a Map for projection)
        match &ir {
            IRNode::Join { .. } => {
                // Direct join
                assert!(true);
            }
            IRNode::Map { input, .. } => {
                // Join wrapped in projection
                assert!(input.is_join(), "Expected join inside map");
            }
            _ => {
                panic!("Expected Join or Map wrapping Join");
            }
        }
    }

    #[test]
    fn test_build_rule_with_ge_constraint() {
        let mut catalog = Catalog::new();
        catalog.register_relation("data".to_string(), vec!["x".to_string(), "y".to_string()]);
        let builder = IRBuilder::new(catalog);

        // result(x, y) :- data(x, y), x >= 2
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            ),
            vec![Atom::new(
                "data".to_string(),
                vec![
                    Term::Variable("x".to_string()),
                    Term::Variable("y".to_string()),
                ],
            )],
            vec![Constraint::GreaterOrEqual(
                Term::Variable("x".to_string()),
                Term::Constant(2),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(ir.is_ok(), "Expected successful IR build: {:?}", ir);
    }

    #[test]
    fn test_build_rule_with_string_equality() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "person".to_string(),
            vec!["id".to_string(), "name".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, name) :- person(id, name), name = "alice"
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("name".to_string()),
                ],
            ),
            vec![Atom::new(
                "person".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("name".to_string()),
                ],
            )],
            vec![Constraint::Equal(
                Term::Variable("name".to_string()),
                Term::StringConstant("alice".to_string()),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for string equality: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnEqStr predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnEqStr(_, s) if s == "alice"),
                    "Expected ColumnEqStr predicate with 'alice', got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for string equality, got {:?}", ir),
        }
    }

    #[test]
    fn test_build_rule_with_string_inequality() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "person".to_string(),
            vec!["id".to_string(), "name".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, name) :- person(id, name), name != "bob"
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("name".to_string()),
                ],
            ),
            vec![Atom::new(
                "person".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("name".to_string()),
                ],
            )],
            vec![Constraint::NotEqual(
                Term::Variable("name".to_string()),
                Term::StringConstant("bob".to_string()),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for string inequality: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnNeStr predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnNeStr(_, s) if s == "bob"),
                    "Expected ColumnNeStr predicate with 'bob', got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for string inequality, got {:?}", ir),
        }
    }

    #[test]
    fn test_build_rule_with_float_equality() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "measurement".to_string(),
            vec!["id".to_string(), "value".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, value) :- measurement(id, value), value = 3.14
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            ),
            vec![Atom::new(
                "measurement".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            )],
            vec![Constraint::Equal(
                Term::Variable("value".to_string()),
                Term::FloatConstant(3.14),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float equality: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnEqFloat predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnEqFloat(_, v) if (*v - 3.14).abs() < 0.001),
                    "Expected ColumnEqFloat predicate with 3.14, got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for float equality, got {:?}", ir),
        }
    }

    #[test]
    fn test_build_rule_with_float_comparison() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "measurement".to_string(),
            vec!["id".to_string(), "value".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, value) :- measurement(id, value), value > 0.5
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            ),
            vec![Atom::new(
                "measurement".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            )],
            vec![Constraint::GreaterThan(
                Term::Variable("value".to_string()),
                Term::FloatConstant(0.5),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float > comparison: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnGtFloat predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnGtFloat(_, v) if (*v - 0.5).abs() < 0.001),
                    "Expected ColumnGtFloat predicate with 0.5, got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for float > comparison, got {:?}", ir),
        }
    }

    #[test]
    fn test_build_rule_with_float_lt_comparison() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "measurement".to_string(),
            vec!["id".to_string(), "value".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, value) :- measurement(id, value), value < 10.0
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            ),
            vec![Atom::new(
                "measurement".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            )],
            vec![Constraint::LessThan(
                Term::Variable("value".to_string()),
                Term::FloatConstant(10.0),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float < comparison: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnLtFloat predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnLtFloat(_, v) if (*v - 10.0).abs() < 0.001),
                    "Expected ColumnLtFloat predicate with 10.0, got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for float < comparison, got {:?}", ir),
        }
    }

    #[test]
    fn test_build_rule_with_float_ge_comparison() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "measurement".to_string(),
            vec!["id".to_string(), "value".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, value) :- measurement(id, value), value >= 1.5
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            ),
            vec![Atom::new(
                "measurement".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            )],
            vec![Constraint::GreaterOrEqual(
                Term::Variable("value".to_string()),
                Term::FloatConstant(1.5),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float >= comparison: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnGeFloat predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnGeFloat(_, v) if (*v - 1.5).abs() < 0.001),
                    "Expected ColumnGeFloat predicate with 1.5, got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for float >= comparison, got {:?}", ir),
        }
    }

    #[test]
    fn test_build_rule_with_float_le_comparison() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "measurement".to_string(),
            vec!["id".to_string(), "value".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, value) :- measurement(id, value), value <= 100.0
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            ),
            vec![Atom::new(
                "measurement".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("value".to_string()),
                ],
            )],
            vec![Constraint::LessOrEqual(
                Term::Variable("value".to_string()),
                Term::FloatConstant(100.0),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float <= comparison: {:?}",
            ir
        );

        // Verify it creates a Filter with ColumnLeFloat predicate
        let ir = ir.unwrap();
        match &ir {
            IRNode::Filter { predicate, .. } => {
                assert!(
                    matches!(predicate, Predicate::ColumnLeFloat(_, v) if (*v - 100.0).abs() < 0.001),
                    "Expected ColumnLeFloat predicate with 100.0, got {:?}",
                    predicate
                );
            }
            _ => panic!("Expected Filter node for float <= comparison, got {:?}", ir),
        }
    }

    #[test]
    fn test_string_ordering_not_supported() {
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "person".to_string(),
            vec!["id".to_string(), "name".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // result(id, name) :- person(id, name), name > "alice"
        // This should fail because string ordering is not supported
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("name".to_string()),
                ],
            ),
            vec![Atom::new(
                "person".to_string(),
                vec![
                    Term::Variable("id".to_string()),
                    Term::Variable("name".to_string()),
                ],
            )],
            vec![Constraint::GreaterThan(
                Term::Variable("name".to_string()),
                Term::StringConstant("alice".to_string()),
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(ir.is_err(), "Expected error for string ordering comparison");
        let err = ir.unwrap_err();
        assert!(
            err.contains("ordering") || err.contains("string"),
            "Error should mention ordering or string: {}",
            err
        );
    }
}
