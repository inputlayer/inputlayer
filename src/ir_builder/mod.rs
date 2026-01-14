//! # IR Builder (Module 05)
//!
//! **Course Context**: Students implement this module in **Module 05: IR Construction**.
//!
//! This module teaches:
//! - Converting Abstract Syntax Tree (AST) to Intermediate Representation (IR)
//! - Logical query plan generation
//! - Schema inference and tracking through pipeline stages
//! - Join key inference from shared variables
//!
//! ## Learning Objectives (Module 05)
//!
//! Students learn to:
//! 1. Build IRNode trees from Datalog rules
//! 2. Generate Scan nodes for base relations
//! 3. Construct Join nodes with proper key inference
//! 4. Build Filter nodes from body predicates
//! 5. Generate Map nodes for projections
//! 6. Track schemas through all transformations
//! 7. Integrate with Catalog for schema management
//!
//! ## Key Concepts
//!
//! - **Logical Query Plan**: High-level representation of query operations
//! - **Schema Propagation**: Maintaining variable bindings through pipeline
//! - **Join Key Inference**: Determining join columns from shared variables
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

use crate::ast::{Atom, BodyPredicate, BuiltinFunc, ComparisonOp, Rule, Term};
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

        // 3. Apply computed columns (function calls in body)
        current = self.build_computed_columns(current, rule)?;

        // 4. Apply comparison filters (X = Y, X < 5, etc.)
        current = self.build_comparison_filters(current, rule)?;

        // 5. Apply antijoins for negated predicates
        current = self.build_antijoins(current, rule)?;

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
                // Also handles string constants like user(X, "admin") and floats like price(X, 9.99)
                for (i, term) in atom.args.iter().enumerate() {
                    match term {
                        Term::Constant(val) => {
                            scan = IRNode::Filter {
                                input: Box::new(scan),
                                predicate: Predicate::ColumnEqConst(i, *val),
                            };
                        }
                        Term::StringConstant(s) => {
                            scan = IRNode::Filter {
                                input: Box::new(scan),
                                predicate: Predicate::ColumnEqStr(i, s.clone()),
                            };
                        }
                        Term::FloatConstant(f) => {
                            scan = IRNode::Filter {
                                input: Box::new(scan),
                                predicate: Predicate::ColumnEqFloat(i, *f),
                            };
                        }
                        _ => {} // Variables, placeholders, aggregates, etc. - no filter needed
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
        // Also handles string constants like !blocked(X, "admin") and floats
        for (i, term) in negated_atom.args.iter().enumerate() {
            match term {
                Term::Constant(val) => {
                    right = IRNode::Filter {
                        input: Box::new(right),
                        predicate: Predicate::ColumnEqConst(i, *val),
                    };
                }
                Term::StringConstant(s) => {
                    right = IRNode::Filter {
                        input: Box::new(right),
                        predicate: Predicate::ColumnEqStr(i, s.clone()),
                    };
                }
                Term::FloatConstant(f) => {
                    right = IRNode::Filter {
                        input: Box::new(right),
                        predicate: Predicate::ColumnEqFloat(i, *f),
                    };
                }
                _ => {} // Variables, placeholders, etc. - no filter needed
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

    /// Build computed columns for function call assignments
    ///
    /// Handles comparisons like `Dist = euclidean(V, Q)` by creating a Compute node
    /// that adds the computed column to the schema.
    fn build_computed_columns(&self, input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        let mut expressions = Vec::new();
        // Track schema progressively - each computed column extends the schema for subsequent ones
        let mut schema = input.output_schema();

        for pred in &rule.body {
            if let BodyPredicate::Comparison(left, op, right) = pred {
                // Only process function call assignments (Y = func(X))
                let (var_name, func, args) = match (left, op, right) {
                    (Term::Variable(v), ComparisonOp::Equal, Term::FunctionCall(f, a)) => {
                        (v, f, a)
                    }
                    (Term::FunctionCall(f, a), ComparisonOp::Equal, Term::Variable(v)) => {
                        (v, f, a)
                    }
                    _ => continue,
                };

                // Convert AST function to IR function
                let ir_func = Self::ast_func_to_ir_func(func)?;

                // Convert AST arguments to IR expressions using current (progressive) schema
                let ir_args: Vec<IRExpression> = args
                    .iter()
                    .map(|term| Self::term_to_ir_expr(term, &schema))
                    .collect::<Result<Vec<_>, _>>()?;

                expressions.push((var_name.clone(), IRExpression::FunctionCall(ir_func, ir_args)));

                // Extend schema with the newly computed column for subsequent expressions
                schema.push(var_name.clone());
            }
        }

        if expressions.is_empty() {
            // No function call assignments, return input unchanged
            Ok(input)
        } else {
            Ok(IRNode::Compute {
                input: Box::new(input),
                expressions,
            })
        }
    }

    /// Convert AST BuiltinFunc to IR BuiltinFunction
    fn ast_func_to_ir_func(func: &BuiltinFunc) -> Result<BuiltinFunction, String> {
        match func {
            // Distance functions
            BuiltinFunc::Euclidean => Ok(BuiltinFunction::Euclidean),
            BuiltinFunc::Cosine => Ok(BuiltinFunction::Cosine),
            BuiltinFunc::DotProduct => Ok(BuiltinFunction::DotProduct),
            BuiltinFunc::Manhattan => Ok(BuiltinFunction::Manhattan),
            // LSH functions
            BuiltinFunc::LshBucket => Ok(BuiltinFunction::LshBucket),
            BuiltinFunc::LshProbes => Ok(BuiltinFunction::LshProbes),
            BuiltinFunc::LshMultiProbe => Ok(BuiltinFunction::LshMultiProbe),
            // Vector operations
            BuiltinFunc::VecNormalize => Ok(BuiltinFunction::VecNormalize),
            BuiltinFunc::VecDim => Ok(BuiltinFunction::VecDim),
            BuiltinFunc::VecAdd => Ok(BuiltinFunction::VecAdd),
            BuiltinFunc::VecScale => Ok(BuiltinFunction::VecScale),
            // Temporal functions
            BuiltinFunc::TimeNow => Ok(BuiltinFunction::TimeNow),
            BuiltinFunc::TimeDiff => Ok(BuiltinFunction::TimeDiff),
            BuiltinFunc::TimeAdd => Ok(BuiltinFunction::TimeAdd),
            BuiltinFunc::TimeSub => Ok(BuiltinFunction::TimeSub),
            BuiltinFunc::TimeDecay => Ok(BuiltinFunction::TimeDecay),
            BuiltinFunc::TimeDecayLinear => Ok(BuiltinFunction::TimeDecayLinear),
            BuiltinFunc::TimeBefore => Ok(BuiltinFunction::TimeBefore),
            BuiltinFunc::TimeAfter => Ok(BuiltinFunction::TimeAfter),
            BuiltinFunc::TimeBetween => Ok(BuiltinFunction::TimeBetween),
            BuiltinFunc::WithinLast => Ok(BuiltinFunction::WithinLast),
            BuiltinFunc::IntervalsOverlap => Ok(BuiltinFunction::IntervalsOverlap),
            BuiltinFunc::IntervalContains => Ok(BuiltinFunction::IntervalContains),
            BuiltinFunc::IntervalDuration => Ok(BuiltinFunction::IntervalDuration),
            BuiltinFunc::PointInInterval => Ok(BuiltinFunction::PointInInterval),
            // Quantization functions
            BuiltinFunc::QuantizeLinear => Ok(BuiltinFunction::QuantizeLinear),
            BuiltinFunc::QuantizeSymmetric => Ok(BuiltinFunction::QuantizeSymmetric),
            BuiltinFunc::Dequantize => Ok(BuiltinFunction::Dequantize),
            BuiltinFunc::DequantizeScaled => Ok(BuiltinFunction::DequantizeScaled),
            // Int8 distance functions
            BuiltinFunc::EuclideanInt8 => Ok(BuiltinFunction::EuclideanInt8),
            BuiltinFunc::CosineInt8 => Ok(BuiltinFunction::CosineInt8),
            BuiltinFunc::DotProductInt8 => Ok(BuiltinFunction::DotProductInt8),
            BuiltinFunc::ManhattanInt8 => Ok(BuiltinFunction::ManhattanInt8),
            // Math utility functions
            BuiltinFunc::AbsInt64 => Ok(BuiltinFunction::AbsInt64),
            BuiltinFunc::AbsFloat64 => Ok(BuiltinFunction::AbsFloat64),
        }
    }

    /// Convert AST Term to IR Expression
    fn term_to_ir_expr(term: &Term, schema: &[String]) -> Result<IRExpression, String> {
        match term {
            Term::Variable(name) => {
                let col_idx = schema
                    .iter()
                    .position(|s| s == name)
                    .ok_or_else(|| format!("Variable '{}' not found in schema {:?}", name, schema))?;
                Ok(IRExpression::Column(col_idx))
            }
            Term::Constant(val) => Ok(IRExpression::IntConstant(*val)),
            Term::FloatConstant(val) => Ok(IRExpression::FloatConstant(*val)),
            Term::StringConstant(s) => Ok(IRExpression::StringConstant(s.clone())),
            Term::VectorLiteral(v) => {
                // Convert f64 to f32 for IR representation
                let v32: Vec<f32> = v.iter().map(|&x| x as f32).collect();
                Ok(IRExpression::VectorLiteral(v32))
            }
            Term::FunctionCall(func, args) => {
                let ir_func = Self::ast_func_to_ir_func(func)?;
                let ir_args: Vec<IRExpression> = args
                    .iter()
                    .map(|t| Self::term_to_ir_expr(t, schema))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(IRExpression::FunctionCall(ir_func, ir_args))
            }
            _ => Err(format!("Unsupported term type in expression: {:?}", term)),
        }
    }

    /// Build filter nodes for comparison predicates in the rule body
    ///
    /// Handles predicates like X = Y, X != 5, X < Y, etc.
    /// Skips function call assignments which are handled by build_computed_columns.
    fn build_comparison_filters(&self, mut input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        let schema = input.output_schema();

        for pred in &rule.body {
            if let BodyPredicate::Comparison(left, op, right) = pred {
                // Skip function call assignments - they're handled in build_computed_columns
                if Self::is_function_call_assignment(left, op, right) {
                    continue;
                }

                let predicate = self.comparison_to_predicate(left, op, right, &schema)?;
                input = IRNode::Filter {
                    input: Box::new(input),
                    predicate,
                };
            }
        }

        Ok(input)
    }

    /// Check if a comparison is a function call assignment (e.g., Y = abs_int64(X))
    fn is_function_call_assignment(left: &Term, op: &ComparisonOp, right: &Term) -> bool {
        if !matches!(op, ComparisonOp::Equal) {
            return false;
        }
        matches!(
            (left, right),
            (Term::Variable(_), Term::FunctionCall(_, _))
                | (Term::FunctionCall(_, _), Term::Variable(_))
        )
    }

    /// Convert a comparison predicate to an IR Predicate
    fn comparison_to_predicate(
        &self,
        left: &Term,
        op: &ComparisonOp,
        right: &Term,
        schema: &[String],
    ) -> Result<Predicate, String> {
        // Get column index for a variable
        let get_col = |name: &str| -> Result<usize, String> {
            schema
                .iter()
                .position(|s| s == name)
                .ok_or_else(|| format!("Variable '{}' not found in schema {:?}", name, schema))
        };

        match (left, right) {
            // Variable vs Variable: X = Y, X < Y, etc.
            (Term::Variable(left_var), Term::Variable(right_var)) => {
                let left_col = get_col(left_var)?;
                let right_col = get_col(right_var)?;
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnsEq(left_col, right_col)),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnsNe(left_col, right_col)),
                    ComparisonOp::LessThan => Ok(Predicate::ColumnsLt(left_col, right_col)),
                    ComparisonOp::LessOrEqual => Ok(Predicate::ColumnsLe(left_col, right_col)),
                    ComparisonOp::GreaterThan => Ok(Predicate::ColumnsGt(left_col, right_col)),
                    ComparisonOp::GreaterOrEqual => Ok(Predicate::ColumnsGe(left_col, right_col)),
                }
            }
            // Variable vs Integer constant: X = 5, X < 10, etc.
            (Term::Variable(var), Term::Constant(val)) => {
                let col = get_col(var)?;
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnEqConst(col, *val)),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnNeConst(col, *val)),
                    ComparisonOp::LessThan => Ok(Predicate::ColumnLtConst(col, *val)),
                    ComparisonOp::LessOrEqual => Ok(Predicate::ColumnLeConst(col, *val)),
                    ComparisonOp::GreaterThan => Ok(Predicate::ColumnGtConst(col, *val)),
                    ComparisonOp::GreaterOrEqual => Ok(Predicate::ColumnGeConst(col, *val)),
                }
            }
            // Integer constant vs Variable: 5 = X, 10 > X, etc. (swap operands)
            (Term::Constant(val), Term::Variable(var)) => {
                let col = get_col(var)?;
                // Swap the operation: 5 < X becomes X > 5
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnEqConst(col, *val)),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnNeConst(col, *val)),
                    ComparisonOp::LessThan => Ok(Predicate::ColumnGtConst(col, *val)),
                    ComparisonOp::LessOrEqual => Ok(Predicate::ColumnGeConst(col, *val)),
                    ComparisonOp::GreaterThan => Ok(Predicate::ColumnLtConst(col, *val)),
                    ComparisonOp::GreaterOrEqual => Ok(Predicate::ColumnLeConst(col, *val)),
                }
            }
            // Variable vs Float constant
            (Term::Variable(var), Term::FloatConstant(val)) => {
                let col = get_col(var)?;
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnEqFloat(col, *val)),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnNeFloat(col, *val)),
                    ComparisonOp::LessThan => Ok(Predicate::ColumnLtFloat(col, *val)),
                    ComparisonOp::LessOrEqual => Ok(Predicate::ColumnLeFloat(col, *val)),
                    ComparisonOp::GreaterThan => Ok(Predicate::ColumnGtFloat(col, *val)),
                    ComparisonOp::GreaterOrEqual => Ok(Predicate::ColumnGeFloat(col, *val)),
                }
            }
            // Float constant vs Variable (swap operands)
            (Term::FloatConstant(val), Term::Variable(var)) => {
                let col = get_col(var)?;
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnEqFloat(col, *val)),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnNeFloat(col, *val)),
                    ComparisonOp::LessThan => Ok(Predicate::ColumnGtFloat(col, *val)),
                    ComparisonOp::LessOrEqual => Ok(Predicate::ColumnGeFloat(col, *val)),
                    ComparisonOp::GreaterThan => Ok(Predicate::ColumnLtFloat(col, *val)),
                    ComparisonOp::GreaterOrEqual => Ok(Predicate::ColumnLeFloat(col, *val)),
                }
            }
            // Variable vs String constant (only = and != supported)
            (Term::Variable(var), Term::StringConstant(s)) => {
                let col = get_col(var)?;
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnEqStr(col, s.clone())),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnNeStr(col, s.clone())),
                    _ => Err(format!(
                        "Comparison {:?} not supported for string constants",
                        op
                    )),
                }
            }
            // String constant vs Variable (swap operands)
            (Term::StringConstant(s), Term::Variable(var)) => {
                let col = get_col(var)?;
                match op {
                    ComparisonOp::Equal => Ok(Predicate::ColumnEqStr(col, s.clone())),
                    ComparisonOp::NotEqual => Ok(Predicate::ColumnNeStr(col, s.clone())),
                    _ => Err(format!(
                        "Comparison {:?} not supported for string constants",
                        op
                    )),
                }
            }
            _ => Err(format!(
                "Unsupported comparison: {:?} {:?} {:?}",
                left, op, right
            )),
        }
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
        let has_constants = head.args.iter().any(|t| {
            matches!(
                t,
                Term::Constant(_) | Term::FloatConstant(_) | Term::StringConstant(_)
            )
        });
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
                    // Should not reach here - handled by has_constants check above
                    unreachable!("Float constants should be handled by build_projection_with_computed");
                }
                Term::StringConstant(_) => {
                    // Should not reach here - handled by has_constants check above
                    unreachable!("String constants should be handled by build_projection_with_computed");
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
                Term::FloatConstant(val) => {
                    // Float constants in head are computed as constant columns
                    let ir_expr = IRExpression::FloatConstant(*val);

                    // Generate a name for the constant column
                    let col_name = format!("_fconst_{}", head_idx);
                    compute_expressions.push((col_name.clone(), ir_expr));

                    // The computed column will be appended at the end of extended schema
                    let computed_col_idx = extended_schema.len();
                    extended_schema.push(col_name.clone());
                    final_projection.push(computed_col_idx);
                    final_output_schema.push(col_name.clone());
                    if std::env::var("DATALOG_DEBUG").is_ok() {
                        eprintln!(
                            "  head[{}] FloatConstant({}) -> compute col {} ({})",
                            head_idx, val, computed_col_idx, col_name
                        );
                    }
                }
                Term::StringConstant(s) => {
                    // String constants in head are computed as constant columns
                    let ir_expr = IRExpression::StringConstant(s.clone());

                    // Generate a name for the constant column
                    let col_name = format!("_sconst_{}", head_idx);
                    compute_expressions.push((col_name.clone(), ir_expr));

                    // The computed column will be appended at the end of extended schema
                    let computed_col_idx = extended_schema.len();
                    extended_schema.push(col_name.clone());
                    final_projection.push(computed_col_idx);
                    final_output_schema.push(col_name.clone());
                    if std::env::var("DATALOG_DEBUG").is_ok() {
                        eprintln!(
                            "  head[{}] StringConstant({}) -> compute col {} ({})",
                            head_idx, s, computed_col_idx, col_name
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

        // Check if this is a ranking aggregate (TopK, TopKThreshold, WithinRadius)
        // Ranking aggregates should NOT group by head variables - they select globally
        let has_ranking_agg = head.args.iter().any(|term| {
            matches!(
                term,
                Term::Aggregate(
                    AggregateFunc::TopK { .. }
                        | AggregateFunc::TopKThreshold { .. }
                        | AggregateFunc::WithinRadius { .. },
                    _
                )
            )
        });

        // Separate group-by variables from aggregate terms
        let mut group_by = Vec::new();
        let mut aggregations = Vec::new();
        let mut output_schema = Vec::new();

        for term in &head.args {
            match term {
                Term::Variable(v) => {
                    // Find the column position for this variable
                    let pos = input_schema
                        .iter()
                        .position(|s| s == v)
                        .ok_or_else(|| format!("Variable {} not found in schema", v))?;

                    // For ranking aggregates, don't add to group_by (process globally)
                    // For standard aggregates, this is a group-by variable
                    if !has_ranking_agg {
                        group_by.push(pos);
                    }
                    output_schema.push(v.clone());
                }
                Term::Aggregate(func, var_name) => {
                    // Handle ranking aggregates specially - they store their order variable
                    // internally and don't use the var_name parameter (which is empty)
                    let (ir_func, agg_col_pos, agg_var_name) = match func {
                        // Standard aggregates use var_name
                        AggregateFunc::Count | AggregateFunc::Sum | AggregateFunc::Min
                        | AggregateFunc::Max | AggregateFunc::Avg => {
                            let col_pos = input_schema
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
                                _ => unreachable!(),
                            };
                            (ir_func, col_pos, var_name.clone())
                        }
                        // Ranking aggregates extract order_var from the function itself
                        AggregateFunc::TopK { k, order_var, descending } => {
                            let order_col = input_schema
                                .iter()
                                .position(|s| s == order_var)
                                .ok_or_else(|| {
                                    format!("Variable {} not found in schema for top_k", order_var)
                                })?;
                            (
                                AggregateFunction::TopK {
                                    k: *k,
                                    order_col,
                                    descending: *descending,
                                },
                                order_col,
                                order_var.clone(),
                            )
                        }
                        AggregateFunc::TopKThreshold {
                            k,
                            order_var,
                            threshold,
                            descending,
                        } => {
                            let order_col = input_schema
                                .iter()
                                .position(|s| s == order_var)
                                .ok_or_else(|| {
                                    format!("Variable {} not found in schema for top_k_threshold", order_var)
                                })?;
                            (
                                AggregateFunction::TopKThreshold {
                                    k: *k,
                                    order_col,
                                    threshold: *threshold,
                                    descending: *descending,
                                },
                                order_col,
                                order_var.clone(),
                            )
                        }
                        AggregateFunc::WithinRadius { distance_var, max_distance } => {
                            let dist_col = input_schema
                                .iter()
                                .position(|s| s == distance_var)
                                .ok_or_else(|| {
                                    format!("Variable {} not found in schema for within_radius", distance_var)
                                })?;
                            (
                                AggregateFunction::WithinRadius {
                                    distance_col: dist_col,
                                    max_distance: *max_distance,
                                },
                                dist_col,
                                distance_var.clone(),
                            )
                        }
                    };

                    aggregations.push((ir_func, agg_col_pos));
                    // Name the output column
                    // For ranking aggregates, use just the variable name (since output is the value)
                    // For standard aggregates, use func_var format (e.g., "count_X")
                    if has_ranking_agg {
                        output_schema.push(agg_var_name);
                    } else {
                        output_schema.push(format!("{}_{}", func_to_str(func), agg_var_name));
                    }
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
    fn test_string_constant_in_body_atom() {
        // Tests that string constants in body atoms create proper filters
        // e.g., active(Id, Name) :- user(Id, Name, "true")
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "user".to_string(),
            vec!["id".to_string(), "name".to_string(), "active".to_string()],
        );
        let builder = IRBuilder::new(catalog);

        // active(Id, Name) :- user(Id, Name, "true")
        let rule = Rule::new_simple(
            Atom::new(
                "active".to_string(),
                vec![
                    Term::Variable("Id".to_string()),
                    Term::Variable("Name".to_string()),
                ],
            ),
            vec![Atom::new(
                "user".to_string(),
                vec![
                    Term::Variable("Id".to_string()),
                    Term::Variable("Name".to_string()),
                    Term::StringConstant("true".to_string()),
                ],
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for string constant in body atom: {:?}",
            ir
        );

        // The IR should contain a Filter with ColumnEqStr predicate
        let ir = ir.unwrap();
        fn contains_string_filter(node: &IRNode) -> bool {
            match node {
                IRNode::Filter { predicate, input } => {
                    if matches!(predicate, Predicate::ColumnEqStr(2, s) if s == "true") {
                        return true;
                    }
                    contains_string_filter(input)
                }
                IRNode::Map { input, .. } => contains_string_filter(input),
                IRNode::Join { left, right, .. } => {
                    contains_string_filter(left) || contains_string_filter(right)
                }
                IRNode::Distinct { input } => contains_string_filter(input),
                _ => false,
            }
        }

        assert!(
            contains_string_filter(&ir),
            "Expected IR to contain ColumnEqStr filter for 'true', got: {:?}",
            ir
        );
    }

    #[test]
    fn test_integer_constant_in_head() {
        // Tests that integer constants in rule heads create Compute nodes
        // e.g., result(X, 42) :- data(X)
        let mut catalog = Catalog::new();
        catalog.register_relation("data".to_string(), vec!["x".to_string()]);

        let builder = IRBuilder::new(catalog);

        // result(X, 42) :- data(X).
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Constant(42),
                ],
            ),
            vec![Atom::new("data".to_string(), vec![Term::Variable("X".to_string())])],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for integer constant in head: {:?}",
            ir
        );

        // The IR should contain a Compute node with IntConstant expression
        let ir = ir.unwrap();
        fn contains_compute_int_constant(node: &IRNode) -> bool {
            match node {
                IRNode::Compute { expressions, input } => {
                    let has_int_const = expressions.iter().any(|(_, expr)| {
                        matches!(expr, crate::ir::IRExpression::IntConstant(42))
                    });
                    has_int_const || contains_compute_int_constant(input)
                }
                IRNode::Map { input, .. } => contains_compute_int_constant(input),
                IRNode::Filter { input, .. } => contains_compute_int_constant(input),
                IRNode::Distinct { input } => contains_compute_int_constant(input),
                _ => false,
            }
        }

        assert!(
            contains_compute_int_constant(&ir),
            "Expected IR to contain Compute with IntConstant(42), got: {:?}",
            ir
        );
    }

    #[test]
    fn test_float_constant_in_head() {
        // Tests that float constants in rule heads create Compute nodes
        // e.g., result(X, 3.14) :- data(X)
        let mut catalog = Catalog::new();
        catalog.register_relation("data".to_string(), vec!["x".to_string()]);

        let builder = IRBuilder::new(catalog);

        // result(X, 3.14) :- data(X).
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::FloatConstant(3.14),
                ],
            ),
            vec![Atom::new("data".to_string(), vec![Term::Variable("X".to_string())])],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float constant in head: {:?}",
            ir
        );

        // The IR should contain a Compute node with FloatConstant expression
        let ir = ir.unwrap();
        fn contains_compute_float_constant(node: &IRNode) -> bool {
            match node {
                IRNode::Compute { expressions, input } => {
                    let has_float_const = expressions.iter().any(|(_, expr)| {
                        matches!(expr, crate::ir::IRExpression::FloatConstant(f) if (*f - 3.14).abs() < 0.001)
                    });
                    has_float_const || contains_compute_float_constant(input)
                }
                IRNode::Map { input, .. } => contains_compute_float_constant(input),
                IRNode::Filter { input, .. } => contains_compute_float_constant(input),
                IRNode::Distinct { input } => contains_compute_float_constant(input),
                _ => false,
            }
        }

        assert!(
            contains_compute_float_constant(&ir),
            "Expected IR to contain Compute with FloatConstant(3.14), got: {:?}",
            ir
        );
    }

    #[test]
    fn test_string_constant_in_head() {
        // Tests that string constants in rule heads create Compute nodes
        // e.g., result(X, "active") :- data(X)
        let mut catalog = Catalog::new();
        catalog.register_relation("data".to_string(), vec!["x".to_string()]);

        let builder = IRBuilder::new(catalog);

        // result(X, "active") :- data(X).
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::StringConstant("active".to_string()),
                ],
            ),
            vec![Atom::new("data".to_string(), vec![Term::Variable("X".to_string())])],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for string constant in head: {:?}",
            ir
        );

        // The IR should contain a Compute node with StringConstant expression
        let ir = ir.unwrap();
        fn contains_compute_string_constant(node: &IRNode) -> bool {
            match node {
                IRNode::Compute { expressions, input } => {
                    let has_str_const = expressions.iter().any(|(_, expr)| {
                        matches!(expr, crate::ir::IRExpression::StringConstant(s) if s == "active")
                    });
                    has_str_const || contains_compute_string_constant(input)
                }
                IRNode::Map { input, .. } => contains_compute_string_constant(input),
                IRNode::Filter { input, .. } => contains_compute_string_constant(input),
                IRNode::Distinct { input } => contains_compute_string_constant(input),
                _ => false,
            }
        }

        assert!(
            contains_compute_string_constant(&ir),
            "Expected IR to contain Compute with StringConstant(\"active\"), got: {:?}",
            ir
        );
    }

    #[test]
    fn test_mixed_constants_in_head() {
        // Tests that mixed constant types in rule heads all work together
        // e.g., result(X, 42, 3.14, "label") :- data(X)
        let mut catalog = Catalog::new();
        catalog.register_relation("data".to_string(), vec!["x".to_string()]);

        let builder = IRBuilder::new(catalog);

        // result(X, 42, 3.14, "label") :- data(X).
        let rule = Rule::new_simple(
            Atom::new(
                "result".to_string(),
                vec![
                    Term::Variable("X".to_string()),
                    Term::Constant(42),
                    Term::FloatConstant(3.14),
                    Term::StringConstant("label".to_string()),
                ],
            ),
            vec![Atom::new("data".to_string(), vec![Term::Variable("X".to_string())])],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for mixed constants in head: {:?}",
            ir
        );

        // The IR should contain a Compute node with all three constant types
        let ir = ir.unwrap();
        fn count_constants(node: &IRNode) -> (bool, bool, bool) {
            match node {
                IRNode::Compute { expressions, input } => {
                    let has_int = expressions.iter().any(|(_, expr)| {
                        matches!(expr, crate::ir::IRExpression::IntConstant(42))
                    });
                    let has_float = expressions.iter().any(|(_, expr)| {
                        matches!(expr, crate::ir::IRExpression::FloatConstant(f) if (*f - 3.14).abs() < 0.001)
                    });
                    let has_str = expressions.iter().any(|(_, expr)| {
                        matches!(expr, crate::ir::IRExpression::StringConstant(s) if s == "label")
                    });
                    let (i, f, s) = count_constants(input);
                    (has_int || i, has_float || f, has_str || s)
                }
                IRNode::Map { input, .. } => count_constants(input),
                IRNode::Filter { input, .. } => count_constants(input),
                IRNode::Distinct { input } => count_constants(input),
                _ => (false, false, false),
            }
        }

        let (has_int, has_float, has_str) = count_constants(&ir);
        assert!(has_int, "Expected IR to contain IntConstant(42)");
        assert!(has_float, "Expected IR to contain FloatConstant(3.14)");
        assert!(has_str, "Expected IR to contain StringConstant(\"label\")");
    }

    #[test]
    fn test_float_constant_in_body_atom() {
        // Tests that float constants in body atoms create proper ColumnEqFloat filters
        // e.g., cheap(Id, Name) :- product(Id, Name, 9.99)
        let mut catalog = Catalog::new();
        catalog.register_relation(
            "product".to_string(),
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        let builder = IRBuilder::new(catalog);

        // cheap(Id, Name) :- product(Id, Name, 9.99)
        let rule = Rule::new_simple(
            Atom::new(
                "cheap".to_string(),
                vec![
                    Term::Variable("Id".to_string()),
                    Term::Variable("Name".to_string()),
                ],
            ),
            vec![Atom::new(
                "product".to_string(),
                vec![
                    Term::Variable("Id".to_string()),
                    Term::Variable("Name".to_string()),
                    Term::FloatConstant(9.99),
                ],
            )],
        );

        let ir = builder.build_ir(&rule);
        assert!(
            ir.is_ok(),
            "Expected successful IR build for float constant in body atom: {:?}",
            ir
        );

        // The IR should contain a Filter with ColumnEqFloat predicate
        let ir = ir.unwrap();
        fn contains_float_filter(node: &IRNode) -> bool {
            match node {
                IRNode::Filter { predicate, input } => {
                    if matches!(predicate, Predicate::ColumnEqFloat(2, f) if (*f - 9.99).abs() < 0.001) {
                        return true;
                    }
                    contains_float_filter(input)
                }
                IRNode::Map { input, .. } => contains_float_filter(input),
                IRNode::Join { left, right, .. } => {
                    contains_float_filter(left) || contains_float_filter(right)
                }
                IRNode::Distinct { input } => contains_float_filter(input),
                _ => false,
            }
        }

        assert!(
            contains_float_filter(&ir),
            "Expected IR to contain ColumnEqFloat filter for 9.99, got: {:?}",
            ir
        );
    }
}
