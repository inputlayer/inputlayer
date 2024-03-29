//! # IR Builder
//!
//! AST -> IR conversion. Builds `IRNode` trees from Datalog rules:
//! Scan for base relations, Join with key inference from shared variables,
//! Filter from body predicates, Map for projections.
//!
//! Tracks schemas through all transformations and uses the Catalog for
//! relation lookups.
//!
//! ```text
//! AST (Program/Rules) -> [IR Builder] -> IRNode tree -> Optimizer
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
        if std::env::var("IL_DEBUG").is_ok() {
            eprintln!("DEBUG IR build: first scan = {:?}", current.output_schema());
        }
        for scan in scans {
            if std::env::var("IL_DEBUG").is_ok() {
                eprintln!(
                    "DEBUG IR build: joining with scan = {:?}",
                    scan.output_schema()
                );
            }
            current = self.build_join(current, scan)?;
            if std::env::var("IL_DEBUG").is_ok() {
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

        for (atom_idx, pred) in rule.body.iter().enumerate() {
            if let BodyPredicate::Positive(atom) = pred {
                let mut scan = self.build_scan(atom, atom_idx)?;

                // Apply filters for any constants in positive body atoms
                // For example, color(X, 0) needs to filter color to only rows where col1 == 0
                // Also handles string constants like user(X, "admin") and floats like price(X, 9.99)
                for (i, term) in atom.args.iter().cloned().enumerate() {
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

                // Apply equality filters for repeated variables in the same atom.
                // For example, edge(X, X) must filter to rows where col0 == col1.
                let mut seen_vars: Vec<(usize, &str)> = Vec::new();
                for (i, term) in atom.args.iter().enumerate() {
                    if let Term::Variable(v) = term {
                        if let Some((first_idx, _)) =
                            seen_vars.iter().find(|(_, name)| *name == v.as_str())
                        {
                            scan = IRNode::Filter {
                                input: Box::new(scan),
                                predicate: Predicate::ColumnsEq(*first_idx, i),
                            };
                        }
                        seen_vars.push((i, v));
                    }
                }

                scans.push(scan);
            }
        }

        Ok(scans)
    }

    /// Build a single scan node
    ///
    /// `atom_idx` is the index of the body predicate, used to generate unique
    /// column names for constants. This prevents naming collisions when the same
    /// relation appears multiple times with different constants (e.g., self-joins).
    fn build_scan(&self, atom: &Atom, atom_idx: usize) -> Result<IRNode, String> {
        // Schema comes from the atom's arguments (variable bindings)
        // Each occurrence of the same relation can have different variable names
        let schema: Vec<String> = atom
            .args
            .iter()
            .enumerate()
            .map(|(i, term)| match term {
                Term::Variable(v) => v.clone(),
                Term::Constant(_) => format!("_const_a{atom_idx}_c{i}"),
                Term::Placeholder => format!("_ph_{}_{}", atom.relation, i),
                // Aggregates in body atoms refer to the variable they aggregate
                Term::Aggregate(_, v) => v.clone(),
                // Arithmetic expressions - use the variables they reference
                Term::Arithmetic(expr) => {
                    // Use the first variable referenced, or generate a name
                    let vars = expr.variables();
                    vars.into_iter()
                        .next()
                        .unwrap_or_else(|| format!("expr{i}"))
                }
                // Function calls - generate a name
                Term::FunctionCall(_, _) => format!("func{i}"),
                // Vector literals - generate a name
                Term::VectorLiteral(_) => format!("vec{i}"),
                // Float constants - generate a name
                Term::FloatConstant(_) => format!("_float_a{atom_idx}_c{i}"),
                // String constants - generate a name
                Term::StringConstant(_) => format!("_str_a{atom_idx}_c{i}"),
                // Field access - use the field name
                Term::FieldAccess(_, field) => field.clone(),
                // Record pattern - generate a name
                Term::RecordPattern(_) => format!("rec{i}"),
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
            .map(|s| (*s).clone())
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
        for (pred_idx, pred) in rule.body.iter().enumerate() {
            if let BodyPredicate::Negated(atom) = pred {
                input = self.build_antijoin(input, atom, rule, pred_idx)?;
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
        atom_idx: usize,
    ) -> Result<IRNode, String> {
        // 1. Build scan for the negated relation
        let mut right = self.build_scan(negated_atom, atom_idx)?;

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
            // Skip generated column names for constants/placeholders/expressions
            if right_col.starts_with("_const_")
                || right_col.starts_with("_str_")
                || right_col.starts_with("_float_")
                || right_col.starts_with("_ph_")
                || right_col.starts_with("expr")
                || right_col.starts_with("func")
                || right_col.starts_with("vec")
                || right_col.starts_with("rec")
            {
                continue;
            }

            // Find matching variable in left schema
            if let Some(left_idx) = left_schema.iter().position(|s| s == right_col) {
                left_keys.push(left_idx.clone());
                right_keys.push(right_idx);
            }
        }

        Ok((left_keys, right_keys))
    }

    /// Build computed columns for function call and arithmetic assignments
    ///
    /// Handles comparisons like `Dist = euclidean(V, Q)` or `Y = X * 2` by creating
    /// a Compute node that adds the computed column to the schema.
    fn build_computed_columns(&self, input: IRNode, rule: &Rule) -> Result<IRNode, String> {
        let mut expressions = Vec::new();
        // Track schema progressively - each computed column extends the schema for subsequent ones
        let mut schema = input.output_schema();

        for pred in &rule.body {
            if let BodyPredicate::Comparison(left, op, right) = pred {
                // Only process equality assignments
                if !matches!(op, ComparisonOp::Equal) {
                    continue;
                }

                // Try function call assignment (Y = func(X))
                if let Some((var_name, func, args)) = match (left, right) {
                    (Term::Variable(v), Term::FunctionCall(f, a)) => Some((v, f, a)),
                    (Term::FunctionCall(f, a), Term::Variable(v)) => Some((v, f, a)),
                    _ => None,
                } {
                    // Validate argument count
                    let expected_arity = func.arity();
                    if args.len() != expected_arity {
                        return Err(format!(
                            "Function '{}' requires {} argument(s), but {} provided",
                            func.as_str(),
                            expected_arity,
                            args.len()
                        ));
                    }

                    // Convert AST function to IR function
                    let ir_func = Self::ast_func_to_ir_func(func)?;

                    // Convert AST arguments to IR expressions using current (progressive) schema
                    let ir_args: Vec<IRExpression> = args
                        .iter()
                        .map(|term| Self::term_to_ir_expr(term, &schema))
                        .collect::<Result<Vec<_>, _>>()?;

                    expressions.push((
                        var_name.clone(),
                        IRExpression::FunctionCall(ir_func, ir_args),
                    ));

                    // Extend schema with the newly computed column for subsequent expressions
                    schema.push(var_name.clone());
                    continue;
                }

                // Try arithmetic assignment (Y = X * 2)
                if let Some((var_name, arith_expr)) = match (left, right) {
                    (Term::Variable(v), Term::Arithmetic(a)) => Some((v, a)),
                    (Term::Arithmetic(a), Term::Variable(v)) => Some((v, a)),
                    _ => None,
                } {
                    // Convert arithmetic expression to IR expression
                    let ir_expr = Self::arith_expr_to_ir_expression(arith_expr, &schema)?;

                    expressions.push((var_name.clone(), ir_expr));

                    // Extend schema with the newly computed column for subsequent expressions
                    schema.push(var_name.clone());
                    continue;
                }

                // Try variable alias (Y = X) - only if the target variable is not already in schema
                if let Some((new_var, source_var)) = match (left, right) {
                    (Term::Variable(v1), Term::Variable(v2))
                        if !schema.contains(v1) && schema.contains(v2) =>
                    {
                        Some((v1, v2))
                    }
                    (Term::Variable(v1), Term::Variable(v2))
                        if schema.contains(v1) && !schema.contains(v2) =>
                    {
                        Some((v2, v1))
                    }
                    _ => None,
                } {
                    if let Some(col_idx) = schema.iter().position(|s| s == source_var) {
                        expressions.push((new_var.clone(), IRExpression::Column(col_idx)));
                        schema.push(new_var.clone());
                    }
                    continue;
                }

                // Try constant assignment (Y = 100, Y = "str", Y = 1.5)
                if let Some((var_name, ir_expr)) = match (left, right) {
                    (Term::Variable(v), Term::Constant(val)) if !schema.contains(v) => {
                        Some((v, IRExpression::IntConstant(*val)))
                    }
                    (Term::Constant(val), Term::Variable(v)) if !schema.contains(v) => {
                        Some((v, IRExpression::IntConstant(*val)))
                    }
                    (Term::Variable(v), Term::FloatConstant(val)) if !schema.contains(v) => {
                        Some((v, IRExpression::FloatConstant(*val)))
                    }
                    (Term::FloatConstant(val), Term::Variable(v)) if !schema.contains(v) => {
                        Some((v, IRExpression::FloatConstant(*val)))
                    }
                    (Term::Variable(v), Term::StringConstant(val)) if !schema.contains(v) => {
                        Some((v, IRExpression::StringConstant(val.clone())))
                    }
                    (Term::StringConstant(val), Term::Variable(v)) if !schema.contains(v) => {
                        Some((v, IRExpression::StringConstant(val.clone())))
                    }
                    _ => None,
                } {
                    expressions.push((var_name.clone(), ir_expr));
                    schema.push(var_name.clone());
                }
            }
        }

        if expressions.is_empty() {
            // No computed column assignments, return input unchanged
            Ok(input)
        } else {
            Ok(IRNode::Compute {
                input: Box::new(input),
                expressions,
            })
        }
    }

    /// Convert AST `BuiltinFunc` to IR `BuiltinFunction`
