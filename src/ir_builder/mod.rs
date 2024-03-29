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
        current = self.build_computed_columns(current, rule.clone())?;

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
