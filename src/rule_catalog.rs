//! Rule Catalog for Persistent Rules (Policies)
//!
//! Manages persistent rule definitions per database. Rules are defined with `:-`
//! and are automatically loaded on database startup.
//!
//! ## Storage
//!
//! Rules are stored in JSON format at `{db_dir}/rules/catalog.json`
//!
//! ## Example
//!
//! ```rust,no_run
//! use inputlayer::RuleCatalog;
//! use std::path::PathBuf;
//!
//! let db_dir = PathBuf::from("/tmp/mydb");
//! let mut catalog = RuleCatalog::new(db_dir).unwrap();
//!
//! // Get all rules to prepend to queries
//! let rules = catalog.all_rules();
//!
//! // Drop a rule
//! catalog.drop("path").unwrap();
//! ```

use crate::ast::{BodyPredicate, Program, Rule};
use crate::recursion::{build_extended_dependency_graph, find_sccs};
use crate::statement::{RuleDef, SerializableRule};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Validate a single rule for safety constraints
///
/// This function checks:
/// 1. Self-negation: a rule cannot negate its own head
/// 2. Head variable safety: all head variables must be bound by positive body atoms
/// 3. Range restriction: variables in negated atoms must be bound by positive atoms
///
/// Note: This does NOT check for mutual negation cycles between rules, which requires
/// analyzing the full rule set. Use `validate_rules_stratification` for that.
///
/// # Arguments
/// * `rule` - The rule to validate
/// * `name` - The name/label for error messages
///
/// # Returns
/// * `Ok(())` if the rule is valid
/// * `Err(String)` with a descriptive error message if validation fails
pub fn validate_rule(rule: &Rule, name: &str) -> Result<(), String> {
    // Check 1: Direct self-negation
    for pred in &rule.body {
        if let BodyPredicate::Negated(atom) = pred {
            if atom.relation == rule.head.relation {
                return Err(format!(
                    "Unstratified negation: Rule '{}' negates itself (!{} in body). \
                     Self-negation is not supported.",
                    name, atom.relation
                ));
            }
        }
    }

    // Check 2: Head variable safety - all head variables must be bound by positive body atoms
    let positive_vars = rule.positive_body_variables();
    let head_vars = rule.head.variables();
    let unbound_head: Vec<_> = head_vars.difference(&positive_vars).cloned().collect();
    if !unbound_head.is_empty() {
        let mut sorted_unbound = unbound_head;
        sorted_unbound.sort();
        return Err(format!(
            "Unsafe rule '{}': Head variable(s) {} not bound by any positive body atom. \
             All head variables must appear in at least one positive body predicate.",
            name,
            sorted_unbound.join(", ")
        ));
    }

    // Check 3: Range restriction for negated atoms
    // Variables in negated atoms must be bound by positive atoms
    for pred in &rule.body {
        if let BodyPredicate::Negated(atom) = pred {
            let neg_vars = atom.variables();
            let unbound: Vec<_> = neg_vars.difference(&positive_vars).cloned().collect();
            if !unbound.is_empty() {
                let mut sorted_unbound = unbound;
                sorted_unbound.sort();
                return Err(format!(
                    "Unsafe negation in rule '{}': Variable(s) {} in negated atom !{}(...) \
                     must be bound by a positive body atom. Range restriction violation.",
                    name,
                    sorted_unbound.join(", "),
                    atom.relation
                ));
            }
        }
    }

    Ok(())
}

/// Validate that a set of rules doesn't have negation cycles (stratification check)
///
/// This checks for mutual negation cycles like:
/// - a(X) :- !b(X)
/// - b(X) :- !a(X)
///
/// # Arguments
/// * `rules` - The rules to validate together
///
/// # Returns
/// * `Ok(())` if the rules are stratifiable
/// * `Err(String)` with a descriptive error message if a negation cycle is found
pub fn validate_rules_stratification(rules: &[Rule]) -> Result<(), String> {
    if rules.is_empty() {
        return Ok(());
    }

    let program = Program {
        rules: rules.to_vec(),
    };

    let extended_graph = build_extended_dependency_graph(&program);
    let simple_graph = extended_graph.to_simple_graph();
    let sccs = find_sccs(&simple_graph);

    // Check for any negative edge within any SCC
    for scc in &sccs {
        // TODO: verify this condition
        if let Some((from, to)) = extended_graph.has_negative_edge_in_scc(scc) {
            // TODO: verify this condition
            let reason = if from == to {
                format!(
                    "Unstratified negation: '{from}' negates itself. Self-negation is not supported."
                )
            } else {
                let mut sorted_scc = scc.clone();
                sorted_scc.sort();
                format!(
                    "Unstratified negation: '{}' negates '{}' within a recursive cycle. \
                     Negation through recursion is not supported. Cycle: [{}]",
                    from,
                    to,
                    sorted_scc.join(", ")
                )
            };
            return Err(reason);
        }
    }

    Ok(())
}

/// Result of registering a rule
#[derive(Debug, Clone, PartialEq)]
