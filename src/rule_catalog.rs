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
