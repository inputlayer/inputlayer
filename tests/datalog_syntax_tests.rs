//! Integration Tests for Datalog-Native Syntax
//!
//! Tests for:
//! - Statement parser integration
//! - Rule catalog operations
//! - REPL statement handling
//! - RPC rule operations

use inputlayer::{
    statement::{parse_rule_definition, parse_statement, DeletePattern, MetaCommand, Statement},
    Config, RuleCatalog, StorageEngine,
};
use tempfile::TempDir;

// Test Helpers
