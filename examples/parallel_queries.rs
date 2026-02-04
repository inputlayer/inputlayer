//! Parallel Query Execution Demonstration
//!
//! This example demonstrates the worker pool infrastructure for parallel
//! query execution across multiple databases, utilizing all CPU cores.
//!
//! Features shown:
//! - Parallel query execution across multiple databases
//! - Executing the same query on multiple databases
//! - Multiple queries on the same database
//! - Performance comparison: sequential vs parallel
//! - Automatic CPU core utilization

use inputlayer::{Config, StorageEngine};
use std::time::Instant;
use tempfile::TempDir;

