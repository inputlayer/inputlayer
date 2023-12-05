//! Boundary Value Tests
//!
//! Tests at exact boundary conditions - many bugs occur at exact limits.
//!
//! Tests for value handling at system boundaries:
//! - Integer boundaries (INT64_MIN, INT64_MAX)
//! - Float boundaries
//! - String boundaries (empty, long, Unicode)
//! - Vector boundaries
//! - Arity boundaries
//! - Collection size boundaries

use inputlayer::{Tuple, Value};
use std::i32;
use std::i64;
