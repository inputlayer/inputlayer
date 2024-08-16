//! Session and Schema Tests
//!
//! Tests for:
//! - Session fact and rule lifecycle
//! - Session isolation and cleanup
//! - Schema type and arity enforcement
//! - Schema validation engine

use inputlayer::schema::{
    validator::ViolationType, ColumnSchema, RelationSchema, SchemaCatalog, SchemaType,
    ValidationEngine, ValidationError,
};
use inputlayer::value::{Tuple, Value};
use std::sync::Arc;

// Session Data Structure Pattern Tests
//
// NOTE: These are unit tests for the expected behavior patterns of session-related
// data structures (Vec operations, clearing, isolation). They test the data structure
// patterns that session management relies on.
//
// For integration tests of actual StorageEngine session functionality, see:
// - examples/datalog/04_session/*.dl (snapshot tests for session lifecycle)
// - tests/storage_engine_tests.rs (integration tests)
