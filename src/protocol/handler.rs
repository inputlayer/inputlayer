//! Handler for `InputLayer`
//!
//! Core business logic for Datalog queries and data operations, used by the REST API.
//! Uses `parking_lot::RwLock` (no poisoning) and `AtomicU64` (lock-free counters).

use crate::ast::Term;
use crate::rule_catalog::validate_rule;
use crate::schema::{ColumnSchema, RelationSchema};
use crate::statement;
use crate::storage_engine::StorageEngine;
use crate::value::{Tuple, Value};
use crate::Config;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::wire::{ColumnDef, QueryResult, WireDataType, WireTuple, WireValue};

/// Term -> Value (constants only, rejects variables/placeholders).
fn term_to_value(term: &Term) -> Result<Value, String> {
    match term {
        Term::Constant(n) => Ok(Value::Int64(*n)),
        Term::FloatConstant(f) => Ok(Value::Float64(*f)),
        Term::StringConstant(s) => Ok(Value::string(s)),
        Term::VectorLiteral(v) => Ok(Value::vector(v.iter().map(|x| *x as f32).collect())),
        Term::Variable(v) => Err(format!("Cannot insert variable '{v}' - use constants only")),
        Term::Placeholder => Err("Cannot insert placeholder '_' - use constants only".to_string()),
        Term::Arithmetic(_) => {
            Err("Cannot insert arithmetic expression - use constants only".to_string())
        }
        Term::Aggregate(_, _) => Err("Cannot insert aggregate - use constants only".to_string()),
        Term::FunctionCall(_, _) => {
            Err("Cannot insert function call - use constants only".to_string())
        }
        Term::FieldAccess(_, _) => {
            Err("Cannot insert field access - use constants only".to_string())
        }
        Term::RecordPattern(_) => {
            Err("Cannot insert record pattern - use constants only".to_string())
        }
    }
}

/// Thread-safe wrapper around StorageEngine for concurrent API calls.
/// Per-KG schema validation via isolated SchemaCatalogs.
