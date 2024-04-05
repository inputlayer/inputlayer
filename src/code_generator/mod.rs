//! # Code Generator
//!
//! Converts IR to Differential Dataflow code and executes it, returning results.
//!
//! ## Pipeline Position
//!
//! ```text
//! Optimized IRNode -> [Code Generator] -> DD Execution -> Results
//! ```
//!
//! ## Capabilities
//!
//! - Arbitrary arity tuples with multiple data types
//! - Complex joins with multi-column keys
//! - Generic projections (any column reordering or selection)
//! - Recursive evaluation via `.iterative()` scopes with `SemigroupVariable`
//! - Semi-naive evaluation for efficient fixpoint computation

use crate::boolean_specialization::SemiringType;
use crate::ir::{AggregateFunction, ArithOp, BuiltinFunction, IRExpression, IRNode, Predicate};
use crate::semiring_types::{BooleanDiff, DiffType};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::operators::join::{Join, JoinCore};
use differential_dataflow::operators::{Reduce, Threshold};
use differential_dataflow::Collection;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use timely::dataflow::operators::{Inspect, Map, Probe, ToStream};
use timely::dataflow::ProbeHandle;
use timely::dataflow::Scope;
use timely::order::Product;

