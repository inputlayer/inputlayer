//! # Rel - Wrapper for DD Collections and Variables
//!
//! This module provides a wrapper type `Rel` that abstracts over
//! Differential Dataflow's `Collection` and `SemigroupVariable` types.
//!
//! ## Purpose
//!
//! In recursive Datalog evaluation, we need to handle two kinds of relations:
//! - **Collections**: Fixed, immutable relations (EDBs and computed IDBs)
//! - **Variables**: Mutable relations that evolve during iteration (recursive IDBs)
//!
//! The `Rel` enum wraps both and provides a unified interface for:
//! - Entering/leaving iterative scopes
//! - Setting variable values for next iteration
//! - Threshold operations for semi-naive evaluation
//!
//! ## Key DD Concepts
//!
//! - **`SemigroupVariable`**: A mutable collection that can be updated during iteration
//! - **`enter()`**: Bring a collection into a nested iterative scope
//! - **`leave()`**: Export results from an iterative scope
//! - **`set()`**: Update a variable's value for the next iteration
//! - **`distinct()`**: Semi-naive style deduplication
//!
//! ## Note on Implementation
//!
//! The actual recursive execution uses `SemigroupVariable` directly in the
//! `execute_transitive_closure_dd` and `execute_reachability_dd` methods.
//! This module provides the abstraction layer for potential future use
//! in more complex stratified execution.

use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::operators::Threshold;
use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::scopes::Child;
use timely::dataflow::Scope;
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;

use crate::value::Tuple;

/// Wrapper enum for DD Collection and SemigroupVariable
///
/// This allows us to write code that works with both regular collections
/// and recursive variables using the same interface.
pub enum Rel<G: Scope>
where
    G::Timestamp: Lattice + TotalOrder,
{
    /// A fixed collection (EDBs, computed non-recursive IDBs)
    Collection(Collection<G, Tuple, isize>),
    /// A mutable variable for recursive computation
    Variable(SemigroupVariable<G, Tuple, isize>),
}

impl<G: Scope> Rel<G>
where
    G::Timestamp: Lattice + TotalOrder,
{
    /// Create a new Rel from a Collection
    pub fn from_collection(coll: Collection<G, Tuple, isize>) -> Self {
        Rel::Collection(coll)
    }

    /// Get a reference to the underlying collection
    ///
    /// For Variable, this dereferences to the current iteration's collection.
    pub fn as_collection(&self) -> &Collection<G, Tuple, isize> {
        match self {
            Rel::Collection(coll) => coll,
            Rel::Variable(var) => &**var,
        }
    }

    /// Check if this is a variable (mutable)
    pub fn is_variable(&self) -> bool {
        matches!(self, Rel::Variable(_))
    }

    /// Check if this is a collection (immutable)
    pub fn is_collection(&self) -> bool {
        matches!(self, Rel::Collection(_))
    }

    /// Concatenate two relations
    pub fn concat(&self, other: &Rel<G>) -> Rel<G> {
        Rel::Collection(self.as_collection().concat(other.as_collection()))
    }

    /// Apply distinct to remove duplicates
    ///
    /// This is key for semi-naive evaluation: ensures each tuple
    /// appears at most once.
    pub fn distinct(&self) -> Rel<G> {
        let result = self.as_collection().distinct();
        Rel::Collection(result)
    }
}

// Methods that require entering a child scope
impl<G: Scope> Rel<G>
where
    G::Timestamp: Lattice + TotalOrder,
{
    /// Enter a nested iterative scope
    ///
    /// This is required to use a collection inside a `.iterative()` block.
    pub fn enter<'a, T>(&self, child: &Child<'a, G, T>) -> Rel<Child<'a, G, T>>
    where
        T: Refines<G::Timestamp> + Lattice + TotalOrder,
    {
        match self {
            Rel::Collection(coll) => Rel::Collection(coll.enter(child)),
            Rel::Variable(var) => {
                // Variables can also enter scope - they dereference to collection
                Rel::Collection((**var).enter(child))
            }
        }
    }
}

// Methods for setting variable values (only valid for Variable)
impl<G: Scope> Rel<G>
where
    G::Timestamp: Lattice + TotalOrder,
{
    /// Set the variable's value for the next iteration
    ///
    /// This consumes the variable and returns the resulting collection.
    /// Only valid for `Rel::Variable`.
    pub fn set_from(self, result: &Rel<G>) -> Rel<G>
    where
        G::Timestamp: Timestamp,
    {
        match self {
            Rel::Variable(var) => {
                let coll = var.set(result.as_collection());
                Rel::Collection(coll)
            }
            Rel::Collection(_) => {
                panic!("Cannot call set_from on a Collection - only Variables can be set")
            }
        }
    }
}

// Methods for leaving scope (implemented on Child scope)
impl<'a, G: Scope, T> Rel<Child<'a, G, T>>
where
    G::Timestamp: Lattice + TotalOrder,
    T: Refines<G::Timestamp> + Lattice + TotalOrder,
{
    /// Leave the nested scope, exporting results to the parent scope
    pub fn leave(&self) -> Rel<G> {
        match self {
            Rel::Collection(coll) => Rel::Collection(coll.leave()),
            Rel::Variable(var) => {
                // Convert variable to collection before leaving
                Rel::Collection((**var).leave())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use timely::dataflow::operators::ToStream;
    use timely::dataflow::operators::Map;

    #[test]
    fn test_rel_from_collection() {
        timely::execute_directly(|worker| {
            worker.dataflow::<(), _, _>(|scope| {
                let data = vec![
                    Tuple::new(vec![Value::Int32(1), Value::Int32(2)]),
                    Tuple::new(vec![Value::Int32(3), Value::Int32(4)]),
                ];

                let coll = Collection::new(data.to_stream(scope).map(|x| (x, (), 1)));
                let rel = Rel::from_collection(coll);

                assert!(rel.is_collection());
                assert!(!rel.is_variable());
            });
        });
    }
}
