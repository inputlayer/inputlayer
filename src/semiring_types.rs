//! # Semiring Diff Types for Differential Dataflow
//!
//! Defines the `DiffType` supertrait and concrete diff type implementations:
//! - `isize` (Counting semiring)  -  full bag semantics, 8 bytes per tuple
//! - `BooleanDiff(i8)` (Boolean semiring)  -  set semantics, 1 byte per tuple
//! - `MinDiff` / `MaxDiff`  -  infrastructure for recursive min/max aggregation
//!
//! The `DiffType` trait combines all DD trait requirements (`Semigroup`, `Monoid`, `Abelian`)
//! with helpers needed by the code generator (`one()`, `to_count()`).

use differential_dataflow::difference::{Monoid, Semigroup};
use std::ops::AddAssign;

/// Supertrait combining all DD requirements for a diff type,
/// plus helpers needed by our code generator.
///
/// Any type implementing `DiffType` can be used as the `R` parameter
/// in `Collection<G, D, R>` throughout the code generator.
pub trait DiffType:
    Semigroup
    + Monoid
    + differential_dataflow::difference::Abelian
    + std::ops::Mul<Self, Output = Self>
    + From<i8>
    + Copy
    + Default
    + std::hash::Hash
    + std::fmt::Debug
    + Send
    + Sync
    + 'static
    + abomonation::Abomonation
    + Ord
{
    /// The multiplicative identity (1 for counting, BooleanDiff(1) for boolean).
    fn one() -> Self;

    /// Convert to isize for iteration counts in reduce closures.
    fn to_count(&self) -> isize;

    /// Whether this diff type has a true mathematical inverse (Neg).
    /// If false, `distinct_core()` must NOT be called  -  use `reduce()` instead.
    /// isize and BooleanDiff are Abelian; MinDiff and MaxDiff are not.
    const IS_ABELIAN: bool = true;
}

// impl DiffType for isize (Counting semiring)
