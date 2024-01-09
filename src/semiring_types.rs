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
impl DiffType for isize {
    #[inline]
    fn one() -> Self {
        1
    }

    #[inline]
    fn to_count(&self) -> isize {
        *self
    }

    const IS_ABELIAN: bool = true;
}

// BooleanDiff(i8)  -  1-byte diff type for set semantics
/// A 1-byte diff type for set-semantic (boolean) queries.
///
/// Uses `i8` internally with saturating arithmetic. Saves 7 bytes per tuple
/// compared to `isize`. All DD traits (`Semigroup`, `Monoid`, `Abelian`) are
/// satisfied via delegation to `i8` arithmetic.
///
/// # Why not DD's `Present`?
///
/// `Present` only implements `Semigroup` (no `Monoid`, `Abelian`, or `Neg`).
/// Our code uses `distinct()` and `reduce()` which require `Abelian`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(transparent)]
pub struct BooleanDiff(pub i8);

impl BooleanDiff {
    pub const ZERO: Self = BooleanDiff(0);
    pub const ONE: Self = BooleanDiff(1);
}

impl std::ops::Neg for BooleanDiff {
    type Output = Self;
    #[inline]
    fn neg(self) -> Self {
        BooleanDiff(-self.0)
    }
}

impl std::ops::Mul for BooleanDiff {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: Self) -> Self {
        BooleanDiff(self.0.saturating_mul(rhs.0))
    }
}

impl AddAssign<&Self> for BooleanDiff {
    #[inline]
    fn add_assign(&mut self, rhs: &Self) {
        self.0 = self.0.saturating_add(rhs.0);
    }
}

impl Semigroup for BooleanDiff {
    #[inline]
    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl Monoid for BooleanDiff {
    #[inline]
    fn zero() -> Self {
        BooleanDiff(0)
    }
}

// Abelian is a blanket impl in DD for types implementing Monoid + Neg<Output=Self>.
// Since BooleanDiff implements both, the blanket impl applies automatically.

impl From<i8> for BooleanDiff {
    #[inline]
    fn from(v: i8) -> Self {
        BooleanDiff(v)
    }
}

// Manual Abomonation impl for the 1-byte repr(transparent) newtype.
// Safety: BooleanDiff is repr(transparent) over i8 which is a single byte
// with no pointers or heap allocations. The abomonation encode/decode
// is equivalent to a memcpy of 1 byte.
