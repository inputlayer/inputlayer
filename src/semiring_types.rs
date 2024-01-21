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
impl abomonation::Abomonation for BooleanDiff {
    unsafe fn entomb<W: std::io::Write>(&self, write: &mut W) -> std::io::Result<()> {
        write.write_all(&[self.0 as u8])
    }
    unsafe fn exhume<'b>(&mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        if bytes.is_empty() {
            None
        } else {
            self.0 = bytes[0] as i8;
            Some(&mut bytes[1..])
        }
    }
    fn extent(&self) -> usize {
        1
    }
}

impl DiffType for BooleanDiff {
    #[inline]
    fn one() -> Self {
        BooleanDiff(1)
    }

    #[inline]
    fn to_count(&self) -> isize {
        self.0 as isize
    }

    const IS_ABELIAN: bool = true;
}

// MinDiff  -  diff type for recursive min aggregation
/// Diff type for min-semiring: addition is min, zero is +infinity.
///
/// Used for recursive aggregation (e.g., shortest path) where the code
/// generator applies early min-aggregation inside the fixpoint loop to
/// prune non-optimal paths. DD's semi-naive evaluation converges naturally
/// because min is monotone: if a <= b, then a+c <= b+c.
///
/// ## Neg implementation
///
/// Min has no mathematical inverse. The `Neg` impl exists solely to satisfy
/// DD's `Abelian` blanket impl (required by `DiffType` / `distinct_core`).
/// The code generator MUST NOT call `distinct_core()` on `MinDiff` collections.
/// Instead, it uses `reduce()` with min-aggregation for deduplication.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct MinDiff(pub i64);

impl Default for MinDiff {
    fn default() -> Self {
        MinDiff(i64::MAX) // Identity for min
    }
}

impl std::ops::Neg for MinDiff {
    type Output = Self;
    #[inline]
    fn neg(self) -> Self {
        // Min has no mathematical inverse. This exists to satisfy the Abelian
        // blanket impl. The code generator never calls distinct() on MinDiff.
        MinDiff(self.0.wrapping_neg())
    }
}

impl std::ops::Mul for MinDiff {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: Self) -> Self {
        // Tropical semiring: multiplication = addition of distances.
        // MinDiff(a) * MinDiff(b) = MinDiff(a + b).
        // Used when DD joins produce combined path costs.
        MinDiff(self.0.saturating_add(rhs.0))
    }
}

impl AddAssign<&Self> for MinDiff {
    #[inline]
    fn add_assign(&mut self, rhs: &Self) {
        self.0 = self.0.min(rhs.0);
    }
}

impl Semigroup for MinDiff {
    #[inline]
    fn is_zero(&self) -> bool {
        self.0 == i64::MAX
    }
}

impl Monoid for MinDiff {
    #[inline]
    fn zero() -> Self {
        MinDiff(i64::MAX)
    }
}

impl From<i8> for MinDiff {
    #[inline]
    fn from(v: i8) -> Self {
        MinDiff(v as i64)
    }
}

// Manual Abomonation impl for 8-byte i64 newtype.
// Safety: MinDiff is repr(transparent)-equivalent over i64 (no padding, no pointers).
impl abomonation::Abomonation for MinDiff {
    unsafe fn entomb<W: std::io::Write>(&self, write: &mut W) -> std::io::Result<()> {
        write.write_all(&self.0.to_le_bytes())
    }
    unsafe fn exhume<'b>(&mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        if bytes.len() < 8 {
            None
        } else {
            self.0 = i64::from_le_bytes(bytes[..8].try_into().ok()?);
            Some(&mut bytes[8..])
        }
    }
    fn extent(&self) -> usize {
        8
    }
}

impl DiffType for MinDiff {
    #[inline]
    fn one() -> Self {
        MinDiff(0) // Additive identity for tropical mul (0 + x = x)
    }

    #[inline]
    fn to_count(&self) -> isize {
        // For min semiring, each tuple represents one derivation
        isize::from(self.0 != i64::MAX)
    }

    const IS_ABELIAN: bool = false;
}

// MaxDiff  -  diff type for recursive max aggregation
/// Diff type for max-semiring: addition is max, zero is -infinity.
///
/// Used for recursive aggregation (e.g., widest path) where the code
/// generator applies early max-aggregation inside the fixpoint loop.
///
/// ## Neg implementation
///
/// Same caveat as MinDiff: `Neg` exists only for trait compliance.
/// The code generator MUST NOT call `distinct_core()` on `MaxDiff` collections.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct MaxDiff(pub i64);

impl Default for MaxDiff {
    fn default() -> Self {
        MaxDiff(i64::MIN) // Identity for max
    }
}

impl std::ops::Neg for MaxDiff {
    type Output = Self;
    #[inline]
    fn neg(self) -> Self {
        // Max has no mathematical inverse. See MinDiff::neg() for rationale.
        MaxDiff(self.0.wrapping_neg())
    }
}

impl std::ops::Mul for MaxDiff {
    type Output = Self;
    #[inline]
    fn mul(self, rhs: Self) -> Self {
        // Tropical semiring: multiplication = addition of capacities.
        MaxDiff(self.0.saturating_add(rhs.0))
    }
}

impl AddAssign<&Self> for MaxDiff {
    #[inline]
    fn add_assign(&mut self, rhs: &Self) {
        self.0 = self.0.max(rhs.0);
    }
}

impl Semigroup for MaxDiff {
    #[inline]
    fn is_zero(&self) -> bool {
        self.0 == i64::MIN
    }
}

impl Monoid for MaxDiff {
    #[inline]
    fn zero() -> Self {
        MaxDiff(i64::MIN)
    }
}

impl From<i8> for MaxDiff {
    #[inline]
    fn from(v: i8) -> Self {
        MaxDiff(v as i64)
    }
}

impl abomonation::Abomonation for MaxDiff {
    unsafe fn entomb<W: std::io::Write>(&self, write: &mut W) -> std::io::Result<()> {
        write.write_all(&self.0.to_le_bytes())
    }
    unsafe fn exhume<'b>(&mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        if bytes.len() < 8 {
            None
        } else {
            self.0 = i64::from_le_bytes(bytes[..8].try_into().ok()?);
            Some(&mut bytes[8..])
        }
    }
    fn extent(&self) -> usize {
        8
    }
}

impl DiffType for MaxDiff {
    #[inline]
    fn one() -> Self {
        MaxDiff(0) // Additive identity for tropical mul (0 + x = x)
    }

    #[inline]
    fn to_count(&self) -> isize {
        isize::from(self.0 != i64::MIN)
    }

    const IS_ABELIAN: bool = false;
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::difference::{Monoid, Semigroup};

    // BooleanDiff arithmetic
    #[test]
    fn test_boolean_diff_add() {
        let mut a = BooleanDiff(1);
        a += &BooleanDiff(1);
        // Saturating: 1 + 1 = 2 (within i8 range)
        assert_eq!(a, BooleanDiff(2));
    }

    #[test]
    fn test_boolean_diff_saturating_add() {
        let mut a = BooleanDiff(i8::MAX);
        a += &BooleanDiff(1);
        assert_eq!(a, BooleanDiff(i8::MAX));
    }

    #[test]
    fn test_boolean_diff_neg() {
        assert_eq!(-BooleanDiff(1), BooleanDiff(-1));
        assert_eq!(-BooleanDiff(0), BooleanDiff(0));
        assert_eq!(-BooleanDiff(-1), BooleanDiff(1));
    }

    #[test]
    fn test_boolean_diff_mul() {
        assert_eq!(BooleanDiff(2) * BooleanDiff(3), BooleanDiff(6));
        assert_eq!(BooleanDiff(1) * BooleanDiff(0), BooleanDiff(0));
    }

    #[test]
    fn test_boolean_diff_saturating_mul() {
        assert_eq!(BooleanDiff(i8::MAX) * BooleanDiff(2), BooleanDiff(i8::MAX));
    }

    // BooleanDiff DD traits
    #[test]
    fn test_boolean_diff_is_zero() {
        assert!(BooleanDiff(0).is_zero());
        assert!(!BooleanDiff(1).is_zero());
        assert!(!BooleanDiff(-1).is_zero());
    }

    #[test]
    fn test_boolean_diff_zero() {
        assert_eq!(BooleanDiff::zero(), BooleanDiff(0));
    }

    #[test]
    fn test_boolean_diff_from_i8() {
        assert_eq!(BooleanDiff::from(42i8), BooleanDiff(42));
    }

    // DiffType trait
    #[test]
    fn test_isize_diff_type() {
        assert_eq!(isize::one(), 1);
        assert_eq!(42isize.to_count(), 42);
    }

    #[test]
    fn test_boolean_diff_type() {
        assert_eq!(BooleanDiff::one(), BooleanDiff(1));
        assert_eq!(BooleanDiff(3).to_count(), 3);
    }

    // MinDiff
    #[test]
    fn test_min_diff_plus_equals_takes_min() {
        let mut a = MinDiff(10);
        a += &MinDiff(5);
        assert_eq!(a, MinDiff(5));
    }

    #[test]
    fn test_min_diff_zero_is_max() {
        assert_eq!(MinDiff::zero(), MinDiff(i64::MAX));
        assert!(MinDiff(i64::MAX).is_zero());
    }

    #[test]
    fn test_min_diff_identity() {
        let mut a = MinDiff(42);
        a += &MinDiff::zero();
        assert_eq!(a, MinDiff(42)); // min(42, MAX) = 42
    }

    // MinDiff extended traits
    #[test]
    fn test_min_diff_mul_tropical() {
        // Tropical semiring: mul = addition of distances
        assert_eq!(MinDiff(3) * MinDiff(5), MinDiff(8));
        assert_eq!(MinDiff(0) * MinDiff(10), MinDiff(10));
    }

    #[test]
    fn test_min_diff_mul_saturating() {
        assert_eq!(MinDiff(i64::MAX) * MinDiff(1), MinDiff(i64::MAX));
    }

    #[test]
    fn test_min_diff_one() {
        assert_eq!(MinDiff::one(), MinDiff(0));
        // one * x = x (additive identity for tropical mul)
        assert_eq!(MinDiff::one() * MinDiff(42), MinDiff(42));
    }

    #[test]
    fn test_min_diff_to_count() {
        assert_eq!(MinDiff(42).to_count(), 1);
        assert_eq!(MinDiff(0).to_count(), 1);
        assert_eq!(MinDiff(i64::MAX).to_count(), 0); // zero element
    }

    #[test]
    fn test_min_diff_from_i8() {
        assert_eq!(MinDiff::from(5i8), MinDiff(5));
        assert_eq!(MinDiff::from(-1i8), MinDiff(-1));
    }

    #[test]
    fn test_min_diff_is_not_abelian() {
        assert!(!MinDiff::IS_ABELIAN);
    }

    // MaxDiff
    #[test]
    fn test_max_diff_plus_equals_takes_max() {
        let mut a = MaxDiff(5);
        a += &MaxDiff(10);
        assert_eq!(a, MaxDiff(10));
    }

    #[test]
    fn test_max_diff_zero_is_min() {
        assert_eq!(MaxDiff::zero(), MaxDiff(i64::MIN));
        assert!(MaxDiff(i64::MIN).is_zero());
    }

    #[test]
    fn test_max_diff_identity() {
        let mut a = MaxDiff(42);
        a += &MaxDiff::zero();
        assert_eq!(a, MaxDiff(42)); // max(42, MIN) = 42
    }

    // MaxDiff extended traits
