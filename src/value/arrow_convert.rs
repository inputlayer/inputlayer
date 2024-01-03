//! Arrow Conversion Utilities
//!
//! Provides conversion between our Tuple/Value types and Arrow's `RecordBatch` format.
//! This enables efficient columnar operations and Parquet persistence.

use super::{DataType, Tuple, TupleSchema, Value};
use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array,
    Int64Array, Int8Array, LargeListArray, ListArray, StringArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType as ArrowDataType, Field};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Error type for Arrow conversion operations
#[derive(Debug, thiserror::Error)]
pub enum ArrowConvertError {
    /// Schema mismatch between tuples and expected schema
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    /// Unsupported data type
    #[error("Unsupported type: {0}")]
    UnsupportedType(String),
    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
}

/// Convert a vector of tuples to an Arrow `RecordBatch`
///
/// # Arguments
/// * `tuples` - The tuples to convert
/// * `schema` - The schema describing the tuple structure
///
/// # Returns
/// A `RecordBatch` containing the tuple data in columnar format
