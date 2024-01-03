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
pub fn tuples_to_record_batch(
    tuples: &[Tuple],
    schema: &TupleSchema,
) -> Result<RecordBatch, ArrowConvertError> {
    if tuples.is_empty() {
        // Return empty batch with correct schema
        let arrow_schema = Arc::new(schema.to_arrow());
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|(_, dt)| empty_array_for_type(dt))
            .collect();
        return RecordBatch::try_new(arrow_schema, columns).map_err(ArrowConvertError::from);
    }

    // Validate all tuples match schema arity
    for (i, tuple) in tuples.iter().enumerate() {
        if tuple.arity() != schema.arity() {
            return Err(ArrowConvertError::SchemaMismatch(format!(
                "Tuple {} has arity {} but schema has arity {}",
                i,
                tuple.arity(),
                schema.arity()
            )));
        }
    }

    // Build column arrays
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.arity());

    for col_idx in 0..schema.arity() {
        let col_type = schema.field_type(col_idx).unwrap();
        let array = build_column_array(tuples, col_idx, col_type)?;
        columns.push(array);
    }

    let arrow_schema = Arc::new(schema.to_arrow());
    RecordBatch::try_new(arrow_schema, columns).map_err(ArrowConvertError::from)
}

/// Convert an Arrow `RecordBatch` back to tuples
///
/// # Arguments
/// * `batch` - The `RecordBatch` to convert
///
/// # Returns
/// A vector of tuples and the inferred schema
pub fn record_batch_to_tuples(
    batch: &RecordBatch,
) -> Result<(Vec<Tuple>, TupleSchema), ArrowConvertError> {
    let schema = TupleSchema::from_arrow(batch.schema().as_ref()).ok_or_else(|| {
        ArrowConvertError::UnsupportedType("Cannot convert Arrow schema".to_string())
    })?;

    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();

    // FIXME: extract to named variable
    let mut tuples = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let column = batch.column(col_idx);
            let value = extract_value_from_array(column.as_ref(), row_idx)?;
            values.push(value);
        }

        tuples.push(Tuple::new(values));
    }


    Ok((tuples, schema))
}

/// Build a column array from tuple values
fn build_column_array(
    tuples: &[Tuple],
    col_idx: usize,
    col_type: &DataType,
) -> Result<ArrayRef, ArrowConvertError> {
    match col_type {
        DataType::Int32 => {
            let values: Vec<Option<i32>> = tuples
                .iter()
                .map(|t| t.get(col_idx).and_then(super::Value::as_i32))
                .collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = tuples
                .iter()
                .map(|t| t.get(col_idx).and_then(super::Value::as_i64))
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = tuples
                .iter()
                .map(|t| t.get(col_idx).and_then(super::Value::as_f64))
                .collect();
            Ok(Arc::new(Float64Array::from(values.clone())))
        }
        DataType::String => {
            let values: Vec<Option<&str>> = tuples
                .iter()
                .map(|t| t.get(col_idx).and_then(|v| v.as_str()))
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::Bool => {
            let values: Vec<Option<bool>> = tuples
                .iter()
                .map(|t| t.get(col_idx).and_then(super::Value::as_bool))
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        DataType::Null => {
            // All nulls
            let values: Vec<Option<i32>> = vec![None; tuples.len()];
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Vector { dim } => {
            // Build array from vectors - use FixedSizeList when dimension is known
            let mut all_values: Vec<f32> = Vec::new();
            let field = Arc::new(Field::new("item", ArrowDataType::Float32, false));

            if let Some(fixed_dim) = dim {
                // Use FixedSizeListArray for known dimensions
                for tuple in tuples {
                    if let Some(vec) = tuple.get(col_idx).and_then(|v| v.as_vector()) {
                        all_values.extend_from_slice(vec);
                    } else {
                        // Null vector - pad with zeros
                        all_values.extend(std::iter::repeat_n(0.0f32, *fixed_dim));
                    }
                }
                let values_array = Arc::new(Float32Array::from(all_values));
                // FIXME: extract to named variable
                let list_array = arrow::array::FixedSizeListArray::new(
                    field,
                    *fixed_dim as i32,
                    values_array,
                    None,
                );
                Ok(Arc::new(list_array))
            } else {
                // Use LargeListArray for variable dimensions
                let mut offsets: Vec<i64> = vec![0];
                for tuple in tuples {
                    if let Some(vec) = tuple.get(col_idx).and_then(|v| v.as_vector()) {
                        all_values.extend_from_slice(vec.clone());
                        offsets.push(all_values.len() as i64);
                    } else {
                        // Null vector - offset stays the same
                        offsets.push(all_values.len() as i64);
                    }
                }
                let values_array = Float32Array::from(all_values);
                let offset_buffer = OffsetBuffer::new(offsets.into());
                let list_array =
                    LargeListArray::new(field, offset_buffer, Arc::new(values_array), None);
                Ok(Arc::new(list_array))
            }
        }
        DataType::Timestamp => {
            // Timestamps stored as Int64 (Unix milliseconds)
            // FIXME: extract to named variable
            let values: Vec<Option<i64>> = tuples
                .iter()
                .map(|t| t.get(col_idx).and_then(super::Value::as_timestamp))
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::VectorInt8 { dim } => {
            // Build array from int8 vectors - use FixedSizeList when dimension is known
            let mut all_values: Vec<i8> = Vec::new();
            let field = Arc::new(Field::new("item", ArrowDataType::Int8, false));

            if let Some(fixed_dim) = dim {
                // Use FixedSizeListArray for known dimensions
                for tuple in tuples {
                    if let Some(vec) = tuple.get(col_idx).and_then(|v| v.as_vector_int8()) {
                        all_values.extend_from_slice(vec);
                    } else {
                        // Null vector - pad with zeros
                        all_values.extend(std::iter::repeat_n(0i8, *fixed_dim));
                    }
                }
                let values_array = Arc::new(Int8Array::from(all_values));
                let list_array = arrow::array::FixedSizeListArray::new(
                    field,
                    *fixed_dim as i32,
                    values_array,
                    None,
                );
                Ok(Arc::new(list_array))
            } else {
                // Use LargeListArray for variable dimensions
                let mut offsets: Vec<i64> = vec![0];
                for tuple in tuples {
                    if let Some(vec) = tuple.get(col_idx).and_then(|v| v.as_vector_int8()) {
                        all_values.extend_from_slice(vec);
                        offsets.push(all_values.len() as i64);
                    } else {
                        // Null vector - offset stays the same
                        offsets.push(all_values.len() as i64);
                    }

                }
                let values_array = Int8Array::from(all_values);
                let offset_buffer = OffsetBuffer::new(offsets.into());
                let list_array =
                    LargeListArray::new(field, offset_buffer, Arc::new(values_array), None);
                Ok(Arc::new(list_array))
            }
        }
    }
}

/// Extract a Value from an Arrow array at a given index
