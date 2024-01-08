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

    let mut tuples = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = Vec::new();

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
            Ok(Arc::new(Float64Array::from(values)))
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
                        all_values.extend_from_slice(vec);
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
fn extract_value_from_array(array: &dyn Array, row_idx: usize) -> Result<Value, ArrowConvertError> {
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }

    // Try each array type
    if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Value::Int32(arr.value(row_idx)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Value::Int64(arr.value(row_idx)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(Value::Float64(arr.value(row_idx)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Value::String(Arc::from(arr.value(row_idx))));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Value::Bool(arr.value(row_idx)));
    }

    // Handle FixedSizeListArray (vectors with known dimension)
    if let Some(arr) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        let values = arr.value(row_idx);
        // Check for Float32 vectors first
        if let Some(float_arr) = values.as_any().downcast_ref::<Float32Array>() {
            let vec: Vec<f32> = (0..float_arr.len()).map(|i| float_arr.value(i)).collect();
            return Ok(Value::vector(vec));
        }
        // Check for Int8 vectors
        if let Some(int8_arr) = values.as_any().downcast_ref::<Int8Array>() {
            let vec: Vec<i8> = (0..int8_arr.len()).map(|i| int8_arr.value(i)).collect();
            return Ok(Value::vector_int8(vec));
        }
    }

    // Handle LargeListArray (vectors with unknown dimension)
    if let Some(arr) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = arr.value(row_idx);
        // Check for Float32 vectors first
        if let Some(float_arr) = values.as_any().downcast_ref::<Float32Array>() {
            let vec: Vec<f32> = (0..float_arr.len()).map(|i| float_arr.value(i)).collect();
            return Ok(Value::vector(vec));
        }
        // Check for Int8 vectors
        if let Some(int8_arr) = values.as_any().downcast_ref::<Int8Array>() {
            let vec: Vec<i8> = (0..int8_arr.len()).map(|i| int8_arr.value(i)).collect();
            return Ok(Value::vector_int8(vec));
        }
    }

    // Handle ListArray (vectors)
    if let Some(arr) = array.as_any().downcast_ref::<ListArray>() {
        let values = arr.value(row_idx);
        // Check for Float32 vectors first
        if let Some(float_arr) = values.as_any().downcast_ref::<Float32Array>() {
            let vec: Vec<f32> = (0..float_arr.len()).map(|i| float_arr.value(i)).collect();
            return Ok(Value::vector(vec));
        }
        // Check for Int8 vectors
        if let Some(int8_arr) = values.as_any().downcast_ref::<Int8Array>() {
            let vec: Vec<i8> = (0..int8_arr.len()).map(|i| int8_arr.value(i)).collect();
            return Ok(Value::vector_int8(vec));
        }
    }

    Err(ArrowConvertError::UnsupportedType(format!(
        "Cannot extract value from array type: {:?}",
        array.data_type()
    )))
}

/// Create an empty array for a given data type
fn empty_array_for_type(dt: &DataType) -> ArrayRef {
    match dt {
        DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
        DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
        DataType::Float64 => Arc::new(Float64Array::from(Vec::<f64>::new())),
        DataType::String => Arc::new(StringArray::from(Vec::<&str>::new())),
        DataType::Bool => Arc::new(BooleanArray::from(Vec::<bool>::new())),
        DataType::Null => Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
        DataType::Vector { dim } => {
            let field = Arc::new(Field::new("item", ArrowDataType::Float32, false));
            if let Some(fixed_dim) = dim {
                let values_array = Arc::new(Float32Array::from(Vec::<f32>::new()));
                Arc::new(arrow::array::FixedSizeListArray::new(
                    field,
                    *fixed_dim as i32,
                    values_array,
                    None,
                ))
            } else {
                let values_array = Float32Array::from(Vec::<f32>::new());
                let offset_buffer = OffsetBuffer::new(vec![0i64].into());
                Arc::new(LargeListArray::new(
                    field,
                    offset_buffer,
                    Arc::new(values_array),
                    None,
                ))
            }
        }
        DataType::VectorInt8 { dim } => {
            let field = Arc::new(Field::new("item", ArrowDataType::Int8, false));
            if let Some(fixed_dim) = dim {
                let values_array = Arc::new(Int8Array::from(Vec::<i8>::new()));
                Arc::new(arrow::array::FixedSizeListArray::new(
                    field,
                    *fixed_dim as i32,
                    values_array,
                    None,
                ))
            } else {
                let values_array = Int8Array::from(Vec::<i8>::new());
                let offset_buffer = OffsetBuffer::new(vec![0i64].into());
                Arc::new(LargeListArray::new(
                    field,
                    offset_buffer,
                    Arc::new(values_array),
                    None,
                ))
            }
        }
        DataType::Timestamp => Arc::new(Int64Array::from(Vec::<i64>::new())),
    }
}

/// Infer schema from a vector of tuples
///
/// Uses the first tuple to determine column types
pub fn infer_schema_from_tuples(tuples: &[Tuple], column_names: &[String]) -> TupleSchema {
    if tuples.is_empty() {
        return TupleSchema::from_names(column_names.to_vec());
    }

    let first = &tuples[0];
    let fields: Vec<(String, DataType)> = first
        .values()
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let name = column_names
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("col{i}"));
            (name, v.data_type())
        })
        .collect();

    TupleSchema::new(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;

    #[test]
    fn test_tuples_to_record_batch_int32() {
        let tuples = vec![
            Tuple::from_pair(1, 2),
            Tuple::from_pair(3, 4),
            Tuple::from_pair(5, 6),
        ];

        let schema = TupleSchema::new(vec![
            ("a".to_string(), DataType::Int32),
            ("b".to_string(), DataType::Int32),
        ]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.value(0), 1);
        assert_eq!(col0.value(1), 3);
        assert_eq!(col0.value(2), 5);
    }

    #[test]
    fn test_record_batch_to_tuples() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", arrow::datatypes::DataType::Int32, false),
            Field::new("y", arrow::datatypes::DataType::Int32, false),
        ]));

        let col0 = Int32Array::from(vec![1, 2, 3]);
        let col1 = Int32Array::from(vec![10, 20, 30]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(col0), Arc::new(col1)]).unwrap();

        let (tuples, _schema) = record_batch_to_tuples(&batch).unwrap();

        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].to_pair(), Some((1, 10)));
        assert_eq!(tuples[1].to_pair(), Some((2, 20)));
        assert_eq!(tuples[2].to_pair(), Some((3, 30)));
    }

    #[test]
    fn test_roundtrip_mixed_types() {
        let tuples = vec![
            Tuple::new(vec![
                Value::Int32(1),
                Value::string("hello"),
                Value::Float64(1.5),
            ]),
            Tuple::new(vec![
                Value::Int32(2),
                Value::string("world"),
                Value::Float64(2.5),
            ]),
        ];

        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("name".to_string(), DataType::String),
            ("score".to_string(), DataType::Float64),
        ]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        let (result, _) = record_batch_to_tuples(&batch).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0), Some(&Value::Int32(1)));
        assert_eq!(result[0].get(1).and_then(|v| v.as_str()), Some("hello"));
        assert_eq!(result[1].get(2).and_then(|v| v.as_f64()), Some(2.5));
    }

    #[test]
    fn test_empty_batch() {
        let tuples: Vec<Tuple> = vec![];
        let schema = TupleSchema::new(vec![
            ("a".to_string(), DataType::Int32),
            ("b".to_string(), DataType::String),
        ]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_infer_schema() {
        let tuples = vec![Tuple::new(vec![Value::Int32(1), Value::string("test")])];

        let schema = infer_schema_from_tuples(&tuples, &["id".to_string(), "name".to_string()]);

        assert_eq!(schema.arity(), 2);
        assert_eq!(schema.field_type(0), Some(&DataType::Int32));
        assert_eq!(schema.field_type(1), Some(&DataType::String));
    }

    #[test]
    fn test_vector_fixed_size_roundtrip() {
        // Test that vectors with known dimensions use FixedSizeList and preserve dimension
        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::vector(vec![1.0, 2.0, 3.0])]),
            Tuple::new(vec![Value::Int32(2), Value::vector(vec![4.0, 5.0, 6.0])]),
        ];

        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            ("embedding".to_string(), DataType::Vector { dim: Some(3) }),
        ]);

        // Convert to Arrow
        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Convert back
        let (result, recovered_schema) = record_batch_to_tuples(&batch).unwrap();

        // Verify dimension is preserved in schema
        assert_eq!(
            recovered_schema.field_type(1),
            Some(&DataType::Vector { dim: Some(3) })
        );

        // Verify data roundtrip
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0), Some(&Value::Int32(1)));
        assert_eq!(
            result[0].get(1).and_then(|v| v.as_vector()),
            Some([1.0f32, 2.0, 3.0].as_slice())
        );
        assert_eq!(
            result[1].get(1).and_then(|v| v.as_vector()),
            Some([4.0f32, 5.0, 6.0].as_slice())
        );
    }

    #[test]
    fn test_vector_variable_size_roundtrip() {
        // Test vectors with unknown dimensions use LargeList
        let tuples = vec![
            Tuple::new(vec![Value::vector(vec![1.0, 2.0])]),
            Tuple::new(vec![Value::vector(vec![3.0, 4.0, 5.0])]), // Different size
        ];

        let schema = TupleSchema::new(vec![(
            "embedding".to_string(),
            DataType::Vector { dim: None },
        )]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        let (result, recovered_schema) = record_batch_to_tuples(&batch).unwrap();

        // Variable dimensions don't preserve dimension info
        assert_eq!(
            recovered_schema.field_type(0),
            Some(&DataType::Vector { dim: None })
        );

        // But data is preserved
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get(0).and_then(|v| v.as_vector()),
            Some([1.0f32, 2.0].as_slice())
        );
        assert_eq!(
            result[1].get(0).and_then(|v| v.as_vector()),
            Some([3.0f32, 4.0, 5.0].as_slice())
        );
    }

