//! Arrow Conversion Utilities
//!
//! Provides conversion between our Tuple/Value types and Arrow's `RecordBatch` format.
//! This enables efficient columnar operations and Parquet persistence.

use super::{DataType, Tuple, TupleSchema, Value};
use arrow::array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int32Array,
    Int64Array, Int8Array, LargeListArray, ListArray, NullArray, StringArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
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
        let col_type = schema
            .field_type(col_idx)
            .expect("col_idx is within 0..schema.arity() so field_type is always Some");
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

/// Build a column array from tuple values.
///
/// Fails if a non-null value does not exactly match `col_type`; never stores a lossy NULL.
fn build_column_array(
    tuples: &[Tuple],
    col_idx: usize,
    col_type: &DataType,
) -> Result<ArrayRef, ArrowConvertError> {
    match col_type {
        DataType::Int32 => Ok(Arc::new(Int32Array::from(scalar_column(
            tuples,
            col_idx,
            col_type,
            |v| match v {
                Value::Int32(x) => Some(*x),
                _ => None,
            },
        )?))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(scalar_column(
            tuples,
            col_idx,
            col_type,
            |v| match v {
                Value::Int64(x) => Some(*x),
                _ => None,
            },
        )?))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(scalar_column(
            tuples,
            col_idx,
            col_type,
            |v| match v {
                Value::Float64(x) => Some(*x),
                _ => None,
            },
        )?))),
        DataType::String => Ok(Arc::new(StringArray::from(scalar_column(
            tuples,
            col_idx,
            col_type,
            |v| match v {
                Value::String(s) => Some(s.as_ref()),
                _ => None,
            },
        )?))),
        DataType::Bool => Ok(Arc::new(BooleanArray::from(scalar_column(
            tuples,
            col_idx,
            col_type,
            |v| match v {
                Value::Bool(b) => Some(*b),
                _ => None,
            },
        )?))),
        DataType::Timestamp => Ok(Arc::new(Int64Array::from(scalar_column(
            tuples,
            col_idx,
            col_type,
            |v| match v {
                Value::Timestamp(t) => Some(*t),
                _ => None,
            },
        )?))),
        DataType::Null => {
            scalar_column(tuples, col_idx, col_type, |_| None::<()>)?;
            Ok(Arc::new(NullArray::new(tuples.len())))
        }
        DataType::Vector { dim } => {
            let field = Arc::new(Field::new("item", ArrowDataType::Float32, false));
            let (values, offsets, nulls) =
                vector_column(tuples, col_idx, col_type, *dim, |v| match v {
                    Value::Vector(x) => Some(x.as_slice()),
                    _ => None,
                })?;
            Ok(list_array(
                field,
                *dim,
                Arc::new(Float32Array::from(values)),
                offsets,
                nulls,
            ))
        }
        DataType::VectorInt8 { dim } => {
            let field = Arc::new(Field::new("item", ArrowDataType::Int8, false));
            let (values, offsets, nulls) =
                vector_column(tuples, col_idx, col_type, *dim, |v| match v {
                    Value::VectorInt8(x) => Some(x.as_slice()),
                    _ => None,
                })?;
            Ok(list_array(
                field,
                *dim,
                Arc::new(Int8Array::from(values)),
                offsets,
                nulls,
            ))
        }
    }
}

fn type_mismatch(
    col_idx: usize,
    row: usize,
    value: &Value,
    col_type: &DataType,
) -> ArrowConvertError {
    ArrowConvertError::SchemaMismatch(format!(
        "column {col_idx} row {row}: {value:?} does not fit {col_type:?}"
    ))
}

/// Extract one column, mapping `Value::Null` to `None` and failing on any other mismatch.
fn scalar_column<'a, T>(
    tuples: &'a [Tuple],
    col_idx: usize,
    col_type: &DataType,
    extract: impl Fn(&'a Value) -> Option<T>,
) -> Result<Vec<Option<T>>, ArrowConvertError> {
    tuples
        .iter()
        .enumerate()
        .map(|(row, t)| match t.get(col_idx) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => extract(v)
                .map(Some)
                .ok_or_else(|| type_mismatch(col_idx, row, v, col_type)),
        })
        .collect()
}

type VectorParts<T> = (Vec<T>, Vec<i64>, Option<NullBuffer>);

/// Flatten one vector column into values, offsets and a validity mask.
/// Null rows of a fixed-dim column are zero-padded and marked invalid.
fn vector_column<'a, T: Copy + Default + 'a>(
    tuples: &'a [Tuple],
    col_idx: usize,
    col_type: &DataType,
    dim: Option<usize>,
    extract: impl Fn(&'a Value) -> Option<&'a [T]>,
) -> Result<VectorParts<T>, ArrowConvertError> {
    let mut values = Vec::new();
    let mut offsets = vec![0i64];
    let mut valid = Vec::with_capacity(tuples.len());
    for (row, t) in tuples.iter().enumerate() {
        match t.get(col_idx) {
            None | Some(Value::Null) => {
                values.extend(std::iter::repeat_n(T::default(), dim.unwrap_or(0)));
                valid.push(false);
            }
            Some(v) => {
                let vec = extract(v)
                    .filter(|x| dim.is_none_or(|d| x.len() == d))
                    .ok_or_else(|| type_mismatch(col_idx, row, v, col_type))?;
                values.extend_from_slice(vec);
                valid.push(true);
            }
        }
        offsets.push(values.len() as i64);
    }
    let nulls = valid.contains(&false).then(|| NullBuffer::from(valid));
    Ok((values, offsets, nulls))
}

fn list_array(
    field: Arc<Field>,
    dim: Option<usize>,
    values: ArrayRef,
    offsets: Vec<i64>,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    match dim {
        Some(d) => Arc::new(FixedSizeListArray::new(field, d as i32, values, nulls)),
        None => Arc::new(LargeListArray::new(
            field,
            OffsetBuffer::new(offsets.into()),
            values,
            nulls,
        )),
    }
}

/// Extract a Value from an Arrow array at a given index
fn extract_value_from_array(array: &dyn Array, row_idx: usize) -> Result<Value, ArrowConvertError> {
    if array.is_null(row_idx) || array.data_type() == &ArrowDataType::Null {
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
        DataType::Null => Arc::new(NullArray::new(0)),
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
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;

    #[test]
    fn test_tuples_to_record_batch_int32() {
        let tuples: Vec<Tuple> = [(1, 2), (3, 4), (5, 6)]
            .into_iter()
            .map(|(a, b)| Tuple::new(vec![Value::Int32(a), Value::Int32(b)]))
            .collect();

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

    #[test]
    fn test_empty_batch_with_vector() {
        let tuples: Vec<Tuple> = vec![];
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            (
                "embedding".to_string(),
                DataType::Vector { dim: Some(1536) },
            ),
        ]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_vector_int8_fixed_size_roundtrip() {
        // Test that int8 vectors with known dimensions use FixedSizeList and preserve dimension
        let tuples = vec![
            Tuple::new(vec![Value::Int32(1), Value::vector_int8(vec![10, 20, 30])]),
            Tuple::new(vec![Value::Int32(2), Value::vector_int8(vec![40, 50, 60])]),
        ];

        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            (
                "embedding".to_string(),
                DataType::VectorInt8 { dim: Some(3) },
            ),
        ]);

        // Convert to Arrow
        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Convert back
        let (result, recovered_schema) = record_batch_to_tuples(&batch).unwrap();

        // Verify dimension is preserved in schema
        assert_eq!(
            recovered_schema.field_type(1),
            Some(&DataType::VectorInt8 { dim: Some(3) })
        );

        // Verify data roundtrip
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0), Some(&Value::Int32(1)));
        assert_eq!(
            result[0].get(1).and_then(|v| v.as_vector_int8()),
            Some([10i8, 20, 30].as_slice())
        );
        assert_eq!(
            result[1].get(1).and_then(|v| v.as_vector_int8()),
            Some([40i8, 50, 60].as_slice())
        );
    }

    #[test]
    fn test_vector_int8_variable_size_roundtrip() {
        // Test int8 vectors with unknown dimensions use LargeList
        let tuples = vec![
            Tuple::new(vec![Value::vector_int8(vec![1, 2])]),
            Tuple::new(vec![Value::vector_int8(vec![3, 4, 5])]), // Different size
        ];

        let schema = TupleSchema::new(vec![(
            "embedding".to_string(),
            DataType::VectorInt8 { dim: None },
        )]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        let (result, recovered_schema) = record_batch_to_tuples(&batch).unwrap();

        // Variable dimensions don't preserve dimension info
        assert_eq!(
            recovered_schema.field_type(0),
            Some(&DataType::VectorInt8 { dim: None })
        );

        // But data is preserved
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].get(0).and_then(|v| v.as_vector_int8()),
            Some([1i8, 2].as_slice())
        );
        assert_eq!(
            result[1].get(0).and_then(|v| v.as_vector_int8()),
            Some([3i8, 4, 5].as_slice())
        );
    }

    // === Additional Coverage ===

    #[test]
    fn test_schema_mismatch_error() {
        let tuples = vec![Tuple::new(vec![Value::Int32(1), Value::Int32(2)])];
        // Schema has 3 columns, tuple has 2
        let schema = TupleSchema::new(vec![
            ("a".to_string(), DataType::Int32),
            ("b".to_string(), DataType::Int32),
            ("c".to_string(), DataType::Int32),
        ]);

        let result = tuples_to_record_batch(&tuples, &schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ArrowConvertError::SchemaMismatch(_)));
        assert!(err.to_string().contains("arity"));
    }

    #[test]
    fn test_bool_roundtrip() {
        let tuples = vec![
            Tuple::new(vec![Value::Bool(true), Value::string("yes")]),
            Tuple::new(vec![Value::Bool(false), Value::string("no")]),
        ];

        let schema = TupleSchema::new(vec![
            ("flag".to_string(), DataType::Bool),
            ("label".to_string(), DataType::String),
        ]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        let (result, _) = record_batch_to_tuples(&batch).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0), Some(&Value::Bool(true)));
        assert_eq!(result[1].get(0), Some(&Value::Bool(false)));
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let tuples = vec![
            Tuple::new(vec![Value::Timestamp(1000000)]),
            Tuple::new(vec![Value::Timestamp(2000000)]),
        ];

        let schema = TupleSchema::new(vec![("ts".to_string(), DataType::Timestamp)]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_infer_schema_empty_tuples() {
        let tuples: Vec<Tuple> = vec![];
        let schema = infer_schema_from_tuples(&tuples, &["a".to_string(), "b".to_string()]);
        // Empty tuples should produce schema from names only
        assert_eq!(schema.arity(), 2);
    }

    #[test]
    fn test_infer_schema_fewer_names() {
        let tuples = vec![Tuple::new(vec![
            Value::Int32(1),
            Value::string("x"),
            Value::Float64(3.14),
        ])];
        // Only provide 1 name for 3 columns
        let schema = infer_schema_from_tuples(&tuples, &["id".to_string()]);
        assert_eq!(schema.arity(), 3);
        // Extra columns get auto-generated names col1, col2
    }

    #[test]
    fn test_int64_roundtrip() {
        let tuples = vec![
            Tuple::new(vec![Value::Int64(i64::MAX)]),
            Tuple::new(vec![Value::Int64(i64::MIN)]),
        ];

        let schema = TupleSchema::new(vec![("big".to_string(), DataType::Int64)]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        let (result, _) = record_batch_to_tuples(&batch).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].get(0), Some(&Value::Int64(i64::MAX)));
        assert_eq!(result[1].get(0), Some(&Value::Int64(i64::MIN)));
    }

    #[test]
    fn test_arrow_convert_error_display() {
        let e1 = ArrowConvertError::SchemaMismatch("bad schema".to_string());
        assert!(e1.to_string().contains("bad schema"));

        let e2 = ArrowConvertError::UnsupportedType("weird type".to_string());
        assert!(e2.to_string().contains("weird type"));
    }

    #[test]
    fn test_empty_batch_with_vector_int8() {
        let tuples: Vec<Tuple> = vec![];
        let schema = TupleSchema::new(vec![
            ("id".to_string(), DataType::Int32),
            (
                "embedding".to_string(),
                DataType::VectorInt8 { dim: Some(256) },
            ),
        ]);

        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_mismatched_type_fails_instead_of_nulling() {
        let schema = TupleSchema::new(vec![("v".to_string(), DataType::Int64)]);
        for bad in [Value::string("Sam"), Value::Float64(1.5), Value::Int32(1)] {
            let tuples = vec![Tuple::new(vec![Value::Int64(30)]), Tuple::new(vec![bad])];
            assert!(matches!(
                tuples_to_record_batch(&tuples, &schema),
                Err(ArrowConvertError::SchemaMismatch(_))
            ));
        }
    }

    #[test]
    fn test_mismatched_vector_dim_fails() {
        let schema = TupleSchema::new(vec![("v".to_string(), DataType::Vector { dim: Some(2) })]);
        let tuples = vec![
            Tuple::new(vec![Value::vector(vec![1.0, 2.0])]),
            Tuple::new(vec![Value::vector(vec![1.0, 2.0, 3.0])]),
        ];
        assert!(tuples_to_record_batch(&tuples, &schema).is_err());
    }

    #[test]
    fn test_nulls_roundtrip_in_every_column_kind() {
        let schema = TupleSchema::new(vec![
            ("i".to_string(), DataType::Int32),
            ("v".to_string(), DataType::Vector { dim: Some(2) }),
            ("q".to_string(), DataType::VectorInt8 { dim: None }),
            ("n".to_string(), DataType::Null),
        ]);
        let tuples = vec![
            Tuple::new(vec![
                Value::Int32(1),
                Value::vector(vec![1.0, 2.0]),
                Value::vector_int8(vec![3]),
                Value::Null,
            ]),
            Tuple::new(vec![Value::Null, Value::Null, Value::Null, Value::Null]),
        ];
        let batch = tuples_to_record_batch(&tuples, &schema).unwrap();
        let (result, _) = record_batch_to_tuples(&batch).unwrap();
        assert_eq!(result, tuples);
    }
}
