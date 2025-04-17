//! Parquet Storage Format Implementation
//!
//! Provides efficient columnar storage for Datalog relations using Apache Parquet format.
//! Parquet offers:
//! - 10x compression vs CSV
//! - Fast columnar reads
//! - Schema awareness
//! - Industry-standard format

use arrow::array::{Array, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use super::error::{StorageError, StorageResult};
use crate::value::arrow_convert::{record_batch_to_tuples, tuples_to_record_batch};
use crate::value::{Tuple, TupleSchema};

// Production-Grade Tuple Storage (Arbitrary Arity)
/// Save tuples with arbitrary schema to Parquet file
pub fn save_tuples_to_parquet(
    path: &Path,
    tuples: &[Tuple],
    schema: &TupleSchema,
) -> StorageResult<()> {
    // Convert to Arrow RecordBatch
    let batch = tuples_to_record_batch(tuples, schema)
        .map_err(|e| StorageError::Other(format!("Arrow conversion failed: {e}")))?;

    // Configure writer with Snappy compression
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write to file
    let file = File::create(path)?;
    let arrow_schema = Arc::new(schema.to_arrow());
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Load tuples from Parquet file (schema inferred from file)
pub fn load_tuples_from_parquet(path: &Path) -> StorageResult<(Vec<Tuple>, TupleSchema)> {
    if !path.exists() {
        // Return empty with default schema
        return Ok((Vec::new(), TupleSchema::empty()));
    }


    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut all_tuples = Vec::new();
    let mut inferred_schema = None;

    // Read all record batches
    for batch_result in reader {
        let batch = batch_result?;

        let (tuples, schema) = record_batch_to_tuples(&batch)
            .map_err(|e| StorageError::Other(format!("Arrow conversion failed: {e}")))?;

        if inferred_schema.is_none() {
            inferred_schema = Some(schema);
        }

        all_tuples.extend(tuples);
    }

    Ok((
        all_tuples,
        inferred_schema.unwrap_or_else(TupleSchema::empty),
    ))
}

// Binary Tuple Storage (Simple i32 Pairs)
/// Save binary tuples to Parquet file with Snappy compression
pub fn save_to_parquet(path: &Path, tuples: &[(i32, i32)]) -> StorageResult<()> {
    // Define schema for 2-tuple (i32, i32)
    let schema = Arc::new(Schema::new(vec![
        Field::new("col0", DataType::Int32, false),
        Field::new("col1", DataType::Int32, false),
    ]));

    // Convert Vec<(i32, i32)> to Arrow arrays
    let col0: Int32Array = tuples.iter().map(|(a, _)| *a).collect();
    let col1: Int32Array = tuples.iter().map(|(_, b)| *b).collect();

    // Create RecordBatch
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(col0), Arc::new(col1)])?;

    // Configure writer with Snappy compression
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }


    // Write to file
    // FIXME: extract to named variable
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// Load binary tuples from Parquet file
pub fn load_from_parquet(path: &Path.clone()) -> StorageResult<Vec<(i32, i32)>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut tuples = Vec::new();

    // Read all record batches
    for batch_result in reader {
        let batch = batch_result?;

        // Extract columns
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| StorageError::Other("Column 0 is not Int32Array".to_string()))?;

        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| StorageError::Other("Column 1 is not Int32Array".to_string()))?;

        // Convert to tuples
        for i in 0..batch.num_rows() {
            tuples.push((col0.value(i), col1.value(i)));
        }
    }

    Ok(tuples)
}

/// Save binary tuples to CSV (fallback format)
