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

    // Ensure data is durably written to disk
    File::open(path)?.sync_all()?;

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
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    // Ensure data is durably written to disk
    File::open(path)?.sync_all()?;

    Ok(())
}

/// Load binary tuples from Parquet file
pub fn load_from_parquet(path: &Path) -> StorageResult<Vec<(i32, i32)>> {
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
pub fn save_to_csv(path: &Path, tuples: &[(i32, i32)]) -> StorageResult<()> {
    use std::io::Write;

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut file = File::create(path)?;

    for (a, b) in tuples {
        writeln!(file, "{a},{b}")?;
    }

    // Ensure data is durably written to disk
    file.flush()?;
    file.sync_all()?;

    Ok(())
}

/// Load binary tuples from CSV
pub fn load_from_csv(path: &Path) -> StorageResult<Vec<(i32, i32)>> {
    use std::io::{BufRead, BufReader};

    if !path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut tuples = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.trim().split(',').collect();

        if parts.len() == 2 {
            let a: i32 = parts[0]
                .parse()
                .map_err(|e| StorageError::Other(format!("Failed to parse integer: {e}")))?;
            let b: i32 = parts[1]
                .parse()
                .map_err(|e| StorageError::Other(format!("Failed to parse integer: {e}")))?;
            tuples.push((a, b));
        }
    }

    Ok(tuples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{DataType as ValueDataType, Value};
    use tempfile::TempDir;

    #[test]
    fn test_parquet_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.parquet");

        let original = vec![(1, 2), (3, 4), (5, 6), (7, 8)];

        // Save
        save_to_parquet(&path, &original).unwrap();

        // Load
        let loaded = load_from_parquet(&path).unwrap();

        assert_eq!(original, loaded);
    }

    #[test]
    fn test_parquet_empty() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("empty.parquet");

        let original: Vec<(i32, i32)> = vec![];

        save_to_parquet(&path, &original).unwrap();
        let loaded = load_from_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn test_csv_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.csv");

        let original = vec![(1, 2), (3, 4), (5, 6)];

        save_to_csv(&path, &original).unwrap();
        let loaded = load_from_csv(&path).unwrap();

        assert_eq!(original, loaded);
    }

    #[test]
    fn test_load_nonexistent() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("nonexistent.parquet");

        let loaded = load_from_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 0);
    }

    // Production Tuple Tests
    #[test]
    fn test_tuples_parquet_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("tuples.parquet");

        let tuples = vec![
            Tuple::from_pair(1, 2),
            Tuple::from_pair(3, 4),
            Tuple::from_pair(5, 6),
        ];

        let schema = TupleSchema::new(vec![
            ("x".to_string(), ValueDataType::Int32),
            ("y".to_string(), ValueDataType::Int32),
        ]);

        save_tuples_to_parquet(&path, &tuples, &schema).unwrap();
        let (loaded, loaded_schema) = load_tuples_from_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].to_pair(), Some((1, 2)));
        assert_eq!(loaded[1].to_pair(), Some((3, 4)));
        assert_eq!(loaded[2].to_pair(), Some((5, 6)));
        assert_eq!(loaded_schema.arity(), 2);
    }

    #[test]
    fn test_tuples_mixed_types_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("mixed.parquet");

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
            ("id".to_string(), ValueDataType::Int32),
            ("name".to_string(), ValueDataType::String),
            ("score".to_string(), ValueDataType::Float64),
        ]);

        save_tuples_to_parquet(&path, &tuples, &schema).unwrap();
        let (loaded, loaded_schema) = load_tuples_from_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded_schema.arity(), 3);

        assert_eq!(loaded[0].get(0), Some(&Value::Int32(1)));
        assert_eq!(loaded[0].get(1).and_then(|v| v.as_str()), Some("hello"));
        assert_eq!(loaded[0].get(2).and_then(|v| v.as_f64()), Some(1.5));
    }

    #[test]
    fn test_tuples_empty_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("empty_tuples.parquet");

        let tuples: Vec<Tuple> = vec![];
        let schema = TupleSchema::new(vec![
            ("a".to_string(), ValueDataType::Int32),
            ("b".to_string(), ValueDataType::String),
        ]);

        save_tuples_to_parquet(&path, &tuples, &schema).unwrap();
        let (loaded, _) = load_tuples_from_parquet(&path).unwrap();

        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn test_tuples_load_nonexistent() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("nonexistent_tuples.parquet");

        let (loaded, schema) = load_tuples_from_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 0);
        assert_eq!(schema.arity(), 0);
    }

    // === Regression tests for P0 durability fixes ===

    /// Regression: save_to_parquet must fsync so data survives power loss.
    /// Verify the file is readable and valid immediately after save.
    #[test]
    fn test_save_to_parquet_is_durable() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("durable.parquet");

        let data = vec![(10, 20), (30, 40)];
        save_to_parquet(&path, &data).unwrap();

        // File must exist and be readable
        assert!(path.exists());
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(
            metadata.len() > 0,
            "Parquet file must not be empty after durable write"
        );

        let loaded = load_from_parquet(&path).unwrap();
        assert_eq!(loaded, data);
    }

    /// Regression: save_tuples_to_parquet must fsync so data survives power loss.
    #[test]
    fn test_save_tuples_to_parquet_is_durable() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("durable_tuples.parquet");

        let tuples = vec![Tuple::from_pair(1, 2), Tuple::from_pair(3, 4)];
        let schema = TupleSchema::new(vec![
            ("x".to_string(), ValueDataType::Int32),
            ("y".to_string(), ValueDataType::Int32),
        ]);

        save_tuples_to_parquet(&path, &tuples, &schema).unwrap();

        assert!(path.exists());
        let metadata = std::fs::metadata(&path).unwrap();
        assert!(metadata.len() > 0);

        let (loaded, _) = load_tuples_from_parquet(&path).unwrap();
        assert_eq!(loaded.len(), 2);
    }

    /// Regression: save_to_csv must flush and fsync so data survives power loss.
    #[test]
    fn test_save_to_csv_is_durable() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("durable.csv");

        let data = vec![(100, 200), (300, 400)];
        save_to_csv(&path, &data).unwrap();

        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("100,200"));
        assert!(content.contains("300,400"));

        let loaded = load_from_csv(&path).unwrap();
        assert_eq!(loaded, data);
    }

    /// Regression: Parquet writers must create parent directories if needed.
    #[test]
    fn test_parquet_creates_parent_dirs() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("nested").join("dir").join("test.parquet");

        let data = vec![(1, 2)];
        save_to_parquet(&path, &data).unwrap();

        assert!(path.exists());
        let loaded = load_from_parquet(&path).unwrap();
        assert_eq!(loaded, data);
    }
}
