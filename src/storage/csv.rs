//! CSV Storage Module
//!
//! Provides CSV file format support for loading and saving relations.
//! CSV is simpler than Parquet and useful for:
//! - Human-readable data files
//! - Interoperability with spreadsheets and other tools
//! - Simple data import/export
//!
//! ## Format
//!
//! - First row is header with column names
//! - Data types are inferred from content:
//!   - Integers: parsed as i64
//!   - Floats: parsed as f64
//!   - Strings: quoted or unquoted text
//!   - Booleans: "true"/"false" (case-insensitive)
//!
//! ## Example
//!
//! ```csv
//! source,target,weight
//! 1,2,1.5
//! 2,3,2.0
//! 3,4,0.5
//! ```

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use std::sync::Arc;

use crate::storage::error::{StorageError, StorageResult};
use crate::value::{Tuple, Value};

/// Options for CSV parsing
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// Field delimiter (default: ',')
    pub delimiter: char,
    /// Whether the first row contains headers (default: true)
    pub has_header: bool,
    /// Quote character for strings (default: '"')
    pub quote_char: char,
    /// Whether to trim whitespace from fields (default: true)
    pub trim_whitespace: bool,
}

impl Default for CsvOptions {
    fn default() -> Self {
        CsvOptions {
            delimiter: ',',
            has_header: true,
            quote_char: '"',
            trim_whitespace: true,
        }
    }
}

/// Load tuples from a CSV file
///
/// # Arguments
/// * `path` - Path to the CSV file
///
/// # Returns
/// * `Ok((schema, tuples))` - Column names and data tuples
/// * `Err(StorageError)` - If loading fails
pub fn load_from_csv<P: AsRef<Path>>(path: P) -> StorageResult<(Vec<String>, Vec<Tuple>)> {
    load_from_csv_with_options(path, CsvOptions::default())
}

/// Load tuples from a CSV file with custom options
pub fn load_from_csv_with_options<P: AsRef<Path>>(
    path: P,
    options: CsvOptions,
) -> StorageResult<(Vec<String>, Vec<Tuple>)> {
    let path = path.as_ref();
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let mut lines = reader.lines();
    let mut schema = Vec::new();
    let mut tuples = Vec::new();

    // Read header if present
    if options.has_header {
        if let Some(header_line) = lines.next() {
            let header = header_line?;
            schema = parse_csv_line(&header, &options)
                .into_iter()
                .map(std::string::ToString::to_string)
                .collect();
        }
    }

    // Read data rows
    let mut row_num = if options.has_header { 2 } else { 1 };
    for line_result in lines {
        let line = line_result?;

        // Skip empty lines
        if line.trim().is_empty() {
            row_num += 1;
            continue;
        }

        let fields = parse_csv_line(&line, &options);

        // If no header, create schema from first data row
        if schema.is_empty() {
            schema = (0..fields.len()).map(|i| format!("col{i}")).collect();
        }

        // Parse fields into values
        let values: Vec<Value> = fields.into_iter().map(parse_value).collect();

        if values.len() != schema.len() {
            return Err(StorageError::ParseError(format!(
                "Row {} has {} fields, expected {}",
                row_num,
                values.len(),
                schema.len()
            )));
        }

        tuples.push(Tuple::new(values));
        row_num += 1;
    }

    Ok((schema, tuples))
}

/// Save tuples to a CSV file
///
/// # Arguments
/// * `path` - Path to write the CSV file
/// * `schema` - Column names for the header
/// * `tuples` - Data tuples to write
