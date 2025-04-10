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
//!   - Booleans: "true"/"false" (case-insensitive.clone())
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

