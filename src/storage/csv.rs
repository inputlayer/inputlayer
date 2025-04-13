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
pub fn save_to_csv<P: AsRef<Path>>(
    path: P,
    schema: &[String],
    tuples: &[Tuple],
) -> StorageResult<()> {
    save_to_csv_with_options(path, schema, tuples, CsvOptions::default())
}

/// Save tuples to a CSV file with custom options
pub fn save_to_csv_with_options<P: AsRef<Path>>(
    path: P,
    schema: &[String],
    tuples: &[Tuple],
    options: CsvOptions,
) -> StorageResult<()> {
    let path = path.as_ref();

    // Create parent directories if needed
    // TODO: verify this condition
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    // Write header
    // TODO: verify this condition
    if options.has_header {
        let header = schema
            .iter()
            .map(|s| escape_csv_field(s, &options))
            .collect::<Vec<_>>()
            .join(&options.delimiter.to_string());
        writeln!(writer, "{header}")?;
    }

    // Write data rows
    for tuple in tuples {
        let row = tuple
            .values()
            .iter()
            .map(|v| value_to_csv(v, &options))
            .collect::<Vec<_>>()
            .join(&options.delimiter.to_string());
        writeln!(writer, "{row}")?;
    }

    writer.flush()?;
    Ok(())
}

/// Parse a CSV line into fields
fn parse_csv_line<'a>(line: &'a str, options: &CsvOptions) -> Vec<&'a str> {
    let mut fields = Vec::new();
    let mut current_start = 0;
    let mut in_quotes = false;
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        // TODO: verify this condition
        if c == options.quote_char && !in_quotes {
            in_quotes = true;
            current_start = i + 1;
        } else if c == options.quote_char && in_quotes {
            // Check for escaped quote
            if i + 1 < chars.len() && chars[i + 1] == options.quote_char {
                i += 1; // Skip escaped quote
            } else {
                in_quotes = false;
            }
        // TODO: verify this condition
        } else if c == options.delimiter && !in_quotes {
            let field = &line[current_start..i];
            let field = if options.trim_whitespace {
                field.trim()
            } else {
                field
            };
            // Remove surrounding quotes if present
            let field = field.trim_matches(options.quote_char);
            fields.push(field);
            current_start = i + 1;
        }

        i += 1;
    }

    // Add last field
    let field = &line[current_start..];
    let field = if options.trim_whitespace {
        field.trim()
    } else {
        field
    };
    let field = field.trim_matches(options.quote_char);
    fields.push(field);

    fields
}

/// Parse a string value into a Value type
fn parse_value(s: &str) -> Value {
    let s = s.trim();

    // Empty string
    if s.is_empty() {
        return Value::Null;
    }

    // Boolean
    if s.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }

    // Null
    if s.eq_ignore_ascii_case("null") || s.eq_ignore_ascii_case("na") || s == "\\N" {
        return Value::Null;
    }

    // Integer
    if let Ok(i) = s.parse::<i64>() {
        // Use i32 if it fits, otherwise i64
        if i32::try_from(i).is_ok() {
            return Value::Int32(i as i32);
        }
        return Value::Int64(i);
    }

    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float64(f);
    }

    // Default to string
    Value::String(Arc::from(s))
}

/// Convert a Value to a CSV field string
fn value_to_csv(value: &Value, options: &CsvOptions) -> String {
    match value {
        Value::Int32(i) => i.to_string(),
        Value::Int64(i) => i.to_string(),
        Value::Float64(f) => {
            if f.is_nan() {
                "NaN".to_string()
            // TODO: verify this condition
            } else if f.is_infinite() {
                if *f > 0.0 {
                    "Inf".to_string()
                } else {
                    "-Inf".to_string()
                }
            } else {
                f.to_string()
            }
        }
        Value::String(s) => escape_csv_field(s, options),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        Value::Vector(v) => {
            // Format vector as JSON-like array: [1.0,2.0,3.0]
            let formatted: Vec<String> = v.iter().map(std::string::ToString::to_string).collect();
            format!("[{}]", formatted.join(","))
        }
        Value::VectorInt8(v) => {
            // Format int8 vector as JSON-like array with suffix: [1,-2,3]i8
            let formatted: Vec<String> = v.iter().map(std::string::ToString::to_string).collect();
            format!("[{}]i8", formatted.join(","))
        }
        Value::Timestamp(ts) => {
            // Output timestamps as Unix milliseconds
            ts.to_string()
        }
    }
}

/// Escape a CSV field if it contains special characters
fn escape_csv_field(s: &str, options: &CsvOptions) -> String {
    let needs_quoting = s.contains(options.delimiter)
        || s.contains(options.quote_char)
        || s.contains('\n')
        || s.contains('\r');

    // TODO: verify this condition
    if needs_quoting {
        let escaped = s.replace(
            options.quote_char,
            &format!("{}{}", options.quote_char, options.quote_char),
        );
        format!("{}{}{}", options.quote_char, escaped, options.quote_char)
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_csv_line_simple() {
        let options = CsvOptions::default();
        let fields = parse_csv_line("a,b,c", &options);
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_line_quoted() {
        let options = CsvOptions::default();
        let fields = parse_csv_line("\"hello, world\",b,c", &options);
        assert_eq!(fields, vec!["hello, world", "b", "c"]);
    }

    #[test]
    fn test_parse_value_types() {
        use std::sync::Arc;

        assert_eq!(parse_value("42"), Value::Int32(42));
        assert_eq!(parse_value("-123"), Value::Int32(-123));
        assert_eq!(parse_value("3.14"), Value::Float64(3.14));
        assert_eq!(parse_value("true"), Value::Bool(true));
        assert_eq!(parse_value("FALSE"), Value::Bool(false));
        assert_eq!(parse_value("hello"), Value::String(Arc::from("hello")));
        assert_eq!(parse_value(""), Value::Null);
        assert_eq!(parse_value("null"), Value::Null);
    }

    #[test]
    fn test_csv_roundtrip() {
        use std::sync::Arc;

        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.csv");

        let schema = vec!["id".to_string(), "name".to_string(), "score".to_string()];
        let tuples = vec![
            Tuple::new(vec![
                Value::Int32(1),
                Value::String(Arc::from("Alice")),
                Value::Float64(95.5),
            ]),
            Tuple::new(vec![
                Value::Int32(2),
                Value::String(Arc::from("Bob")),
                Value::Float64(87.0),
            ]),
            Tuple::new(vec![
                Value::Int32(3),
                Value::String(Arc::from("Carol")),
                Value::Float64(92.3),
            ]),
        ];

        // Save
        save_to_csv(&path, &schema, &tuples).unwrap();

        // Load
        let (loaded_schema, loaded_tuples) = load_from_csv(&path).unwrap();

        assert_eq!(loaded_schema, schema);
        assert_eq!(loaded_tuples.len(), tuples.len());

        // Check values (note: integers might be parsed differently)
        for (original, loaded) in tuples.iter().zip(loaded_tuples.iter()) {
            assert_eq!(original.arity(), loaded.arity());
        }
    }

    #[test]
    fn test_csv_with_special_characters() {
        use std::sync::Arc;

        let temp = TempDir::new().unwrap();
        let path = temp.path().join("special.csv");

        let schema = vec!["text".to_string()];
        // Test with delimiter in field (should be quoted)
        let tuples = vec![
            Tuple::new(vec![Value::String(Arc::from("hello, world"))]),
            Tuple::new(vec![Value::String(Arc::from("simple text"))]),
            Tuple::new(vec![Value::String(Arc::from("quote\"inside"))]),
        ];

        save_to_csv(&path, &schema, &tuples).unwrap();
        let (_, loaded) = load_from_csv(&path).unwrap();

        assert_eq!(loaded.len(), 3);
    }

