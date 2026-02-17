//! Wire Format Types
//!
//! Serializable types for client-server communication: WireValue, WireTuple, QueryResult, ColumnDef.

use serde::{Deserialize, Serialize};

// Wire Data Type
/// Wire-serializable data type enum.
///
/// Represents the schema type of a column, used for describing relation schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireDataType {
    Int32,
    Int64,
    Float64,
    String,
    Bool,
    Timestamp,
    Vector { dim: Option<usize> },
    VectorInt8 { dim: Option<usize> },
    Bytes,
}

impl std::fmt::Display for WireDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireDataType::Int32 => write!(f, "Int32"),
            WireDataType::Int64 => write!(f, "Int64"),
            WireDataType::Float64 => write!(f, "Float64"),
            WireDataType::String => write!(f, "String"),
            WireDataType::Bool => write!(f, "Bool"),
            WireDataType::Timestamp => write!(f, "Timestamp"),
            WireDataType::Vector { dim: Some(d) } => write!(f, "Vector[{d}]"),
            WireDataType::Vector { dim: None } => write!(f, "Vector"),
            WireDataType::VectorInt8 { dim: Some(d) } => write!(f, "VectorInt8[{d}]"),
            WireDataType::VectorInt8 { dim: None } => write!(f, "VectorInt8"),
            WireDataType::Bytes => write!(f, "Bytes"),
        }
    }
}

// Wire Value
/// Wire-serializable value enum.
///
/// Represents a single cell value in a tuple. Supports all `InputLayer` value types
/// including vectors and timestamps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    /// Timestamp as Unix milliseconds
    Timestamp(i64),
    /// Full-precision f32 vector
    Vector(Vec<f32>),
    /// Quantized int8 vector
    VectorInt8(Vec<i8>),
    /// Binary data
    Bytes(Vec<u8>),
}

impl WireValue {
    pub fn data_type(&self) -> WireDataType {
        match self {
            WireValue::Null => WireDataType::Int64, // Default null type
            WireValue::Int32(_) => WireDataType::Int32,
            WireValue::Int64(_) => WireDataType::Int64,
            WireValue::Float64(_) => WireDataType::Float64,
            WireValue::String(_) => WireDataType::String,
            WireValue::Bool(_) => WireDataType::Bool,
            WireValue::Timestamp(_) => WireDataType::Timestamp,
            WireValue::Vector(v) => WireDataType::Vector { dim: Some(v.len()) },
            WireValue::VectorInt8(v) => WireDataType::VectorInt8 { dim: Some(v.len()) },
            WireValue::Bytes(_) => WireDataType::Bytes,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, WireValue::Null)
    }

    /// Try to get as i32
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            WireValue::Int32(v) => Some(*v),
            WireValue::Int64(v) => i32::try_from(*v).ok(),
            _ => None,
        }
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            WireValue::Int32(v) => Some(i64::from(*v)),
            WireValue::Int64(v) => Some(*v),
            WireValue::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            WireValue::Int32(v) => Some(f64::from(*v)),
            WireValue::Int64(v) => Some(*v as f64),
            WireValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to get as string reference
    pub fn as_str(&self) -> Option<&str> {
        match self {
            WireValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            WireValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl std::fmt::Display for WireValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireValue::Null => write!(f, "NULL"),
            WireValue::Int32(v) => write!(f, "{v}"),
            WireValue::Int64(v) => write!(f, "{v}"),
            WireValue::Float64(v) => {
                // Normalize exponent format across platforms (e+20 -> e20)
                let s = format!("{v}");
                write!(f, "{}", s.replace("e+", "e"))
            }
            WireValue::String(s) => write!(f, "\"{s}\""),
            WireValue::Bool(b) => write!(f, "{b}"),
            WireValue::Timestamp(t) => write!(f, "ts:{t}"),
            WireValue::Vector(v) => write!(f, "vec[{}]", v.len()),
            WireValue::VectorInt8(v) => write!(f, "vec8[{}]", v.len()),
            WireValue::Bytes(b) => write!(f, "bytes[{}]", b.len()),
        }
    }
}

// Wire Tuple
/// Wire-serializable tuple (row of values).
///
/// Represents a single row in a relation or query result.
/// Optionally includes per-tuple provenance when executing in a session context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WireTuple {
    pub values: Vec<WireValue>,
    /// Per-tuple provenance (present only for session queries with ephemeral data)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provenance: Option<crate::session::Provenance>,
}

impl WireTuple {
    pub fn new(values: Vec<WireValue>) -> Self {
        Self {
            values,
            provenance: None,
        }
    }

    pub fn with_provenance(values: Vec<WireValue>, provenance: crate::session::Provenance) -> Self {
        Self {
            values,
            provenance: Some(provenance),
        }
    }

    pub fn empty() -> Self {
        Self {
            values: Vec::new(),
            provenance: None,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&WireValue> {
        self.values.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut WireValue> {
        self.values.get_mut(index)
    }
}

// Conversions: (i32, i32) <-> WireTuple
impl From<(i32, i32)> for WireTuple {
    fn from((a, b): (i32, i32)) -> Self {
        WireTuple {
            values: vec![WireValue::Int32(a), WireValue::Int32(b)],
            provenance: None,
        }
    }
}

impl TryFrom<WireTuple> for (i32, i32) {
    type Error = String;

    fn try_from(tuple: WireTuple) -> Result<Self, Self::Error> {
        if tuple.values.len() < 2 {
            return Err(format!("Expected 2 values, got {}", tuple.values.len()));
        }

        let a = tuple.values[0]
            .as_i32()
            .ok_or_else(|| format!("First value is not an integer: {:?}", tuple.values[0]))?;

        let b = tuple.values[1]
            .as_i32()
            .ok_or_else(|| format!("Second value is not an integer: {:?}", tuple.values[1]))?;

        Ok((a, b))
    }
}

impl From<&(i32, i32)> for WireTuple {
    fn from((a, b): &(i32, i32)) -> Self {
        WireTuple {
            values: vec![WireValue::Int32(*a), WireValue::Int32(*b)],
            provenance: None,
        }
    }
}

// Column Definition
/// Column definition for schema description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: WireDataType,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: WireDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    pub fn int32(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::Int32)
    }

    pub fn int64(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::Int64)
    }

    pub fn float64(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::Float64)
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self::new(name, WireDataType::String)
    }

    pub fn vector(name: impl Into<String>, dim: Option<usize>) -> Self {
        Self::new(name, WireDataType::Vector { dim })
    }
}

// Query Result
/// Result of a query execution.
///
/// Includes optional provenance metadata when ephemeral session data
/// participates in the query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result rows
    pub rows: Vec<WireTuple>,
    /// Schema of the result
    pub schema: Vec<ColumnDef>,
    /// Total number of rows before limit/offset (equals rows.len() when no pagination)
    #[serde(default)]
    pub total_count: usize,
    /// Whether pagination truncated the result set
    #[serde(default)]
    pub truncated: bool,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Provenance metadata (present when ephemeral data participates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ResultMetadata>,
}

/// Provenance and audit metadata for a query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// Whether any ephemeral data participated in this result
    pub has_ephemeral: bool,
    /// Relations that contributed ephemeral data
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ephemeral_sources: Vec<String>,
    /// Warnings about ephemeral/persistent mixing
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Session ID that produced this result (if session-scoped)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<u64>,
}

impl ResultMetadata {
    /// Create metadata from session query metadata
    pub fn from_session(meta: &crate::session::QueryMetadata, session_id: u64) -> Option<Self> {
        if !meta.has_ephemeral {
            return None;
        }
        Some(Self {
            has_ephemeral: true,
            ephemeral_sources: meta.ephemeral_sources.clone(),
            warnings: meta.warnings.clone(),
            session_id: Some(session_id),
        })
    }
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            schema: Vec::new(),
            total_count: 0,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
        }
    }

    pub fn new(rows: Vec<WireTuple>, schema: Vec<ColumnDef>, execution_time_ms: u64) -> Self {
        let total_count = rows.len();
        Self {
            rows,
            schema,
            total_count,
            truncated: false,
            execution_time_ms,
            metadata: None,
        }
    }

    /// Create a query result with provenance metadata
    pub fn with_metadata(
        rows: Vec<WireTuple>,
        schema: Vec<ColumnDef>,
        execution_time_ms: u64,
        metadata: Option<ResultMetadata>,
    ) -> Self {
        let total_count = rows.len();
        Self {
            rows,
            schema,
            total_count,
            truncated: false,
            execution_time_ms,
            metadata,
        }
    }
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_value_types() {
        assert_eq!(WireValue::Int32(42).data_type(), WireDataType::Int32);
        assert_eq!(WireValue::Int64(42).data_type(), WireDataType::Int64);
        assert_eq!(WireValue::Float64(3.14).data_type(), WireDataType::Float64);
        assert_eq!(
            WireValue::String("hello".to_string()).data_type(),
            WireDataType::String
        );
        assert_eq!(WireValue::Bool(true).data_type(), WireDataType::Bool);
        assert_eq!(
            WireValue::Timestamp(1234567890).data_type(),
            WireDataType::Timestamp
        );
    }

    #[test]
    fn test_wire_value_accessors() {
        assert_eq!(WireValue::Int32(42).as_i32(), Some(42));
        assert_eq!(WireValue::Int64(42).as_i64(), Some(42));
        assert_eq!(WireValue::Float64(3.14).as_f64(), Some(3.14));
        assert_eq!(
            WireValue::String("hello".to_string()).as_str(),
            Some("hello")
        );
        assert_eq!(WireValue::Bool(true).as_bool(), Some(true));
        assert!(WireValue::Null.is_null());
    }

    #[test]
    fn test_wire_tuple_from_tuple2() {
        let tuple: WireTuple = (1, 2).into();
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), Some(&WireValue::Int32(1)));
        assert_eq!(tuple.get(1), Some(&WireValue::Int32(2)));
    }

    #[test]
    fn test_wire_tuple_to_tuple2() {
        let wire = WireTuple::new(vec![WireValue::Int32(1), WireValue::Int32(2)]);
        let tuple: (i32, i32) = wire.try_into().unwrap();
        assert_eq!(tuple, (1, 2));
    }

    #[test]
    fn test_wire_tuple_to_tuple2_error() {
        let wire = WireTuple::new(vec![WireValue::Int32(1)]);
        let result: Result<(i32, i32), _> = wire.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_column_def() {
        let col = ColumnDef::int32("id");
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, WireDataType::Int32);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original = WireTuple::new(vec![
            WireValue::Int32(42),
            WireValue::String("hello".to_string()),
            WireValue::Vector(vec![1.0, 2.0, 3.0]),
        ]);

        // JSON roundtrip (primary wire format)
        let json = serde_json::to_string(&original).unwrap();
        let restored: WireTuple = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);

        // Verify provenance is omitted when None
        assert!(!json.contains("provenance"));

        // Roundtrip with provenance present
        let with_prov = WireTuple::with_provenance(
            vec![WireValue::Int32(1)],
            crate::session::Provenance::Ephemeral,
        );
        let json2 = serde_json::to_string(&with_prov).unwrap();
        let restored2: WireTuple = serde_json::from_str(&json2).unwrap();
        assert_eq!(with_prov, restored2);
        assert!(json2.contains("\"provenance\":\"ephemeral\""));
    }

    #[test]
    fn test_wire_tuple_with_provenance() {
        use crate::session::Provenance;

        let tuple = WireTuple::with_provenance(
            vec![WireValue::Int32(1), WireValue::Int32(2)],
            Provenance::Ephemeral,
        );
        assert_eq!(tuple.provenance, Some(Provenance::Ephemeral));

        // Provenance present → serialized in JSON
        let json = serde_json::to_string(&tuple).unwrap();
        assert!(json.contains("\"provenance\":\"ephemeral\""));
    }

    #[test]
    fn test_wire_tuple_without_provenance() {
        let tuple = WireTuple::new(vec![WireValue::Int32(1)]);
        assert_eq!(tuple.provenance, None);

        // No provenance → field omitted from JSON
        let json = serde_json::to_string(&tuple).unwrap();
        assert!(!json.contains("provenance"));
    }

    #[test]
    fn test_result_metadata_from_session() {
        use crate::session::QueryMetadata;

        // Clean session → no metadata
        let clean = QueryMetadata::default();
        assert!(ResultMetadata::from_session(&clean, 1).is_none());

        // Dirty session → metadata with session_id
        let dirty = QueryMetadata {
            has_ephemeral: true,
            ephemeral_sources: vec!["edge".to_string()],
            warnings: vec!["test warning".to_string()],
        };
        let meta = ResultMetadata::from_session(&dirty, 42).unwrap();
        assert!(meta.has_ephemeral);
        assert_eq!(meta.session_id, Some(42));
        assert_eq!(meta.ephemeral_sources, vec!["edge"]);
    }

    // --- Additional edge case tests ---

    #[test]
    fn test_wire_value_cross_type_accessors() {
        // Int32 → i64 widening works
        assert_eq!(WireValue::Int32(42).as_i64(), Some(42));
        // Int64 → i32 narrowing works if in range
        assert_eq!(WireValue::Int64(42).as_i32(), Some(42));
        // Int64 → i32 fails if out of range
        assert_eq!(WireValue::Int64(i64::MAX).as_i32(), None);
        // Int32 → f64 widening works
        assert_eq!(WireValue::Int32(42).as_f64(), Some(42.0));
        // Int64 → f64 conversion works
        assert_eq!(WireValue::Int64(100).as_f64(), Some(100.0));
        // Wrong type returns None
        assert_eq!(WireValue::Int32(42).as_str(), None);
        assert_eq!(WireValue::Int32(42).as_bool(), None);
        assert_eq!(WireValue::String("x".to_string()).as_i32(), None);
        assert_eq!(WireValue::Float64(3.14).as_i32(), None);
        assert_eq!(WireValue::Bool(true).as_i32(), None);
        assert!(!WireValue::Int32(42).is_null());
    }

    #[test]
    fn test_wire_value_vector_types() {
        let vec_val = WireValue::Vector(vec![1.0, 2.0, 3.0]);
        assert_eq!(vec_val.data_type(), WireDataType::Vector { dim: Some(3) });

        let vec_int8 = WireValue::VectorInt8(vec![1, 2, 3, 4]);
        assert_eq!(
            vec_int8.data_type(),
            WireDataType::VectorInt8 { dim: Some(4) }
        );
    }

    #[test]
    fn test_wire_value_bytes_type() {
        let bytes = WireValue::Bytes(vec![0xFF, 0x00, 0xAB]);
        assert_eq!(bytes.data_type(), WireDataType::Bytes);
    }

    #[test]
    fn test_wire_value_null_type() {
        assert!(WireValue::Null.is_null());
        assert_eq!(WireValue::Null.as_i32(), None);
        assert_eq!(WireValue::Null.as_str(), None);
    }

    #[test]
    fn test_wire_value_timestamp() {
        let ts = WireValue::Timestamp(1700000000);
        assert_eq!(ts.data_type(), WireDataType::Timestamp);
    }

    #[test]
    fn test_wire_tuple_empty() {
        let empty = WireTuple::empty();
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());
        assert_eq!(empty.get(0), None);
    }

    #[test]
    fn test_wire_tuple_get_out_of_bounds() {
        let tuple = WireTuple::new(vec![WireValue::Int32(1)]);
        assert_eq!(tuple.get(0), Some(&WireValue::Int32(1)));
        assert_eq!(tuple.get(1), None);
        assert_eq!(tuple.get(100), None);
    }

    #[test]
    fn test_wire_tuple_from_ref_tuple() {
        let pair = (5, 10);
        let tuple: WireTuple = (&pair).into();
        assert_eq!(tuple.len(), 2);
        assert_eq!(tuple.get(0), Some(&WireValue::Int32(5)));
    }

    #[test]
    fn test_wire_tuple_try_into_extra_values_ok() {
        // TryFrom<WireTuple> for (i32, i32) only requires >= 2 values
        let wire = WireTuple::new(vec![
            WireValue::Int32(1),
            WireValue::Int32(2),
            WireValue::Int32(3),
        ]);
        let result: Result<(i32, i32), _> = wire.try_into();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (1, 2));
    }

    #[test]
    fn test_wire_tuple_try_into_wrong_type() {
        let wire = WireTuple::new(vec![
            WireValue::String("hello".to_string()),
            WireValue::Int32(2),
        ]);
        let result: Result<(i32, i32), _> = wire.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_wire_data_type_display() {
        assert_eq!(WireDataType::Int32.to_string(), "Int32");
        assert_eq!(WireDataType::Int64.to_string(), "Int64");
        assert_eq!(WireDataType::Float64.to_string(), "Float64");
        assert_eq!(WireDataType::String.to_string(), "String");
        assert_eq!(WireDataType::Bool.to_string(), "Bool");
        assert_eq!(WireDataType::Bytes.to_string(), "Bytes");
        assert_eq!(
            WireDataType::Vector { dim: Some(128) }.to_string(),
            "Vector[128]"
        );
        assert_eq!(WireDataType::Vector { dim: None }.to_string(), "Vector");
        assert_eq!(
            WireDataType::VectorInt8 { dim: Some(64) }.to_string(),
            "VectorInt8[64]"
        );
    }

    #[test]
    fn test_column_def_constructors() {
        assert_eq!(ColumnDef::int32("a").data_type, WireDataType::Int32);
        assert_eq!(ColumnDef::int64("b").data_type, WireDataType::Int64);
        assert_eq!(ColumnDef::float64("c").data_type, WireDataType::Float64);
        assert_eq!(ColumnDef::string("d").data_type, WireDataType::String);
    }

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult {
            rows: vec![],
            schema: vec![ColumnDef::int32("x")],
            total_count: 0,
            truncated: false,
            execution_time_ms: 0,
            metadata: None,
        };
        assert_eq!(result.rows.len(), 0);
        assert_eq!(result.schema.len(), 1);
    }

    #[test]
    fn test_wire_tuple_provenance_variants() {
        use crate::session::Provenance;

        let persistent =
            WireTuple::with_provenance(vec![WireValue::Int32(1)], Provenance::Persistent);
        let json = serde_json::to_string(&persistent).unwrap();
        assert!(json.contains("\"provenance\":\"persistent\""));

        let mixed = WireTuple::with_provenance(vec![WireValue::Int32(1)], Provenance::Mixed);
        let json = serde_json::to_string(&mixed).unwrap();
        assert!(json.contains("\"provenance\":\"mixed\""));
    }

    #[test]
    fn test_wire_value_json_roundtrip_all_types() {
        let values = vec![
            WireValue::Null,
            WireValue::Int32(i32::MAX),
            WireValue::Int64(i64::MAX),
            WireValue::Float64(f64::MIN),
            WireValue::String("test\nwith\nnewlines".to_string()),
            WireValue::Bool(false),
            WireValue::Timestamp(0),
            WireValue::Vector(vec![]),
            WireValue::VectorInt8(vec![-128, 127]),
            WireValue::Bytes(vec![]),
        ];
        for val in values {
            let json = serde_json::to_string(&val).unwrap();
            let restored: WireValue = serde_json::from_str(&json).unwrap();
            assert_eq!(val, restored);
        }
    }
}
