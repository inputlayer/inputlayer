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

