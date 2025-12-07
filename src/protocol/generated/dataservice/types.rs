//! Type definitions for the service.
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataError {
    RelationNotFound { relation: String, database: String },
    SchemaViolation { expected: String, got: String },
    TypeMismatch { expected: String, got: String },
    DatabaseNotFound { name: String },
    Internal { message: String },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkInsertResponse {
    pub total_rows_inserted: usize,
    pub batches_processed: u32,
    pub duration_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {
    pub rows_affected: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireValue {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Bool(bool),
    Timestamp(i64),
    Vector(Vec<f32>),
    VectorInt8(Vec<i8>),
    Bytes(Vec<u8>),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSchemaRequest {
    pub database: Option<String>,
    pub relation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResponse {
    pub rows_affected: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSchemaResponse {
    pub schema: Vec<ColumnDef>,
    pub tuple_count: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireTuple {
    pub values: Vec<WireValue>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertBatch {
    pub database: Option<String>,
    pub relation: String,
    pub tuples: Vec<WireTuple>,
    pub batch_number: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertRequest {
    pub database: Option<String>,
    pub relation: String,
    pub tuples: Vec<WireTuple>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub database: Option<String>,
    pub relation: String,
    pub tuples: Vec<WireTuple>,
}
