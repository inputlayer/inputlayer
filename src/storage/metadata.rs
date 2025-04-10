//! Metadata Management for Storage Engine

use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;

use super::error::{StorageError, StorageResult};

/// System-wide metadata for all knowledge graphs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeGraphsMetadata {
    pub version: String,
    pub knowledge_graphs: Vec<KnowledgeGraphInfo>,
}

/// Information about a single knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeGraphInfo {
    pub name: String,
    pub created_at: String,
    pub last_accessed: String,
    pub relations_count: usize,
    pub total_tuples: usize,
}

/// Metadata for a single knowledge graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeGraphMetadata {
    pub name: String,
    pub version: String,
    pub created_at: String,
    pub schema_version: u32,
    pub relations: HashMap<String, RelationMetadata>,
}

/// Metadata for a single relation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationMetadata {
    pub file: String,
    pub schema: Vec<String>,
    pub tuple_count: usize,
    pub last_modified: String,
}

