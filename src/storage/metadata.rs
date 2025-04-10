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

impl KnowledgeGraphsMetadata {
    /// Create new empty metadata
    pub fn new() -> Self {
        KnowledgeGraphsMetadata {
            version: "1.0".to_string(),
            knowledge_graphs: Vec::new(),
        }
    }

    /// Load from file
    pub fn load(path: &Path) -> StorageResult<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let file = File::open(path)?;
        let metadata = serde_json::from_reader(file)?;
        Ok(metadata)
    }

    /// Save to file
    pub fn save(&self, path: &Path) -> StorageResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        serde_json::to_writer_pretty(&file, self)?;
        // Ensure metadata is durably written to disk
        file.sync_all()?;
        Ok(())
    }
}

impl Default for KnowledgeGraphsMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl KnowledgeGraphMetadata {
    /// Create new knowledge graph metadata
    pub fn new(name: String) -> Self {
        KnowledgeGraphMetadata {
            name,
            version: "1.0".to_string(),
            created_at: Utc::now().to_rfc3339(),
            schema_version: 1,
            relations: HashMap::new(),
        }
    }

    /// Load from file
    pub fn load(path: &Path) -> StorageResult<Self> {
        let file = File::open(path).map_err(|e| {
            StorageError::MetadataError(format!("Failed to open metadata file: {e}"))
        })?;

        let metadata = serde_json::from_reader(file)
            .map_err(|e| StorageError::MetadataError(format!("Failed to parse metadata: {e}")))?;

        Ok(metadata)
    }

    /// Save to file
    pub fn save(&self, path: &Path) -> StorageResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(path)?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }

    /// Add or update relation metadata
    pub fn add_relation(&mut self, name: String, schema: Vec<String>, tuple_count: usize) {
        let metadata = RelationMetadata {
            file: format!("relations/{name}.parquet"),
            schema,
            tuple_count,
            last_modified: Utc::now().to_rfc3339(),
        };
        self.relations.insert(name, metadata);
    }

    /// Get total tuple count across all relations
    pub fn total_tuples(&self) -> usize {
        self.relations.values().map(|r| r.tuple_count).sum()
    }
}

impl RelationMetadata {
    /// Create new relation metadata
    pub fn new(name: &str, schema: Vec<String>, tuple_count: usize) -> Self {
        RelationMetadata {
            file: format!("relations/{name}.parquet"),
            schema,
            tuple_count,
            last_modified: Utc::now().to_rfc3339(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_knowledge_graphs_metadata_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("knowledge_graphs.json");

        let mut metadata = KnowledgeGraphsMetadata::new();
        metadata.knowledge_graphs.push(KnowledgeGraphInfo {
            name: "test_kg".to_string(),
            created_at: Utc::now().to_rfc3339(),
            last_accessed: Utc::now().to_rfc3339(),
            relations_count: 2,
            total_tuples: 100,
        });

        metadata.save(&path).unwrap();
        let loaded = KnowledgeGraphsMetadata::load(&path).unwrap();

        assert_eq!(loaded.knowledge_graphs.len(), 1);
        assert_eq!(loaded.knowledge_graphs[0].name, "test_kg");
    }

    #[test]
    fn test_knowledge_graph_metadata_roundtrip() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("kg_metadata.json");

        let mut metadata = KnowledgeGraphMetadata::new("test_kg".to_string());
        metadata.add_relation(
            "edge".to_string(),
            vec!["col0".to_string(), "col1".to_string()],
            50,
        );

        metadata.save(&path).unwrap();
        let loaded = KnowledgeGraphMetadata::load(&path).unwrap();

        assert_eq!(loaded.name, "test_kg");
        assert_eq!(loaded.relations.len(), 1);
        assert!(loaded.relations.contains_key("edge"));
        assert_eq!(loaded.relations["edge"].tuple_count, 50);
    }
}
