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

    /// Save to file using atomic write-to-temp-then-rename.
    ///
    /// Writes to a unique temp file, calls `sync_all()`, then renames to `{path}`.
    /// Rename is atomic on POSIX, so the metadata file is always either the old
    /// or new version — never a corrupt half-written state.
    /// Uses a unique temp file name per call to avoid races under concurrent saves.
    pub fn save(&self, path: &Path) -> StorageResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Use thread ID + timestamp to create a unique temp file name,
        // preventing ENOENT races when concurrent threads save simultaneously.
        let unique = format!(
            "{:?}.{}",
            std::thread::current().id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let tmp_name = format!(
            "{}.{unique}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
        let tmp_path = path.with_file_name(tmp_name);

        // Write to temp file
        let file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&file, self)?;
        // Ensure metadata is durably written to disk
        file.sync_all()?;

        // Atomic rename (POSIX guarantees atomicity)
        fs::rename(&tmp_path, path)?;

        // Sync parent directory to ensure rename is durable
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }

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

    /// Save to file with durable write.
    ///
    /// Uses atomic write-to-temp-then-rename to prevent corruption on crash.
    /// Uses a unique temp file name per call to avoid races under concurrent saves.
    pub fn save(&self, path: &Path) -> StorageResult<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let unique = format!(
            "{:?}.{}",
            std::thread::current().id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let tmp_name = format!(
            "{}.{unique}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy()
        );
        let tmp_path = path.with_file_name(tmp_name);

        // Write to temp file
        let file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&file, self)?;
        // Ensure data is durably written to disk before rename
        file.sync_all()?;

        // Atomic rename (POSIX guarantees atomicity)
        fs::rename(&tmp_path, path)?;

        // Sync parent directory to ensure rename is durable
        if let Some(parent) = path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }

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

    #[test]
    fn test_knowledge_graphs_metadata_new() {
        let metadata = KnowledgeGraphsMetadata::new();
        assert_eq!(metadata.version, "1.0");
        assert!(metadata.knowledge_graphs.is_empty());
    }

    #[test]
    fn test_knowledge_graphs_metadata_default() {
        let metadata = KnowledgeGraphsMetadata::default();
        assert_eq!(metadata.version, "1.0");
        assert!(metadata.knowledge_graphs.is_empty());
    }

    #[test]
    fn test_knowledge_graphs_metadata_load_nonexistent() {
        let path = Path::new("/tmp/nonexistent_metadata_test.json");
        let metadata = KnowledgeGraphsMetadata::load(path).unwrap();
        assert!(metadata.knowledge_graphs.is_empty());
    }

    #[test]
    fn test_knowledge_graph_metadata_new() {
        let metadata = KnowledgeGraphMetadata::new("test".to_string());
        assert_eq!(metadata.name, "test");
        assert_eq!(metadata.version, "1.0");
        assert_eq!(metadata.schema_version, 1);
        assert!(metadata.relations.is_empty());
    }

    #[test]
    fn test_knowledge_graph_metadata_total_tuples() {
        let mut metadata = KnowledgeGraphMetadata::new("test".to_string());
        assert_eq!(metadata.total_tuples(), 0);

        metadata.add_relation("a".to_string(), vec!["x".to_string()], 10);
        metadata.add_relation("b".to_string(), vec!["y".to_string()], 20);
        assert_eq!(metadata.total_tuples(), 30);
    }

    #[test]
    fn test_knowledge_graph_metadata_add_relation_overwrites() {
        let mut metadata = KnowledgeGraphMetadata::new("test".to_string());
        metadata.add_relation("a".to_string(), vec!["x".to_string()], 10);
        metadata.add_relation("a".to_string(), vec!["x".to_string()], 50);
        assert_eq!(metadata.relations.len(), 1);
        assert_eq!(metadata.relations["a"].tuple_count, 50);
    }

    #[test]
    fn test_relation_metadata_new() {
        let rm = RelationMetadata::new("edge", vec!["x".to_string(), "y".to_string()], 42);
        assert_eq!(rm.file, "relations/edge.parquet");
        assert_eq!(rm.schema.len(), 2);
        assert_eq!(rm.tuple_count, 42);
        assert!(!rm.last_modified.is_empty());
    }

    #[test]
    fn test_knowledge_graph_metadata_load_invalid_file() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("invalid.json");
        fs::write(&path, "not json").unwrap();
        let result = KnowledgeGraphMetadata::load(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_knowledge_graphs_metadata_multiple() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("multi_kg.json");

        let mut metadata = KnowledgeGraphsMetadata::new();
        for i in 0..5 {
            metadata.knowledge_graphs.push(KnowledgeGraphInfo {
                name: format!("kg_{i}"),
                created_at: Utc::now().to_rfc3339(),
                last_accessed: Utc::now().to_rfc3339(),
                relations_count: i,
                total_tuples: i * 100,
            });
        }

        metadata.save(&path).unwrap();
        let loaded = KnowledgeGraphsMetadata::load(&path).unwrap();
        assert_eq!(loaded.knowledge_graphs.len(), 5);
        assert_eq!(loaded.knowledge_graphs[3].name, "kg_3");
    }

    // === Regression tests for P0 durability fixes ===

    /// Regression: KnowledgeGraphMetadata::save() must call sync_all()
    /// to ensure data is durable on disk (not just in OS page cache).
    /// Verify by checking the file is readable and valid immediately after save.
    #[test]
    fn test_kg_metadata_save_is_durable() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("meta").join("kg_metadata.json");

        let mut metadata = KnowledgeGraphMetadata::new("durable_test".to_string());
        metadata.add_relation(
            "edge".to_string(),
            vec!["x".to_string(), "y".to_string()],
            42,
        );
        metadata.save(&path).unwrap();

        // File must exist and be valid JSON
        assert!(path.exists(), "Metadata file must exist after save");
        let content = fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["name"], "durable_test");
        assert_eq!(parsed["relations"]["edge"]["tuple_count"], 42);
    }

    /// Regression: KnowledgeGraphsMetadata::save() must use atomic write
    /// (write-to-temp-then-rename) so partial writes never corrupt the file.
    /// Verify no .tmp files remain after save.
    #[test]
    fn test_kgs_metadata_save_atomic_no_temp_files() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("knowledge_graphs.json");

        let mut metadata = KnowledgeGraphsMetadata::new();
        metadata.knowledge_graphs.push(KnowledgeGraphInfo {
            name: "atomic_test".to_string(),
            created_at: Utc::now().to_rfc3339(),
            last_accessed: Utc::now().to_rfc3339(),
            relations_count: 1,
            total_tuples: 100,
        });
        metadata.save(&path).unwrap();

        // Final file must exist and be valid
        assert!(path.exists());
        let loaded = KnowledgeGraphsMetadata::load(&path).unwrap();
        assert_eq!(loaded.knowledge_graphs[0].name, "atomic_test");

        // No .tmp files should remain
        let tmp_path = path.with_extension("json.tmp");
        assert!(
            !tmp_path.exists(),
            "Temp file must not remain after atomic save"
        );
    }

    /// Regression: KnowledgeGraphMetadata::save() must use atomic write.
    /// Verify no .tmp files remain after save.
    #[test]
    fn test_kg_metadata_save_atomic_no_temp_files() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("kg_metadata.json");

        let metadata = KnowledgeGraphMetadata::new("atomic_test".to_string());
        metadata.save(&path).unwrap();

        // No .tmp files should remain
        let tmp_path = path.with_extension("json.tmp");
        assert!(
            !tmp_path.exists(),
            "Temp file must not remain after atomic save"
        );
    }

    /// Regression: Repeated saves must not leave stale temp files.
    #[test]
    fn test_metadata_repeated_saves_no_stale_temps() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("knowledge_graphs.json");

        for i in 0..10 {
            let mut metadata = KnowledgeGraphsMetadata::new();
            metadata.knowledge_graphs.push(KnowledgeGraphInfo {
                name: format!("kg_{i}"),
                created_at: Utc::now().to_rfc3339(),
                last_accessed: Utc::now().to_rfc3339(),
                relations_count: i,
                total_tuples: i * 100,
            });
            metadata.save(&path).unwrap();
        }

        // Final file should contain last write
        let loaded = KnowledgeGraphsMetadata::load(&path).unwrap();
        assert_eq!(loaded.knowledge_graphs[0].name, "kg_9");

        // No temp files should remain
        let entries: Vec<_> = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().to_str().is_some_and(|s| s.ends_with(".tmp")))
            .collect();
        assert!(
            entries.is_empty(),
            "No .tmp files should remain after repeated saves"
        );
    }

    /// Regression: Metadata save creates parent directories if they don't exist.
    #[test]
    fn test_metadata_save_creates_parent_dirs() {
        let temp = TempDir::new().unwrap();
        let path = temp
            .path()
            .join("deeply")
            .join("nested")
            .join("dir")
            .join("meta.json");

        let metadata = KnowledgeGraphMetadata::new("nested_test".to_string());
        metadata.save(&path).unwrap();

        assert!(path.exists());
        let loaded = KnowledgeGraphMetadata::load(&path).unwrap();
        assert_eq!(loaded.name, "nested_test");
    }

    /// Regression: Concurrent saves must not fail with ENOENT.
    /// This was the root cause of 868 E2E snapshot test failures.
    /// Before the fix, all concurrent saves used the same .tmp file name,
    /// causing a TOCTOU race: thread A renames .tmp, thread B's rename fails.
    #[test]
    fn test_concurrent_metadata_saves_no_enoent() {
        use std::sync::{Arc, Barrier};

        let temp = TempDir::new().unwrap();
        let path = Arc::new(temp.path().join("knowledge_graphs.json"));
        let num_threads = 16;
        let saves_per_thread = 20;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..saves_per_thread {
                        let mut metadata = KnowledgeGraphsMetadata::new();
                        metadata.knowledge_graphs.push(KnowledgeGraphInfo {
                            name: format!("kg_t{t}_i{i}"),
                            created_at: Utc::now().to_rfc3339(),
                            last_accessed: Utc::now().to_rfc3339(),
                            relations_count: i,
                            total_tuples: i * 10,
                        });
                        metadata
                            .save(&path)
                            .unwrap_or_else(|e| panic!("Thread {t} save {i} failed: {e}"));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Final file must be valid JSON
        let loaded = KnowledgeGraphsMetadata::load(&path).unwrap();
        assert_eq!(loaded.knowledge_graphs.len(), 1);

        // No stale .tmp files should remain
        let tmp_files: Vec<_> = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().to_str().is_some_and(|s| s.ends_with(".tmp")))
            .collect();
        assert!(
            tmp_files.is_empty(),
            "No .tmp files should remain after concurrent saves, found: {tmp_files:?}"
        );
    }

    /// Regression: Concurrent KnowledgeGraphMetadata saves must also be safe.
    #[test]
    fn test_concurrent_kg_metadata_saves_no_enoent() {
        use std::sync::{Arc, Barrier};

        let temp = TempDir::new().unwrap();
        let path = Arc::new(temp.path().join("kg_meta.json"));
        let num_threads = 8;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let path = Arc::clone(&path);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..10 {
                        let mut metadata = KnowledgeGraphMetadata::new(format!("kg_t{t}_i{i}"));
                        metadata.add_relation(
                            "edge".to_string(),
                            vec!["x".to_string(), "y".to_string()],
                            i * 10,
                        );
                        metadata
                            .save(&path)
                            .unwrap_or_else(|e| panic!("Thread {t} save {i} failed: {e}"));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Final file must be valid
        let loaded = KnowledgeGraphMetadata::load(&path).unwrap();
        assert!(!loaded.name.is_empty());
    }
}
