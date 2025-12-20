//! Unified Handler for InputLayer RPC Services
//!
//! Implements all generated handler traits using the StorageEngine backend.
//!
//! Performance: Uses parking_lot::RwLock for faster lock acquisition (no poisoning)
//! and AtomicU64 for lock-free statistics counters.

use crate::storage_engine::StorageEngine;
use crate::Config;
use async_trait::async_trait;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// Import generated types and handler traits
use super::generated::adminservice::server::AdminServiceHandler;
use super::generated::adminservice::types as admin;
use super::generated::databaseservice::server::DatabaseServiceHandler;
use super::generated::databaseservice::types as db;
use super::generated::dataservice::server::DataServiceHandler;
use super::generated::dataservice::types as data;
use super::generated::queryservice::server::QueryServiceHandler;
use super::generated::queryservice::types as query;

// ============================================================================
// UnifiedHandler
// ============================================================================

/// Unified handler implementing all InputLayer RPC service handlers.
///
/// This struct wraps a StorageEngine and provides thread-safe access
/// for concurrent RPC calls across all services.
///
/// Performance optimizations:
/// - Uses parking_lot::RwLock instead of std::sync::RwLock (no poisoning, faster)
/// - Uses AtomicU64 for counters (lock-free statistics)
pub struct UnifiedHandler {
    storage: Arc<RwLock<StorageEngine>>,
    start_time: Instant,
    query_count: AtomicU64,
    insert_count: AtomicU64,
}

impl UnifiedHandler {
    /// Create a new unified handler with the given storage engine.
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage: Arc::new(RwLock::new(storage)),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
        }
    }

    /// Create a new handler from configuration.
    pub fn from_config(config: Config) -> Result<Self, String> {
        let storage =
            StorageEngine::new(config).map_err(|e| format!("Failed to create storage: {}", e))?;
        Ok(Self::new(storage))
    }

    /// Get uptime in seconds.
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    fn inc_query_count(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_insert_count(&self) {
        self.insert_count.fetch_add(1, Ordering::Relaxed);
    }

    fn total_queries(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    fn total_inserts(&self) -> u64 {
        self.insert_count.load(Ordering::Relaxed)
    }
}

// ============================================================================
// DatabaseServiceHandler Implementation
// ============================================================================

#[async_trait]
impl DatabaseServiceHandler for UnifiedHandler {
    async fn create_database(
        &self,
        request: db::CreateDatabaseRequest,
    ) -> Result<db::CreateDatabaseResponse, db::DatabaseError> {
        let mut storage = self.storage.write();

        storage
            .create_database(&request.name)
            .map_err(|e| db::DatabaseError::Internal {
                message: e.to_string(),
            })?;

        Ok(db::CreateDatabaseResponse {
            success: true,
            message: format!("Database '{}' created successfully", request.name),
        })
    }

    async fn drop_database(
        &self,
        request: db::DropDatabaseRequest,
    ) -> Result<db::DropDatabaseResponse, db::DatabaseError> {
        if !request.confirm {
            return Ok(db::DropDatabaseResponse {
                success: false,
                message: "Drop not confirmed. Set confirm=true to drop database.".to_string(),
            });
        }

        let mut storage = self.storage.write();

        storage
            .drop_database(&request.name)
            .map_err(|e| db::DatabaseError::Internal {
                message: e.to_string(),
            })?;

        Ok(db::DropDatabaseResponse {
            success: true,
            message: format!("Database '{}' dropped successfully", request.name),
        })
    }

    async fn list_databases(
        &self,
        _request: db::ListDatabasesRequest,
    ) -> Result<db::ListDatabasesResponse, db::DatabaseError> {
        let storage = self.storage.read();

        let databases = storage
            .list_databases()
            .into_iter()
            .map(|name| db::DatabaseInfo {
                name,
                created_at: 0,
                relations_count: 0,
                total_tuples: 0,
            })
            .collect();

        Ok(db::ListDatabasesResponse { databases })
    }

    async fn database_info(
        &self,
        request: db::DatabaseInfoRequest,
    ) -> Result<db::DatabaseInfoResponse, db::DatabaseError> {
        let storage = self.storage.read();

        let db_names = storage.list_databases();
        if !db_names.contains(&request.name) {
            return Err(db::DatabaseError::NotFound { name: request.name });
        }

        Ok(db::DatabaseInfoResponse {
            info: db::DatabaseInfo {
                name: request.name,
                created_at: 0,
                relations_count: 0,
                total_tuples: 0,
            },
            relations: vec![],
        })
    }

    async fn register_view(
        &self,
        request: db::RegisterViewRequest,
    ) -> Result<db::RegisterViewResponse, db::DatabaseError> {
        // Parse the rule using statement parser
        let view_def = crate::statement::parse_view_definition(&request.rule)
            .map_err(|e| db::DatabaseError::Internal {
                message: format!("Failed to parse view definition: {}", e),
            })?;

        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db_name) = request.database {
            storage.use_database(db_name).map_err(|e| db::DatabaseError::NotFound {
                name: e.to_string(),
            })?;
        }

        storage.register_view(&view_def).map_err(|e| db::DatabaseError::Internal {
            message: e.to_string(),
        })?;

        Ok(db::RegisterViewResponse {
            success: true,
            message: format!("View '{}' registered successfully", view_def.name),
        })
    }

    async fn drop_view(
        &self,
        request: db::DropViewRequest,
    ) -> Result<db::DropViewResponse, db::DatabaseError> {
        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db_name) = request.database {
            storage.use_database(db_name).map_err(|e| db::DatabaseError::NotFound {
                name: e.to_string(),
            })?;
        }

        storage.drop_view(&request.name).map_err(|e| db::DatabaseError::Internal {
            message: e.to_string(),
        })?;

        Ok(db::DropViewResponse {
            success: true,
            message: format!("View '{}' dropped successfully", request.name),
        })
    }

    async fn list_views(
        &self,
        request: db::ListViewsRequest,
    ) -> Result<db::ListViewsResponse, db::DatabaseError> {
        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db_name) = request.database {
            storage.use_database(db_name).map_err(|e| db::DatabaseError::NotFound {
                name: e.to_string(),
            })?;
        }

        let view_names = storage.list_views().map_err(|e| db::DatabaseError::Internal {
            message: e.to_string(),
        })?;

        let views = view_names
            .into_iter()
            .map(|name| db::ViewInfo {
                name,
                rules_count: 0,
                created_at: String::new(),
                description: None,
            })
            .collect();

        Ok(db::ListViewsResponse { views })
    }

    async fn describe_view(
        &self,
        request: db::DescribeViewRequest,
    ) -> Result<db::DescribeViewResponse, db::DatabaseError> {
        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db_name) = request.database {
            storage.use_database(db_name).map_err(|e| db::DatabaseError::NotFound {
                name: e.to_string(),
            })?;
        }

        let desc = storage.describe_view(&request.name).map_err(|e| db::DatabaseError::Internal {
            message: e.to_string(),
        })?;

        match desc {
            Some(d) => Ok(db::DescribeViewResponse { description: d }),
            None => Err(db::DatabaseError::NotFound { name: request.name }),
        }
    }
}

// ============================================================================
// QueryServiceHandler Implementation
// ============================================================================

#[async_trait]
impl QueryServiceHandler for UnifiedHandler {
    async fn query(
        &self,
        request: query::QueryRequest,
    ) -> Result<query::QueryResponse, query::QueryError> {
        self.inc_query_count();
        let start = Instant::now();

        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db) = request.database {
            storage.use_database(db).map_err(|e| query::QueryError::DatabaseNotFound {
                name: e.to_string(),
            })?;
        }

        // Execute query
        let results = storage
            .execute_query(&request.program)
            .map_err(|e| query::QueryError::ExecutionError {
                message: e.to_string(),
            })?;

        let execution_time = start.elapsed().as_millis() as u64;

        // Apply limit if specified
        let results = if let Some(limit) = request.limit {
            results.into_iter().take(limit).collect()
        } else {
            results
        };

        // Convert to wire format
        let rows: Vec<query::WireTuple> = results
            .iter()
            .map(|(a, b)| query::WireTuple {
                values: vec![query::WireValue::Int32(*a), query::WireValue::Int32(*b)],
            })
            .collect();

        let rows_returned = rows.len() as u64;

        Ok(query::QueryResponse {
            rows,
            schema: vec![
                query::ColumnDef {
                    name: "col0".to_string(),
                    data_type: "Int32".to_string(),
                },
                query::ColumnDef {
                    name: "col1".to_string(),
                    data_type: "Int32".to_string(),
                },
            ],
            stats: query::QueryStats {
                execution_time_ms: execution_time,
                rows_scanned: rows_returned,
                rows_returned,
            },
        })
    }

    async fn query_stream(
        &self,
        _request: query::QueryRequest,
    ) -> Result<query::QueryResultBatch, query::QueryError> {
        // Streaming not fully implemented yet
        Err(query::QueryError::Internal {
            message: "Streaming queries not yet implemented".to_string(),
        })
    }

    async fn explain(
        &self,
        request: query::ExplainRequest,
    ) -> Result<query::ExplainResponse, query::QueryError> {
        let plan_text = format!(
            "Query Plan for: {}\nDatabase: {}\n\n(Query plan generation not yet implemented)",
            request.program,
            request.database.unwrap_or_else(|| "current".to_string())
        );

        Ok(query::ExplainResponse { plan_text })
    }
}

// ============================================================================
// DataServiceHandler Implementation
// ============================================================================

#[async_trait]
impl DataServiceHandler for UnifiedHandler {
    async fn insert(
        &self,
        request: data::InsertRequest,
    ) -> Result<data::InsertResponse, data::DataError> {
        self.inc_insert_count();

        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db) = request.database {
            storage.use_database(db).map_err(|e| data::DataError::DatabaseNotFound {
                name: e.to_string(),
            })?;
        }

        // Convert wire tuples to Tuple2
        let tuples: Vec<(i32, i32)> = request
            .tuples
            .into_iter()
            .filter_map(|t| {
                if t.values.len() >= 2 {
                    match (&t.values[0], &t.values[1]) {
                        (data::WireValue::Int32(a), data::WireValue::Int32(b)) => Some((*a, *b)),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect();

        let rows_affected = tuples.len();

        // Insert with WAL-based durability (O(1) append to WAL file)
        // No need to call save_database - WAL ensures crash recovery
        storage
            .insert(&request.relation, tuples)
            .map_err(|e| data::DataError::Internal {
                message: e.to_string(),
            })?;

        Ok(data::InsertResponse { rows_affected })
    }

    async fn delete(
        &self,
        request: data::DeleteRequest,
    ) -> Result<data::DeleteResponse, data::DataError> {
        let mut storage = self.storage.write();

        // Switch to target database if specified
        if let Some(ref db) = request.database {
            storage.use_database(db).map_err(|e| data::DataError::DatabaseNotFound {
                name: e.to_string(),
            })?;
        }

        // Convert wire tuples to Tuple2
        let tuples: Vec<(i32, i32)> = request
            .tuples
            .into_iter()
            .filter_map(|t| {
                if t.values.len() >= 2 {
                    match (&t.values[0], &t.values[1]) {
                        (data::WireValue::Int32(a), data::WireValue::Int32(b)) => Some((*a, *b)),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .collect();

        let rows_affected = tuples.len();

        // Delete with WAL-based durability (O(1) append to WAL file)
        // No need to call save_database - WAL ensures crash recovery
        storage
            .delete(&request.relation, tuples)
            .map_err(|e| data::DataError::Internal {
                message: e.to_string(),
            })?;

        Ok(data::DeleteResponse { rows_affected })
    }

    async fn bulk_insert(
        &self,
        _batch: data::InsertBatch,
    ) -> Result<data::BulkInsertResponse, data::DataError> {
        // Bulk insert not fully implemented yet
        Err(data::DataError::Internal {
            message: "Bulk insert not yet implemented".to_string(),
        })
    }

    async fn get_schema(
        &self,
        _request: data::GetSchemaRequest,
    ) -> Result<data::GetSchemaResponse, data::DataError> {
        Ok(data::GetSchemaResponse {
            schema: vec![
                data::ColumnDef {
                    name: "col0".to_string(),
                    data_type: "Int32".to_string(),
                },
                data::ColumnDef {
                    name: "col1".to_string(),
                    data_type: "Int32".to_string(),
                },
            ],
            tuple_count: 0,
        })
    }
}

// ============================================================================
// AdminServiceHandler Implementation
// ============================================================================

#[async_trait]
impl AdminServiceHandler for UnifiedHandler {
    async fn health(
        &self,
        _request: admin::HealthRequest,
    ) -> Result<admin::HealthResponse, admin::AdminError> {
        let storage = self.storage.read();

        let databases_loaded = storage.list_databases();

        Ok(admin::HealthResponse {
            status: admin::HealthStatus::Healthy,
            uptime_seconds: self.uptime_seconds(),
            memory_used_bytes: 0,
            active_queries: 0,
            databases_loaded,
        })
    }

    async fn stats(
        &self,
        _request: admin::StatsRequest,
    ) -> Result<admin::StatsResponse, admin::AdminError> {
        Ok(admin::StatsResponse {
            total_queries: self.total_queries(),
            total_inserts: self.total_inserts(),
            cache_hit_rate: 0.0,
            avg_query_time_ms: 0.0,
        })
    }

    async fn shutdown(
        &self,
        request: admin::ShutdownRequest,
    ) -> Result<admin::ShutdownResponse, admin::AdminError> {
        if request.graceful {
            let storage = self.storage.read();

            storage.save_all().map_err(|e| admin::AdminError::ShutdownFailed {
                reason: format!("Failed to save databases: {}", e),
            })?;
        }

        Ok(admin::ShutdownResponse { success: true })
    }

    async fn backup(
        &self,
        request: admin::BackupRequest,
    ) -> Result<admin::BackupResponse, admin::AdminError> {
        let storage = self.storage.read();

        storage
            .save_database(&request.database)
            .map_err(|e| admin::AdminError::BackupFailed {
                reason: format!("Backup failed: {}", e),
            })?;

        Ok(admin::BackupResponse {
            success: true,
            size_bytes: 0,
        })
    }

    async fn clear_caches(
        &self,
        _request: admin::ClearCachesRequest,
    ) -> Result<admin::ClearCachesResponse, admin::AdminError> {
        crate::clear_lsh_cache();

        Ok(admin::ClearCachesResponse {
            caches_cleared: vec!["lsh_cache".to_string()],
        })
    }
}
