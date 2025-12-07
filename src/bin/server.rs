//! InputLayer RPC Server Binary
//!
//! Starts an InputLayer RPC server that accepts client connections over QUIC+TLS.
//! Exposes all InputLayer services: DatabaseService, DataService, QueryService, AdminService.
//!
//! ## Usage
//!
//! ```bash
//! # Start server with default settings
//! cargo run --bin inputlayer-server
//!
//! # Start with custom address and certs
//! cargo run --bin inputlayer-server -- --addr 0.0.0.0:5433 --cert certs/server.pem --key certs/server.key
//! ```

use datalog_engine::protocol::generated::databaseservice::server::DatabaseServiceHandler;
use datalog_engine::protocol::generated::databaseservice::types::*;
use datalog_engine::protocol::generated::dataservice::server::DataServiceHandler;
use datalog_engine::protocol::generated::dataservice::types::{
    BulkInsertResponse, DataError, DeleteRequest, DeleteResponse, GetSchemaRequest,
    GetSchemaResponse, InsertBatch, InsertRequest, InsertResponse,
};
use datalog_engine::protocol::generated::queryservice::server::QueryServiceHandler;
use datalog_engine::protocol::generated::queryservice::types::{
    ExplainRequest, ExplainResponse, QueryError, QueryRequest, QueryResponse, QueryResultBatch,
};
use datalog_engine::protocol::generated::adminservice::server::AdminServiceHandler;
use datalog_engine::protocol::generated::adminservice::types::{
    AdminError, BackupRequest, BackupResponse, ClearCachesRequest, ClearCachesResponse,
    HealthRequest, HealthResponse, ShutdownRequest, ShutdownResponse, StatsRequest, StatsResponse,
};
use datalog_engine::protocol::UnifiedHandler;
use datalog_engine::Config;

use async_trait::async_trait;
use rpcnet::{RpcConfig, RpcError, RpcServer};
use std::env;
use std::sync::Arc;

const DEFAULT_ADDR: &str = "127.0.0.1:5433";
const DEFAULT_CERT: &str = "certs/server.pem";
const DEFAULT_KEY: &str = "certs/server.key";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let addr = get_arg(&args, "--addr").unwrap_or_else(|| DEFAULT_ADDR.to_string());
    let cert_path = get_arg(&args, "--cert").unwrap_or_else(|| DEFAULT_CERT.to_string());
    let key_path = get_arg(&args, "--key").unwrap_or_else(|| DEFAULT_KEY.to_string());

    println!("InputLayer RPC Server");
    println!("=====================");
    println!("Address: {}", addr);
    println!("Cert:    {}", cert_path);
    println!("Key:     {}", key_path);
    println!();

    // Load configuration
    let config = Config::load().unwrap_or_else(|_| {
        println!("Using default configuration");
        Config::default()
    });

    // Create unified handler
    let handler = Arc::new(UnifiedHandler::from_config(config).expect("Failed to create handler"));

    println!("Storage engine initialized");
    println!("Starting RPC server...");
    println!();

    // Create RPC config and server
    let rpc_config = RpcConfig::new(&cert_path, &addr).with_key_path(&key_path);
    let mut rpc_server = RpcServer::new(rpc_config);

    // Register all services
    let wrapper = Arc::new(HandlerWrapper(handler));
    register_database_service(&mut rpc_server, wrapper.clone()).await;
    register_data_service(&mut rpc_server, wrapper.clone()).await;
    register_query_service(&mut rpc_server, wrapper.clone()).await;
    register_admin_service(&mut rpc_server, wrapper).await;

    println!("Services registered:");
    println!("  - DatabaseService");
    println!("  - DataService");
    println!("  - QueryService");
    println!("  - AdminService");
    println!();

    // Start server
    let quic_server = rpc_server.bind()?;
    println!("Server listening on: {:?}", rpc_server.socket_addr);

    if let Err(e) = rpc_server.start(quic_server).await {
        eprintln!("Server error: {:?}", e);
    }

    Ok(())
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1).cloned())
}

/// Wrapper to implement handler traits for Arc<UnifiedHandler>
struct HandlerWrapper(Arc<UnifiedHandler>);

// ============================================================================
// DatabaseService Registration
// ============================================================================

async fn register_database_service<H: DatabaseServiceHandler>(
    server: &mut RpcServer,
    handler: Arc<H>,
) {
    {
        let h = handler.clone();
        server
            .register("DatabaseService.create_database", move |params| {
                let h = h.clone();
                async move {
                    let request: CreateDatabaseRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.create_database(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("DatabaseService.drop_database", move |params| {
                let h = h.clone();
                async move {
                    let request: DropDatabaseRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.drop_database(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("DatabaseService.list_databases", move |params| {
                let h = h.clone();
                async move {
                    let request: ListDatabasesRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.list_databases(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("DatabaseService.database_info", move |params| {
                let h = h.clone();
                async move {
                    let request: DatabaseInfoRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.database_info(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
}

// ============================================================================
// DataService Registration
// ============================================================================

async fn register_data_service<H: DataServiceHandler>(server: &mut RpcServer, handler: Arc<H>) {
    {
        let h = handler.clone();
        server
            .register("DataService.insert", move |params| {
                let h = h.clone();
                async move {
                    let request: InsertRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.insert(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("DataService.delete", move |params| {
                let h = h.clone();
                async move {
                    let request: DeleteRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.delete(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("DataService.bulk_insert", move |params| {
                let h = h.clone();
                async move {
                    let request: InsertBatch =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.bulk_insert(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("DataService.get_schema", move |params| {
                let h = h.clone();
                async move {
                    let request: GetSchemaRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.get_schema(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
}

// ============================================================================
// QueryService Registration
// ============================================================================

async fn register_query_service<H: QueryServiceHandler>(server: &mut RpcServer, handler: Arc<H>) {
    {
        let h = handler.clone();
        server
            .register("QueryService.query", move |params| {
                let h = h.clone();
                async move {
                    let request: QueryRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.query(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("QueryService.query_stream", move |params| {
                let h = h.clone();
                async move {
                    let request: QueryRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.query_stream(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("QueryService.explain", move |params| {
                let h = h.clone();
                async move {
                    let request: ExplainRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.explain(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
}

// ============================================================================
// AdminService Registration
// ============================================================================

async fn register_admin_service<H: AdminServiceHandler>(server: &mut RpcServer, handler: Arc<H>) {
    {
        let h = handler.clone();
        server
            .register("AdminService.health", move |params| {
                let h = h.clone();
                async move {
                    let request: HealthRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.health(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("AdminService.stats", move |params| {
                let h = h.clone();
                async move {
                    let request: StatsRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.stats(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("AdminService.backup", move |params| {
                let h = h.clone();
                async move {
                    let request: BackupRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.backup(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("AdminService.shutdown", move |params| {
                let h = h.clone();
                async move {
                    let request: ShutdownRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.shutdown(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
    {
        let h = handler.clone();
        server
            .register("AdminService.clear_caches", move |params| {
                let h = h.clone();
                async move {
                    let request: ClearCachesRequest =
                        bincode::deserialize(&params).map_err(RpcError::SerializationError)?;
                    match h.clear_caches(request).await {
                        Ok(response) => {
                            bincode::serialize(&response).map_err(RpcError::SerializationError)
                        }
                        Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                    }
                }
            })
            .await;
    }
}

// ============================================================================
// Handler Trait Implementations
// ============================================================================

#[async_trait]
impl DatabaseServiceHandler for HandlerWrapper {
    async fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, DatabaseError> {
        self.0.create_database(request).await
    }

    async fn drop_database(
        &self,
        request: DropDatabaseRequest,
    ) -> Result<DropDatabaseResponse, DatabaseError> {
        self.0.drop_database(request).await
    }

    async fn list_databases(
        &self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, DatabaseError> {
        self.0.list_databases(request).await
    }

    async fn database_info(
        &self,
        request: DatabaseInfoRequest,
    ) -> Result<DatabaseInfoResponse, DatabaseError> {
        self.0.database_info(request).await
    }

    async fn register_view(
        &self,
        request: RegisterViewRequest,
    ) -> Result<RegisterViewResponse, DatabaseError> {
        self.0.register_view(request).await
    }

    async fn drop_view(
        &self,
        request: DropViewRequest,
    ) -> Result<DropViewResponse, DatabaseError> {
        self.0.drop_view(request).await
    }

    async fn list_views(
        &self,
        request: ListViewsRequest,
    ) -> Result<ListViewsResponse, DatabaseError> {
        self.0.list_views(request).await
    }

    async fn describe_view(
        &self,
        request: DescribeViewRequest,
    ) -> Result<DescribeViewResponse, DatabaseError> {
        self.0.describe_view(request).await
    }
}

#[async_trait]
impl DataServiceHandler for HandlerWrapper {
    async fn insert(&self, request: InsertRequest) -> Result<InsertResponse, DataError> {
        self.0.insert(request).await
    }

    async fn delete(&self, request: DeleteRequest) -> Result<DeleteResponse, DataError> {
        self.0.delete(request).await
    }

    async fn bulk_insert(&self, request: InsertBatch) -> Result<BulkInsertResponse, DataError> {
        self.0.bulk_insert(request).await
    }

    async fn get_schema(
        &self,
        request: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, DataError> {
        self.0.get_schema(request).await
    }
}

#[async_trait]
impl QueryServiceHandler for HandlerWrapper {
    async fn query(&self, request: QueryRequest) -> Result<QueryResponse, QueryError> {
        self.0.query(request).await
    }

    async fn query_stream(
        &self,
        request: QueryRequest,
    ) -> Result<QueryResultBatch, QueryError> {
        self.0.query_stream(request).await
    }

    async fn explain(&self, request: ExplainRequest) -> Result<ExplainResponse, QueryError> {
        self.0.explain(request).await
    }
}

#[async_trait]
impl AdminServiceHandler for HandlerWrapper {
    async fn health(&self, request: HealthRequest) -> Result<HealthResponse, AdminError> {
        self.0.health(request).await
    }

    async fn stats(&self, request: StatsRequest) -> Result<StatsResponse, AdminError> {
        self.0.stats(request).await
    }

    async fn backup(&self, request: BackupRequest) -> Result<BackupResponse, AdminError> {
        self.0.backup(request).await
    }

    async fn shutdown(&self, request: ShutdownRequest) -> Result<ShutdownResponse, AdminError> {
        self.0.shutdown(request).await
    }

    async fn clear_caches(
        &self,
        request: ClearCachesRequest,
    ) -> Result<ClearCachesResponse, AdminError> {
        self.0.clear_caches(request).await
    }
}
