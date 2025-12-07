use super::types::*;
use rpcnet::{RpcServer, RpcConfig, RpcError};
use async_trait::async_trait;
use std::sync::Arc;
/// Handler trait that users implement for the service.
#[async_trait]
pub trait DatabaseServiceHandler: Send + Sync + 'static {
    async fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, DatabaseError>;
    async fn drop_database(
        &self,
        request: DropDatabaseRequest,
    ) -> Result<DropDatabaseResponse, DatabaseError>;
    async fn list_databases(
        &self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, DatabaseError>;
    async fn database_info(
        &self,
        request: DatabaseInfoRequest,
    ) -> Result<DatabaseInfoResponse, DatabaseError>;
    async fn register_view(
        &self,
        request: RegisterViewRequest,
    ) -> Result<RegisterViewResponse, DatabaseError>;
    async fn drop_view(
        &self,
        request: DropViewRequest,
    ) -> Result<DropViewResponse, DatabaseError>;
    async fn list_views(
        &self,
        request: ListViewsRequest,
    ) -> Result<ListViewsResponse, DatabaseError>;
    async fn describe_view(
        &self,
        request: DescribeViewRequest,
    ) -> Result<DescribeViewResponse, DatabaseError>;
}
/// Generated server that manages RPC registration and routing.
pub struct DatabaseServiceServer<H: DatabaseServiceHandler> {
    handler: Arc<H>,
    pub rpc_server: RpcServer,
}
impl<H: DatabaseServiceHandler> DatabaseServiceServer<H> {
    /// Creates a new server with the given handler and configuration.
    pub fn new(handler: H, config: RpcConfig) -> Self {
        Self {
            handler: Arc::new(handler),
            rpc_server: RpcServer::new(config),
        }
    }
    /// Registers all service methods with the RPC server.
    pub async fn register_all(&mut self) {
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.create_database",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: CreateDatabaseRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.create_database(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.drop_database",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: DropDatabaseRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.drop_database(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.list_databases",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: ListDatabasesRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.list_databases(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.database_info",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: DatabaseInfoRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.database_info(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.register_view",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: RegisterViewRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.register_view(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.drop_view",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: DropViewRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.drop_view(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.list_views",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: ListViewsRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.list_views(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
        {
            let handler = self.handler.clone();
            self.rpc_server
                .register(
                    "DatabaseService.describe_view",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: DescribeViewRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.describe_view(request).await {
                                Ok(response) => {
                                    bincode::serialize(&response)
                                        .map_err(RpcError::SerializationError)
                                }
                                Err(e) => Err(RpcError::StreamError(format!("{:?}", e))),
                            }
                        }
                    },
                )
                .await;
        }
    }
    /// Starts the server and begins accepting connections.
    pub async fn serve(mut self) -> Result<(), RpcError> {
        self.register_all().await;
        let quic_server = self.rpc_server.bind()?;
        println!("Server listening on: {:?}", self.rpc_server.socket_addr);
        self.rpc_server.start(quic_server).await
    }
}
