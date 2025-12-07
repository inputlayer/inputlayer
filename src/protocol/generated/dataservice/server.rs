use super::types::*;
use rpcnet::{RpcServer, RpcConfig, RpcError};
use async_trait::async_trait;
use std::sync::Arc;
/// Handler trait that users implement for the service.
#[async_trait]
pub trait DataServiceHandler: Send + Sync + 'static {
    async fn insert(&self, request: InsertRequest) -> Result<InsertResponse, DataError>;
    async fn delete(&self, request: DeleteRequest) -> Result<DeleteResponse, DataError>;
    async fn bulk_insert(
        &self,
        batches: InsertBatch,
    ) -> Result<BulkInsertResponse, DataError>;
    async fn get_schema(
        &self,
        request: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, DataError>;
}
/// Generated server that manages RPC registration and routing.
pub struct DataServiceServer<H: DataServiceHandler> {
    handler: Arc<H>,
    pub rpc_server: RpcServer,
}
impl<H: DataServiceHandler> DataServiceServer<H> {
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
                    "DataService.insert",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: InsertRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.insert(request).await {
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
                    "DataService.delete",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: DeleteRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.delete(request).await {
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
                    "DataService.bulk_insert",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: InsertBatch = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.bulk_insert(request).await {
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
                    "DataService.get_schema",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: GetSchemaRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.get_schema(request).await {
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
