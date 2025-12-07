use super::types::*;
use rpcnet::{RpcServer, RpcConfig, RpcError};
use async_trait::async_trait;
use std::sync::Arc;
/// Handler trait that users implement for the service.
#[async_trait]
pub trait AdminServiceHandler: Send + Sync + 'static {
    async fn health(&self, request: HealthRequest) -> Result<HealthResponse, AdminError>;
    async fn stats(&self, request: StatsRequest) -> Result<StatsResponse, AdminError>;
    async fn shutdown(
        &self,
        request: ShutdownRequest,
    ) -> Result<ShutdownResponse, AdminError>;
    async fn backup(&self, request: BackupRequest) -> Result<BackupResponse, AdminError>;
    async fn clear_caches(
        &self,
        request: ClearCachesRequest,
    ) -> Result<ClearCachesResponse, AdminError>;
}
/// Generated server that manages RPC registration and routing.
pub struct AdminServiceServer<H: AdminServiceHandler> {
    handler: Arc<H>,
    pub rpc_server: RpcServer,
}
impl<H: AdminServiceHandler> AdminServiceServer<H> {
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
                    "AdminService.health",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: HealthRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.health(request).await {
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
                    "AdminService.stats",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: StatsRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.stats(request).await {
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
                    "AdminService.shutdown",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: ShutdownRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.shutdown(request).await {
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
                    "AdminService.backup",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: BackupRequest = bincode::deserialize(&params)
                                .map_err(RpcError::SerializationError)?;
                            match handler.backup(request).await {
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
                    "AdminService.clear_caches",
                    move |params| {
                        let handler = handler.clone();
                        async move {
                            let request: ClearCachesRequest = bincode::deserialize(
                                    &params,
                                )
                                .map_err(RpcError::SerializationError)?;
                            match handler.clear_caches(request).await {
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
