use super::types::*;
use rpcnet::{RpcClient, RpcConfig, RpcError};
use std::net::SocketAddr;
/// Generated client for calling service methods.
pub struct AdminServiceClient {
    inner: RpcClient,
}
impl AdminServiceClient {
    /// Connects to the service at the given address.
    pub async fn connect(addr: SocketAddr, config: RpcConfig) -> Result<Self, RpcError> {
        let inner = RpcClient::connect(addr, config).await?;
        Ok(Self { inner })
    }
    pub async fn health(
        &self,
        request: HealthRequest,
    ) -> Result<HealthResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("AdminService.health", params).await?;
        bincode::deserialize::<HealthResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn stats(&self, request: StatsRequest) -> Result<StatsResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("AdminService.stats", params).await?;
        bincode::deserialize::<StatsResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn shutdown(
        &self,
        request: ShutdownRequest,
    ) -> Result<ShutdownResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("AdminService.shutdown", params).await?;
        bincode::deserialize::<ShutdownResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn backup(
        &self,
        request: BackupRequest,
    ) -> Result<BackupResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("AdminService.backup", params).await?;
        bincode::deserialize::<BackupResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn clear_caches(
        &self,
        request: ClearCachesRequest,
    ) -> Result<ClearCachesResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("AdminService.clear_caches", params).await?;
        bincode::deserialize::<ClearCachesResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
}
