use super::types::*;
use rpcnet::{RpcClient, RpcConfig, RpcError};
use std::net::SocketAddr;
/// Generated client for calling service methods.
pub struct DataServiceClient {
    inner: RpcClient,
}
impl DataServiceClient {
    /// Connects to the service at the given address.
    pub async fn connect(addr: SocketAddr, config: RpcConfig) -> Result<Self, RpcError> {
        let inner = RpcClient::connect(addr, config).await?;
        Ok(Self { inner })
    }
    pub async fn insert(
        &self,
        request: InsertRequest,
    ) -> Result<InsertResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("DataService.insert", params).await?;
        bincode::deserialize::<InsertResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn delete(
        &self,
        request: DeleteRequest,
    ) -> Result<DeleteResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("DataService.delete", params).await?;
        bincode::deserialize::<DeleteResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn bulk_insert(
        &self,
        batches: InsertBatch,
    ) -> Result<BulkInsertResponse, RpcError> {
        let params = bincode::serialize(&batches).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("DataService.bulk_insert", params).await?;
        bincode::deserialize::<BulkInsertResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn get_schema(
        &self,
        request: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("DataService.get_schema", params).await?;
        bincode::deserialize::<GetSchemaResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
}
