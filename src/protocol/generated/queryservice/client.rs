use super::types::*;
use rpcnet::{RpcClient, RpcConfig, RpcError};
use std::net::SocketAddr;
/// Generated client for calling service methods.
pub struct QueryServiceClient {
    inner: RpcClient,
}
impl QueryServiceClient {
    /// Connects to the service at the given address.
    pub async fn connect(addr: SocketAddr, config: RpcConfig) -> Result<Self, RpcError> {
        let inner = RpcClient::connect(addr, config).await?;
        Ok(Self { inner })
    }
    pub async fn query(&self, request: QueryRequest) -> Result<QueryResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("QueryService.query", params).await?;
        bincode::deserialize::<QueryResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn query_stream(
        &self,
        request: QueryRequest,
    ) -> Result<QueryResultBatch, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("QueryService.query_stream", params).await?;
        bincode::deserialize::<QueryResultBatch>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn explain(
        &self,
        request: ExplainRequest,
    ) -> Result<ExplainResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("QueryService.explain", params).await?;
        bincode::deserialize::<ExplainResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
}
