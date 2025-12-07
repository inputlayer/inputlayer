use super::types::*;
use rpcnet::{RpcClient, RpcConfig, RpcError};
use std::net::SocketAddr;
/// Generated client for calling service methods.
pub struct DatabaseServiceClient {
    inner: RpcClient,
}
impl DatabaseServiceClient {
    /// Connects to the service at the given address.
    pub async fn connect(addr: SocketAddr, config: RpcConfig) -> Result<Self, RpcError> {
        let inner = RpcClient::connect(addr, config).await?;
        Ok(Self { inner })
    }
    pub async fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self
            .inner
            .call("DatabaseService.create_database", params)
            .await?;
        bincode::deserialize::<CreateDatabaseResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn drop_database(
        &self,
        request: DropDatabaseRequest,
    ) -> Result<DropDatabaseResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self
            .inner
            .call("DatabaseService.drop_database", params)
            .await?;
        bincode::deserialize::<DropDatabaseResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn list_databases(
        &self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self
            .inner
            .call("DatabaseService.list_databases", params)
            .await?;
        bincode::deserialize::<ListDatabasesResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn database_info(
        &self,
        request: DatabaseInfoRequest,
    ) -> Result<DatabaseInfoResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self
            .inner
            .call("DatabaseService.database_info", params)
            .await?;
        bincode::deserialize::<DatabaseInfoResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn register_view(
        &self,
        request: RegisterViewRequest,
    ) -> Result<RegisterViewResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self
            .inner
            .call("DatabaseService.register_view", params)
            .await?;
        bincode::deserialize::<RegisterViewResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn drop_view(
        &self,
        request: DropViewRequest,
    ) -> Result<DropViewResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("DatabaseService.drop_view", params).await?;
        bincode::deserialize::<DropViewResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn list_views(
        &self,
        request: ListViewsRequest,
    ) -> Result<ListViewsResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self.inner.call("DatabaseService.list_views", params).await?;
        bincode::deserialize::<ListViewsResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
    pub async fn describe_view(
        &self,
        request: DescribeViewRequest,
    ) -> Result<DescribeViewResponse, RpcError> {
        let params = bincode::serialize(&request).map_err(RpcError::SerializationError)?;
        let response_data = self
            .inner
            .call("DatabaseService.describe_view", params)
            .await?;
        bincode::deserialize::<DescribeViewResponse>(&response_data)
            .map_err(RpcError::SerializationError)
    }
}
