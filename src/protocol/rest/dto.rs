//! HTTP API Data Transfer Objects
//!
//! Defines request/response types for admin endpoints and WebSocket metadata.

use serde::Serialize;

/// JSON response: { success, data?, error? }
#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiErrorDto>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }
}

/// Error details in API response
#[derive(Debug, Serialize)]
pub struct ApiErrorDto {
    pub code: String,
    pub message: String,
}

/// Health check response
#[derive(Debug, Serialize)]
pub struct HealthDto {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
}

/// Server statistics response
#[derive(Debug, Serialize)]
pub struct StatsDto {
    pub knowledge_graphs: usize,
    pub relations: usize,
    pub views: usize,
    pub memory_usage_bytes: u64,
    pub query_count: u64,
    pub uptime_secs: u64,
    /// Session statistics
    pub sessions: SessionStatsDto,
}

/// Session statistics within server stats
#[derive(Debug, Serialize)]
pub struct SessionStatsDto {
    pub total: usize,
    pub clean: usize,
    pub dirty: usize,
    pub total_ephemeral_facts: usize,
    pub total_ephemeral_rules: usize,
}

/// Provenance metadata in session query response (used by WS handler)
#[derive(Debug, Serialize)]
pub struct SessionQueryMetadataDto {
    pub has_ephemeral: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ephemeral_sources: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_response_success() {
        let resp = ApiResponse::success("hello");
        assert!(resp.success);
        assert_eq!(resp.data, Some("hello"));
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_api_response_success_serialization() {
        let resp = ApiResponse::success(42);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"success\":true"));
        assert!(json.contains("\"data\":42"));
        assert!(!json.contains("error"));
    }

    #[test]
    fn test_health_dto_serialize() {
        let health = HealthDto {
            status: "healthy".to_string(),
            version: "1.0.0".to_string(),
            uptime_secs: 120,
        };
        let json = serde_json::to_string(&health).unwrap();
        assert!(json.contains("\"status\":\"healthy\""));
        assert!(json.contains("\"version\":\"1.0.0\""));
        assert!(json.contains("\"uptime_secs\":120"));
    }

    #[test]
    fn test_stats_dto_serialize() {
        let stats = StatsDto {
            knowledge_graphs: 2,
            relations: 10,
            views: 3,
            memory_usage_bytes: 1024000,
            query_count: 42,
            uptime_secs: 300,
            sessions: SessionStatsDto {
                total: 5,
                clean: 3,
                dirty: 2,
                total_ephemeral_facts: 100,
                total_ephemeral_rules: 10,
            },
        };
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"knowledge_graphs\":2"));
        assert!(json.contains("\"query_count\":42"));
        assert!(json.contains("\"total\":5"));
        assert!(json.contains("\"total_ephemeral_facts\":100"));
    }
}
