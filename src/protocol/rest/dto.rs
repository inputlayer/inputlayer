//! REST API Data Transfer Objects
//!
//! Defines request/response types for the REST API endpoints.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// JSON response: { success, data?, error? }
#[derive(Debug, Serialize, ToSchema)]
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

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> ApiResponse<()> {
        ApiResponse {
            success: false,
            data: None,
            error: Some(ApiErrorDto {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

/// Error details in API response
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorDto {
    pub code: String,
    pub message: String,
}

// Knowledge Graph DTOs
/// Knowledge Graph information
#[derive(Debug, Serialize, ToSchema)]
pub struct KnowledgeGraphDto {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub relations_count: usize,
    pub views_count: usize,
}

/// List of knowledge graphs
#[derive(Debug, Serialize, ToSchema)]
pub struct KnowledgeGraphListDto {
    pub knowledge_graphs: Vec<KnowledgeGraphDto>,
    /// Currently selected knowledge graph (may not exist if invalid)
    pub current: Option<String>,
    /// Warning message if current KG was not found
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Create knowledge graph request
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateKnowledgeGraphRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

// Query DTOs
/// Query execution request
#[derive(Debug, Deserialize, ToSchema)]
pub struct QueryRequest {
    /// The Datalog query to execute
    pub query: String,
    /// Knowledge graph to execute against
    pub knowledge_graph: String,
    /// Optional timeout in milliseconds
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30000
}

/// Query execution response
#[derive(Debug, Serialize, ToSchema)]
pub struct QueryResponse {
    pub query: String,
    pub status: QueryStatus,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Query execution status
#[derive(Debug, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum QueryStatus {
    Success,
    Error,
}

/// Query explanation request
#[derive(Debug, Deserialize, ToSchema)]
pub struct ExplainRequest {
    pub query: String,
    pub knowledge_graph: String,
}

/// Query explanation response
#[derive(Debug, Serialize, ToSchema)]
pub struct ExplainResponse {
    pub query: String,
    pub plan: String,
    pub optimizations: Vec<String>,
}

// Relation DTOs
/// Relation information
#[derive(Debug, Serialize, ToSchema)]
pub struct RelationDto {
    pub name: String,
    pub arity: usize,
    pub tuple_count: usize,
    pub columns: Vec<String>,
    pub is_view: bool,
}

/// Relation list response
#[derive(Debug, Serialize, ToSchema)]
pub struct RelationListDto {
    pub relations: Vec<RelationDto>,
}

/// Relation data response
#[derive(Debug, Serialize, ToSchema)]
pub struct RelationDataDto {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub total_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Query parameters for relation data
#[derive(Debug, Deserialize, ToSchema)]
pub struct RelationDataQuery {
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
}

// View DTOs
/// View information
#[derive(Debug, Serialize, ToSchema)]
pub struct ViewDto {
    pub name: String,
    pub definition: String,
    pub arity: usize,
    pub columns: Vec<String>,
    pub dependencies: Vec<String>,
}

/// View list response
#[derive(Debug, Serialize, ToSchema)]
pub struct ViewListDto {
    pub views: Vec<ViewDto>,
}

/// Create view request
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateViewRequest {
    pub name: String,
    pub definition: String,
}

// Session DTOs
/// Create session request
#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateSessionRequest {
    /// Knowledge graph to bind this session to
    pub knowledge_graph: String,
}

/// Session information
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionDto {
    pub id: u64,
    pub knowledge_graph: String,
    pub is_clean: bool,
    pub ephemeral_fact_count: usize,
    pub ephemeral_rule_count: usize,
}

/// Create session response
#[derive(Debug, Serialize, ToSchema)]
pub struct CreateSessionResponse {
    pub session_id: u64,
    pub knowledge_graph: String,
}

/// Session list response
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionListDto {
    pub sessions: Vec<SessionDto>,
    pub total: usize,
    pub clean: usize,
    pub dirty: usize,
}

/// Session query request (execute within session context)
#[derive(Debug, Deserialize, ToSchema)]
pub struct SessionQueryRequest {
    /// The Datalog query to execute
    pub query: String,
}

/// Session query response (includes provenance metadata)
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionQueryResponse {
    pub query: String,
    pub status: QueryStatus,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Per-row provenance tags (parallel array to `rows`)
    /// Each entry is "persistent" or "ephemeral"
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub row_provenance: Vec<String>,
    /// Provenance metadata (present when ephemeral data participates)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SessionQueryMetadataDto>,
}

/// Provenance metadata in session query response
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionQueryMetadataDto {
    pub has_ephemeral: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ephemeral_sources: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Ephemeral fact insert request
#[derive(Debug, Deserialize, ToSchema)]
pub struct EphemeralFactsRequest {
    /// Relation name
    pub relation: String,
    /// Tuples as arrays of JSON values
    pub tuples: Vec<Vec<serde_json::Value>>,
}

/// Ephemeral rule request
#[derive(Debug, Deserialize, ToSchema)]
pub struct EphemeralRuleRequest {
    /// Rule text in Datalog syntax (e.g. "path(X,Y) <- edge(X,Y)")
    pub rule: String,
}

// Admin DTOs
/// Health check response
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthDto {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
}

/// Server statistics response
#[derive(Debug, Serialize, ToSchema)]
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
#[derive(Debug, Serialize, ToSchema)]
pub struct SessionStatsDto {
    pub total: usize,
    pub clean: usize,
    pub dirty: usize,
    pub total_ephemeral_facts: usize,
    pub total_ephemeral_rules: usize,
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
    fn test_api_response_error() {
        let resp = ApiResponse::<()>::error("NOT_FOUND", "item gone");
        assert!(!resp.success);
        assert!(resp.data.is_none());
        let err = resp.error.unwrap();
        assert_eq!(err.code, "NOT_FOUND");
        assert_eq!(err.message, "item gone");
    }

    #[test]
    fn test_api_response_error_serialization() {
        let resp = ApiResponse::<()>::error("BAD", "invalid");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"success\":false"));
        assert!(!json.contains("\"data\""));
        assert!(json.contains("\"error\""));
    }

    #[test]
    fn test_query_request_deserialize() {
        let json = r#"{"query": "?edge(X,Y)", "knowledge_graph": "test"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.query, "?edge(X,Y)");
        assert_eq!(req.knowledge_graph, "test");
    }

    #[test]
    fn test_query_request_default_timeout() {
        let json = r#"{"query": "?edge(X,Y)", "knowledge_graph": "test"}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.timeout_ms, 30000);
    }

    #[test]
    fn test_query_request_custom_timeout() {
        let json = r#"{"query": "?edge(X,Y)", "knowledge_graph": "test", "timeout_ms": 5000}"#;
        let req: QueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.timeout_ms, 5000);
    }

    #[test]
    fn test_query_response_serialize() {
        let resp = QueryResponse {
            query: "?x(A)".to_string(),
            status: QueryStatus::Success,
            columns: vec!["col0".to_string()],
            rows: vec![vec![serde_json::json!(1)]],
            row_count: 1,
            execution_time_ms: 5,
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"status\":\"success\""));
        assert!(json.contains("\"row_count\":1"));
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_query_response_with_error() {
        let resp = QueryResponse {
            query: "bad".to_string(),
            status: QueryStatus::Error,
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms: 1,
            error: Some("parse error".to_string()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"status\":\"error\""));
        assert!(json.contains("\"error\":\"parse error\""));
    }

    #[test]
    fn test_create_session_request_deserialize() {
        let json = r#"{"knowledge_graph": "mykg"}"#;
        let req: CreateSessionRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.knowledge_graph, "mykg");
    }

    #[test]
    fn test_session_query_request_deserialize() {
        let json = r#"{"query": "?data(X)"}"#;
        let req: SessionQueryRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.query, "?data(X)");
    }

    #[test]
    fn test_session_query_response_serialize() {
        let resp = SessionQueryResponse {
            query: "?x(A)".to_string(),
            status: QueryStatus::Success,
            columns: vec!["a".to_string()],
            rows: vec![],
            row_count: 0,
            execution_time_ms: 2,
            error: None,
            row_provenance: vec![],
            metadata: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"status\":\"success\""));
        // row_provenance and metadata omitted when empty/none
        assert!(!json.contains("row_provenance"));
        assert!(!json.contains("metadata"));
    }

    #[test]
    fn test_session_query_response_with_provenance() {
        let resp = SessionQueryResponse {
            query: "?x(A)".to_string(),
            status: QueryStatus::Success,
            columns: vec!["a".to_string()],
            rows: vec![vec![serde_json::json!(1)]],
            row_count: 1,
            execution_time_ms: 3,
            error: None,
            row_provenance: vec!["persistent".to_string()],
            metadata: Some(SessionQueryMetadataDto {
                has_ephemeral: true,
                ephemeral_sources: vec!["edge".to_string()],
                warnings: vec![],
            }),
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"row_provenance\""));
        assert!(json.contains("\"has_ephemeral\":true"));
    }

    #[test]
    fn test_ephemeral_facts_request_deserialize() {
        let json = r#"{"relation": "edge", "tuples": [[1,2],[3,4]]}"#;
        let req: EphemeralFactsRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.relation, "edge");
        assert_eq!(req.tuples.len(), 2);
    }

    #[test]
    fn test_ephemeral_rule_request_deserialize() {
        let json = r#"{"rule": "path(X,Y) <- edge(X,Y)"}"#;
        let req: EphemeralRuleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.rule, "path(X,Y) <- edge(X,Y)");
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
    fn test_create_view_request_deserialize() {
        let json = r#"{"name": "my_view", "definition": "view(X) <- base(X)"}"#;
        let req: CreateViewRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "my_view");
        assert_eq!(req.definition, "view(X) <- base(X)");
    }

    #[test]
    fn test_knowledge_graph_dto_serialize() {
        let kg = KnowledgeGraphDto {
            name: "test".to_string(),
            description: Some("A test KG".to_string()),
            relations_count: 5,
            views_count: 2,
        };
        let json = serde_json::to_string(&kg).unwrap();
        assert!(json.contains("\"name\":\"test\""));
        assert!(json.contains("\"relations_count\":5"));
        assert!(json.contains("\"views_count\":2"));
        assert!(json.contains("\"description\":\"A test KG\""));
    }

    #[test]
    fn test_knowledge_graph_dto_no_description() {
        let kg = KnowledgeGraphDto {
            name: "minimal".to_string(),
            description: None,
            relations_count: 0,
            views_count: 0,
        };
        let json = serde_json::to_string(&kg).unwrap();
        assert!(json.contains("\"name\":\"minimal\""));
        // description should be omitted when None
        assert!(!json.contains("description"));
    }

    #[test]
    fn test_relation_dto_serialize() {
        let rel = RelationDto {
            name: "edge".to_string(),
            arity: 2,
            tuple_count: 50,
            columns: vec!["col0".to_string(), "col1".to_string()],
            is_view: false,
        };
        let json = serde_json::to_string(&rel).unwrap();
        assert!(json.contains("\"name\":\"edge\""));
        assert!(json.contains("\"arity\":2"));
        assert!(json.contains("\"is_view\":false"));
    }

    #[test]
    fn test_relation_data_dto_serialize() {
        let data = RelationDataDto {
            name: "edge".to_string(),
            columns: vec!["src".to_string(), "dst".to_string()],
            rows: vec![vec![serde_json::json!(1), serde_json::json!(2)]],
            row_count: 1,
            total_count: 10,
            offset: Some(0),
            limit: Some(100),
        };
        let json = serde_json::to_string(&data).unwrap();
        assert!(json.contains("\"row_count\":1"));
        assert!(json.contains("\"total_count\":10"));
        assert!(json.contains("\"offset\":0"));
    }

    #[test]
    fn test_relation_data_query_deserialize() {
        let json = r#"{"offset": 10, "limit": 50}"#;
        let query: RelationDataQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.offset, Some(10));
        assert_eq!(query.limit, Some(50));
    }

    #[test]
    fn test_relation_data_query_defaults() {
        let json = r#"{}"#;
        let query: RelationDataQuery = serde_json::from_str(json).unwrap();
        assert_eq!(query.offset, None);
        assert_eq!(query.limit, None);
    }

    #[test]
    fn test_explain_request_deserialize() {
        let json = r#"{"query": "path(X,Y) <- edge(X,Y)", "knowledge_graph": "test"}"#;
        let req: ExplainRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.query, "path(X,Y) <- edge(X,Y)");
        assert_eq!(req.knowledge_graph, "test");
    }

    #[test]
    fn test_explain_response_serialize() {
        let resp = ExplainResponse {
            query: "?edge(X,Y)".to_string(),
            plan: "Scan(edge)".to_string(),
            optimizations: vec!["SIP".to_string()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"plan\":\"Scan(edge)\""));
        assert!(json.contains("\"optimizations\""));
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

    #[test]
    fn test_create_kg_request_deserialize() {
        let json = r#"{"name": "mykg", "description": "A knowledge graph"}"#;
        let req: CreateKnowledgeGraphRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "mykg");
        assert_eq!(req.description, Some("A knowledge graph".to_string()));
    }

    #[test]
    fn test_create_kg_request_no_description() {
        let json = r#"{"name": "mykg"}"#;
        let req: CreateKnowledgeGraphRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "mykg");
        assert_eq!(req.description, None);
    }

    #[test]
    fn test_kg_list_dto_serialize() {
        let list = KnowledgeGraphListDto {
            knowledge_graphs: vec![],
            current: Some("default".to_string()),
            warning: None,
        };
        let json = serde_json::to_string(&list).unwrap();
        assert!(json.contains("\"current\":\"default\""));
        assert!(!json.contains("warning"));
    }

    #[test]
    fn test_session_list_dto_serialize() {
        let list = SessionListDto {
            sessions: vec![],
            total: 5,
            clean: 3,
            dirty: 2,
        };
        let json = serde_json::to_string(&list).unwrap();
        assert!(json.contains("\"total\":5"));
        assert!(json.contains("\"clean\":3"));
        assert!(json.contains("\"dirty\":2"));
    }

    #[test]
    fn test_session_dto_serialize() {
        let session = SessionDto {
            id: 42,
            knowledge_graph: "test".to_string(),
            is_clean: true,
            ephemeral_fact_count: 0,
            ephemeral_rule_count: 0,
        };
        let json = serde_json::to_string(&session).unwrap();
        assert!(json.contains("\"id\":42"));
        assert!(json.contains("\"is_clean\":true"));
    }

    #[test]
    fn test_view_dto_serialize() {
        let view = ViewDto {
            name: "reachable".to_string(),
            definition: "reachable(X,Y) <- edge(X,Y)".to_string(),
            arity: 2,
            columns: vec!["col0".to_string(), "col1".to_string()],
            dependencies: vec!["edge".to_string()],
        };
        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("\"name\":\"reachable\""));
        assert!(json.contains("\"arity\":2"));
        assert!(json.contains("\"dependencies\""));
    }
}
