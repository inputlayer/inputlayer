//! `OpenAPI` Specification
//!
//! Defines the `OpenAPI` documentation for the REST API using utoipa.

use utoipa::OpenApi;

use super::dto::{
    ApiErrorDto, ApiResponse, CreateKnowledgeGraphRequest, CreateSessionRequest,
    CreateSessionResponse, CreateViewRequest, EphemeralFactsRequest, EphemeralRuleRequest,
    ExplainRequest, ExplainResponse, HealthDto, KnowledgeGraphDto, KnowledgeGraphListDto,
    QueryRequest, QueryResponse, QueryStatus, RelationDataDto, RelationDataQuery, RelationDto,
    RelationListDto, SessionDto, SessionListDto, SessionQueryMetadataDto, SessionQueryRequest,
    SessionQueryResponse, SessionStatsDto, StatsDto, ViewDto, ViewListDto,
};
use super::handlers::{admin, knowledge_graph, query, relations, sessions, views, ws};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "InputLayer API",
        version = "1.0.0",
        description = "REST API for InputLayer Knowledge Graph Engine",
        license(
            name = "Apache-2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        ),
        contact(
            name = "InputLayer Team",
            url = "https://inputlayer.io"
        )
    ),
    servers(
        (url = "/api/v1", description = "API v1")
    ),
    paths(
        // Knowledge Graph endpoints
        knowledge_graph::list_knowledge_graphs,
        knowledge_graph::get_knowledge_graph,
        knowledge_graph::create_knowledge_graph,
        knowledge_graph::delete_knowledge_graph,
        // Query endpoints
        query::execute_query,
        query::explain_query,
        // Session endpoints
        sessions::create_session,
        sessions::close_session,
        sessions::get_session,
        sessions::list_sessions,
        sessions::session_query,
        sessions::insert_ephemeral_facts,
        sessions::retract_ephemeral_facts,
        sessions::add_ephemeral_rule,
        // WebSocket endpoint
        ws::session_websocket,
        // Relations endpoints
        relations::list_relations,
        relations::get_relation,
        relations::get_relation_data,
        // Views endpoints
        views::list_views,
        views::get_view,
        views::get_view_data,
        views::create_view,
        views::delete_view,
        // Admin endpoints
        admin::health,
        admin::stats,
    ),
    components(schemas(
        ApiResponse<KnowledgeGraphDto>,
        ApiResponse<KnowledgeGraphListDto>,
        ApiResponse<QueryResponse>,
        ApiResponse<ExplainResponse>,
        ApiResponse<RelationDto>,
        ApiResponse<RelationListDto>,
        ApiResponse<RelationDataDto>,
        ApiResponse<ViewDto>,
        ApiResponse<ViewListDto>,
        ApiResponse<HealthDto>,
        ApiResponse<StatsDto>,
        ApiResponse<CreateSessionResponse>,
        ApiResponse<SessionDto>,
        ApiResponse<SessionListDto>,
        ApiResponse<SessionQueryResponse>,
        ApiErrorDto,
        KnowledgeGraphDto,
        KnowledgeGraphListDto,
        CreateKnowledgeGraphRequest,
        QueryRequest,
        QueryResponse,
        QueryStatus,
        ExplainRequest,
        ExplainResponse,
        RelationDto,
        RelationListDto,
        RelationDataDto,
        RelationDataQuery,
        ViewDto,
        ViewListDto,
        CreateViewRequest,
        HealthDto,
        StatsDto,
        SessionStatsDto,
        CreateSessionRequest,
        CreateSessionResponse,
        SessionDto,
        SessionListDto,
        SessionQueryRequest,
        SessionQueryResponse,
        SessionQueryMetadataDto,
        EphemeralFactsRequest,
        EphemeralRuleRequest,
    )),
    tags(
        (name = "knowledge-graphs", description = "Knowledge graph management operations"),
        (name = "queries", description = "Query execution and explanation"),
        (name = "sessions", description = "Session lifecycle and ephemeral data management"),
        (name = "relations", description = "Relation data access"),
        (name = "views", description = "View management"),
        (name = "admin", description = "Server administration and health")
    )
)]
pub struct ApiDoc;
