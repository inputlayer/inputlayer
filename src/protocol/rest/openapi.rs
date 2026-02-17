//! `OpenAPI` Specification
//!
//! Defines the `OpenAPI` documentation for the REST API using utoipa.

use utoipa::OpenApi;

use super::dto::{
    ApiErrorDto, ApiResponse, CreateKnowledgeGraphRequest, CreateSessionRequest,
    CreateSessionResponse, CreateViewRequest, EphemeralFactsRequest, EphemeralRuleRequest,
    ExplainRequest, ExplainResponse, HealthDto, KnowledgeGraphDto, KnowledgeGraphListDto,
    OntologyDefinition, OntologyStatusDto, QueryRequest, QueryResponse, QueryStatus,
    RelationDataDto, RelationDataQuery, RelationDto, RelationListDto, SessionDto, SessionListDto,
    SessionQueryMetadataDto, SessionQueryRequest, SessionQueryResponse, SessionStatsDto, StatsDto,
    ValidationError, ViewDto, ViewListDto,
};
use super::handlers::{admin, data, knowledge_graph, query, relations, rules, sessions, views, ws};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "InputLayer API",
        version = "1.0.0",
        description = "REST and WebSocket API for InputLayer Knowledge Graph Engine.\n\n\
            ## Overview\n\n\
            InputLayer provides a full REST API for knowledge graph management, query execution, \
            and session-based ephemeral data operations. A WebSocket endpoint is available for \
            real-time bidirectional communication within sessions.\n\n\
            ## Authentication\n\n\
            When authentication is enabled, include the JWT token in the Authorization header:\n\
            `Authorization: Bearer <your-jwt-token>`\n\n\
            ## WebSocket\n\n\
            The WebSocket endpoint at `/sessions/{id}/ws` provides real-time query execution, \
            ephemeral data operations, and push notifications for persistent data changes. \
            See the endpoint documentation for the full message protocol specification.",
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
        // Data endpoints
        data::insert_data,
        data::delete_data,
        // Relations endpoints
        relations::list_relations,
        relations::get_relation,
        relations::get_relation_data,
        relations::clear_relations_by_prefix,
        // Rules endpoints
        rules::list_rules,
        rules::get_rule,
        rules::delete_rule,
        rules::delete_rule_clause,
        rules::delete_rules_by_prefix,
        // Views endpoints
        views::list_views,
        views::get_view,
        views::get_view_data,
        views::create_view,
        views::delete_view,
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
        // Admin endpoints
        admin::health,
        admin::stats,
    ),
    components(schemas(
        // Response wrappers
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
        // Knowledge Graph
        KnowledgeGraphDto,
        KnowledgeGraphListDto,
        CreateKnowledgeGraphRequest,
        OntologyDefinition,
        OntologyStatusDto,
        // Queries
        QueryRequest,
        QueryResponse,
        QueryStatus,
        ValidationError,
        ExplainRequest,
        ExplainResponse,
        // Data
        data::InsertDataRequest,
        data::InsertDataResponse,
        data::DeleteDataRequest,
        data::DeleteDataResponse,
        // Relations
        RelationDto,
        RelationListDto,
        RelationDataDto,
        RelationDataQuery,
        relations::ClearByPrefixResult,
        relations::ClearedRelation,
        // Rules
        rules::RuleDto,
        rules::RuleListDto,
        rules::DeleteClauseResult,
        rules::DropByPrefixResult,
        // Views
        ViewDto,
        ViewListDto,
        CreateViewRequest,
        // Sessions
        CreateSessionRequest,
        CreateSessionResponse,
        SessionDto,
        SessionListDto,
        SessionQueryRequest,
        SessionQueryResponse,
        SessionQueryMetadataDto,
        EphemeralFactsRequest,
        EphemeralRuleRequest,
        // Admin
        HealthDto,
        StatsDto,
        SessionStatsDto,
    )),
    tags(
        (name = "knowledge-graphs", description = "Knowledge graph management — create, list, get details, and delete knowledge graphs"),
        (name = "queries", description = "Query execution and explanation — execute Datalog queries and get execution plans"),
        (name = "data", description = "Persistent data operations — insert and delete tuples in relations"),
        (name = "relations", description = "Relation metadata — list relations, get schemas, read data with pagination"),
        (name = "rules", description = "Rule management — list, inspect, and delete persistent rules and individual clauses"),
        (name = "views", description = "View management — create, list, inspect, query, and delete derived views"),
        (name = "sessions", description = "Session lifecycle and ephemeral data — create sessions, manage ephemeral facts/rules, query with provenance"),
        (name = "websocket", description = "WebSocket real-time connection — session-scoped bidirectional communication with push notifications"),
        (name = "admin", description = "Server administration — health checks and statistics")
    )
)]
pub struct ApiDoc;
