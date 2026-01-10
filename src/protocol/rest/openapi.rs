//! OpenAPI Specification
//!
//! Defines the OpenAPI documentation for the REST API using utoipa.

use utoipa::OpenApi;

use super::dto::*;
use super::handlers::{admin, knowledge_graph, query, relations, views};

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
    )),
    tags(
        (name = "knowledge-graphs", description = "Knowledge graph management operations"),
        (name = "queries", description = "Query execution and explanation"),
        (name = "relations", description = "Relation data access"),
        (name = "views", description = "View management"),
        (name = "admin", description = "Server administration and health")
    )
)]
pub struct ApiDoc;
