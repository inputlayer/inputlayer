//! REST API Module
//!
//! Provides HTTP REST API endpoints via Axum for the `InputLayer` GUI and external clients.
//! This is the primary API interface with `OpenAPI` documentation available at `/api/docs`.

pub mod dto;
pub mod error;
pub mod handlers;
pub mod openapi;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Extension, Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::HttpConfig;
use crate::protocol::Handler;

use self::handlers::{admin, data, knowledge_graph, query, relations, rules, sessions, views, ws};
use self::openapi::ApiDoc;

/// Creates the Axum router for the REST API
pub fn create_router(handler: Arc<Handler>, config: &HttpConfig) -> Router {
    // API v1 routes
    let api_routes = Router::new()
        // Knowledge Graph routes
        .route(
            "/knowledge-graphs",
            get(knowledge_graph::list_knowledge_graphs),
        )
        .route(
            "/knowledge-graphs",
            post(knowledge_graph::create_knowledge_graph),
        )
        .route(
            "/knowledge-graphs/:name",
            get(knowledge_graph::get_knowledge_graph),
        )
        .route(
            "/knowledge-graphs/:name",
            delete(knowledge_graph::delete_knowledge_graph),
        )
        // Query routes
        .route("/query/execute", post(query::execute_query))
        .route("/query/explain", post(query::explain_query))
        // Relations routes
        .route(
            "/knowledge-graphs/:kg/relations",
            get(relations::list_relations).delete(relations::clear_relations_by_prefix),
        )
        .route(
            "/knowledge-graphs/:kg/relations/:name",
            get(relations::get_relation),
        )
        .route(
            "/knowledge-graphs/:kg/relations/:name/data",
            get(relations::get_relation_data)
                .post(data::insert_data)
                .delete(data::delete_data),
        )
        // Rules routes
        .route(
            "/knowledge-graphs/:kg/rules",
            get(rules::list_rules).delete(rules::delete_rules_by_prefix),
        )
        .route(
            "/knowledge-graphs/:kg/rules/:name",
            get(rules::get_rule).delete(rules::delete_rule),
        )
        .route(
            "/knowledge-graphs/:kg/rules/:name/:index",
            delete(rules::delete_rule_clause),
        )
        // Views routes
        .route("/knowledge-graphs/:kg/views", get(views::list_views))
        .route("/knowledge-graphs/:kg/views/:name", get(views::get_view))
        .route(
            "/knowledge-graphs/:kg/views/:name/data",
            get(views::get_view_data),
        )
        .route("/knowledge-graphs/:kg/views", post(views::create_view))
        .route(
            "/knowledge-graphs/:kg/views/:name",
            delete(views::delete_view),
        )
        // Session routes
        .route(
            "/sessions",
            get(sessions::list_sessions).post(sessions::create_session),
        )
        .route(
            "/sessions/:id",
            get(sessions::get_session).delete(sessions::close_session),
        )
        .route("/sessions/:id/query", post(sessions::session_query))
        .route(
            "/sessions/:id/facts",
            post(sessions::insert_ephemeral_facts).delete(sessions::retract_ephemeral_facts),
        )
        .route("/sessions/:id/rules", post(sessions::add_ephemeral_rule))
        // WebSocket route (session-scoped real-time connection)
        .route("/sessions/:id/ws", get(ws::session_websocket))
        // Admin routes
        .route("/health", get(admin::health))
        .route("/stats", get(admin::stats));

    // Build CORS layer
    let cors = if config.cors_origins.is_empty() {
        // Development mode: allow all origins
        CorsLayer::permissive()
    } else {
        // Production mode: restrict to configured origins
        let origins: Vec<_> = config
            .cors_origins
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    // Main router with API and Swagger UI
    let mut app = Router::new()
        .nest("/api/v1", api_routes)
        .merge(SwaggerUi::new("/api/docs").url("/api/openapi.json", ApiDoc::openapi()))
        .layer(Extension(handler))
        .layer(cors);

    // Serve GUI static files if enabled
    if config.gui.enabled {
        let static_dir = &config.gui.static_dir;
        let index_file = format!("{static_dir}/index.html");

        // Fallback to index.html for SPA routing
        app = app.fallback_service(ServeDir::new(static_dir).fallback(ServeFile::new(index_file)));
    }

    app
}

/// Starts the HTTP server
pub async fn start_http_server(
    handler: Arc<Handler>,
    config: &HttpConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = create_router(handler, config);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    println!("HTTP server listening on: http://{addr}");
    if config.gui.enabled {
        println!("GUI dashboard available at: http://{addr}/");
    }
    println!("API documentation at: http://{addr}/api/docs");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
