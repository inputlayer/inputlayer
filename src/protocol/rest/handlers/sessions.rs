//! Session Handlers
//!
//! Endpoints for session lifecycle management, ephemeral data operations,
//! and session-scoped query execution.

use std::sync::Arc;
use std::time::Instant;

use axum::{extract::Path, Extension, Json};

use super::wire_value_to_json;
use crate::protocol::rest::dto::{
    ApiResponse, CreateSessionRequest, CreateSessionResponse, EphemeralFactsRequest,
    EphemeralRuleRequest, QueryStatus, SessionDto, SessionListDto, SessionQueryMetadataDto,
    SessionQueryRequest, SessionQueryResponse,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::Handler;
use crate::value::{Tuple, Value};

/// Create a new session
#[utoipa::path(
    post,
    path = "/sessions",
    tag = "sessions",
    request_body = CreateSessionRequest,
    responses(
        (status = 200, description = "Session created", body = ApiResponse<CreateSessionResponse>),
        (status = 400, description = "Max sessions exceeded"),
    )
)]
pub async fn create_session(
    Extension(handler): Extension<Arc<Handler>>,
    Json(request): Json<CreateSessionRequest>,
) -> Result<Json<ApiResponse<CreateSessionResponse>>, RestError> {
    let session_id = handler
        .create_session(&request.knowledge_graph)
        .map_err(RestError::bad_request)?;

    Ok(Json(ApiResponse::success(CreateSessionResponse {
        session_id,
        knowledge_graph: request.knowledge_graph,
    })))
}

/// Close a session
#[utoipa::path(
    delete,
    path = "/sessions/{id}",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    responses(
        (status = 200, description = "Session closed"),
        (status = 404, description = "Session not found"),
    )
)]
pub async fn close_session(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
) -> Result<Json<ApiResponse<String>>, RestError> {
    handler.close_session(id).map_err(RestError::not_found)?;

    Ok(Json(ApiResponse::success(format!("Session {id} closed"))))
}

/// Get session info
#[utoipa::path(
    get,
    path = "/sessions/{id}",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    responses(
        (status = 200, description = "Session info", body = ApiResponse<SessionDto>),
        (status = 404, description = "Session not found"),
    )
)]
pub async fn get_session(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
) -> Result<Json<ApiResponse<SessionDto>>, RestError> {
    let mgr = handler.session_manager();

    let dto = mgr
        .with_session(id, |session| SessionDto {
            id: session.id,
            knowledge_graph: session.knowledge_graph.clone(),
            is_clean: session.is_clean(),
            ephemeral_fact_count: session.ephemeral_fact_count(),
            ephemeral_rule_count: session.ephemeral_rule_count(),
        })
        .map_err(RestError::not_found)?;

    Ok(Json(ApiResponse::success(dto)))
}

/// List all sessions
#[utoipa::path(
    get,
    path = "/sessions",
    tag = "sessions",
    responses(
        (status = 200, description = "Session list", body = ApiResponse<SessionListDto>),
    )
)]
pub async fn list_sessions(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<Json<ApiResponse<SessionListDto>>, RestError> {
    let mgr = handler.session_manager();
    let ids = mgr.list_sessions();

    let sessions: Vec<SessionDto> = ids
        .iter()
        .filter_map(|&id| {
            mgr.with_session(id, |session| SessionDto {
                id: session.id,
                knowledge_graph: session.knowledge_graph.clone(),
                is_clean: session.is_clean(),
                ephemeral_fact_count: session.ephemeral_fact_count(),
                ephemeral_rule_count: session.ephemeral_rule_count(),
            })
            .ok()
        })
        .collect();

    // Compute stats from the collected sessions for consistency
    let total = sessions.len();
    let clean = sessions.iter().filter(|s| s.is_clean).count();
    let dirty = total - clean;

    Ok(Json(ApiResponse::success(SessionListDto {
        sessions,
        total,
        clean,
        dirty,
    })))
}

/// Execute a query within session context
#[utoipa::path(
    post,
    path = "/sessions/{id}/query",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    request_body = SessionQueryRequest,
    responses(
        (status = 200, description = "Query executed with session context", body = ApiResponse<SessionQueryResponse>),
        (status = 404, description = "Session not found"),
    )
)]
pub async fn session_query(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
    Json(request): Json<SessionQueryRequest>,
) -> Result<Json<ApiResponse<SessionQueryResponse>>, RestError> {
    // Verify session exists
    if !handler.session_manager().has_session(id) {
        return Err(RestError::not_found(format!("Session {id} not found")));
    }

    let start = Instant::now();
    let result = handler
        .query_program_with_session(id, request.query.clone())
        .await;
    let execution_time_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(response) => {
            let row_provenance: Vec<String> = response
                .rows
                .iter()
                .map(|row| {
                    row.provenance
                        .as_ref()
                        .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string)
                })
                .collect();

            let rows: Vec<Vec<serde_json::Value>> = response
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(wire_value_to_json).collect())
                .collect();

            let row_count = rows.len();
            let columns: Vec<String> = response.schema.iter().map(|c| c.name.clone()).collect();

            let metadata = response.metadata.map(|m| SessionQueryMetadataDto {
                has_ephemeral: m.has_ephemeral,
                ephemeral_sources: m.ephemeral_sources,
                warnings: m.warnings,
            });

            Ok(Json(ApiResponse::success(SessionQueryResponse {
                query: request.query,
                status: QueryStatus::Success,
                columns,
                rows,
                row_count,
                execution_time_ms,
                error: None,
                row_provenance,
                metadata,
            })))
        }
        Err(e) => Ok(Json(ApiResponse::success(SessionQueryResponse {
            query: request.query,
            status: QueryStatus::Error,
            columns: vec![],
            rows: vec![],
            row_count: 0,
            execution_time_ms,
            error: Some(e),
            row_provenance: vec![],
            metadata: None,
        }))),
    }
}

/// Insert ephemeral facts into a session
#[utoipa::path(
    post,
    path = "/sessions/{id}/facts",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    request_body = EphemeralFactsRequest,
    responses(
        (status = 200, description = "Facts inserted"),
        (status = 404, description = "Session not found"),
        (status = 400, description = "Invalid data"),
    )
)]
pub async fn insert_ephemeral_facts(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
    Json(request): Json<EphemeralFactsRequest>,
) -> Result<Json<ApiResponse<String>>, RestError> {
    let tuples = json_tuples_to_tuples(&request.tuples).map_err(RestError::bad_request)?;

    let inserted = handler
        .session_insert_ephemeral(id, &request.relation, tuples)
        .map_err(|e| {
            if e.contains("not found") {
                RestError::not_found(e)
            } else {
                RestError::bad_request(e)
            }
        })?;

    Ok(Json(ApiResponse::success(format!(
        "Inserted {inserted} ephemeral fact(s) into '{}'",
        request.relation
    ))))
}

/// Retract ephemeral facts from a session
#[utoipa::path(
    delete,
    path = "/sessions/{id}/facts",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    request_body = EphemeralFactsRequest,
    responses(
        (status = 200, description = "Facts retracted"),
        (status = 404, description = "Session not found"),
    )
)]
pub async fn retract_ephemeral_facts(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
    Json(request): Json<EphemeralFactsRequest>,
) -> Result<Json<ApiResponse<String>>, RestError> {
    let tuples = json_tuples_to_tuples(&request.tuples).map_err(RestError::bad_request)?;

    let retracted = handler
        .session_retract_ephemeral(id, &request.relation, tuples)
        .map_err(|e| {
            if e.contains("not found") {
                RestError::not_found(e)
            } else {
                RestError::bad_request(e)
            }
        })?;

    Ok(Json(ApiResponse::success(format!(
        "Retracted {retracted} ephemeral fact(s) from '{}'",
        request.relation
    ))))
}

/// Add an ephemeral rule to a session
#[utoipa::path(
    post,
    path = "/sessions/{id}/rules",
    tag = "sessions",
    params(
        ("id" = u64, Path, description = "Session ID")
    ),
    request_body = EphemeralRuleRequest,
    responses(
        (status = 200, description = "Rule added"),
        (status = 404, description = "Session not found"),
        (status = 400, description = "Invalid rule"),
    )
)]
pub async fn add_ephemeral_rule(
    Extension(handler): Extension<Arc<Handler>>,
    Path(id): Path<u64>,
    Json(request): Json<EphemeralRuleRequest>,
) -> Result<Json<ApiResponse<String>>, RestError> {
    // Parse the rule text into AST
    let program = crate::parser::parse_program(&request.rule)
        .map_err(|e| RestError::bad_request(format!("Invalid rule syntax: {e}")))?;

    if program.rules.is_empty() {
        return Err(RestError::bad_request("No rule found in input"));
    }
    if program.rules.len() > 1 {
        return Err(RestError::bad_request(format!(
            "Expected exactly one rule, got {}. Add rules one at a time.",
            program.rules.len()
        )));
    }

    let rule = program.rules.into_iter().next().unwrap();
    let head_relation = rule.head.relation.clone();
    handler
        .session_add_rule(id, rule, request.rule.clone())
        .map_err(|e| {
            if e.contains("not found") {
                RestError::not_found(e)
            } else {
                RestError::bad_request(e)
            }
        })?;

    Ok(Json(ApiResponse::success(format!(
        "Ephemeral rule added for '{head_relation}'"
    ))))
}

/// Convert JSON value arrays to Tuples
pub fn json_tuples_to_tuples(json_tuples: &[Vec<serde_json::Value>]) -> Result<Vec<Tuple>, String> {
    json_tuples
        .iter()
        .map(|row| {
            let values: Result<Vec<Value>, String> = row.iter().map(super::json_to_value).collect();
            values.map(Tuple::new)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_tuples_to_tuples() {
        let input = vec![
            vec![serde_json::json!(1), serde_json::json!(2)],
            vec![serde_json::json!(3), serde_json::json!(4)],
        ];
        let tuples = json_tuples_to_tuples(&input).unwrap();
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0].arity(), 2);
        assert_eq!(tuples[0].get(0), Some(&Value::Int64(1)));
        assert_eq!(tuples[0].get(1), Some(&Value::Int64(2)));
    }

    #[test]
    fn test_json_tuples_mixed_types() {
        let input = vec![vec![
            serde_json::json!(1),
            serde_json::json!("hello"),
            serde_json::json!(3.14),
        ]];
        let tuples = json_tuples_to_tuples(&input).unwrap();
        assert_eq!(tuples[0].arity(), 3);
        assert_eq!(tuples[0].get(0), Some(&Value::Int64(1)));
        assert_eq!(tuples[0].get(1), Some(&Value::string("hello")));
        assert_eq!(tuples[0].get(2), Some(&Value::Float64(3.14)));
    }

    #[test]
    fn test_json_tuples_empty() {
        let tuples = json_tuples_to_tuples(&[]).unwrap();
        assert!(tuples.is_empty());
    }

    #[test]
    fn test_json_tuples_with_object_error() {
        let input = vec![vec![serde_json::json!({"key": "val"})]];
        let result = json_tuples_to_tuples(&input);
        assert!(result.is_err());
    }

    // --- Session handler integration tests ---

    use crate::protocol::rest::dto::{
        CreateSessionRequest, EphemeralFactsRequest, EphemeralRuleRequest, SessionQueryRequest,
    };
    use crate::Config;

    fn make_handler() -> (Arc<Handler>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.data_dir = tmp.path().to_path_buf();
        (Arc::new(Handler::from_config(config).unwrap()), tmp)
    }

    #[tokio::test]
    async fn test_create_session_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_h_kg")
            .unwrap();
        let request = CreateSessionRequest {
            knowledge_graph: "sess_h_kg".to_string(),
        };
        let result = create_session(Extension(handler), Json(request))
            .await
            .unwrap();
        let resp = result.0;
        assert!(resp.success);
        let data = resp.data.unwrap();
        assert!(data.session_id > 0);
        assert_eq!(data.knowledge_graph, "sess_h_kg");
    }

    #[tokio::test]
    async fn test_close_session_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_close_kg")
            .unwrap();
        let sid = handler.create_session("sess_close_kg").unwrap();
        let result = close_session(Extension(handler), Path(sid)).await.unwrap();
        assert!(result.0.success);
    }

    #[tokio::test]
    async fn test_close_session_not_found() {
        let (handler, _tmp) = make_handler();
        let result = close_session(Extension(handler), Path(999999)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_session_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_get_kg")
            .unwrap();
        let sid = handler.create_session("sess_get_kg").unwrap();
        let result = get_session(Extension(handler), Path(sid)).await.unwrap();
        let dto = result.0.data.unwrap();
        assert_eq!(dto.id, sid);
        assert_eq!(dto.knowledge_graph, "sess_get_kg");
        assert!(dto.is_clean);
        assert_eq!(dto.ephemeral_fact_count, 0);
    }

    #[tokio::test]
    async fn test_list_sessions_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_list_kg")
            .unwrap();
        handler.create_session("sess_list_kg").unwrap();
        handler.create_session("sess_list_kg").unwrap();
        let result = list_sessions(Extension(handler)).await.unwrap();
        let dto = result.0.data.unwrap();
        assert_eq!(dto.total, 2);
        assert_eq!(dto.sessions.len(), 2);
    }

    #[tokio::test]
    async fn test_insert_ephemeral_facts_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_eph_kg")
            .unwrap();
        let sid = handler.create_session("sess_eph_kg").unwrap();
        let request = EphemeralFactsRequest {
            relation: "edge".to_string(),
            tuples: vec![
                vec![serde_json::json!(1), serde_json::json!(2)],
                vec![serde_json::json!(3), serde_json::json!(4)],
            ],
        };
        let result = insert_ephemeral_facts(Extension(handler), Path(sid), Json(request))
            .await
            .unwrap();
        assert!(result.0.success);
        assert!(result.0.data.unwrap().contains("2 ephemeral fact(s)"));
    }

    #[tokio::test]
    async fn test_retract_ephemeral_facts_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_ret_kg")
            .unwrap();
        let sid = handler.create_session("sess_ret_kg").unwrap();
        // Insert first
        handler
            .session_insert_ephemeral(sid, "data", vec![Tuple::new(vec![Value::Int64(1)])])
            .unwrap();
        // Retract via handler
        let request = EphemeralFactsRequest {
            relation: "data".to_string(),
            tuples: vec![vec![serde_json::json!(1)]],
        };
        let result = retract_ephemeral_facts(Extension(handler), Path(sid), Json(request))
            .await
            .unwrap();
        assert!(result.0.success);
        assert!(result.0.data.unwrap().contains("1"));
    }

    #[tokio::test]
    async fn test_add_ephemeral_rule_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_rule_kg")
            .unwrap();
        let sid = handler.create_session("sess_rule_kg").unwrap();
        let request = EphemeralRuleRequest {
            rule: "doubled(X, Y) <- base(X), Y = X * 2".to_string(),
        };
        let result = add_ephemeral_rule(Extension(handler), Path(sid), Json(request))
            .await
            .unwrap();
        assert!(result.0.success);
        assert!(result.0.data.unwrap().contains("doubled"));
    }

    #[tokio::test]
    async fn test_add_ephemeral_rule_invalid() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("sess_badrule_kg")
            .unwrap();
        let sid = handler.create_session("sess_badrule_kg").unwrap();
        let request = EphemeralRuleRequest {
            rule: "totally invalid!!!".to_string(),
        };
        let result = add_ephemeral_rule(Extension(handler), Path(sid), Json(request)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_query_handler() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(
                Some("sess_query_kg".to_string()),
                "+data[(1,), (2,)]".to_string(),
            )
            .await
            .unwrap();
        let sid = handler.create_session("sess_query_kg").unwrap();
        let request = SessionQueryRequest {
            query: "?data(X)".to_string(),
        };
        let result = session_query(Extension(handler), Path(sid), Json(request))
            .await
            .unwrap();
        let data = result.0.data.unwrap();
        assert_eq!(data.row_count, 2);
    }

    #[tokio::test]
    async fn test_session_query_not_found() {
        let (handler, _tmp) = make_handler();
        let request = SessionQueryRequest {
            query: "?data(X)".to_string(),
        };
        let result = session_query(Extension(handler), Path(999999), Json(request)).await;
        assert!(result.is_err());
    }
}
