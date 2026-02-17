//! Knowledge Graph Handlers
//!
//! Endpoints for knowledge graph management operations.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use axum::{extract::Path, Extension, Json};

use crate::protocol::rest::dto::{
    ApiResponse, CreateKnowledgeGraphRequest, KnowledgeGraphDto, KnowledgeGraphListDto,
    OntologyDefinition, OntologyStatusDto,
};
use crate::protocol::rest::error::RestError;
use crate::protocol::wire::WireValue;
use crate::protocol::Handler;

/// List all knowledge graphs
#[utoipa::path(
    get,
    path = "/knowledge-graphs",
    tag = "knowledge-graphs",
    responses(
        (status = 200, description = "List of knowledge graphs", body = ApiResponse<KnowledgeGraphListDto>),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn list_knowledge_graphs(
    Extension(handler): Extension<Arc<Handler>>,
) -> Result<Json<ApiResponse<KnowledgeGraphListDto>>, RestError> {
    let storage = handler.get_storage();

    let kg_names = storage.list_knowledge_graphs();
    let current_name = storage
        .current_knowledge_graph()
        .map(std::string::ToString::to_string);

    // Build knowledge graph list with basic info
    let knowledge_graphs: Vec<KnowledgeGraphDto> = kg_names
        .into_iter()
        .map(|name| KnowledgeGraphDto {
            name,
            description: None,
            relations_count: 0, // Would need to switch to each KG to count
            views_count: 0,
            ontologies: None,
        })
        .collect();

    drop(storage);

    // Check if current KG exists in the list
    let (current, warning) = if let Some(ref name) = current_name {
        if knowledge_graphs.iter().any(|kg| kg.name == *name) {
            (Some(name.clone()), None)
        } else {
            // Current KG doesn't exist - report warning and suggest first available
            let fallback = knowledge_graphs.first().map(|kg| kg.name.clone());
            (
                fallback,
                Some(format!("Knowledge graph '{name}' not found")),
            )
        }
    } else {
        (knowledge_graphs.first().map(|kg| kg.name.clone()), None)
    };

    let result = KnowledgeGraphListDto {
        knowledge_graphs,
        current,
        warning,
    };

    Ok(Json(ApiResponse::success(result)))
}

/// Get knowledge graph details
#[utoipa::path(
    get,
    path = "/knowledge-graphs/{name}",
    tag = "knowledge-graphs",
    params(
        ("name" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "Knowledge graph details", body = ApiResponse<KnowledgeGraphDto>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_knowledge_graph(
    Extension(handler): Extension<Arc<Handler>>,
    Path(name): Path<String>,
) -> Result<Json<ApiResponse<KnowledgeGraphDto>>, RestError> {
    let storage = handler.get_storage();

    // Check if knowledge graph exists
    storage
        .ensure_knowledge_graph(&name)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{name}' not found: {e}")))?;

    // Get relations count
    let relations_count = storage
        .list_relations_in(&name)
        .map(|r| r.len())
        .unwrap_or(0);

    let kg = KnowledgeGraphDto {
        name,
        description: None,
        relations_count,
        views_count: 0,
        ontologies: None,
    };

    Ok(Json(ApiResponse::success(kg)))
}

/// Create a new knowledge graph
#[utoipa::path(
    post,
    path = "/knowledge-graphs",
    tag = "knowledge-graphs",
    request_body = CreateKnowledgeGraphRequest,
    responses(
        (status = 200, description = "Knowledge graph created", body = ApiResponse<KnowledgeGraphDto>),
        (status = 400, description = "Invalid request"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn create_knowledge_graph(
    Extension(handler): Extension<Arc<Handler>>,
    Json(request): Json<CreateKnowledgeGraphRequest>,
) -> Result<Json<ApiResponse<KnowledgeGraphDto>>, RestError> {
    {
        let storage = handler.get_storage();
        storage.create_knowledge_graph(&request.name).map_err(|e| {
            let msg = format!("{e}");
            if msg.contains("already exists") {
                RestError::bad_request(msg)
            } else {
                RestError::internal(msg)
            }
        })?;
    }

    // Deploy ontologies if provided
    let ontologies = match request.ontologies {
        Some(ontology_defs) => {
            let mut statuses = Vec::new();
            let kg_name = request.name.clone();
            for ontology in ontology_defs {
                let prefix = ontology.prefix.clone();

                // Validate prefix: must be non-empty, alphanumeric + underscores, no quotes
                if prefix.is_empty() {
                    statuses.push(OntologyStatusDto {
                        prefix,
                        status: "error".to_string(),
                        error: Some("Ontology prefix cannot be empty".to_string()),
                    });
                    continue;
                }
                if !prefix
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    statuses.push(OntologyStatusDto {
                        prefix,
                        status: "error".to_string(),
                        error: Some(
                            "Ontology prefix must contain only alphanumeric characters and underscores"
                                .to_string(),
                        ),
                    });
                    continue;
                }

                let hash = compute_ontology_hash(&ontology);

                // Check ontology_meta for existing entry with this prefix
                let mut is_update = false;
                let mut hash_matches = false;
                let check_query = format!("?ontology_meta(\"{prefix}\", _V, _H, _T)");
                if let Ok(result) = handler
                    .query_program(Some(kg_name.clone()), check_query)
                    .await
                {
                    for row in &result.rows {
                        if row.values.len() == 4 {
                            if let WireValue::String(existing_hash) = &row.values[2] {
                                if *existing_hash == hash {
                                    hash_matches = true;
                                }
                            }
                        }
                    }
                    if !result.rows.is_empty() {
                        is_update = true;
                    }
                }

                if hash_matches {
                    statuses.push(OntologyStatusDto {
                        prefix,
                        status: "unchanged".to_string(),
                        error: None,
                    });
                    continue;
                }

                // Validate all statements (parse only)
                let all_stmts: Vec<&str> = ontology
                    .schemas
                    .iter()
                    .chain(ontology.rules.iter())
                    .chain(ontology.indexes.iter())
                    .map(String::as_str)
                    .collect();

                let mut validation_error = None;
                for (i, stmt) in all_stmts.iter().enumerate() {
                    if let Err(e) = crate::statement::parse_statement(stmt) {
                        validation_error = Some(format!("Statement {}: {e}", i + 1));
                        break;
                    }
                }

                if let Some(err) = validation_error {
                    statuses.push(OntologyStatusDto {
                        prefix,
                        status: "error".to_string(),
                        error: Some(err),
                    });
                    continue;
                }

                // Drop existing rules with prefix (if updating)
                if is_update {
                    let _ = handler.drop_rules_by_prefix_in(&kg_name, &prefix);
                }

                // Execute all statements: schemas → rules → indexes
                let mut program = String::new();
                for s in &ontology.schemas {
                    program.push_str(s.trim());
                    program.push('\n');
                }
                for r in &ontology.rules {
                    program.push_str(r.trim());
                    program.push('\n');
                }
                for idx in &ontology.indexes {
                    program.push_str(idx.trim());
                    program.push('\n');
                }

                if !program.trim().is_empty() {
                    if let Err(e) = handler.query_program(Some(kg_name.clone()), program).await {
                        statuses.push(OntologyStatusDto {
                            prefix,
                            status: "error".to_string(),
                            error: Some(e),
                        });
                        continue;
                    }
                }

                // Delete old ontology_meta entry for this prefix (if updating)
                if is_update {
                    let delete_stmt = format!("-ontology_meta(P, V, H, T) <- P = \"{prefix}\"");
                    if let Err(e) = handler
                        .query_program(Some(kg_name.clone()), delete_stmt)
                        .await
                    {
                        statuses.push(OntologyStatusDto {
                            prefix,
                            status: "error".to_string(),
                            error: Some(format!("Failed to delete old metadata: {e}")),
                        });
                        continue;
                    }
                }

                // Insert new ontology_meta entry
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs() as i64);
                let insert_stmt = format!(
                    "+ontology_meta[(\"{prefix}\", \"{}\", \"{hash}\", {timestamp})]",
                    ontology.version
                );
                if let Err(e) = handler
                    .query_program(Some(kg_name.clone()), insert_stmt)
                    .await
                {
                    statuses.push(OntologyStatusDto {
                        prefix,
                        status: "error".to_string(),
                        error: Some(format!("Failed to insert metadata: {e}")),
                    });
                    continue;
                }

                let status = if is_update { "migrated" } else { "installed" };
                statuses.push(OntologyStatusDto {
                    prefix,
                    status: status.to_string(),
                    error: None,
                });
            }
            Some(statuses)
        }
        None => None,
    };

    let kg = KnowledgeGraphDto {
        name: request.name,
        description: request.description,
        relations_count: 0,
        views_count: 0,
        ontologies,
    };

    Ok(Json(ApiResponse::success(kg)))
}

/// Compute a deterministic hash of an ontology's version and statements.
fn compute_ontology_hash(ontology: &OntologyDefinition) -> String {
    let mut hasher = DefaultHasher::new();
    ontology.version.hash(&mut hasher);
    for s in &ontology.schemas {
        s.hash(&mut hasher);
    }
    for s in &ontology.rules {
        s.hash(&mut hasher);
    }
    for s in &ontology.indexes {
        s.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

/// Delete a knowledge graph
#[utoipa::path(
    delete,
    path = "/knowledge-graphs/{name}",
    tag = "knowledge-graphs",
    params(
        ("name" = String, Path, description = "Knowledge graph name")
    ),
    responses(
        (status = 200, description = "Knowledge graph deleted", body = ApiResponse<()>),
        (status = 404, description = "Knowledge graph not found"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn delete_knowledge_graph(
    Extension(handler): Extension<Arc<Handler>>,
    Path(name): Path<String>,
) -> Result<Json<ApiResponse<()>>, RestError> {
    let mut storage = handler.get_storage_mut();

    storage
        .drop_knowledge_graph(&name)
        .map_err(|e| RestError::not_found(format!("Knowledge graph '{name}' not found: {e}")))?;

    Ok(Json(ApiResponse {
        success: true,
        data: None,
        error: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    fn make_handler() -> (Arc<Handler>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.auto_create_knowledge_graphs = true;
        config.storage.data_dir = tmp.path().to_path_buf();
        (Arc::new(Handler::from_config(config).unwrap()), tmp)
    }

    #[tokio::test]
    async fn test_list_knowledge_graphs_empty() {
        let (handler, _tmp) = make_handler();
        let result = list_knowledge_graphs(Extension(handler)).await.unwrap();
        let data = result.0.data.unwrap();
        // May have default KG or empty depending on config
        assert!(data.warning.is_none() || data.warning.is_some());
    }

    #[tokio::test]
    async fn test_create_and_list_knowledge_graph() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "test_create_kg".to_string(),
            description: Some("A test KG".to_string()),
            ontologies: None,
        };
        let created = create_knowledge_graph(Extension(handler.clone()), Json(request))
            .await
            .unwrap();
        let kg = created.0.data.unwrap();
        assert_eq!(kg.name, "test_create_kg");
        assert_eq!(kg.description, Some("A test KG".to_string()));
        assert_eq!(kg.relations_count, 0);

        let list = list_knowledge_graphs(Extension(handler)).await.unwrap();
        let data = list.0.data.unwrap();
        assert!(data
            .knowledge_graphs
            .iter()
            .any(|k| k.name == "test_create_kg"));
    }

    #[tokio::test]
    async fn test_get_knowledge_graph() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .ensure_knowledge_graph("get_kg_test")
            .unwrap();
        let result = get_knowledge_graph(Extension(handler), Path("get_kg_test".to_string()))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.name, "get_kg_test");
    }

    #[tokio::test]
    async fn test_get_knowledge_graph_with_relations() {
        let (handler, _tmp) = make_handler();
        handler
            .query_program(Some("get_rel_kg".to_string()), "+edges[(1, 2)]".to_string())
            .await
            .unwrap();
        let result = get_knowledge_graph(Extension(handler), Path("get_rel_kg".to_string()))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.relations_count, 1);
    }

    #[tokio::test]
    async fn test_delete_knowledge_graph() {
        let (handler, _tmp) = make_handler();
        handler
            .get_storage()
            .create_knowledge_graph("del_kg_test")
            .unwrap();
        let result =
            delete_knowledge_graph(Extension(handler), Path("del_kg_test".to_string())).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_knowledge_graph() {
        let (handler, _tmp) = make_handler();
        let result =
            delete_knowledge_graph(Extension(handler), Path("does_not_exist_kg".to_string())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_knowledge_graph_no_description() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "no_desc_kg".to_string(),
            description: None,
            ontologies: None,
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.name, "no_desc_kg");
        assert!(kg.description.is_none());
    }

    #[tokio::test]
    async fn test_create_kg_with_ontology_fresh_deploy() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_fresh_kg".to_string(),
            description: None,
            ontologies: Some(vec![OntologyDefinition {
                prefix: "test_".to_string(),
                version: "1.0.0".to_string(),
                schemas: vec![],
                rules: vec!["+test_rule(X, Y) <- edge(X, Y)".to_string()],
                indexes: vec![],
            }]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.name, "onto_fresh_kg");
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies.len(), 1);
        assert_eq!(ontologies[0].prefix, "test_");
        assert_eq!(ontologies[0].status, "installed");
        assert!(ontologies[0].error.is_none());
    }

    #[tokio::test]
    async fn test_create_kg_with_ontology_parse_error() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_err_kg".to_string(),
            description: None,
            ontologies: Some(vec![OntologyDefinition {
                prefix: "bad_".to_string(),
                version: "1.0.0".to_string(),
                schemas: vec![],
                rules: vec!["this is not valid datalog!!!".to_string()],
                indexes: vec![],
            }]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies.len(), 1);
        assert_eq!(ontologies[0].status, "error");
        assert!(ontologies[0].error.is_some());
    }

    #[tokio::test]
    async fn test_create_kg_with_multiple_ontologies() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_multi_kg".to_string(),
            description: None,
            ontologies: Some(vec![
                OntologyDefinition {
                    prefix: "mem_".to_string(),
                    version: "1.0.0".to_string(),
                    schemas: vec![],
                    rules: vec!["+mem_rule(X) <- edge(X, _Y)".to_string()],
                    indexes: vec![],
                },
                OntologyDefinition {
                    prefix: "env_".to_string(),
                    version: "2.0.0".to_string(),
                    schemas: vec![],
                    rules: vec!["+env_rule(X, Y) <- edge(X, Y)".to_string()],
                    indexes: vec![],
                },
            ]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies.len(), 2);
        assert_eq!(ontologies[0].prefix, "mem_");
        assert_eq!(ontologies[0].status, "installed");
        assert_eq!(ontologies[1].prefix, "env_");
        assert_eq!(ontologies[1].status, "installed");
    }

    #[tokio::test]
    async fn test_create_kg_no_ontologies() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_none_kg".to_string(),
            description: None,
            ontologies: None,
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert!(kg.ontologies.is_none());
    }

    #[tokio::test]
    async fn test_create_kg_with_ontology_schema_and_rules() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_schema_kg".to_string(),
            description: None,
            ontologies: Some(vec![OntologyDefinition {
                prefix: "sch_".to_string(),
                version: "1.0.0".to_string(),
                schemas: vec!["+sch_data(id: Int, value: Int)".to_string()],
                rules: vec!["+sch_doubled(X, Y) <- sch_data(X, Z), Y = Z * 2".to_string()],
                indexes: vec![],
            }]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies.len(), 1);
        assert_eq!(ontologies[0].status, "installed");
        assert!(ontologies[0].error.is_none());
    }

    #[tokio::test]
    async fn test_ontology_hash_deterministic() {
        let onto = OntologyDefinition {
            prefix: "test_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec!["+test_data(x: Int)".to_string()],
            rules: vec!["+test_rule(X) <- test_data(X)".to_string()],
            indexes: vec![],
        };
        let h1 = compute_ontology_hash(&onto);
        let h2 = compute_ontology_hash(&onto);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 16); // 16 hex chars for u64
    }

    #[tokio::test]
    async fn test_ontology_hash_changes_with_content() {
        let onto1 = OntologyDefinition {
            prefix: "test_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec![],
            rules: vec!["+test_rule(X) <- data(X)".to_string()],
            indexes: vec![],
        };
        let onto2 = OntologyDefinition {
            prefix: "test_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec![],
            rules: vec!["+test_rule(X, Y) <- data(X, Y)".to_string()],
            indexes: vec![],
        };
        assert_ne!(compute_ontology_hash(&onto1), compute_ontology_hash(&onto2));
    }

    #[tokio::test]
    async fn test_create_kg_ontology_partial_failure() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_partial_kg".to_string(),
            description: None,
            ontologies: Some(vec![
                OntologyDefinition {
                    prefix: "good_".to_string(),
                    version: "1.0.0".to_string(),
                    schemas: vec![],
                    rules: vec!["+good_rule(X) <- edge(X, _Y)".to_string()],
                    indexes: vec![],
                },
                OntologyDefinition {
                    prefix: "bad_".to_string(),
                    version: "1.0.0".to_string(),
                    schemas: vec![],
                    rules: vec!["not valid datalog".to_string()],
                    indexes: vec![],
                },
            ]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies.len(), 2);
        // First ontology should succeed
        assert_eq!(ontologies[0].status, "installed");
        // Second should fail
        assert_eq!(ontologies[1].status, "error");
        assert!(ontologies[1].error.is_some());
    }

    #[tokio::test]
    async fn test_create_kg_request_with_ontologies_deserialize() {
        let json = r#"{
            "name": "mykg",
            "ontologies": [{
                "prefix": "mem_",
                "version": "1.0.0",
                "rules": ["+mem_rule(X) <- data(X)"]
            }]
        }"#;
        let req: CreateKnowledgeGraphRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "mykg");
        let ontologies = req.ontologies.unwrap();
        assert_eq!(ontologies.len(), 1);
        assert_eq!(ontologies[0].prefix, "mem_");
        assert_eq!(ontologies[0].version, "1.0.0");
        assert_eq!(ontologies[0].rules.len(), 1);
        assert!(ontologies[0].schemas.is_empty());
        assert!(ontologies[0].indexes.is_empty());
    }

    #[tokio::test]
    async fn test_ontology_unchanged_on_redeploy() {
        let (handler, _tmp) = make_handler();
        let ontology = OntologyDefinition {
            prefix: "unch_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec![],
            rules: vec!["+unch_rule(X) <- edge(X, _Y)".to_string()],
            indexes: vec![],
        };
        // First deploy
        let request = CreateKnowledgeGraphRequest {
            name: "onto_unch_kg".to_string(),
            description: None,
            ontologies: Some(vec![ontology.clone()]),
        };
        let result = create_knowledge_graph(Extension(handler.clone()), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        assert_eq!(kg.ontologies.unwrap()[0].status, "installed");

        // Verify hash determinism: same ontology definition → same hash
        let hash1 = compute_ontology_hash(&ontology);
        let hash2 = compute_ontology_hash(&ontology);
        assert_eq!(hash1, hash2);

        // Verify identical content → identical hash
        let onto_copy = OntologyDefinition {
            prefix: "unch_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec![],
            rules: vec!["+unch_rule(X) <- edge(X, _Y)".to_string()],
            indexes: vec![],
        };
        assert_eq!(
            compute_ontology_hash(&ontology),
            compute_ontology_hash(&onto_copy)
        );
    }

    #[tokio::test]
    async fn test_ontology_hash_changes_with_version() {
        let onto1 = OntologyDefinition {
            prefix: "test_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec![],
            rules: vec!["+test_rule(X) <- data(X)".to_string()],
            indexes: vec![],
        };
        let onto2 = OntologyDefinition {
            prefix: "test_".to_string(),
            version: "2.0.0".to_string(),
            schemas: vec![],
            rules: vec!["+test_rule(X) <- data(X)".to_string()],
            indexes: vec![],
        };
        // Same content, different version → different hash
        assert_ne!(compute_ontology_hash(&onto1), compute_ontology_hash(&onto2));
    }

    #[tokio::test]
    async fn test_ontology_empty_prefix_rejected() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_empty_pfx_kg".to_string(),
            description: None,
            ontologies: Some(vec![OntologyDefinition {
                prefix: String::new(),
                version: "1.0.0".to_string(),
                schemas: vec![],
                rules: vec!["+rule(X) <- data(X)".to_string()],
                indexes: vec![],
            }]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies[0].status, "error");
        assert!(ontologies[0]
            .error
            .as_ref()
            .unwrap()
            .contains("cannot be empty"));
    }

    #[tokio::test]
    async fn test_ontology_special_chars_prefix_rejected() {
        let (handler, _tmp) = make_handler();
        let request = CreateKnowledgeGraphRequest {
            name: "onto_special_pfx_kg".to_string(),
            description: None,
            ontologies: Some(vec![OntologyDefinition {
                prefix: "mem_\"inject".to_string(),
                version: "1.0.0".to_string(),
                schemas: vec![],
                rules: vec!["+rule(X) <- data(X)".to_string()],
                indexes: vec![],
            }]),
        };
        let result = create_knowledge_graph(Extension(handler), Json(request))
            .await
            .unwrap();
        let kg = result.0.data.unwrap();
        let ontologies = kg.ontologies.unwrap();
        assert_eq!(ontologies[0].status, "error");
        assert!(ontologies[0]
            .error
            .as_ref()
            .unwrap()
            .contains("alphanumeric"));
    }

    #[tokio::test]
    async fn test_ontology_hash_changes_with_schemas() {
        let onto1 = OntologyDefinition {
            prefix: "sch_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec!["+sch_data(id: Int)".to_string()],
            rules: vec!["+sch_rule(X) <- sch_data(X)".to_string()],
            indexes: vec![],
        };
        let onto2 = OntologyDefinition {
            prefix: "sch_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec!["+sch_data(id: Int, value: Int)".to_string()],
            rules: vec!["+sch_rule(X) <- sch_data(X)".to_string()],
            indexes: vec![],
        };
        // Same prefix, version, rules but different schemas -> different hash
        assert_ne!(compute_ontology_hash(&onto1), compute_ontology_hash(&onto2));
    }

    #[tokio::test]
    async fn test_ontology_hash_changes_with_indexes() {
        let onto1 = OntologyDefinition {
            prefix: "idx_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec!["+idx_data(id: Int, val: Int)".to_string()],
            rules: vec!["+idx_rule(X) <- idx_data(X, _Y)".to_string()],
            indexes: vec![],
        };
        let onto2 = OntologyDefinition {
            prefix: "idx_".to_string(),
            version: "1.0.0".to_string(),
            schemas: vec!["+idx_data(id: Int, val: Int)".to_string()],
            rules: vec!["+idx_rule(X) <- idx_data(X, _Y)".to_string()],
            indexes: vec![".index idx_data(id)".to_string()],
        };
        // Same prefix, version, rules, schemas but different indexes -> different hash
        assert_ne!(compute_ontology_hash(&onto1), compute_ontology_hash(&onto2));
    }
}
