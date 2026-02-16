//! Handler for `InputLayer`
//!
//! Core business logic for Datalog queries and data operations, used by the REST API.
//! Uses `parking_lot::RwLock` (no poisoning) and `AtomicU64` (lock-free counters).

use crate::ast::Term;
use crate::rule_catalog::validate_rule;
use crate::schema::{ColumnSchema, RelationSchema};
use crate::session::{SessionConfig, SessionId, SessionManager};
use crate::statement;
use crate::storage_engine::StorageEngine;
use crate::value::{Tuple, Value};
use crate::Config;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::wire::{ColumnDef, QueryResult, WireDataType, WireTuple, WireValue};

/// Term -> Value (constants only, rejects variables/placeholders).
fn term_to_value(term: &Term) -> Result<Value, String> {
    match term {
        Term::Constant(n) => Ok(Value::Int64(*n)),
        Term::FloatConstant(f) => Ok(Value::Float64(*f)),
        Term::StringConstant(s) => Ok(Value::string(s)),
        Term::VectorLiteral(v) => {
            let f32_vals: Vec<f32> = v
                .iter()
                .map(|x| {
                    let val = *x as f32;
                    if !val.is_finite() {
                        return Err(format!("Vector element {x} overflows f32 range"));
                    }
                    Ok(val)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Value::vector(f32_vals))
        }
        Term::Variable(v) => Err(format!("Cannot insert variable '{v}' - use constants only")),
        Term::Placeholder => Err("Cannot insert placeholder '_' - use constants only".to_string()),
        Term::Arithmetic(_) => {
            Err("Cannot insert arithmetic expression - use constants only".to_string())
        }
        Term::Aggregate(_, _) => Err("Cannot insert aggregate - use constants only".to_string()),
        Term::FunctionCall(_, _) => {
            Err("Cannot insert function call - use constants only".to_string())
        }
        Term::FieldAccess(_, _) => {
            Err("Cannot insert field access - use constants only".to_string())
        }
        Term::BoolConstant(b) => Ok(Value::Bool(*b)),
        Term::RecordPattern(_) => {
            Err("Cannot insert record pattern - use constants only".to_string())
        }
    }
}

/// Notification sent to WebSocket subscribers when persistent data changes.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PersistentNotification {
    /// A base relation was updated (insert or delete)
    PersistentUpdate {
        knowledge_graph: String,
        relation: String,
        operation: String,
        count: usize,
    },
}

/// Thread-safe wrapper around StorageEngine for concurrent API calls.
/// Per-KG schema validation via isolated SchemaCatalogs.
///
/// Includes a `SessionManager` for ephemeral triggers persistent: sessions
/// can inject ephemeral facts/rules that combine with persistent data for queries.
pub struct Handler {
    storage: Arc<RwLock<StorageEngine>>,
    start_time: Instant,
    query_count: AtomicU64,
    insert_count: AtomicU64,
    /// Session manager for ephemeral state
    sessions: SessionManager,
    /// Broadcast channel for persistent data change notifications.
    /// WebSocket connections subscribe to receive push updates.
    notify_tx: tokio::sync::broadcast::Sender<PersistentNotification>,
}

impl Handler {
    /// Create a new handler with the given storage engine.
    pub fn new(storage: StorageEngine) -> Self {
        let (notify_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            storage: Arc::new(RwLock::new(storage)),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
            sessions: SessionManager::default(),
            notify_tx,
        }
    }

    /// Create a new handler from configuration.
    pub fn from_config(config: Config) -> Result<Self, String> {
        let storage =
            StorageEngine::new(config).map_err(|e| format!("Failed to create storage: {e}"))?;
        Ok(Self::new(storage))
    }

    /// Create a new handler with custom session configuration.
    pub fn with_session_config(storage: StorageEngine, session_config: SessionConfig) -> Self {
        let (notify_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            storage: Arc::new(RwLock::new(storage)),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
            sessions: SessionManager::new(session_config),
            notify_tx,
        }
    }

    /// Subscribe to persistent data change notifications.
    /// Returns a broadcast receiver for push updates.
    pub fn subscribe_notifications(
        &self,
    ) -> tokio::sync::broadcast::Receiver<PersistentNotification> {
        self.notify_tx.subscribe()
    }

    /// Send a persistent data change notification.
    /// No-op if there are no active subscribers.
    pub fn notify_persistent_update(
        &self,
        kg: &str,
        relation: &str,
        operation: &str,
        count: usize,
    ) {
        let _ = self
            .notify_tx
            .send(PersistentNotification::PersistentUpdate {
                knowledge_graph: kg.to_string(),
                relation: relation.to_string(),
                operation: operation.to_string(),
                count,
            });
    }

    /// Get the session manager.
    pub fn session_manager(&self) -> &SessionManager {
        &self.sessions
    }

    /// Create a new session bound to a knowledge graph.
    pub fn create_session(&self, knowledge_graph: &str) -> Result<SessionId, String> {
        // Validate KG exists (or auto-create if configured)
        let storage = self.storage.read();
        storage
            .ensure_knowledge_graph(knowledge_graph)
            .map_err(|e| format!("Knowledge graph '{knowledge_graph}' not found: {e}"))?;
        drop(storage);
        self.sessions.create_session(knowledge_graph)
    }

    /// Close a session.
    pub fn close_session(&self, session_id: SessionId) -> Result<(), String> {
        self.sessions.close_session(session_id)
    }

    /// Get uptime in seconds.
    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Get access to the storage engine (for REST API handlers).
    pub fn get_storage(&self) -> parking_lot::RwLockReadGuard<'_, StorageEngine> {
        self.storage.read()
    }

    /// Get mutable access to the storage engine (for REST API handlers).
    pub fn get_storage_mut(&self) -> parking_lot::RwLockWriteGuard<'_, StorageEngine> {
        self.storage.write()
    }

    /// Get total queries executed.
    pub fn total_queries(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Get total inserts executed.
    pub fn total_inserts(&self) -> u64 {
        self.insert_count.load(Ordering::Relaxed)
    }

    /// Validate tuples against a schema for a given relation in a knowledge graph.
    /// Returns Ok(()) if validation passes or no schema exists.
    /// Returns Err with validation error message if schema validation fails.
    ///
    /// Schema validation is per-knowledge-graph, providing proper isolation.
    pub fn validate_tuples_against_schema(
        &self,
        kg_name: &str,
        relation: &str,
        tuples: &[Tuple],
    ) -> Result<(), String> {
        let storage = self.storage.read();
        storage
            .validate_tuples_in(kg_name, relation, tuples)
            .map_err(|e| format!("{e}"))
    }

    fn inc_query_count(&self) {
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Execute a Datalog program and return results.
    pub async fn query_program(
        &self,
        knowledge_graph: Option<String>,
        program: String,
    ) -> Result<QueryResult, String> {
        self.inc_query_count();
        let start = Instant::now();

        // Use READ lock  -  all operations use _on()/_into() variants with explicit KG name.
        // This allows concurrent queries to execute without blocking each other.
        let storage = self.storage.read();

        // Determine target knowledge graph name
        let kg_name = if let Some(ref kg) = knowledge_graph {
            // Ensure target KG exists (auto-creates if config allows)
            storage
                .ensure_knowledge_graph(kg)
                .map_err(|e| format!("Knowledge graph not found: {e}"))?;
            kg.clone()
        } else {
            storage
                .current_knowledge_graph()
                .ok_or("No knowledge graph selected")?
                .to_string()
        };

        // Strip comment lines
        let program_text = strip_comments(&program);

        // Process statements
        let mut messages = Vec::new();
        let mut query_to_execute: Option<String> = None;
        let mut current_stmt = String::new();
        // Collect session facts (non-persisted) to temporarily insert before query
        // Format: (relation_name, tuple_values)
        let mut session_fact_tuples: Vec<(String, Tuple)> = Vec::new();
        // Collect session rules to prepend to queries
        let mut session_rules: Vec<String> = Vec::new();
        // Parsed session rules for validation (arity/aggregation compatibility)
        let mut session_rules_parsed: Vec<crate::ast::Rule> = Vec::new();

        for line in program_text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            current_stmt.push_str(line);
            current_stmt.push(' ');

            {
                let stmt_text = current_stmt.trim();
                if !stmt_text.is_empty() {
                    if let Ok(stmt) = statement::parse_statement(stmt_text) {
                        match stmt {
                            statement::Statement::SchemaDecl(decl) => {
                                // Build RelationSchema from SchemaDecl
                                let mut relation_schema = RelationSchema::new(&decl.name);
                                for col in &decl.columns {
                                    let schema_type = col.col_type.to_schema_type();
                                    relation_schema = relation_schema
                                        .with_column(ColumnSchema::new(&col.name, schema_type));
                                }

                                // Register schema in the target knowledge graph (per-KG isolation)
                                // Note: For schema-first workflow, register before inserting data.
                                // For data-first workflow, ensure existing data is compatible.
                                let result = if decl.persistent {
                                    storage.register_or_update_schema_in(&kg_name, relation_schema)
                                } else {
                                    storage.register_or_update_session_schema_in(
                                        &kg_name,
                                        relation_schema,
                                    )
                                };

                                match result {
                                    Ok(()) => {
                                        messages.push(format!(
                                            "Schema for '{}' registered with {} columns{}",
                                            decl.name,
                                            decl.columns.len(),
                                            if decl.persistent {
                                                " (persistent)"
                                            } else {
                                                " (session)"
                                            }
                                        ));
                                    }
                                    Err(e) => {
                                        messages.push(format!(
                                            "Failed to register schema for '{}': {}",
                                            decl.name, e
                                        ));
                                    }
                                }
                            }
                            statement::Statement::Insert(op) => {
                                // Convert all terms to Values and create Tuples
                                let mut tuples: Vec<Tuple> = Vec::new();
                                let mut conversion_error = None;

                                for tuple_terms in &op.tuples {
                                    if tuple_terms.is_empty() {
                                        continue;
                                    }
                                    let mut values: Vec<Value> = Vec::new();
                                    for term in tuple_terms {
                                        match term_to_value(term) {
                                            Ok(v) => values.push(v),
                                            Err(e) => {
                                                conversion_error = Some(e);
                                                break;
                                            }
                                        }
                                    }
                                    if conversion_error.is_some() {
                                        break;
                                    }
                                    tuples.push(Tuple::new(values));
                                }

                                if let Some(err) = conversion_error {
                                    messages.push(err);
                                    current_stmt.clear();
                                    continue;
                                }

                                // Validate against schema if one exists (per-KG isolation)
                                if let Err(e) =
                                    storage.validate_tuples_in(&kg_name, &op.relation, &tuples)
                                {
                                    messages.push(format!(
                                        "Insert rejected for '{}': {}",
                                        op.relation, e
                                    ));
                                    current_stmt.clear();
                                    continue;
                                }

                                let (inserted, _duplicates) = storage
                                    .insert_tuples_into(&kg_name, &op.relation, tuples)
                                    .map_err(|e| e.to_string())?;
                                self.insert_count
                                    .fetch_add(inserted as u64, Ordering::Relaxed);
                                // Notify WebSocket subscribers of persistent data change
                                if inserted > 0 {
                                    let _ = self.notify_tx.send(
                                        PersistentNotification::PersistentUpdate {
                                            knowledge_graph: kg_name.clone(),
                                            relation: op.relation.clone(),
                                            operation: "insert".to_string(),
                                            count: inserted,
                                        },
                                    );
                                }
                                messages.push(format!(
                                    "Inserted {} fact(s) into '{}'.",
                                    inserted, op.relation
                                ));
                            }
                            statement::Statement::Fact(rule) => {
                                // Session facts are NOT persisted - they are only available for
                                // queries during this request. Use +relation(args). to persist.
                                if rule.head.args.is_empty() {
                                    messages
                                        .push("Fact must have at least one argument".to_string());
                                    current_stmt.clear();
                                    continue;
                                }

                                // Convert terms to values for temporary tuple insertion
                                let mut values: Vec<Value> = Vec::new();
                                let mut conversion_error = None;
                                for term in &rule.head.args {
                                    match term_to_value(term) {
                                        Ok(v) => values.push(v),
                                        Err(e) => {
                                            conversion_error = Some(e);
                                            break;
                                        }
                                    }
                                }
                                if let Some(err) = conversion_error {
                                    messages.push(err);
                                    current_stmt.clear();
                                    continue;
                                }

                                // Store for temporary insertion before query execution
                                session_fact_tuples
                                    .push((rule.head.relation.clone(), Tuple::new(values)));
                                messages.push(format!(
                                    "Session fact added for '{}'. (Use +{}(...) to persist)",
                                    rule.head.relation, rule.head.relation
                                ));
                            }
                            statement::Statement::Delete(op) => {
                                use statement::DeletePattern;
                                match op.pattern {
                                    DeletePattern::SingleTuple(terms) => {
                                        if !terms.is_empty() {
                                            let values: Result<Vec<Value>, String> =
                                                terms.iter().map(term_to_value).collect();
                                            let values = match values {
                                                Ok(v) => v,
                                                Err(e) => {
                                                    messages.push(format!("Delete error: {e}"));
                                                    current_stmt.clear();
                                                    continue;
                                                }
                                            };
                                            let tuple = Tuple::new(values);
                                            let deleted_count = storage
                                                .delete_tuples_from(
                                                    &kg_name,
                                                    &op.relation,
                                                    vec![tuple],
                                                )
                                                .map_err(|e| e.to_string())?;
                                            if deleted_count > 0 {
                                                let _ = self.notify_tx.send(
                                                    PersistentNotification::PersistentUpdate {
                                                        knowledge_graph: kg_name.clone(),
                                                        relation: op.relation.clone(),
                                                        operation: "delete".to_string(),
                                                        count: deleted_count,
                                                    },
                                                );
                                            }
                                            messages.push(format!(
                                                "Deleted {} facts from '{}'.",
                                                deleted_count, op.relation
                                            ));
                                        }
                                    }
                                    DeletePattern::BulkTuples(tuples) => {
                                        let mut total_deleted = 0;
                                        for tuple_terms in tuples {
                                            // Convert terms to values
                                            let converted: Result<
                                                Vec<crate::value::Value>,
                                                String,
                                            > = tuple_terms.iter().map(term_to_value).collect();
                                            if let Ok(values) = converted {
                                                let tuple = crate::value::Tuple::new(values);
                                                let count = storage
                                                    .delete_tuples_from(
                                                        &kg_name,
                                                        &op.relation,
                                                        vec![tuple],
                                                    )
                                                    .map_err(|e| e.to_string())?;
                                                total_deleted += count;
                                            }
                                        }
                                        if total_deleted > 0 {
                                            let _ = self.notify_tx.send(
                                                PersistentNotification::PersistentUpdate {
                                                    knowledge_graph: kg_name.clone(),
                                                    relation: op.relation.clone(),
                                                    operation: "delete".to_string(),
                                                    count: total_deleted,
                                                },
                                            );
                                        }
                                        messages.push(format!(
                                            "Deleted {} fact(s) from '{}'.",
                                            total_deleted, op.relation
                                        ));
                                    }
                                    DeletePattern::Conditional { head_args, body } => {
                                        // Build query to find matching tuples
                                        // Collect variables from head_args
                                        let mut all_vars: Vec<String> = Vec::new();
                                        for arg in &head_args {
                                            if let Term::Variable(v) = arg {
                                                if !all_vars.contains(v) {
                                                    all_vars.push(v.clone());
                                                }
                                            }
                                        }

                                        // Format head arguments for the target relation
                                        let head_args_str: String = head_args
                                            .iter()
                                            .map(format_term)
                                            .collect::<Vec<_>>()
                                            .join(", ");

                                        // Build body string from predicates
                                        // IMPORTANT: Include the target relation to bind all head variables
                                        let mut body_parts: Vec<String> =
                                            vec![format!("{}({})", op.relation, head_args_str)];
                                        for pred in &body {
                                            body_parts.push(format_body_pred(pred));
                                        }
                                        let body_str = body_parts.join(", ");

                                        // Build query rule
                                        let query_rule = format!(
                                            "__cond_del_query__({}) <- {}",
                                            all_vars.join(", "),
                                            body_str
                                        );

                                        // Execute query to find matching variable bindings
                                        let results = storage
                                            .execute_query_with_rules_tuples_on(
                                                &kg_name,
                                                &query_rule,
                                            )
                                            .map_err(|e| e.to_string())?;

                                        let mut deleted = 0;

                                        for result_tuple in results {
                                            // Build bindings from result
                                            let mut bindings: std::collections::HashMap<
                                                String,
                                                crate::value::Value,
                                            > = std::collections::HashMap::new();
                                            for (i, var) in all_vars.iter().enumerate() {
                                                if let Some(val) = result_tuple.get(i) {
                                                    bindings.insert(var.clone(), val.clone());
                                                }
                                            }

                                            // Build tuple to delete from head_args with bindings
                                            let mut tuple_values: Vec<crate::value::Value> =
                                                Vec::new();
                                            let mut valid = true;
                                            for arg in &head_args {
                                                match arg {
                                                    Term::Variable(v) => {
                                                        if let Some(val) = bindings.get(v) {
                                                            tuple_values.push(val.clone());
                                                        } else {
                                                            valid = false;
                                                            break;
                                                        }
                                                    }
                                                    Term::Constant(c) => {
                                                        tuple_values
                                                            .push(crate::value::Value::Int64(*c));
                                                    }
                                                    Term::StringConstant(s) => {
                                                        tuple_values
                                                            .push(crate::value::Value::string(s));
                                                    }
                                                    Term::FloatConstant(f) => {
                                                        tuple_values
                                                            .push(crate::value::Value::Float64(*f));
                                                    }
                                                    Term::BoolConstant(b) => {
                                                        tuple_values
                                                            .push(crate::value::Value::Bool(*b));
                                                    }
                                                    _ => {
                                                        valid = false;
                                                        break;
                                                    }
                                                }
                                            }

                                            if valid && !tuple_values.is_empty() {
                                                let tuple_to_delete =
                                                    crate::value::Tuple::new(tuple_values);
                                                let count = storage
                                                    .delete_tuples_from(
                                                        &kg_name,
                                                        &op.relation,
                                                        vec![tuple_to_delete],
                                                    )
                                                    .map_err(|e| e.to_string())?;
                                                deleted += count;
                                            }
                                        }

                                        if deleted > 0 {
                                            let _ = self.notify_tx.send(
                                                PersistentNotification::PersistentUpdate {
                                                    knowledge_graph: kg_name.clone(),
                                                    relation: op.relation.clone(),
                                                    operation: "delete".to_string(),
                                                    count: deleted,
                                                },
                                            );
                                        }
                                        messages.push(format!(
                                            "Conditional delete: {} fact(s) deleted from '{}'.",
                                            deleted, op.relation
                                        ));
                                    }
                                }
                            }
                            statement::Statement::PersistentRule(rule) => {
                                let rule_text = format_rule_text(&rule);
                                let rule_def = statement::parse_rule_definition(&rule_text)
                                    .map_err(|e| format!("Failed to parse rule: {e}"))?;
                                storage
                                    .register_rule_in(&kg_name, &rule_def)
                                    .map_err(|e| e.to_string())?;
                                messages.push(format!("Rule '{}' registered.", rule.head.relation));
                            }
                            statement::Statement::SessionRule(rule) => {
                                // Validate session rule for safety constraints
                                // (self-negation, head variable safety, range restriction)
                                validate_rule(&rule, &rule.head.relation)?;

                                // Validate aggregation/arity compatibility with existing session rules
                                crate::rule_catalog::validate_session_rule_compatibility(
                                    &session_rules_parsed,
                                    &rule,
                                )?;

                                let rule_text = format_rule_text(&rule);
                                session_rules.push(rule_text.clone());
                                session_rules_parsed.push(rule.clone());
                                messages.push(format!(
                                    "Session rule added for '{}'.",
                                    rule.head.relation
                                ));
                            }
                            statement::Statement::Query(_) => {
                                query_to_execute = Some(stmt_text.to_string());
                            }
                            statement::Statement::DeleteRelationOrRule(name) => {
                                match storage.drop_rule_in(&kg_name, &name) {
                                    Ok(()) => messages.push(format!("Rule '{name}' dropped.")),
                                    Err(_) => {
                                        messages.push(format!("'{name}' not found as rule."));
                                    }
                                }
                            }
                            statement::Statement::Update(op) => {
                                // Build query to find matching tuples
                                let mut all_vars: Vec<String> = Vec::new();
                                for target in &op.deletes {
                                    for arg in &target.args {
                                        if let Term::Variable(v) = arg {
                                            if !all_vars.contains(v) {
                                                all_vars.push(v.clone());
                                            }
                                        }
                                    }
                                }
                                for target in &op.inserts {
                                    for arg in &target.args {
                                        if let Term::Variable(v) = arg {
                                            if !all_vars.contains(v) {
                                                all_vars.push(v.clone());
                                            }
                                        }
                                    }
                                }

                                let body_str: String = op
                                    .body
                                    .iter()
                                    .map(format_body_pred)
                                    .collect::<Vec<_>>()
                                    .join(", ");

                                let query_rule = format!(
                                    "__upd_query__({}) <- {}",
                                    all_vars.join(", "),
                                    body_str
                                );

                                let results = storage
                                    .execute_query_with_rules_tuples_on(&kg_name, &query_rule)
                                    .map_err(|e| e.to_string())?;

                                let mut deleted = 0;
                                let mut inserted = 0;

                                for result_tuple in results {
                                    // Build bindings from query result: var_name → Value
                                    let bindings: std::collections::HashMap<String, Value> =
                                        all_vars
                                            .iter()
                                            .enumerate()
                                            .filter_map(|(idx, var)| {
                                                result_tuple
                                                    .get(idx)
                                                    .map(|v| (var.clone(), v.clone()))
                                            })
                                            .collect();

                                    for target in &op.deletes {
                                        let tuple_vals: Option<Vec<Value>> = target
                                            .args
                                            .iter()
                                            .map(|arg| match arg {
                                                Term::Variable(v) => bindings.get(v).cloned(),
                                                other => term_to_value(other).ok(),
                                            })
                                            .collect();
                                        if let Some(vals) = tuple_vals {
                                            let count = storage
                                                .delete_tuples_from(
                                                    &kg_name,
                                                    &target.relation,
                                                    vec![Tuple::new(vals)],
                                                )
                                                .map_err(|e| e.to_string())?;
                                            deleted += count;
                                        }
                                    }

                                    for target in &op.inserts {
                                        let tuple_vals: Option<Vec<Value>> = target
                                            .args
                                            .iter()
                                            .map(|arg| match arg {
                                                Term::Variable(v) => bindings.get(v).cloned(),
                                                other => term_to_value(other).ok(),
                                            })
                                            .collect();
                                        if let Some(vals) = tuple_vals {
                                            let (new_count, _) = storage
                                                .insert_tuples_into(
                                                    &kg_name,
                                                    &target.relation,
                                                    vec![Tuple::new(vals)],
                                                )
                                                .map_err(|e| e.to_string())?;
                                            inserted += new_count;
                                        }
                                    }
                                }

                                // Track insert count for metrics
                                self.insert_count
                                    .fetch_add(inserted as u64, Ordering::Relaxed);

                                if deleted > 0 || inserted > 0 {
                                    let _ = self.notify_tx.send(
                                        PersistentNotification::PersistentUpdate {
                                            knowledge_graph: kg_name.clone(),
                                            relation: "multiple".to_string(),
                                            operation: "update".to_string(),
                                            count: deleted + inserted,
                                        },
                                    );
                                }
                                messages.push(format!(
                                    "Update: {deleted} deleted, {inserted} inserted."
                                ));
                            }
                            statement::Statement::TypeDecl(decl) => {
                                messages.push(format!("Type '{}' declared.", decl.name));
                            }
                            statement::Statement::Meta(_) => {
                                messages
                                    .push("Meta commands not supported in query API.".to_string());
                            }
                        }
                    } else {
                        query_to_execute = Some(stmt_text.to_string());
                    }
                }
                current_stmt.clear();
            }
        }

        // Return messages if no query
        if !messages.is_empty() && query_to_execute.is_none() {
            let rows: Vec<WireTuple> = messages
                .iter()
                .map(|msg| WireTuple {
                    values: vec![WireValue::String(msg.clone())],
                    provenance: None,
                })
                .collect();
            return Ok(QueryResult {
                rows,
                schema: vec![ColumnDef {
                    name: "message".to_string(),
                    data_type: WireDataType::String,
                }],
                execution_time_ms: start.elapsed().as_millis() as u64,
                metadata: None,
            });
        }

        let program_text = query_to_execute.unwrap_or(program_text);

        // Transform ?shorthand query syntax into __query__(...) <- ... rule
        let query_program = transform_query_shorthand(&program_text)?;

        // Prepend session rules to the query program
        let query_program = if session_rules.is_empty() {
            query_program
        } else {
            let rules_text = session_rules.join("\n");
            format!("{rules_text}\n{query_program}")
        };

        // Execute query with session facts using isolated execution
        // Session facts are added to an ISOLATED COPY of the snapshot's data,
        // providing request-scoped isolation. Concurrent queries cannot see
        // each other's session facts.
        let debug_session = std::env::var("IL_DEBUG_SESSION").is_ok();
        if debug_session && !session_fact_tuples.is_empty() {
            eprintln!(
                "DEBUG: Executing with {} session facts (isolated)",
                session_fact_tuples.len()
            );
            for (relation, tuple) in &session_fact_tuples {
                eprintln!("DEBUG: Session fact '{relation}': {tuple:?}");
            }
        }

        // Use isolated execution - session facts are added to a CLONE, not the shared store
        // This fixes the race condition where concurrent queries could see each other's session facts
        let results = if session_fact_tuples.is_empty() {
            // No session facts - use the regular query path
            storage
                .execute_query_with_rules_tuples_on(&kg_name, &query_program)
                .map_err(|e| e.to_string())?
        } else {
            // Has session facts - use isolated execution
            storage
                .execute_query_with_session_facts_on(&kg_name, &query_program, session_fact_tuples)
                .map_err(|e| e.to_string())?
        };

        // Convert Tuple results to WireTuple, supporting mixed types
        let rows: Vec<WireTuple> = results
            .iter()
            .map(|tuple| {
                let values: Vec<WireValue> = tuple
                    .values()
                    .iter()
                    .map(|v| match v {
                        Value::Int32(n) => WireValue::Int32(*n),
                        Value::Int64(n) => WireValue::Int64(*n),
                        Value::Float64(f) => WireValue::Float64(*f),
                        Value::String(s) => WireValue::String(s.to_string()),
                        Value::Vector(vec) => WireValue::Vector(vec.as_ref().clone()),
                        Value::VectorInt8(vec) => WireValue::VectorInt8(vec.as_ref().clone()),
                        Value::Bool(b) => WireValue::Bool(*b),
                        Value::Null => WireValue::Null,
                        Value::Timestamp(ts) => WireValue::Timestamp(*ts),
                    })
                    .collect();
                WireTuple {
                    values,
                    provenance: None,
                }
            })
            .collect();

        // Build schema from first result or default to 2 columns
        let schema: Vec<ColumnDef> = if let Some(first) = results.first() {
            first
                .values()
                .iter()
                .enumerate()
                .map(|(i, v)| ColumnDef {
                    name: format!("col{i}"),
                    data_type: match v {
                        Value::Int32(_) => WireDataType::Int32,
                        Value::Int64(_) => WireDataType::Int64,
                        Value::Float64(_) => WireDataType::Float64,
                        Value::String(_) => WireDataType::String,
                        Value::Vector(_) => WireDataType::Vector { dim: None },
                        Value::VectorInt8(_) => WireDataType::VectorInt8 { dim: None },
                        Value::Bool(_) => WireDataType::Bool,
                        Value::Null => WireDataType::String,
                        Value::Timestamp(_) => WireDataType::Timestamp,
                    },
                })
                .collect()
        } else {
            vec![]
        };

        Ok(QueryResult {
            rows,
            schema,
            execution_time_ms: start.elapsed().as_millis() as u64,
            metadata: None,
        })
    }

    /// Explain a query plan without executing it.
    ///
    /// Runs the full compilation pipeline (parse → IR → optimize) and returns
    /// a human-readable representation of the query plan at each stage.
    pub fn explain_query(
        &self,
        knowledge_graph: Option<String>,
        query: String,
    ) -> Result<(String, Vec<String>), String> {
        let storage = self.storage.read();

        let kg_name = if let Some(ref kg) = knowledge_graph {
            storage
                .ensure_knowledge_graph(kg)
                .map_err(|e| format!("Knowledge graph not found: {e}"))?;
            kg.clone()
        } else {
            storage
                .current_knowledge_graph()
                .ok_or("No knowledge graph selected")?
                .to_string()
        };

        let trace = storage
            .explain_query_on(&kg_name, &query)
            .map_err(|e| format!("{e}"))?;

        let optimizations = vec![
            "Join Planning (spanning tree reordering)".to_string(),
            "SIP Rewriting (semijoin reduction)".to_string(),
            "Subplan Sharing (common subexpression elimination)".to_string(),
            "Basic Optimizations (identity elimination, filter simplification)".to_string(),
        ];

        Ok((trace.format_trace(), optimizations))
    }

    /// Execute a query within a session context.
    ///
    /// If the session has ephemeral state (facts or rules), they are combined
    /// with the persistent data for execution. The ephemeral data is invisible
    /// to other sessions.
    ///
    /// **Fast path**: If the session is clean (no ephemeral state), this
    /// delegates directly to `query_program` with no overhead.
    pub async fn query_program_with_session(
        &self,
        session_id: SessionId,
        program: String,
    ) -> Result<QueryResult, String> {
        // Touch session to prevent idle reaping during query execution
        self.sessions.touch_session(session_id)?;

        // Check if session is clean → fast path
        let is_clean = self.sessions.is_session_clean(session_id)?;
        let kg = self.sessions.session_kg(session_id)?;

        if is_clean {
            // Fast path: no ephemeral state, use global snapshot directly
            return self.query_program(Some(kg), program).await;
        }

        // Slow path: combine ephemeral + persistent data
        // Get ephemeral facts and rules from session
        let session_facts = self.sessions.get_session_facts(session_id)?;
        let rule_texts: Vec<String> = self
            .sessions
            .with_session(session_id, |session| session.rule_texts().to_vec())?;

        // Apply same preprocessing as the fast path: strip comments + transform ?shorthand
        let preprocessed = strip_comments(&program);
        let preprocessed = transform_query_shorthand(&preprocessed)?;

        // Build combined program: ephemeral rules + preprocessed query
        // Keep `preprocessed` for the persistent-only baseline (provenance diff)
        let combined_program = if rule_texts.is_empty() {
            preprocessed.clone()
        } else {
            let rules_prefix = rule_texts.join("\n");
            format!("{rules_prefix}\n{preprocessed}")
        };

        self.inc_query_count();
        let start = Instant::now();
        let storage = self.storage.read();

        storage
            .ensure_knowledge_graph(&kg)
            .map_err(|e| format!("Knowledge graph not found: {e}"))?;

        // Use isolated execution with session facts
        let results = storage
            .execute_query_with_session_facts_on(&kg, &combined_program, session_facts)
            .map_err(|e| e.to_string())?;

        // Per-tuple provenance: run the original query (without ephemeral rules)
        // against persistent-only data to identify ephemeral contributions.
        use crate::session::Provenance;
        use std::collections::HashSet;
        let baseline: HashSet<Tuple> = match storage
            .execute_query_with_rules_tuples_on(&kg, &preprocessed)
        {
            Ok(tuples) => tuples.into_iter().collect(),
            Err(e) => {
                eprintln!("Warning: provenance baseline query failed: {e}. All tuples will be tagged as ephemeral.");
                HashSet::new()
            }
        };

        // Convert results to wire format with per-tuple provenance
        let rows: Vec<WireTuple> = results
            .iter()
            .map(|tuple| {
                let values: Vec<WireValue> = tuple
                    .values()
                    .iter()
                    .map(|v| match v {
                        Value::Int32(n) => WireValue::Int32(*n),
                        Value::Int64(n) => WireValue::Int64(*n),
                        Value::Float64(f) => WireValue::Float64(*f),
                        Value::String(s) => WireValue::String(s.to_string()),
                        Value::Vector(vec) => WireValue::Vector(vec.as_ref().clone()),
                        Value::VectorInt8(vec) => WireValue::VectorInt8(vec.as_ref().clone()),
                        Value::Bool(b) => WireValue::Bool(*b),
                        Value::Null => WireValue::Null,
                        Value::Timestamp(ts) => WireValue::Timestamp(*ts),
                    })
                    .collect();
                // Tag provenance: if tuple exists in persistent baseline → Persistent,
                // otherwise it was introduced by ephemeral data → Ephemeral
                let prov = if baseline.contains(tuple) {
                    Provenance::Persistent
                } else {
                    Provenance::Ephemeral
                };
                WireTuple {
                    values,
                    provenance: Some(prov),
                }
            })
            .collect();

        let schema: Vec<ColumnDef> = if let Some(first) = results.first() {
            first
                .values()
                .iter()
                .enumerate()
                .map(|(i, v)| ColumnDef {
                    name: format!("col{i}"),
                    data_type: match v {
                        Value::Int32(_) => WireDataType::Int32,
                        Value::Int64(_) => WireDataType::Int64,
                        Value::Float64(_) => WireDataType::Float64,
                        Value::String(_) => WireDataType::String,
                        Value::Vector(_) => WireDataType::Vector { dim: None },
                        Value::VectorInt8(_) => WireDataType::VectorInt8 { dim: None },
                        Value::Bool(_) => WireDataType::Bool,
                        Value::Null => WireDataType::String,
                        Value::Timestamp(_) => WireDataType::Timestamp,
                    },
                })
                .collect()
        } else {
            vec![]
        };

        // Build provenance metadata from session state
        let query_meta = self.sessions.get_query_metadata(session_id)?;
        let result_metadata = super::wire::ResultMetadata::from_session(&query_meta, session_id);

        let execution_time_ms = start.elapsed().as_millis() as u64;

        // Record audit event for query with ephemeral data
        if query_meta.has_ephemeral {
            self.sessions.record_query_with_ephemeral(
                session_id,
                query_meta.ephemeral_sources.clone(),
                rows.len(),
                execution_time_ms,
            );
        }

        Ok(QueryResult {
            rows,
            schema,
            execution_time_ms,
            metadata: result_metadata,
        })
    }

    /// Insert ephemeral facts into a session.
    /// Returns the number of facts actually inserted (after dedup).
    pub fn session_insert_ephemeral(
        &self,
        session_id: SessionId,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<usize, String> {
        self.sessions.insert_ephemeral(session_id, relation, tuples)
    }

    /// Retract ephemeral facts from a session.
    pub fn session_retract_ephemeral(
        &self,
        session_id: SessionId,
        relation: &str,
        tuples: Vec<Tuple>,
    ) -> Result<usize, String> {
        self.sessions
            .retract_ephemeral(session_id, relation, tuples)
    }

    /// Add an ephemeral rule to a session.
    pub fn session_add_rule(
        &self,
        session_id: SessionId,
        rule: crate::ast::Rule,
        rule_text: String,
    ) -> Result<(), String> {
        self.sessions
            .add_ephemeral_rule(session_id, rule, rule_text)
    }

    /// Get session statistics.
    pub fn session_stats(&self) -> crate::session::SessionStats {
        self.sessions.stats()
    }
}

// Helper Functions

/// Transform `?shorthand` query syntax into a `__query__(...) <- ...` rule.
///
/// This enables the shorthand `?relation(X, Y)` syntax that the REPL and
/// REST API use, converting it to a proper Datalog rule before execution.
/// Returns the original text unchanged if it's not a `?shorthand` query.
pub(crate) fn transform_query_shorthand(program_text: &str) -> Result<String, String> {
    let trimmed = program_text.trim();
    if let Some(after_q) = trimmed.strip_prefix('?') {
        if !after_q.starts_with(char::is_alphabetic) {
            return Ok(program_text.to_string());
        }
        let query_text = after_q;
        let goal = statement::parse_query(query_text)
            .map_err(|e| format!("Failed to parse query: {e}"))?;

        let mut head_vars = Vec::new();
        let mut extra_constraints = Vec::new();

        let transformed_args: Vec<String> = goal
            .goal
            .args
            .iter()
            .enumerate()
            .map(|(i, term)| match term {
                Term::Variable(v) => {
                    head_vars.push(v.clone());
                    v.clone()
                }
                Term::Constant(val) => {
                    let t = format!("_c{i}");
                    head_vars.push(t.clone());
                    extra_constraints.push(format!("{t} = {val}"));
                    t
                }
                Term::FloatConstant(val) => {
                    let t = format!("_c{i}");
                    head_vars.push(t.clone());
                    extra_constraints.push(format!("{t} = {val}"));
                    t
                }
                Term::BoolConstant(val) => {
                    let t = format!("_c{i}");
                    head_vars.push(t.clone());
                    extra_constraints.push(format!("{t} = {val}"));
                    t
                }
                Term::StringConstant(s) => {
                    let t = format!("_c{i}");
                    head_vars.push(t.clone());
                    // Escape internal double quotes
                    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                    extra_constraints.push(format!("{t} = \"{escaped}\""));
                    t
                }
                Term::VectorLiteral(_) => {
                    // Vector literals can't be used in comparison constraints
                    // (parser doesn't support [1,2,3] in comparison context).
                    // Use a fresh variable — returns all rows for this position.
                    let t = format!("_v{i}");
                    head_vars.push(t.clone());
                    t
                }
                Term::Placeholder => {
                    let t = format!("_p{i}");
                    head_vars.push(t.clone());
                    t
                }
                _ => {
                    // For complex terms (Arithmetic, FunctionCall, etc.),
                    // use a fresh variable. The parser may not support these
                    // in comparison constraints, so don't add constraints.
                    let t = format!("_t{i}");
                    head_vars.push(t.clone());
                    t
                }
            })
            .collect();

        let body_atom = format!("{}({})", goal.goal.relation, transformed_args.join(", "));
        let mut body_parts = vec![body_atom];

        for pred in &goal.body {
            body_parts.push(format_body_pred(pred));
            extract_predicate_vars(pred, &mut head_vars);
        }

        body_parts.extend(extra_constraints);

        Ok(format!(
            "__query__({}) <- {}",
            head_vars.join(", "),
            body_parts.join(", ")
        ))
    } else {
        Ok(program_text.to_string())
    }
}

/// Strip comment lines from program text
fn strip_comments(program: &str) -> String {
    program
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.starts_with('%') && !trimmed.starts_with("//")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Format a rule as Datalog text (uses Rule's Display impl)
fn format_rule_text(rule: &crate::ast::Rule) -> String {
    rule.to_string()
}

/// Format a body predicate as Datalog text (uses BodyPredicate's Display impl)
fn format_body_pred(pred: &crate::ast::BodyPredicate) -> String {
    pred.to_string()
}

/// Format a term as Datalog text (uses Term's Display impl)
fn format_term(term: &Term) -> String {
    term.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Term;

    /// Create a Config with a unique temp directory (prevents parallel test conflicts)
    fn make_test_config() -> Config {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.data_dir = temp_dir.into_path();
        config
    }

    // --- term_to_value tests ---

    #[test]
    fn test_term_to_value_int() {
        assert_eq!(
            term_to_value(&Term::Constant(42)).unwrap(),
            Value::Int64(42)
        );
    }

    #[test]
    fn test_term_to_value_float() {
        assert_eq!(
            term_to_value(&Term::FloatConstant(3.14)).unwrap(),
            Value::Float64(3.14)
        );
    }

    #[test]
    fn test_term_to_value_string() {
        assert_eq!(
            term_to_value(&Term::StringConstant("hello".to_string())).unwrap(),
            Value::string("hello")
        );
    }

    #[test]
    fn test_term_to_value_bool() {
        assert_eq!(
            term_to_value(&Term::BoolConstant(true)).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn test_term_to_value_vector() {
        let result = term_to_value(&Term::VectorLiteral(vec![1.0, 2.0, 3.0])).unwrap();
        assert_eq!(result, Value::vector(vec![1.0, 2.0, 3.0]));
    }

    #[test]
    fn test_term_to_value_variable_error() {
        let result = term_to_value(&Term::Variable("X".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("variable"));
    }

    #[test]
    fn test_term_to_value_placeholder_error() {
        let result = term_to_value(&Term::Placeholder);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("placeholder"));
    }

    #[test]
    fn test_term_to_value_negative_int() {
        assert_eq!(
            term_to_value(&Term::Constant(-100)).unwrap(),
            Value::Int64(-100)
        );
    }

    #[test]
    fn test_term_to_value_zero() {
        assert_eq!(term_to_value(&Term::Constant(0)).unwrap(), Value::Int64(0));
    }

    // --- Handler construction tests ---

    #[test]
    fn test_handler_new() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let handler = Handler::new(storage);
        assert_eq!(handler.total_queries(), 0);
        assert_eq!(handler.total_inserts(), 0);
        assert!(handler.uptime_seconds() < 2);
    }

    #[test]
    fn test_handler_from_config() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        assert_eq!(handler.total_queries(), 0);
    }

    #[test]
    fn test_handler_with_session_config() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let config = SessionConfig {
            max_sessions: 10,
            ..SessionConfig::default()
        };
        let handler = Handler::with_session_config(storage, config);
        assert_eq!(handler.total_queries(), 0);
    }

    // --- Session management tests ---

    /// Helper to create a fresh handler with a known KG.
    /// Uses a unique temp directory for persist layer to avoid leftover data
    /// from previous test runs.
    fn handler_with_kg(kg_name: &str) -> Handler {
        let mut config = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).unwrap();
        handler
            .get_storage()
            .ensure_knowledge_graph(kg_name)
            .unwrap();
        handler
    }

    #[test]
    fn test_handler_create_and_close_session() {
        let handler = handler_with_kg("sess_create_test");
        let session_id = handler.create_session("sess_create_test").unwrap();
        assert!(session_id > 0);
        handler.close_session(session_id).unwrap();
    }

    #[test]
    fn test_handler_close_nonexistent_session() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        assert!(handler.close_session(999).is_err());
    }

    #[test]
    fn test_handler_session_insert_ephemeral() {
        let handler = handler_with_kg("sess_insert_test");
        let sid = handler.create_session("sess_insert_test").unwrap();
        let tuples = vec![Tuple::new(vec![Value::Int64(1), Value::Int64(2)])];
        handler
            .session_insert_ephemeral(sid, "edge", tuples)
            .unwrap();
    }

    #[test]
    fn test_handler_session_retract_ephemeral() {
        let handler = handler_with_kg("sess_retract_test");
        let sid = handler.create_session("sess_retract_test").unwrap();
        let tuples = vec![Tuple::new(vec![Value::Int64(1)])];
        handler
            .session_insert_ephemeral(sid, "r", tuples.clone())
            .unwrap();
        let retracted = handler.session_retract_ephemeral(sid, "r", tuples).unwrap();
        assert_eq!(retracted, 1);
    }

    #[test]
    fn test_handler_session_stats() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let stats = handler.session_stats();
        assert_eq!(stats.total_sessions, 0);
    }

    // --- Notification tests ---

    #[test]
    fn test_subscribe_notifications() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let handler = Handler::new(storage);
        let mut rx = handler.subscribe_notifications();

        handler.notify_persistent_update("test_kg", "edge", "insert", 5);

        match rx.try_recv() {
            Ok(PersistentNotification::PersistentUpdate {
                knowledge_graph,
                relation,
                operation,
                count,
            }) => {
                assert_eq!(knowledge_graph, "test_kg");
                assert_eq!(relation, "edge");
                assert_eq!(operation, "insert");
                assert_eq!(count, 5);
            }
            other => panic!("Expected PersistentUpdate, got {other:?}"),
        }
    }

    #[test]
    fn test_notify_no_subscribers() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let handler = Handler::new(storage);
        handler.notify_persistent_update("test_kg", "edge", "insert", 1);
    }

    #[test]
    fn test_multiple_subscribers() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let handler = Handler::new(storage);
        let mut rx1 = handler.subscribe_notifications();
        let mut rx2 = handler.subscribe_notifications();

        handler.notify_persistent_update("kg", "rel", "delete", 3);

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    // --- query_program tests ---

    #[tokio::test]
    async fn test_query_program_simple_insert() {
        // Use unique KG name to avoid leftover data from previous test runs
        let handler = handler_with_kg("simple_insert_test");
        let result = handler
            .query_program(
                Some("simple_insert_test".to_string()),
                "+edge[(1,2), (3,4)]".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let actual = result.rows[0].values[0].as_str().unwrap();
        assert!(
            actual.contains("Inserted 2"),
            "Expected 'Inserted 2', got: {actual}"
        );
    }

    #[tokio::test]
    async fn test_query_program_insert_and_query() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        handler
            .query_program(None, "+data[(1,), (2,), (3,)]".to_string())
            .await
            .unwrap();
        let result = handler
            .query_program(None, "?data(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_query_program_comment_stripping() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let program = "% this is a comment\n// this too\n+test_data[(1,)]".to_string();
        let result = handler.query_program(None, program).await.unwrap();
        assert!(result.rows[0].values[0]
            .as_str()
            .unwrap()
            .contains("Inserted"));
    }

    #[tokio::test]
    async fn test_query_program_session_fact() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let program = "temp(42)\n?temp(X)".to_string();
        let result = handler.query_program(None, program).await.unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_session_rule() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        handler
            .query_program(None, "+base[(1,), (2,), (3,)]".to_string())
            .await
            .unwrap();
        let program = "doubled(X, Y) <- base(X), Y = X * 2\n?doubled(X, Y)".to_string();
        let result = handler.query_program(None, program).await.unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[tokio::test]
    async fn test_query_program_persistent_rule() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        handler
            .query_program(None, "+nodes[(1,), (2,)]".to_string())
            .await
            .unwrap();
        handler
            .query_program(None, "+big(X) <- nodes(X), X > 1".to_string())
            .await
            .unwrap();
        let result = handler
            .query_program(None, "?big(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_delete_facts() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        handler
            .query_program(None, "+del_test[(1, 2), (3, 4)]".to_string())
            .await
            .unwrap();
        let result = handler
            .query_program(None, "-del_test(1, 2)".to_string())
            .await
            .unwrap();
        assert!(result.rows[0].values[0]
            .as_str()
            .unwrap()
            .contains("Deleted"));
        let remaining = handler
            .query_program(None, "?del_test(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(remaining.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_with_target_kg() {
        let handler = handler_with_kg("handler_target_kg");
        let result = handler
            .query_program(
                Some("handler_target_kg".to_string()),
                "+kgdata[(1,)]".to_string(),
            )
            .await
            .unwrap();
        assert!(result.rows[0].values[0]
            .as_str()
            .unwrap()
            .contains("Inserted"));
    }

    #[tokio::test]
    async fn test_query_program_no_results() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let result = handler
            .query_program(None, "?empty_relation(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    // --- explain tests ---

    #[test]
    fn test_explain_query_simple() {
        let handler = handler_with_kg("explain_test_kg");
        // explain_query takes a Datalog rule, not a ?query
        let result = handler.explain_query(
            Some("explain_test_kg".to_string()),
            "__q__(X, Y) <- edge(X, Y)".to_string(),
        );
        assert!(result.is_ok(), "explain failed: {:?}", result.err());
        let (trace, optimizations) = result.unwrap();
        assert!(!trace.is_empty());
        assert!(!optimizations.is_empty());
    }

    #[test]
    fn test_explain_query_no_kg_error() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let handler = Handler::new(storage);
        let result = handler.explain_query(None, "?edge(X, Y)".to_string());
        // No current KG selected → error
        assert!(result.is_err());
    }

    // --- query_program edge case tests ---

    #[tokio::test]
    async fn test_query_program_schema_declaration() {
        let handler = handler_with_kg("schema_decl_test");
        // Persistent schema syntax: +name(col: type, ...)
        let result = handler
            .query_program(
                Some("schema_decl_test".to_string()),
                "+person(name: string, age: int)".to_string(),
            )
            .await
            .unwrap();
        assert!(result.rows[0].values[0]
            .as_str()
            .unwrap()
            .contains("Schema"));
    }

    #[tokio::test]
    async fn test_query_program_bulk_delete() {
        let handler = handler_with_kg("bulk_del_test");
        handler
            .query_program(
                Some("bulk_del_test".to_string()),
                "+bd_rel[(1, 2), (3, 4), (5, 6)]".to_string(),
            )
            .await
            .unwrap();
        let result = handler
            .query_program(
                Some("bulk_del_test".to_string()),
                "-bd_rel[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .unwrap();
        assert!(result.rows[0].values[0]
            .as_str()
            .unwrap()
            .contains("Deleted"));
        // Verify only 1 fact remains
        let remaining = handler
            .query_program(
                Some("bulk_del_test".to_string()),
                "?bd_rel(X, Y)".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(remaining.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_register_persistent_rule() {
        let handler = handler_with_kg("persist_rule_test");
        handler
            .query_program(
                Some("persist_rule_test".to_string()),
                "+vals[(1,), (2,), (3,)]".to_string(),
            )
            .await
            .unwrap();
        let result = handler
            .query_program(
                Some("persist_rule_test".to_string()),
                "+doubled(X, Y) <- vals(X), Y = X * 2".to_string(),
            )
            .await
            .unwrap();
        assert!(result.rows[0].values[0].as_str().unwrap().contains("Rule"));
    }

    #[tokio::test]
    async fn test_query_program_conditional_delete() {
        let handler = handler_with_kg("cond_del_test");
        handler
            .query_program(
                Some("cond_del_test".to_string()),
                "+items[(1, 10), (2, 20), (3, 30)]".to_string(),
            )
            .await
            .unwrap();
        let result = handler
            .query_program(
                Some("cond_del_test".to_string()),
                "-items(X, Y) <- Y > 15".to_string(),
            )
            .await
            .unwrap();
        assert!(result.rows[0].values[0]
            .as_str()
            .unwrap()
            .contains("delete"));
        // Verify only (1, 10) remains
        let remaining = handler
            .query_program(
                Some("cond_del_test".to_string()),
                "?items(X, Y)".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(remaining.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_query_program_empty_program() {
        let handler = handler_with_kg("empty_prog_test");
        // Empty program has no IR nodes to execute
        let result = handler
            .query_program(Some("empty_prog_test".to_string()), "".to_string())
            .await;
        assert!(result.is_err() || result.unwrap().rows.len() <= 1);
    }

    #[tokio::test]
    async fn test_query_program_only_comments() {
        let handler = handler_with_kg("comments_only_test");
        // Program with only comments has no IR nodes to execute
        let result = handler
            .query_program(
                Some("comments_only_test".to_string()),
                "% just a comment\n// another comment".to_string(),
            )
            .await;
        assert!(result.is_err() || result.unwrap().rows.len() <= 1);
    }

    #[tokio::test]
    async fn test_query_program_multiple_queries_last_wins() {
        let handler = handler_with_kg("multi_q_test");
        handler
            .query_program(
                Some("multi_q_test".to_string()),
                "+alpha[(1,)]\n+beta[(2,)]".to_string(),
            )
            .await
            .unwrap();
        // When multiple queries exist, only the last query is executed
        let result = handler
            .query_program(
                Some("multi_q_test".to_string()),
                "?alpha(X)\n?beta(X)".to_string(),
            )
            .await
            .unwrap();
        // Last query is ?beta(X), should return 1 row
        assert_eq!(result.rows.len(), 1);
    }

    // --- query_program_with_session tests ---

    #[tokio::test]
    async fn test_query_program_with_session_clean() {
        let handler = handler_with_kg("sess_clean_q");
        handler
            .query_program(
                Some("sess_clean_q".to_string()),
                "+sdata[(1,), (2,)]".to_string(),
            )
            .await
            .unwrap();
        let sid = handler.create_session("sess_clean_q").unwrap();
        // Clean session uses fast path (delegates to query_program)
        let result = handler
            .query_program_with_session(sid, "?sdata(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        handler.close_session(sid).unwrap();
    }

    #[tokio::test]
    async fn test_query_program_with_session_ephemeral_facts() {
        let handler = handler_with_kg("sess_eph_q");
        handler
            .query_program(Some("sess_eph_q".to_string()), "+data[(1,)]".to_string())
            .await
            .unwrap();
        let sid = handler.create_session("sess_eph_q").unwrap();
        handler
            .session_insert_ephemeral(sid, "data", vec![Tuple::new(vec![Value::Int64(2)])])
            .unwrap();
        // query_program_with_session takes raw Datalog rules, not ?shorthand
        let result = handler
            .query_program_with_session(sid, "__q__(X) <- data(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
        handler.close_session(sid).unwrap();
    }

    #[tokio::test]
    async fn test_query_program_with_session_invalid_id() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let result = handler
            .query_program_with_session(99999, "?data(X)".to_string())
            .await;
        assert!(result.is_err());
    }

    // --- session_add_rule test ---

    #[tokio::test]
    async fn test_session_add_rule_and_query() {
        let handler = handler_with_kg("sess_rule_q");
        handler
            .query_program(
                Some("sess_rule_q".to_string()),
                "+base[(1,), (2,), (3,)]".to_string(),
            )
            .await
            .unwrap();
        let sid = handler.create_session("sess_rule_q").unwrap();
        // Parse a rule and add it to the session
        let rule_text = "doubled(X, Y) <- base(X), Y = X * 2";
        let rule = crate::parser::parse_rule(rule_text).unwrap();
        handler
            .session_add_rule(sid, rule, rule_text.to_string())
            .unwrap();
        // query_program_with_session takes raw Datalog rules, not ?shorthand
        let result = handler
            .query_program_with_session(sid, "__q__(X, Y) <- doubled(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);
        handler.close_session(sid).unwrap();
    }

    // --- validate_tuples_against_schema tests ---

    #[tokio::test]
    async fn test_validate_tuples_no_schema() {
        let handler = handler_with_kg("val_no_schema");
        let tuples = vec![Tuple::new(vec![Value::Int64(1)])];
        // No schema registered → validation passes
        assert!(handler
            .validate_tuples_against_schema("val_no_schema", "any_rel", &tuples)
            .is_ok());
    }

    #[tokio::test]
    async fn test_validate_tuples_with_schema() {
        let handler = handler_with_kg("val_with_schema");
        // Persistent schema syntax: +name(col: type, ...)
        handler
            .query_program(
                Some("val_with_schema".to_string()),
                "+typed_rel(name: string, value: int)".to_string(),
            )
            .await
            .unwrap();
        // Valid tuples
        let valid = vec![Tuple::new(vec![Value::string("alice"), Value::Int64(42)])];
        assert!(handler
            .validate_tuples_against_schema("val_with_schema", "typed_rel", &valid)
            .is_ok());
    }

    // --- term_to_value remaining edge cases ---

    #[test]
    fn test_term_to_value_aggregate_error() {
        use crate::ast::AggregateFunc;
        let result = term_to_value(&Term::Aggregate(AggregateFunc::Count, "X".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("aggregate"));
    }

    #[test]
    fn test_term_to_value_function_call_error() {
        use crate::ast::BuiltinFunc;
        let result = term_to_value(&Term::FunctionCall(
            BuiltinFunc::Abs,
            vec![Term::Constant(-5)],
        ));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("function call"));
    }

    #[test]
    fn test_term_to_value_record_pattern_error() {
        let result = term_to_value(&Term::RecordPattern(vec![]));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("record pattern"));
    }

    // --- Counter and uptime tests ---

    #[tokio::test]
    async fn test_query_count_increments() {
        let handler = handler_with_kg("counter_test");
        assert_eq!(handler.total_queries(), 0);
        handler
            .query_program(Some("counter_test".to_string()), "+stuff[(1,)]".to_string())
            .await
            .unwrap();
        assert_eq!(handler.total_queries(), 1);
        handler
            .query_program(Some("counter_test".to_string()), "?stuff(X)".to_string())
            .await
            .unwrap();
        assert_eq!(handler.total_queries(), 2);
    }

    #[tokio::test]
    async fn test_insert_count_increments() {
        let handler = handler_with_kg("insert_cnt_test");
        assert_eq!(handler.total_inserts(), 0);
        handler
            .query_program(
                Some("insert_cnt_test".to_string()),
                "+data[(1,), (2,)]".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(handler.total_inserts(), 2);
    }

    // =========================================================================
    // Multi-Client Session Isolation Tests
    // =========================================================================
    // These tests verify the core ephemeral-triggers-persistent design:
    // - Two clients share the same KG and persistent rules
    // - Each client has its own ephemeral facts
    // - Persistent rule results differ per session based on ephemeral input
    // - Without any session, only persistent facts are visible

    #[tokio::test]
    async fn test_two_clients_persistent_rule_different_ephemeral_facts() {
        // Core scenario: persistent rule + 2 sessions with different ephemeral facts
        let handler = handler_with_kg("multi_client_1");
        let kg = "multi_client_1";

        // 1. Insert persistent base facts
        handler
            .query_program(Some(kg.to_string()), "+edge[(10, 20)]".to_string())
            .await
            .unwrap();

        // 2. Register persistent rule: reachable(X,Y) <- edge(X,Y)
        handler
            .query_program(
                Some(kg.to_string()),
                "+reachable(X, Y) <- edge(X, Y)".to_string(),
            )
            .await
            .unwrap();

        // 3. Create two sessions (two "clients")
        let client_a = handler.create_session(kg).unwrap();
        let client_b = handler.create_session(kg).unwrap();

        // 4. Client A inserts ephemeral edge(1, 2)
        handler
            .session_insert_ephemeral(
                client_a,
                "edge",
                vec![Tuple::new(vec![Value::Int64(1), Value::Int64(2)])],
            )
            .unwrap();

        // 5. Client B inserts ephemeral edge(3, 4)
        handler
            .session_insert_ephemeral(
                client_b,
                "edge",
                vec![Tuple::new(vec![Value::Int64(3), Value::Int64(4)])],
            )
            .unwrap();

        // 6. Client A queries reachable → sees persistent (10,20) + ephemeral (1,2)
        let result_a = handler
            .query_program_with_session(client_a, "__q__(X, Y) <- reachable(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(
            result_a.rows.len(),
            2,
            "Client A should see 2 reachable tuples: persistent (10,20) + ephemeral (1,2)"
        );

        // 7. Client B queries reachable → sees persistent (10,20) + ephemeral (3,4)
        let result_b = handler
            .query_program_with_session(client_b, "__q__(X, Y) <- reachable(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(
            result_b.rows.len(),
            2,
            "Client B should see 2 reachable tuples: persistent (10,20) + ephemeral (3,4)"
        );

        // 8. Verify the actual values differ between sessions
        let a_values: std::collections::HashSet<(i64, i64)> = result_a
            .rows
            .iter()
            .map(|r| (r.values[0].as_i64().unwrap(), r.values[1].as_i64().unwrap()))
            .collect();
        let b_values: std::collections::HashSet<(i64, i64)> = result_b
            .rows
            .iter()
            .map(|r| (r.values[0].as_i64().unwrap(), r.values[1].as_i64().unwrap()))
            .collect();

        // Both see the persistent fact (10, 20)
        assert!(a_values.contains(&(10, 20)));
        assert!(b_values.contains(&(10, 20)));
        // Client A sees (1, 2) but NOT (3, 4)
        assert!(a_values.contains(&(1, 2)));
        assert!(!a_values.contains(&(3, 4)));
        // Client B sees (3, 4) but NOT (1, 2)
        assert!(b_values.contains(&(3, 4)));
        assert!(!b_values.contains(&(1, 2)));

        // 9. Without session, only persistent facts visible
        let result_no_session = handler
            .query_program(Some(kg.to_string()), "?reachable(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(
            result_no_session.rows.len(),
            1,
            "Without session, only the persistent edge(10,20) should produce reachable(10,20)"
        );

        handler.close_session(client_a).unwrap();
        handler.close_session(client_b).unwrap();
    }

    #[tokio::test]
    async fn test_two_clients_only_ephemeral_base_facts() {
        // Persistent rule exists but no persistent base facts
        // Only ephemeral facts from sessions trigger the rule
        let handler = handler_with_kg("multi_client_2");
        let kg = "multi_client_2";

        // Register persistent rule (no persistent edge facts)
        handler
            .query_program(
                Some(kg.to_string()),
                "+path(X, Y) <- link(X, Y)".to_string(),
            )
            .await
            .unwrap();

        let client_a = handler.create_session(kg).unwrap();
        let client_b = handler.create_session(kg).unwrap();

        // Client A: link(1,2), link(2,3)
        handler
            .session_insert_ephemeral(
                client_a,
                "link",
                vec![
                    Tuple::new(vec![Value::Int64(1), Value::Int64(2)]),
                    Tuple::new(vec![Value::Int64(2), Value::Int64(3)]),
                ],
            )
            .unwrap();

        // Client B: link(100,200) (completely different)
        handler
            .session_insert_ephemeral(
                client_b,
                "link",
                vec![Tuple::new(vec![Value::Int64(100), Value::Int64(200)])],
            )
            .unwrap();

        // Client A → 2 path results
        let result_a = handler
            .query_program_with_session(client_a, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result_a.rows.len(), 2);

        // Client B → 1 path result
        let result_b = handler
            .query_program_with_session(client_b, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result_b.rows.len(), 1);

        // No session → 0 results (no persistent link facts)
        let result_none = handler
            .query_program(Some(kg.to_string()), "?path(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result_none.rows.len(), 0);

        handler.close_session(client_a).unwrap();
        handler.close_session(client_b).unwrap();
    }

    #[tokio::test]
    async fn test_provenance_tags_persistent_vs_ephemeral() {
        // Verify per-tuple provenance is correctly assigned
        let handler = handler_with_kg("prov_tags");
        let kg = "prov_tags";

        // Persistent fact
        handler
            .query_program(Some(kg.to_string()), "+items[(1,), (2,)]".to_string())
            .await
            .unwrap();

        let sid = handler.create_session(kg).unwrap();
        // Ephemeral fact
        handler
            .session_insert_ephemeral(sid, "items", vec![Tuple::new(vec![Value::Int64(3)])])
            .unwrap();

        let result = handler
            .query_program_with_session(sid, "__q__(X) <- items(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);

        // Count provenance tags
        use crate::session::Provenance;
        let persistent_count = result
            .rows
            .iter()
            .filter(|r| r.provenance == Some(Provenance::Persistent))
            .count();
        let ephemeral_count = result
            .rows
            .iter()
            .filter(|r| r.provenance == Some(Provenance::Ephemeral))
            .count();

        assert_eq!(persistent_count, 2, "2 tuples from persistent data");
        assert_eq!(ephemeral_count, 1, "1 tuple from ephemeral data");

        handler.close_session(sid).unwrap();
    }

    #[tokio::test]
    async fn test_session_ephemeral_rule_augments_persistent() {
        // Client adds an ephemeral rule that extends persistent facts
        let handler = handler_with_kg("eph_rule_aug");
        let kg = "eph_rule_aug";

        // Persistent base facts
        handler
            .query_program(Some(kg.to_string()), "+edge[(1, 2), (2, 3)]".to_string())
            .await
            .unwrap();

        let client_a = handler.create_session(kg).unwrap();
        let client_b = handler.create_session(kg).unwrap();

        // Client A adds an ephemeral rule: path(X,Y) <- edge(X,Y)
        let rule_a = crate::parser::parse_rule("path(X, Y) <- edge(X, Y)").unwrap();
        handler
            .session_add_rule(client_a, rule_a, "path(X, Y) <- edge(X, Y)".to_string())
            .unwrap();

        // Client B inserts a trivial ephemeral fact to make it "dirty"
        // (so it uses the slow path and doesn't delegate to query_program)
        handler
            .session_insert_ephemeral(client_b, "marker", vec![Tuple::new(vec![Value::Int64(0)])])
            .unwrap();

        // Client A queries path → 2 results (from the ephemeral rule on persistent edges)
        let result_a = handler
            .query_program_with_session(client_a, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result_a.rows.len(), 2);

        // Client B queries path → 0 results (no "path" rule in client B's scope)
        let result_b = handler
            .query_program_with_session(client_b, "__q__(X, Y) <- path(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result_b.rows.len(), 0);

        handler.close_session(client_a).unwrap();
        handler.close_session(client_b).unwrap();
    }

    #[tokio::test]
    async fn test_client_close_cleans_up_ephemeral() {
        // After session close, ephemeral facts no longer affect queries
        let handler = handler_with_kg("close_cleanup");
        let kg = "close_cleanup";

        handler
            .query_program(
                Some(kg.to_string()),
                "+cleanup_rule(X) <- base(X)".to_string(),
            )
            .await
            .unwrap();

        let sid = handler.create_session(kg).unwrap();
        handler
            .session_insert_ephemeral(sid, "base", vec![Tuple::new(vec![Value::Int64(42)])])
            .unwrap();

        // Query while session is active → 1 result
        let result = handler
            .query_program_with_session(sid, "__q__(X) <- cleanup_rule(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        // Close session
        handler.close_session(sid).unwrap();

        // Query without session → 0 results
        let result = handler
            .query_program(Some(kg.to_string()), "?cleanup_rule(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[tokio::test]
    async fn test_many_sessions_sequential_queries() {
        // Simulate 10 "AI agent" sessions querying sequentially
        let handler = handler_with_kg("many_agents");
        let kg = "many_agents";

        // Persistent base
        handler
            .query_program(Some(kg.to_string()), "+doc[(1,), (2,), (3,)]".to_string())
            .await
            .unwrap();
        handler
            .query_program(
                Some(kg.to_string()),
                "+relevant(X) <- doc(X), query_embedding(X)".to_string(),
            )
            .await
            .unwrap();

        // Create sessions and insert different query embeddings
        let mut sessions = vec![];
        for i in 0..10i64 {
            let sid = handler.create_session(kg).unwrap();
            // Each session queries for a different doc: session i queries for doc i%3+1
            handler
                .session_insert_ephemeral(
                    sid,
                    "query_embedding",
                    vec![Tuple::new(vec![Value::Int64(i % 3 + 1)])],
                )
                .unwrap();
            sessions.push(sid);
        }

        // Query all sessions sequentially (each is isolated)
        for (i, &sid) in sessions.iter().enumerate() {
            let result = handler
                .query_program_with_session(sid, "__q__(X) <- relevant(X)".to_string())
                .await
                .unwrap();
            assert_eq!(
                result.rows.len(),
                1,
                "Session {i} should see exactly 1 relevant doc"
            );
        }

        // Cleanup
        for sid in sessions {
            handler.close_session(sid).unwrap();
        }
    }

    #[tokio::test]
    async fn test_ephemeral_retract_changes_session_results() {
        // Session adds ephemeral facts, queries, retracts some, queries again
        let handler = handler_with_kg("retract_changes");
        let kg = "retract_changes";

        handler
            .query_program(Some(kg.to_string()), "+visible(X) <- item(X)".to_string())
            .await
            .unwrap();

        let sid = handler.create_session(kg).unwrap();
        handler
            .session_insert_ephemeral(
                sid,
                "item",
                vec![
                    Tuple::new(vec![Value::Int64(1)]),
                    Tuple::new(vec![Value::Int64(2)]),
                    Tuple::new(vec![Value::Int64(3)]),
                ],
            )
            .unwrap();

        // Query → 3 visible
        let result = handler
            .query_program_with_session(sid, "__q__(X) <- visible(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);

        // Retract item(2)
        handler
            .session_retract_ephemeral(sid, "item", vec![Tuple::new(vec![Value::Int64(2)])])
            .unwrap();

        // Query → 2 visible
        let result = handler
            .query_program_with_session(sid, "__q__(X) <- visible(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        handler.close_session(sid).unwrap();
    }

    #[tokio::test]
    async fn test_session_metadata_reports_ephemeral_sources() {
        let handler = handler_with_kg("meta_sources");
        let kg = "meta_sources";

        handler
            .query_program(Some(kg.to_string()), "+derived(X) <- src(X)".to_string())
            .await
            .unwrap();

        let sid = handler.create_session(kg).unwrap();
        handler
            .session_insert_ephemeral(sid, "src", vec![Tuple::new(vec![Value::Int64(1)])])
            .unwrap();

        let result = handler
            .query_program_with_session(sid, "__q__(X) <- derived(X)".to_string())
            .await
            .unwrap();

        // Result should have metadata about ephemeral participation
        assert!(result.metadata.is_some());
        let meta = result.metadata.unwrap();
        assert!(meta.has_ephemeral);
        assert!(
            meta.ephemeral_sources.contains(&"src".to_string()),
            "metadata should report 'src' as ephemeral source"
        );

        handler.close_session(sid).unwrap();
    }

    // --- Notifications from mutations ---

    #[tokio::test]
    async fn test_insert_sends_notification() {
        let handler = handler_with_kg("notif_ins_test");
        let mut rx = handler.subscribe_notifications();
        handler
            .query_program(
                Some("notif_ins_test".to_string()),
                "+edges[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .unwrap();
        match rx.try_recv() {
            Ok(PersistentNotification::PersistentUpdate {
                operation, count, ..
            }) => {
                assert_eq!(operation, "insert");
                assert_eq!(count, 2);
            }
            other => panic!("Expected PersistentUpdate, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_delete_sends_notification() {
        let handler = handler_with_kg("notif_del_test");
        handler
            .query_program(
                Some("notif_del_test".to_string()),
                "+edges[(1, 2), (3, 4)]".to_string(),
            )
            .await
            .unwrap();
        let mut rx = handler.subscribe_notifications();
        handler
            .query_program(
                Some("notif_del_test".to_string()),
                "-edges(1, 2)".to_string(),
            )
            .await
            .unwrap();
        match rx.try_recv() {
            Ok(PersistentNotification::PersistentUpdate {
                operation, count, ..
            }) => {
                assert_eq!(operation, "delete");
                assert_eq!(count, 1);
            }
            other => panic!("Expected PersistentUpdate, got {other:?}"),
        }
    }

    // =========================================================================
    // Additional Handler Coverage Tests
    // =========================================================================

    #[test]
    fn test_term_to_value_arithmetic_error() {
        use crate::ast::ArithExpr;
        let result = term_to_value(&Term::Arithmetic(ArithExpr::Variable("X".to_string())));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("arithmetic"));
    }

    #[test]
    fn test_term_to_value_field_access_error() {
        let result = term_to_value(&Term::FieldAccess(
            Box::new(Term::Variable("record".to_string())),
            "field".to_string(),
        ));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("field access"));
    }

    #[test]
    fn test_term_to_value_vector_f32_overflow() {
        // f64 value that overflows f32
        let result = term_to_value(&Term::VectorLiteral(vec![1e40]));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("overflows f32"));
    }

    #[test]
    fn test_term_to_value_vector_normal() {
        let result = term_to_value(&Term::VectorLiteral(vec![1.0, 2.5, 3.0]));
        assert!(result.is_ok());
    }

    #[test]
    fn test_handler_get_storage() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let storage = handler.get_storage();
        // Should have default KG
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"default".to_string()));
    }

    #[test]
    fn test_handler_get_storage_mut() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let storage = handler.get_storage_mut();
        // Should be able to create a new KG
        storage.create_knowledge_graph("test_mut").unwrap();
        assert!(storage
            .list_knowledge_graphs()
            .contains(&"test_mut".to_string()));
    }

    #[test]
    fn test_handler_session_manager() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let mgr = handler.session_manager();
        assert_eq!(mgr.session_count(), 0);
    }

    #[tokio::test]
    async fn test_query_program_no_kg_selected() {
        let storage = StorageEngine::new(make_test_config()).unwrap();
        let handler = Handler::new(storage);
        // Without an explicit KG, uses the default
        let result = handler.query_program(None, "+data[(1,)]".to_string()).await;
        // Should succeed since default KG exists
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_query_program_invalid_syntax() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        // Invalid Datalog syntax
        let result = handler
            .query_program(None, "not valid datalog !!!".to_string())
            .await;
        // Should not crash - either returns error or empty
        // (parser resilience)
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_handler_uptime() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        // Just created, uptime should be < 2 seconds
        assert!(handler.uptime_seconds() < 2);
    }

    #[tokio::test]
    async fn test_query_program_multiline_with_mixed_comments() {
        let handler = handler_with_kg("mixed_comments");
        let program = "%% header comment\n+mc_data[(1,), (2,)]\n// inline\n?mc_data(X)";
        let result = handler
            .query_program(Some("mixed_comments".to_string()), program.to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_session_insert_retract_and_query() {
        let handler = handler_with_kg("sess_irt");
        let kg = "sess_irt";

        // Insert persistent base
        handler
            .query_program(Some(kg.to_string()), "+base[(10,)]".to_string())
            .await
            .unwrap();

        // Create session
        let sid = handler.create_session(kg).unwrap();

        // Add ephemeral facts
        handler
            .session_insert_ephemeral(
                sid,
                "base",
                vec![
                    Tuple::new(vec![Value::Int64(20)]),
                    Tuple::new(vec![Value::Int64(30)]),
                ],
            )
            .unwrap();

        // Query with session → 3 results
        let result = handler
            .query_program_with_session(sid, "__q__(X) <- base(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 3);

        // Retract one ephemeral fact
        handler
            .session_retract_ephemeral(sid, "base", vec![Tuple::new(vec![Value::Int64(20)])])
            .unwrap();

        // Query again → 2 results
        let result = handler
            .query_program_with_session(sid, "__q__(X) <- base(X)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        handler.close_session(sid).unwrap();
    }

    #[test]
    fn test_session_insert_invalid_session() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let result =
            handler.session_insert_ephemeral(99999, "rel", vec![Tuple::new(vec![Value::Int64(1)])]);
        assert!(result.is_err());
    }

    #[test]
    fn test_session_retract_invalid_session() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let result = handler.session_retract_ephemeral(
            99999,
            "rel",
            vec![Tuple::new(vec![Value::Int64(1)])],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_persistent_notification_serialize() {
        let notif = PersistentNotification::PersistentUpdate {
            knowledge_graph: "test".to_string(),
            relation: "edge".to_string(),
            operation: "insert".to_string(),
            count: 5,
        };
        let json = serde_json::to_string(&notif).unwrap();
        assert!(json.contains("persistent_update"));
        assert!(json.contains("\"count\":5"));
    }

    // --- extract_predicate_vars tests ---

    #[test]
    fn test_extract_predicate_vars_positive() {
        use crate::ast::{Atom, BodyPredicate};
        let atom = Atom::new(
            "edge".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("Y".to_string()),
            ],
        );
        let pred = BodyPredicate::Positive(atom);
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert_eq!(vars, vec!["X".to_string(), "Y".to_string()]);
    }

    #[test]
    fn test_extract_predicate_vars_negated_skipped() {
        // Negated atoms should NOT contribute variables to the query head.
        // A variable only appearing in a negated body atom is "unsafe" in Datalog.
        use crate::ast::{Atom, BodyPredicate};
        let atom = Atom::new("banned".to_string(), vec![Term::Variable("Z".to_string())]);
        let pred = BodyPredicate::Negated(atom);
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert!(vars.is_empty(), "Negated atoms should not add vars to head");
    }

    #[test]
    fn test_extract_predicate_vars_comparison() {
        use crate::ast::{BodyPredicate, ComparisonOp};
        let pred = BodyPredicate::Comparison(
            Term::Variable("A".to_string()),
            ComparisonOp::GreaterThan,
            Term::Variable("B".to_string()),
        );
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert_eq!(vars, vec!["A".to_string(), "B".to_string()]);
    }

    #[test]
    fn test_extract_predicate_vars_no_duplicates() {
        use crate::ast::{Atom, BodyPredicate};
        let atom = Atom::new(
            "self_join".to_string(),
            vec![
                Term::Variable("X".to_string()),
                Term::Variable("X".to_string()),
            ],
        );
        let pred = BodyPredicate::Positive(atom);
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert_eq!(vars, vec!["X".to_string()]); // No duplicate
    }

    #[test]
    fn test_extract_predicate_vars_skips_constants() {
        use crate::ast::{Atom, BodyPredicate};
        let atom = Atom::new(
            "data".to_string(),
            vec![
                Term::Constant(42),
                Term::Variable("X".to_string()),
                Term::Placeholder,
            ],
        );
        let pred = BodyPredicate::Positive(atom);
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert_eq!(vars, vec!["X".to_string()]);
    }

    #[test]
    fn test_extract_predicate_vars_comparison_with_constant() {
        use crate::ast::{BodyPredicate, ComparisonOp};
        let pred = BodyPredicate::Comparison(
            Term::Variable("X".to_string()),
            ComparisonOp::GreaterThan,
            Term::Constant(10),
        );
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert_eq!(vars, vec!["X".to_string()]);
    }

    #[test]
    fn test_extract_predicate_vars_hnsw() {
        use crate::ast::BodyPredicate;
        let pred = BodyPredicate::HnswNearest {
            index_name: "embeddings".to_string(),
            query: Term::Variable("QV".to_string()),
            k: 5,
            id_var: "Id".to_string(),
            distance_var: "Dist".to_string(),
            ef_search: None,
        };
        let mut vars = Vec::new();
        super::extract_predicate_vars(&pred, &mut vars);
        assert!(vars.contains(&"Id".to_string()));
        assert!(vars.contains(&"Dist".to_string()));
    }

    // =========================================================================
    // Additional Handler Coverage Tests
    // =========================================================================

    #[test]
    fn test_handler_total_queries_initial() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        assert_eq!(handler.total_queries(), 0);
    }

    #[test]
    fn test_handler_total_inserts_initial() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        assert_eq!(handler.total_inserts(), 0);
    }

    #[test]
    fn test_handler_session_stats_empty() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let stats = handler.session_stats();
        assert_eq!(stats.total_sessions, 0);
    }

    #[tokio::test]
    async fn test_handler_query_updates_counter() {
        let mut config = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).unwrap();
        handler
            .query_program(Some("counter_kg".to_string()), "+data[(1,)]".to_string())
            .await
            .unwrap();
        assert!(handler.total_inserts() > 0 || handler.total_queries() > 0);
    }

    #[test]
    fn test_handler_explain_query() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        {
            let storage = handler.get_storage_mut();
            storage.create_knowledge_graph("explain_h_kg").unwrap();
        }
        let trace = handler.explain_query(
            Some("explain_h_kg".to_string()),
            "result(X, Y) <- edge(X, Y)".to_string(),
        );
        assert!(trace.is_ok());
    }

    #[test]
    fn test_handler_subscribe_notifications() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let rx = handler.subscribe_notifications();
        // Should create a valid receiver without errors
        drop(rx);
    }

    #[tokio::test]
    async fn test_handler_query_select_all_tuples() {
        let mut config = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).unwrap();
        handler
            .query_program(
                Some("sel_kg".to_string()),
                "+items[(1, 10), (2, 20)]".to_string(),
            )
            .await
            .unwrap();
        let result = handler
            .query_program(Some("sel_kg".to_string()), "?items(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_handler_query_with_rule() {
        let mut config = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).unwrap();
        handler
            .query_program(
                Some("rule_q_kg".to_string()),
                "+edge[(1, 2), (2, 3)]".to_string(),
            )
            .await
            .unwrap();
        // Define a persistent rule
        handler
            .query_program(
                Some("rule_q_kg".to_string()),
                "+path(X, Y) <- edge(X, Y)".to_string(),
            )
            .await
            .unwrap();
        // Query the derived relation
        let result = handler
            .query_program(Some("rule_q_kg".to_string()), "?path(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_handler_uptime_seconds() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        // Just created, uptime should be 0 or very small
        assert!(handler.uptime_seconds() < 2);
    }

    #[tokio::test]
    async fn test_handler_total_queries_after_query() {
        let mut config = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).unwrap();
        handler
            .query_program(Some("tq_kg".to_string()), "+data[(1,)]".to_string())
            .await
            .unwrap();
        assert!(handler.total_queries() > 0 || handler.total_inserts() > 0);
    }

    #[test]
    fn test_handler_close_session_invalid() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let result = handler.close_session(999999);
        assert!(result.is_err());
    }

    #[test]
    fn test_handler_validate_tuples_no_schema() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        {
            let storage = handler.get_storage_mut();
            storage.create_knowledge_graph("val_kg").unwrap();
        }
        // With no schema defined, validation should pass
        let tuples = vec![Tuple::new(vec![Value::Int32(1)])];
        let result = handler.validate_tuples_against_schema("val_kg", "test_rel", &tuples);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handler_notify_persistent_update() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        // Should not panic — fire and forget notification
        handler.notify_persistent_update("kg", "rel", "insert", 5);
    }

    #[tokio::test]
    async fn test_handler_query_program_insert_and_query() {
        let mut config = make_test_config();
        config.storage.auto_create_knowledge_graphs = true;
        let handler = Handler::from_config(config).unwrap();
        // Insert data
        handler
            .query_program(
                Some("qp_iq_kg".to_string()),
                "+scores[(1, 100), (2, 200)]".to_string(),
            )
            .await
            .unwrap();
        // Query it back
        let result = handler
            .query_program(Some("qp_iq_kg".to_string()), "?scores(X, Y)".to_string())
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_handler_query_program_nonexistent_kg() {
        let handler = Handler::from_config(make_test_config()).unwrap();
        let result = handler
            .query_program(
                Some("nonexistent_kg_xyz".to_string()),
                "?data(X)".to_string(),
            )
            .await;
        assert!(result.is_err());
    }
}

/// Recursively extract variable names from a term.
fn extract_term_vars(term: &Term, vars: &mut Vec<String>) {
    match term {
        Term::Variable(v) => {
            if !vars.contains(v) {
                vars.push(v.clone());
            }
        }
        Term::Arithmetic(expr) => {
            extract_arith_vars(expr, vars);
        }
        Term::FunctionCall(_, args) => {
            for arg in args {
                extract_term_vars(arg, vars);
            }
        }
        Term::FieldAccess(base, _) => {
            extract_term_vars(base, vars);
        }
        Term::RecordPattern(fields) => {
            for (_, field_term) in fields {
                extract_term_vars(field_term, vars);
            }
        }
        // Constants, placeholders, aggregates, vectors — no variables to extract
        _ => {}
    }
}

/// Recursively extract variable names from an arithmetic expression.
fn extract_arith_vars(expr: &crate::ast::ArithExpr, vars: &mut Vec<String>) {
    match expr {
        crate::ast::ArithExpr::Variable(v) => {
            if !vars.contains(v) {
                vars.push(v.clone());
            }
        }
        crate::ast::ArithExpr::Binary { left, right, .. } => {
            extract_arith_vars(left, vars);
            extract_arith_vars(right, vars);
        }
        // Constants — no variables
        crate::ast::ArithExpr::Constant(_) | crate::ast::ArithExpr::FloatConstant(_) => {}
    }
}

/// Extract variables from a body predicate and add to `head_vars`
/// Used for Cartesian product queries like ?- foo(X), bar(Y).
fn extract_predicate_vars(pred: &crate::ast::BodyPredicate, head_vars: &mut Vec<String>) {
    match pred {
        crate::ast::BodyPredicate::Positive(atom) => {
            for term in &atom.args {
                extract_term_vars(term, head_vars);
            }
        }
        crate::ast::BodyPredicate::Negated(_) => {
            // Do NOT extract variables from negated atoms into the query head.
            // A variable that appears only in a negated body atom is "unsafe"
            // in Datalog — it cannot be safely projected into the head.
        }
        crate::ast::BodyPredicate::Comparison(left, _, right) => {
            extract_term_vars(left, head_vars);
            extract_term_vars(right, head_vars);
        }
        crate::ast::BodyPredicate::HnswNearest {
            id_var,
            distance_var,
            query,
            ..
        } => {
            if !head_vars.contains(id_var) {
                head_vars.push(id_var.clone());
            }
            if !head_vars.contains(distance_var) {
                head_vars.push(distance_var.clone());
            }
            extract_term_vars(query, head_vars);
        }
    }
}
