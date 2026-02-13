//! Handler for `InputLayer`
//!
//! Core business logic for Datalog queries and data operations, used by the REST API.
//! Uses `parking_lot::RwLock` (no poisoning) and `AtomicU64` (lock-free counters).

use crate::ast::Term;
use crate::rule_catalog::validate_rule;
use crate::schema::{ColumnSchema, RelationSchema};
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
        Term::VectorLiteral(v) => Ok(Value::vector(v.iter().map(|x| *x as f32).collect())),
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

/// Thread-safe wrapper around StorageEngine for concurrent API calls.
/// Per-KG schema validation via isolated SchemaCatalogs.
pub struct Handler {
    storage: Arc<RwLock<StorageEngine>>,
    start_time: Instant,
    query_count: AtomicU64,
    insert_count: AtomicU64,
}

impl Handler {
    /// Create a new handler with the given storage engine.
    pub fn new(storage: StorageEngine) -> Self {
        Self {
            storage: Arc::new(RwLock::new(storage)),
            start_time: Instant::now(),
            query_count: AtomicU64::new(0),
            insert_count: AtomicU64::new(0),
        }
    }

    /// Create a new handler from configuration.
    pub fn from_config(config: Config) -> Result<Self, String> {
        let storage =
            StorageEngine::new(config).map_err(|e| format!("Failed to create storage: {e}"))?;
        Ok(Self::new(storage))
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
        let program_text: String = program
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.starts_with('%') && !trimmed.starts_with("//")
            })
            .collect::<Vec<_>>()
            .join("\n");

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

                                let count = tuples.len();
                                self.insert_count.fetch_add(count as u64, Ordering::Relaxed);
                                storage
                                    .insert_tuples_into(&kg_name, &op.relation, tuples)
                                    .map_err(|e| e.to_string())?;
                                messages.push(format!(
                                    "Inserted {} fact(s) into '{}'.",
                                    count, op.relation
                                ));
                            }
                            statement::Statement::Fact(rule) => {
                                // Session facts are NOT persisted - they are only available for
                                // queries during this request. Use +relation(args). to persist.
                                if rule.head.args.is_empty() {
                                    messages
                                        .push("Fact must have at least one argument".to_string());
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
                                        if terms.len() >= 2 {
                                            let a = if let Term::Constant(n) = &terms[0] {
                                                *n as i32
                                            } else {
                                                messages.push(
                                                    "Delete arguments must be constants"
                                                        .to_string(),
                                                );
                                                continue;
                                            };
                                            let b = if let Term::Constant(n) = &terms[1] {
                                                *n as i32
                                            } else {
                                                messages.push(
                                                    "Delete arguments must be constants"
                                                        .to_string(),
                                                );
                                                continue;
                                            };
                                            let deleted_count = storage
                                                .delete_from(&kg_name, &op.relation, vec![(a, b)])
                                                .map_err(|e| e.to_string())?;
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
                                            let values: Vec<crate::value::Value> = tuple_terms
                                                .iter()
                                                .filter_map(|t| match t {
                                                    Term::Constant(n) => {
                                                        Some(crate::value::Value::Int64(*n))
                                                    }
                                                    Term::StringConstant(s) => {
                                                        Some(crate::value::Value::String(
                                                            s.clone().into(),
                                                        ))
                                                    }
                                                    Term::FloatConstant(f) => {
                                                        Some(crate::value::Value::Float64(*f))
                                                    }
                                                    _ => None,
                                                })
                                                .collect();
                                            if values.len() == tuple_terms.len() {
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
                                    .execute_query_with_rules_on(&kg_name, &query_rule)
                                    .map_err(|e| e.to_string())?;

                                let mut deleted = 0;
                                let mut inserted = 0;

                                for (a, b) in results {
                                    let bindings: std::collections::HashMap<String, i32> =
                                        if all_vars.len() >= 2 {
                                            let mut m = std::collections::HashMap::new();
                                            m.insert(all_vars[0].clone(), a);
                                            m.insert(all_vars[1].clone(), b);
                                            m
                                        } else if all_vars.len() == 1 {
                                            let mut m = std::collections::HashMap::new();
                                            m.insert(all_vars[0].clone(), a);
                                            m
                                        } else {
                                            std::collections::HashMap::new()
                                        };

                                    for target in &op.deletes {
                                        let tuple: Vec<i32> = target
                                            .args
                                            .iter()
                                            .filter_map(|arg| match arg {
                                                Term::Variable(v) => bindings.get(v).copied(),
                                                Term::Constant(c) => Some(*c as i32),
                                                _ => None,
                                            })
                                            .collect();
                                        if tuple.len() >= 2 {
                                            storage
                                                .delete_from(
                                                    &kg_name,
                                                    &target.relation,
                                                    vec![(tuple[0], tuple[1])],
                                                )
                                                .map_err(|e| e.to_string())?;
                                            deleted += 1;
                                        }
                                    }

                                    for target in &op.inserts {
                                        let tuple: Vec<i32> = target
                                            .args
                                            .iter()
                                            .filter_map(|arg| match arg {
                                                Term::Variable(v) => bindings.get(v).copied(),
                                                Term::Constant(c) => Some(*c as i32),
                                                _ => None,
                                            })
                                            .collect();
                                        if tuple.len() >= 2 {
                                            storage
                                                .insert_into(
                                                    &kg_name,
                                                    &target.relation,
                                                    vec![(tuple[0], tuple[1])],
                                                )
                                                .map_err(|e| e.to_string())?;
                                            inserted += 1;
                                        }
                                    }
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
                })
                .collect();
            return Ok(QueryResult {
                rows,
                schema: vec![ColumnDef {
                    name: "message".to_string(),
                    data_type: WireDataType::String,
                }],
                execution_time_ms: start.elapsed().as_millis() as u64,
            });
        }

        let program_text = query_to_execute.unwrap_or(program_text);

        // Transform query
        let query_program = if program_text.trim().starts_with('?')
            && program_text
                .trim()
                .chars()
                .nth(1)
                .is_some_and(char::is_alphabetic)
        {
            let query_text = &program_text.trim()[1..];
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
                    Term::StringConstant(s) => {
                        let t = format!("_c{i}");
                        head_vars.push(t.clone());
                        extra_constraints.push(format!("{t} = \"{s}\""));
                        t
                    }
                    Term::Placeholder => {
                        let t = format!("_p{i}");
                        head_vars.push(t.clone());
                        t
                    }
                    _ => {
                        let t = format!("_t{i}");
                        head_vars.push(t.clone());
                        t
                    }
                })
                .collect();

            let body_atom = format!("{}({})", goal.goal.relation, transformed_args.join(", "));
            let mut body_parts = vec![body_atom];

            // Add additional body predicates (for complex queries like ?- foo(X), bar(Y).)
            // ALSO extract variables from additional body atoms for Cartesian product queries
            for pred in &goal.body {
                body_parts.push(format_body_pred(pred));
                // Extract variables from this predicate to add to head
                extract_predicate_vars(pred, &mut head_vars);
            }

            body_parts.extend(extra_constraints);

            format!(
                "__query__({}) <- {}",
                head_vars.join(", "),
                body_parts.join(", ")
            )
        } else {
            program_text
        };

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
                WireTuple { values }
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
            vec![
                ColumnDef {
                    name: "col0".to_string(),
                    data_type: WireDataType::Int32,
                },
                ColumnDef {
                    name: "col1".to_string(),
                    data_type: WireDataType::Int32,
                },
            ]
        };

        Ok(QueryResult {
            rows,
            schema,
            execution_time_ms: start.elapsed().as_millis() as u64,
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
}

// Helper Functions
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

/// Extract variables from a body predicate and add to `head_vars`
/// Used for Cartesian product queries like ?- foo(X), bar(Y).
fn extract_predicate_vars(pred: &crate::ast::BodyPredicate, head_vars: &mut Vec<String>) {
    match pred {
        crate::ast::BodyPredicate::Positive(atom) | crate::ast::BodyPredicate::Negated(atom) => {
            for term in &atom.args {
                if let Term::Variable(v) = term {
                    if !head_vars.contains(v) {
                        head_vars.push(v.clone());
                    }
                }
            }
        }
        crate::ast::BodyPredicate::Comparison(left, _, right) => {
            if let Term::Variable(v) = left {
                if !head_vars.contains(v) {
                    head_vars.push(v.clone());
                }
            }
            if let Term::Variable(v) = right {
                if !head_vars.contains(v) {
                    head_vars.push(v.clone());
                }
            }
        }
        crate::ast::BodyPredicate::HnswNearest {
            id_var,
            distance_var,
            ..
        } => {
            if !head_vars.contains(id_var) {
                head_vars.push(id_var.clone());
            }
            if !head_vars.contains(distance_var) {
                head_vars.push(distance_var.clone());
            }
        }
    }
}
