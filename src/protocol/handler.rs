//! Handler for InputLayer
//!
//! Provides the core business logic for handling Datalog queries and data operations.
//! This handler is used by the REST API handlers.
//!
//! Performance: Uses parking_lot::RwLock for faster lock acquisition (no poisoning)
//! and AtomicU64 for lock-free statistics counters.

use crate::ast::Term;
use crate::statement;
use crate::storage_engine::StorageEngine;
use crate::value::{Tuple, Value};
use crate::Config;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::wire::{ColumnDef, QueryResult, WireDataType, WireTuple, WireValue};

/// Convert a Term (from parsed statement) to a Value (for storage)
fn term_to_value(term: &Term) -> Result<Value, String> {
    match term {
        Term::Constant(n) => Ok(Value::Int64(*n)),
        Term::FloatConstant(f) => Ok(Value::Float64(*f)),
        Term::StringConstant(s) => Ok(Value::string(s)),
        Term::VectorLiteral(v) => Ok(Value::vector(v.iter().map(|x| *x as f32).collect())),
        Term::Variable(v) => Err(format!(
            "Cannot insert variable '{}' - use constants only",
            v
        )),
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
        Term::RecordPattern(_) => {
            Err("Cannot insert record pattern - use constants only".to_string())
        }
    }
}

// ============================================================================
// Handler
// ============================================================================

/// Handler implementing InputLayer business logic.
///
/// This struct wraps a StorageEngine and provides thread-safe access
/// for concurrent API calls.
///
/// Performance optimizations:
/// - Uses parking_lot::RwLock instead of std::sync::RwLock (no poisoning, faster)
/// - Uses AtomicU64 for counters (lock-free statistics)
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
            StorageEngine::new(config).map_err(|e| format!("Failed to create storage: {}", e))?;
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

        let mut storage = self.storage.write();

        // Switch to target knowledge graph if specified
        if let Some(ref kg) = knowledge_graph {
            storage
                .use_knowledge_graph(kg)
                .map_err(|e| format!("Knowledge graph not found: {}", e))?;
        }

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

        for line in program_text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            current_stmt.push_str(line);
            current_stmt.push(' ');

            if line.ends_with('.') {
                let stmt_text = current_stmt.trim();
                if !stmt_text.is_empty() {
                    if let Ok(stmt) = statement::parse_statement(stmt_text) {
                        match stmt {
                            statement::Statement::SchemaDecl(decl) => {
                                messages.push(format!(
                                    "Schema for '{}' declared with {} columns{}",
                                    decl.name,
                                    decl.columns.len(),
                                    if decl.persistent {
                                        " (persistent)"
                                    } else {
                                        " (session)"
                                    }
                                ));
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

                                let count = tuples.len();
                                self.insert_count.fetch_add(count as u64, Ordering::Relaxed);
                                storage
                                    .insert_tuples(&op.relation, tuples)
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
                                            let a = match &terms[0] {
                                                Term::Constant(n) => *n as i32,
                                                _ => {
                                                    messages.push(
                                                        "Delete arguments must be constants"
                                                            .to_string(),
                                                    );
                                                    continue;
                                                }
                                            };
                                            let b = match &terms[1] {
                                                Term::Constant(n) => *n as i32,
                                                _ => {
                                                    messages.push(
                                                        "Delete arguments must be constants"
                                                            .to_string(),
                                                    );
                                                    continue;
                                                }
                                            };
                                            storage
                                                .delete(&op.relation, vec![(a, b)])
                                                .map_err(|e| e.to_string())?;
                                            messages.push(format!(
                                                "Deleted fact from '{}'.",
                                                op.relation
                                            ));
                                        }
                                    }
                                    DeletePattern::Conditional { .. } => {
                                        messages.push(
                                            "Conditional delete not yet implemented".to_string(),
                                        );
                                    }
                                }
                            }
                            statement::Statement::PersistentRule(rule) => {
                                let rule_text = format_rule_text(&rule);
                                let rule_def = statement::parse_rule_definition(&rule_text)
                                    .map_err(|e| format!("Failed to parse rule: {}", e))?;
                                storage
                                    .register_rule(&rule_def)
                                    .map_err(|e| e.to_string())?;
                                messages.push(format!("Rule '{}' registered.", rule.head.relation));
                            }
                            statement::Statement::SessionRule(rule) => {
                                let rule_text = format_rule_text(&rule);
                                session_rules.push(rule_text.clone());
                                messages.push(format!(
                                    "Session rule added for '{}'.",
                                    rule.head.relation
                                ));
                            }
                            statement::Statement::Query(_) => {
                                query_to_execute = Some(stmt_text.to_string());
                            }
                            statement::Statement::DeleteRelationOrRule(name) => {
                                match storage.drop_rule(&name) {
                                    Ok(_) => messages.push(format!("Rule '{}' dropped.", name)),
                                    Err(_) => {
                                        messages.push(format!("'{}' not found as rule.", name))
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
                                    .map(|pred| format_body_pred(pred))
                                    .collect::<Vec<_>>()
                                    .join(", ");

                                let query_rule = format!(
                                    "__upd_query__({}) :- {}.",
                                    all_vars.join(", "),
                                    body_str
                                );

                                let results = storage
                                    .execute_query_with_rules(&query_rule)
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
                                                .delete(
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
                                                .insert(
                                                    &target.relation,
                                                    vec![(tuple[0], tuple[1])],
                                                )
                                                .map_err(|e| e.to_string())?;
                                            inserted += 1;
                                        }
                                    }
                                }

                                messages.push(format!(
                                    "Update: {} deleted, {} inserted.",
                                    deleted, inserted
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
        let query_program = if program_text.trim().starts_with("?-") {
            let query_text = program_text.trim().trim_start_matches("?-").trim();
            let goal = statement::parse_query(query_text)
                .map_err(|e| format!("Failed to parse query: {}", e))?;

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
                        let t = format!("_c{}", i);
                        head_vars.push(t.clone());
                        extra_constraints.push(format!("{} = {}", t, val));
                        t
                    }
                    Term::StringConstant(s) => {
                        let t = format!("_c{}", i);
                        head_vars.push(t.clone());
                        extra_constraints.push(format!("{} = \"{}\"", t, s));
                        t
                    }
                    Term::Placeholder => {
                        let t = format!("_p{}", i);
                        head_vars.push(t.clone());
                        t
                    }
                    _ => {
                        let t = format!("_t{}", i);
                        head_vars.push(t.clone());
                        t
                    }
                })
                .collect();

            let body_atom = format!("{}({})", goal.goal.relation, transformed_args.join(", "));
            let mut body_parts = vec![body_atom];
            body_parts.extend(extra_constraints);

            format!(
                "__query__({}) :- {}.",
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
            format!("{}\n{}", rules_text, query_program)
        };

        // Temporarily insert session facts before query execution
        // These will be cleaned up after the query completes
        let mut inserted_session_facts: Vec<(String, Tuple)> = Vec::new();
        let debug_session = std::env::var("DEBUG_SESSION").is_ok();
        for (relation, tuple) in &session_fact_tuples {
            if debug_session {
                eprintln!(
                    "DEBUG: Inserting session fact into '{}': {:?}",
                    relation, tuple
                );
            }
            // Insert the session fact tuple
            storage
                .insert_tuples(relation, vec![tuple.clone()])
                .map_err(|e| e.to_string())?;
            inserted_session_facts.push((relation.clone(), tuple.clone()));
        }

        // Execute query (session facts are now in storage)
        // Use the tuples API to support arbitrary types (strings, etc.)
        let query_result = storage.execute_query_with_rules_tuples(&query_program);

        // Clean up session facts after query execution (even if query failed)
        if debug_session && !inserted_session_facts.is_empty() {
            eprintln!(
                "DEBUG: Cleaning up {} session facts",
                inserted_session_facts.len()
            );
        }
        for (relation, tuple) in &inserted_session_facts {
            // Delete the temporarily inserted tuple using the tuple API
            if debug_session {
                eprintln!(
                    "DEBUG: Deleting session fact from '{}': {:?}",
                    relation, tuple
                );
            }
            if let Err(e) = storage.delete_tuple(relation, tuple) {
                if debug_session {
                    eprintln!(
                        "Warning: failed to clean up session fact {:?}: {}",
                        tuple, e
                    );
                }
            }
        }

        // Now handle the query result
        let results = query_result.map_err(|e| e.to_string())?;

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
                    name: format!("col{}", i),
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
}

// ============================================================================
// Helper Functions
// ============================================================================

fn format_rule_text(rule: &crate::ast::Rule) -> String {
    let head = format_atom(&rule.head);

    if rule.body.is_empty() && rule.constraints.is_empty() {
        return format!("{}.", head);
    }

    let mut parts = Vec::new();
    for pred in &rule.body {
        parts.push(format_body_pred(pred));
    }
    for constraint in &rule.constraints {
        parts.push(format_constraint(constraint));
    }

    format!("{} :- {}.", head, parts.join(", "))
}

fn format_atom(atom: &crate::ast::Atom) -> String {
    let args: Vec<String> = atom.args.iter().map(format_term).collect();
    format!("{}({})", atom.relation, args.join(", "))
}

fn format_term(term: &Term) -> String {
    match term {
        Term::Variable(v) => v.clone(),
        Term::Constant(c) => c.to_string(),
        Term::StringConstant(s) => format!("\"{}\"", s),
        Term::FloatConstant(f) => f.to_string(),
        Term::Placeholder => "_".to_string(),
        Term::Arithmetic(expr) => format_arith_expr(expr),
        Term::Aggregate(func, var) => {
            let func_name = match func {
                crate::ast::AggregateFunc::Count => "count",
                crate::ast::AggregateFunc::Sum => "sum",
                crate::ast::AggregateFunc::Min => "min",
                crate::ast::AggregateFunc::Max => "max",
                crate::ast::AggregateFunc::Avg => "avg",
                crate::ast::AggregateFunc::TopK { k, order_var, descending } => {
                    let dir = if *descending { ", desc" } else { "" };
                    return format!("top_k<{}, {}{}>", k, order_var, dir);
                }
                crate::ast::AggregateFunc::TopKThreshold { k, order_var, threshold, descending } => {
                    let dir = if *descending { ", desc" } else { "" };
                    return format!("top_k_threshold<{}, {}, {}{}>", k, order_var, threshold, dir);
                }
                crate::ast::AggregateFunc::WithinRadius { distance_var, max_distance } => {
                    return format!("within_radius<{}, {}>", distance_var, max_distance);
                }
            };
            format!("{}<{}>", func_name, var)
        }
        Term::VectorLiteral(values) => {
            let vals: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            format!("[{}]", vals.join(", "))
        }
        Term::FunctionCall(func, args) => {
            let args_str: Vec<String> = args.iter().map(format_term).collect();
            format!("{}({})", func.as_str(), args_str.join(", "))
        }
        Term::FieldAccess(term, field) => {
            format!("{}.{}", format_term(term), field)
        }
        Term::RecordPattern(fields) => {
            let fields_str: Vec<String> = fields
                .iter()
                .map(|(name, term)| format!("{}: {}", name, format_term(term)))
                .collect();
            format!("{{ {} }}", fields_str.join(", "))
        }
    }
}

fn format_arith_expr(expr: &crate::ast::ArithExpr) -> String {
    match expr {
        crate::ast::ArithExpr::Variable(v) => v.clone(),
        crate::ast::ArithExpr::Constant(c) => c.to_string(),
        crate::ast::ArithExpr::Binary { op, left, right } => {
            format!(
                "{}{}{}",
                format_arith_expr(left),
                op.as_str(),
                format_arith_expr(right)
            )
        }
    }
}

fn format_body_pred(pred: &crate::ast::BodyPredicate) -> String {
    match pred {
        crate::ast::BodyPredicate::Positive(atom) => format_atom(atom),
        crate::ast::BodyPredicate::Negated(atom) => format!("!{}", format_atom(atom)),
    }
}

fn format_constraint(constraint: &crate::ast::Constraint) -> String {
    match constraint {
        crate::ast::Constraint::Equal(l, r) => format!("{} = {}", format_term(l), format_term(r)),
        crate::ast::Constraint::NotEqual(l, r) => {
            format!("{} != {}", format_term(l), format_term(r))
        }
        crate::ast::Constraint::LessThan(l, r) => {
            format!("{} < {}", format_term(l), format_term(r))
        }
        crate::ast::Constraint::LessOrEqual(l, r) => {
            format!("{} <= {}", format_term(l), format_term(r))
        }
        crate::ast::Constraint::GreaterThan(l, r) => {
            format!("{} > {}", format_term(l), format_term(r))
        }
        crate::ast::Constraint::GreaterOrEqual(l, r) => {
            format!("{} >= {}", format_term(l), format_term(r))
        }
    }
}
